// ICT RegimeAI Trading Bot
// Fully autonomous server-side trading bot for Tradovate
// Runs 24/7 on Render.com - direct WebSocket to Tradovate (server-to-server works)

const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const fs = require('fs');

// ─── CONFIG ────────────────────────────────────────────────────────────────
const CFG = {
  username: process.env.TV_USERNAME || '',
  password: process.env.TV_PASSWORD || '',
  cid: parseInt(process.env.TV_CID || '0'),
  sec: process.env.TV_SEC || '',
  demo: process.env.TV_DEMO !== 'false', // true = demo, false = live
  paperOnly: process.env.PAPER_ONLY !== 'false', // true = no real orders
  riskPct: parseFloat(process.env.RISK_PCT || '0.02'),
  maxDailyLossPct: parseFloat(process.env.MAX_DAILY_LOSS || '0.05'),
  maxOpen: parseInt(process.env.MAX_OPEN || '4'),
  maxDailyTrades: parseInt(process.env.MAX_DAILY_TRADES || '15'),
  maxStreak: parseInt(process.env.MAX_STREAK || '3'),
  minScore: parseInt(process.env.MIN_SCORE || '60'),
  scanSec: parseInt(process.env.SCAN_SEC || '20'),
  balance: parseFloat(process.env.ACCOUNT_BALANCE || '10000'),
};

const CONTRACT_MAP = {
  MNQ: 'MNQM6', MES: 'MESM6', MYM: 'MYMM6', MCL: 'MCLN6', MGC: 'MGCM6'
};
const INSTRUMENTS = {
  MNQ: { tv: 2, ts: 0.25, margin: 40 },
  MES: { tv: 5, ts: 0.25, margin: 40 },
  MYM: { tv: 0.5, ts: 1, margin: 30 },
  MCL: { tv: 100, ts: 0.01, margin: 50 },
  MGC: { tv: 10, ts: 0.1, margin: 50 },
};
const ACTIVE_SYMS = ['MNQ', 'MES'];

// ─── STATE ─────────────────────────────────────────────────────────────────
let accessToken = null;
let mdWs = null;
let orderWs = null;
let bars = {}; // { sym: { M1:[], M5:[], M15:[], H1:[] } }
let livePrices = {};
let positions = [];
let closedTrades = [];
let dailyPnl = 0;
let dailyLoss = 0;
let dailyTrades = 0;
let streak = 0;
let scanTimer = null;
let reconnectTimer = null;
let lastScan = null;
let logs = [];
let signals = [];

// Initialize bars with simulated data
function initBars() {
  const BASE = { MNQ: 19500, MES: 5500, MYM: 43000, MCL: 75, MGC: 2650 };
  ACTIVE_SYMS.forEach(sym => {
    const base = BASE[sym];
    const vol = { MNQ: 0.004, MES: 0.003 }[sym] || 0.003;
    const arr = [];
    let p = base;
    for (let i = 0; i < 60; i++) {
      const c = Math.max(p * 0.5, p + (Math.random() - 0.5) * p * vol);
      arr.push({ o: p, h: Math.max(p, c) + Math.abs(c - p) * 0.2, l: Math.min(p, c) - Math.abs(c - p) * 0.2, c, v: 500 + Math.random() * 2000 });
      p = c;
    }
    bars[sym] = { M1: arr.slice(-20), M5: arr.slice(-30), M15: arr.slice(-24), H1: arr.slice(-12) };
    livePrices[sym] = base;
  });
}

// ─── LOGGING ───────────────────────────────────────────────────────────────
function log(type, msg) {
  const entry = { type, msg, ts: new Date().toISOString(), id: Date.now() + Math.random() };
  logs.unshift(entry);
  if (logs.length > 500) logs = logs.slice(0, 500);
  console.log(`[${type.toUpperCase()}] ${msg}`);
}

// ─── REST API ──────────────────────────────────────────────────────────────
function apiRequest(method, path, body, token) {
  return new Promise((resolve, reject) => {
    const host = CFG.demo ? 'demo.tradovateapi.com' : 'live.tradovateapi.com';
    const payload = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: host, path: `/v1${path}`, method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
        ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
      }
    };
    const req = https.request(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { resolve(data); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ─── AUTH ──────────────────────────────────────────────────────────────────
async function authenticate() {
  log('system', `Authenticating with Tradovate (${CFG.demo ? 'demo' : 'live'})...`);
  try {
    const res = await apiRequest('POST', '/auth/accesstokenrequest', {
      name: CFG.username, password: CFG.password,
      appId: 'ICTRegimeAI', appVersion: '4.0',
      cid: CFG.cid, sec: CFG.sec, deviceId: 'ict-regime-bot-001'
    });
    if (res.accessToken) {
      accessToken = res.accessToken;
      log('system', 'Auth OK -- token acquired');
      return true;
    } else {
      log('warn', `Auth failed: ${res.errorText || JSON.stringify(res).slice(0, 100)}`);
      return false;
    }
  } catch (e) {
    log('warn', `Auth error: ${e.message}`);
    return false;
  }
}

// ─── BAR AGGREGATOR ────────────────────────────────────────────────────────
class BarAgg {
  constructor(sym) {
    this.sym = sym;
    this.cur = {};
    this.TFS = { M1: 60, M5: 300, M15: 900, H1: 3600 };
  }
  tick(price, volume) {
    const now = Math.floor(Date.now() / 1000);
    Object.entries(this.TFS).forEach(([tf, secs]) => {
      const barOpen = Math.floor(now / secs) * secs;
      if (!this.cur[tf] || this.cur[tf].t !== barOpen) {
        if (this.cur[tf]) {
          const b = this.cur[tf];
          if (!bars[this.sym]) bars[this.sym] = { M1: [], M5: [], M15: [], H1: [] };
          bars[this.sym][tf] = [...(bars[this.sym][tf] || []).slice(-59), { o: b.o, h: b.h, l: b.l, c: b.c, v: b.v }];
        }
        this.cur[tf] = { o: price, h: price, l: price, c: price, v: volume || 0, t: barOpen };
      } else {
        const b = this.cur[tf];
        b.h = Math.max(b.h, price); b.l = Math.min(b.l, price);
        b.c = price; b.v += (volume || 0);
      }
    });
    livePrices[this.sym] = price;
  }
}

const aggregators = {};

// ─── MARKET DATA WEBSOCKET ─────────────────────────────────────────────────
function connectMD() {
  if (mdWs) { try { mdWs.close(); } catch (e) {} }
  const url = CFG.demo ? 'wss://md-demo.tradovateapi.com/v1/websocket' : 'wss://md.tradovateapi.com/v1/websocket';
  log('system', `MD WebSocket connecting to ${url}...`);

  mdWs = new WebSocket(url);
  let frameId = 1;
  let hb = null;

  function send(op, body) {
    if (mdWs.readyState !== WebSocket.OPEN) return;
    mdWs.send(`${op}\n${frameId++}\n\n${body ? JSON.stringify(body) : ''}`);
  }

  mdWs.on('open', () => {
    log('system', 'MD socket open -- authorizing...');
    send('authorize', { token: accessToken });
    hb = setInterval(() => { try { mdWs.send('[]'); } catch (e) {} }, 2500);
  });

  mdWs.on('message', (raw) => {
    const str = raw.toString();
    if (str === 'o') { log('system', 'MD Atmosphere open'); return; }
    if (str === 'h' || !str || str.length < 2) return;

    let frames = [];
    try {
      if (str[0] === 'a') frames = JSON.parse(str.slice(1));
      else if (str[0] === '{') frames = [JSON.parse(str)];
    } catch (e) { return; }

    for (const frame of frames) {
      if (!frame) continue;

      // Auth response
      const isAuth = frame.e === 'authorize' || frame.i === 1 ||
        (frame.s === 'ok' && frame.i !== undefined) ||
        (frame.d && frame.d.accessToken !== undefined);

      if (isAuth) {
        const failed = (frame.d && frame.d.errorCode) || frame.s === 'error';
        if (!failed) {
          log('system', 'MD authorized -- subscribing to quotes...');
          ACTIVE_SYMS.forEach(sym => {
            const contract = CONTRACT_MAP[sym];
            if (contract) {
              send('md/subscribeQuote', { symbol: contract });
              log('system', `Subscribed to ${contract}`);
            }
          });
        } else {
          log('warn', `MD auth failed: ${frame.d?.errorCode || frame.s}`);
        }
        continue;
      }

      // Quote data
      if (frame.e === 'md' || frame.e === 'quote' || (frame.d && frame.d.quotes)) {
        const quotes = frame.d?.quotes || (frame.d ? [frame.d] : []);
        for (const q of quotes) {
          let sym = null;
          for (const [s, c] of Object.entries(CONTRACT_MAP)) {
            if (c === q.contractId || c === q.symbol) { sym = s; break; }
          }
          if (!sym) continue;

          const price = q.entries?.Trade?.price || q.price || q.ap || q.bp;
          const vol = q.entries?.Trade?.size || q.volume || 1;
          if (!price) continue;

          if (!aggregators[sym]) aggregators[sym] = new BarAgg(sym);
          aggregators[sym].tick(price, vol);
        }
      }
    }
  });

  mdWs.on('close', (code) => {
    clearInterval(hb);
    log('warn', `MD closed ${code} -- reconnecting in 5s...`);
    reconnectTimer = setTimeout(() => {
      if (accessToken) connectMD();
      else authenticate().then(ok => ok && connectMD());
    }, 5000);
  });

  mdWs.on('error', (e) => {
    log('warn', `MD error: ${e.message}`);
  });
}

// ─── ORDER PLACEMENT ───────────────────────────────────────────────────────
async function placeOrder(sym, action, contracts) {
  if (CFG.paperOnly) {
    log('buy', `[PAPER] ${sym} ${action} ${contracts}c -- no real order placed`);
    return { orderId: `PAPER_${Date.now()}`, paper: true };
  }
  try {
    const contract = CONTRACT_MAP[sym];
    const res = await apiRequest('POST', '/order/placeorder', {
      accountSpec: CFG.username,
      symbol: contract,
      orderQty: contracts,
      orderType: 'Market',
      action: action === 'Buy' ? 'Buy' : 'Sell',
      isAutomated: true,
    }, accessToken);
    if (res.orderId || res.d?.orderId) {
      log('buy', `[LIVE] Order placed: ${sym} ${action} ${contracts}c -- id: ${res.orderId || res.d?.orderId}`);
      return res;
    } else {
      log('warn', `Order failed: ${JSON.stringify(res).slice(0, 100)}`);
      return null;
    }
  } catch (e) {
    log('warn', `Order error: ${e.message}`);
    return null;
  }
}

async function closeOrder(sym, action, contracts) {
  if (CFG.paperOnly) {
    log('exit', `[PAPER] Close ${sym} ${contracts}c`);
    return true;
  }
  try {
    const contract = CONTRACT_MAP[sym];
    const closeAction = action === 'Buy' ? 'Sell' : 'Buy';
    const res = await apiRequest('POST', '/order/placeorder', {
      accountSpec: CFG.username,
      symbol: contract,
      orderQty: contracts,
      orderType: 'Market',
      action: closeAction,
      isAutomated: true,
    }, accessToken);
    return !!(res.orderId || res.d?.orderId);
  } catch (e) {
    log('warn', `Close order error: ${e.message}`);
    return false;
  }
}

// ─── MATH HELPERS ──────────────────────────────────────────────────────────
function atr(bars, p = 14) {
  if (!bars || bars.length < 2) return 1;
  if (bars.length < p + 1) return (bars[bars.length-1].h - bars[bars.length-1].l) || 1;
  const trs = bars.slice(-p-1).map((b, i, a) => i === 0 ? b.h - b.l : Math.max(b.h-b.l, Math.abs(b.h-a[i-1].c), Math.abs(b.l-a[i-1].c))).slice(1);
  return trs.reduce((s,t) => s+t, 0) / p;
}
function ema(arr, p) {
  const k = 2/(p+1); let e = arr[0];
  return arr.map(v => { e = v*k + e*(1-k); return e; });
}
function vwap(bars) {
  let tv=0, v=0;
  bars.forEach(b => { const tp=(b.h+b.l+b.c)/3; tv+=tp*b.v; v+=b.v; });
  return v > 0 ? tv/v : bars[bars.length-1].c;
}
function stdDev(arr) {
  const m = arr.reduce((s,v)=>s+v,0)/arr.length;
  return Math.sqrt(arr.reduce((s,v)=>s+(v-m)**2,0)/arr.length);
}
function slope(arr, n=5) {
  if (arr.length < n) return 0;
  const sl = arr.slice(-n);
  return (sl[sl.length-1] - sl[0]) / (n-1);
}
function bodyRatio(b) { return b.h===b.l ? 0 : Math.abs(b.c-b.o)/(b.h-b.l); }

// ─── REGIME DETECTOR ───────────────────────────────────────────────────────
function detectRegime(m5, m15, h1) {
  if (!m5 || m5.length < 10) return { regime: 'CHOP', factors: {} };
  const cur = m5[m5.length-1];
  const a5 = atr(m5, 10), a5_20 = atr(m5, 20);
  const atrExpand = a5 / Math.max(a5_20, 0.001);
  const vw = vwap(m5.slice(-30));
  const vwSlope = slope(m5.slice(-10).map(b=>(b.h+b.l+b.c)/3), 8);
  const priceDist = (cur.c - vw) / Math.max(a5, 0.001);
  const closes = m5.map(b=>b.c);
  const e9 = ema(closes,9), e21 = ema(closes,21), e50 = ema(closes,50);
  const n = m5.length-1;
  const emaAlign = e9[n]>e21[n]&&e21[n]>e50[n] ? 'bullish' : e9[n]<e21[n]&&e21[n]<e50[n] ? 'bearish' : 'mixed';
  const avgVol = m5.slice(-20).reduce((s,b)=>s+b.v,0)/20;
  const volRatio = cur.v / Math.max(avgVol,1);
  const avgBody = m5.slice(-5).reduce((s,b)=>s+bodyRatio(b),0)/5;
  const ranges8 = m5.slice(-8).map(b=>b.h-b.l);
  const rSD = stdDev(ranges8), avgR = ranges8.reduce((s,v)=>s+v,0)/8;
  const compressed = rSD < avgR*0.25 && atrExpand < 0.85;
  const ph = Math.max(...m5.slice(-8,-1).map(b=>b.h));
  const pl = Math.min(...m5.slice(-8,-1).map(b=>b.l));
  const fakeUp = cur.h>ph && cur.c<ph;
  const fakeDown = cur.l<pl && cur.c>pl;
  const tolLiq = a5*0.15;
  const eqH = m5.slice(-15).some((b,i,a)=>a.slice(i+1).some(b2=>Math.abs(b.h-b2.h)<tolLiq));
  const eqL = m5.slice(-15).some((b,i,a)=>a.slice(i+1).some(b2=>Math.abs(b.l-b2.l)<tolLiq));
  const deadZone = atrExpand<0.70 && volRatio<0.55 && avgBody<0.30;
  let regime = 'CHOP';
  if (deadZone) regime = 'DEAD_ZONE';
  else if (volRatio>2.5 && atrExpand>1.5) regime = 'NEWS_SPIKE';
  else if (compressed) regime = 'COMPRESSION';
  else if (atrExpand>1.5 && volRatio>1.4) regime = 'EXPANSION';
  else if (fakeUp||fakeDown) regime = 'REVERSAL';
  else if (atrExpand>1.1 && emaAlign!=='mixed' && avgBody>0.50) regime = 'TREND';
  else if (Math.abs(priceDist)<0.5 && atrExpand>0.85 && avgBody>0.45) regime = 'MEAN_REVERSION';
  return { regime, factors: { atrExpand, volRatio, avgBody, emaAlign, compressed, deadZone, fakeUp, fakeDown, eqH, eqL }, vw, a5, e9: e9[n], e21: e21[n], e50: e50[n], vwSlope, priceDist, atrExpand, volRatio, eqH, eqL, fakeUp, fakeDown };
}

// ─── STRUCTURE HELPERS ─────────────────────────────────────────────────────
function swings(bars, lb=3) {
  const out=[];
  for(let i=lb;i<bars.length-lb;i++){
    const sl=bars.slice(i-lb,i+lb+1);
    if(bars[i].h===Math.max(...sl.map(b=>b.h))) out.push({i,type:'H',price:bars[i].h});
    if(bars[i].l===Math.min(...sl.map(b=>b.l))) out.push({i,type:'L',price:bars[i].l});
  }
  return out;
}
function structure(bars) {
  if(!bars||bars.length<10) return {bias:'ranging',lastHH:null,lastLL:null,bos:null};
  const sw=swings(bars,3), highs=sw.filter(s=>s.type==='H').slice(-4), lows=sw.filter(s=>s.type==='L').slice(-4);
  if(highs.length<2||lows.length<2) return {bias:'ranging',lastHH:null,lastLL:null,bos:null};
  const lHH=highs[highs.length-1],pHH=highs[highs.length-2],lLL=lows[lows.length-1],pLL=lows[lows.length-2];
  const bull=lHH.price>pHH.price&&lLL.price>pLL.price, bear=lLL.price<pLL.price&&lHH.price<pHH.price;
  const lc=bars[bars.length-1].c;
  return {bias:bull?'bullish':bear?'bearish':'ranging',lastHH:lHH.price,lastLL:lLL.price,bos:lc>pHH.price?'bullish':lc<pLL.price?'bearish':null};
}
function detectOBs(bars,bias,a) {
  const obs=[];
  for(let i=2;i<bars.length-2;i++){
    const b=bars[i],nxt=bars.slice(i+1,i+4);
    if(b.c<b.o){const imp=nxt.reduce((s,x)=>s+(x.c-x.o),0);if(imp>a*0.7)obs.push({type:'bullish',top:b.o,bottom:b.l,mid:(b.o+b.l)/2,fresh:!bars.slice(i+1).some(x=>x.l<=(b.o+b.l)/2)});}
    if(b.c>b.o){const imp=nxt.reduce((s,x)=>s+(x.o-x.c),0);if(imp>a*0.7)obs.push({type:'bearish',top:b.h,bottom:b.c,mid:(b.h+b.c)/2,fresh:!bars.slice(i+1).some(x=>x.h>=(b.h+b.c)/2)});}
  }
  return obs.filter(o=>o.fresh&&(bias==='bullish'?o.type==='bullish':bias==='bearish'?o.type==='bearish':true)).slice(-2);
}
function detectFVGs(bars,a) {
  const fvgs=[];
  for(let i=1;i<bars.length-1;i++){
    const p=bars[i-1],n=bars[i+1];
    if(n.l>p.h) fvgs.push({type:'bullish',top:n.l,bottom:p.h,mid:(n.l+p.h)/2,filled:bars.slice(i+1).some(b=>b.l<=p.h)});
    if(p.l>n.h) fvgs.push({type:'bearish',top:p.l,bottom:n.h,mid:(p.l+n.h)/2,filled:bars.slice(i+1).some(b=>b.h>=p.l)});
  }
  return fvgs.filter(f=>!f.filled).slice(-2);
}
function detectLiq(bars,a) {
  const pools=[];const tol=a*0.15;
  for(let i=0;i<bars.length-3;i++) for(let j=i+2;j<bars.length-1;j++){
    if(Math.abs(bars[i].h-bars[j].h)<tol) pools.push({type:'BSL',price:Math.max(bars[i].h,bars[j].h),idx2:j,swept:bars.slice(j+1).some(b=>b.h>bars[j].h+tol)});
    if(Math.abs(bars[i].l-bars[j].l)<tol) pools.push({type:'SSL',price:Math.min(bars[i].l,bars[j].l),idx2:j,swept:bars.slice(j+1).some(b=>b.l<bars[j].l-tol)});
  }
  return pools.filter(p=>p.idx2>bars.length-20);
}

// ─── STRATEGIES ────────────────────────────────────────────────────────────
function runICT(sym, rd) {
  const b = bars[sym];
  if (!b) return null;
  const h1=b.H1, m15=b.M15, m5=b.M5;
  if (!h1||h1.length<10||!m15||m15.length<10||!m5||m5.length<10) return null;
  const aM=atr(m15,10), aL=atr(m5,10);
  const htf=structure(h1), mtf=structure(m15);
  const bias=htf.bias!=='ranging'?htf.bias:mtf.bias;
  if(bias==='ranging') return null;
  const obs=detectOBs(m15,bias,aM);
  const fvgs=detectFVGs(m15,aM).filter(f=>f.type===(bias==='bullish'?'bullish':'bearish'));
  const liq=detectLiq(m15,aM);
  const freshOB=obs.find(o=>o.fresh);
  const activeFVG=fvgs[0];
  const recentSweep=liq.find(p=>bias==='bullish'?p.type==='SSL'&&p.swept:p.type==='BSL'&&p.swept);
  const targetLiq=liq.find(p=>bias==='bullish'?p.type==='BSL'&&!p.swept:p.type==='SSL'&&!p.swept);
  if(!freshOB&&!activeFVG) return null;
  const cur=m5[m5.length-1].c;
  const stopRef=freshOB?(bias==='bullish'?freshOB.bottom:freshOB.top):activeFVG?(bias==='bullish'?activeFVG.bottom:activeFVG.top):cur;
  const stopDist=Math.min(Math.max(Math.abs(cur-stopRef),aL*1.2), cur*0.005);
  const stop=bias==='bullish'?cur-stopDist:cur+stopDist;
  const tp1=bias==='bullish'?cur+stopDist:cur-stopDist;
  const tp2=bias==='bullish'?cur+stopDist*2:cur-stopDist*2;
  const tp3=targetLiq?targetLiq.price:(bias==='bullish'?cur+stopDist*3.5:cur-stopDist*3.5);
  return { sym, strategy:'ICT_SMC', action:bias==='bullish'?'Buy':'Sell', entry:cur, stop, tp1, tp2, tp3, stopDist, riskPerContract:stopDist*INSTRUMENTS[sym].tv, expectedRR:2.0, biasAligned:htf.bias===bias, hasLiqSweep:!!recentSweep, hasOB:!!freshOB, hasBreakout:false };
}

function runVWAP(sym, rd) {
  const b = bars[sym];
  if (!b || !b.M5 || b.M5.length < 20) return null;
  const m5=b.M5, m15=b.M15;
  const a5=atr(m5,10);
  const struct15=structure(m15||[]), bias=struct15.bias;
  if(bias==='ranging') return null;
  const n=m5.length-1, cur=m5[n];
  const closes=m5.map(b=>b.c);
  const e9=ema(closes,9), e21=ema(closes,21);
  const vw=rd.vw;
  const prevAbove=m5[n-1].c>vw, curAbove=cur.c>vw;
  const bounceUp=!prevAbove&&curAbove&&bias==='bullish';
  const bounceDown=prevAbove&&!curAbove&&bias==='bearish';
  const crossUp=e9[n-1]<=e21[n-1]&&e9[n]>e21[n];
  const crossDown=e9[n-1]>=e21[n-1]&&e9[n]<e21[n];
  if(!(bounceUp||bounceDown)&&!(crossUp||crossDown)) return null;
  const action=bounceUp||crossUp?'Buy':'Sell';
  if((action==='Buy'&&bias==='bearish')||(action==='Sell'&&bias==='bullish')) return null;
  const rawSD=action==='Buy'?Math.max(cur.c-cur.l,a5*0.5):Math.max(cur.h-cur.c,a5*0.5);
  const stopDist=Math.min(rawSD, cur.c*0.005);
  const stop=action==='Buy'?cur.c-stopDist:cur.c+stopDist;
  const tp1=action==='Buy'?cur.c+stopDist*1.5:cur.c-stopDist*1.5;
  const tp2=action==='Buy'?cur.c+stopDist*2.5:cur.c-stopDist*2.5;
  return { sym, strategy:'VWAP_SCALP', action, entry:cur.c, stop, tp1, tp2, tp3:tp2, stopDist, riskPerContract:stopDist*INSTRUMENTS[sym].tv, expectedRR:2.5, biasAligned:true, hasLiqSweep:rd.eqH||rd.eqL, hasOB:false, hasBreakout:false };
}

function runCE(sym, rd) {
  const b = bars[sym];
  if (!b || !b.M5 || b.M5.length < 20) return null;
  const m5=b.M5, m15=b.M15;
  if(!rd.compressed && rd.atrExpand<1.3) return null;
  const a5=atr(m5,10);
  const cur=m5[m5.length-1], prev=m5[m5.length-2];
  const last10=m5.slice(-10);
  const cH=Math.max(...last10.map(b=>b.h)), cL=Math.min(...last10.map(b=>b.l));
  const struct15=structure(m15||[]), bias=struct15.bias;
  const bullBO=cur.c>cH&&cur.c>prev.c&&bodyRatio(cur)>0.55;
  const bearBO=cur.c<cL&&cur.c<prev.c&&bodyRatio(cur)>0.55;
  if(!bullBO&&!bearBO) return null;
  const action=bullBO?'Buy':'Sell';
  const biasOk=bias==='ranging'||(action==='Buy'&&bias==='bullish')||(action==='Sell'&&bias==='bearish');
  if(!biasOk) return null;
  const stopDist=Math.min(Math.max((cH-cL)*0.5,a5*0.8), cur.c*0.005);
  const stop=action==='Buy'?cur.c-stopDist:cur.c+stopDist;
  const tp1=action==='Buy'?cur.c+stopDist*1.5:cur.c-stopDist*1.5;
  const tp2=action==='Buy'?cur.c+stopDist*2.5:cur.c-stopDist*2.5;
  const tp3=action==='Buy'?cur.c+stopDist*3.5:cur.c-stopDist*3.5;
  return { sym, strategy:'COMP_EXPAND', action, entry:cur.c, stop, tp1, tp2, tp3, stopDist, riskPerContract:stopDist*INSTRUMENTS[sym].tv, expectedRR:2.5, biasAligned:biasOk, hasLiqSweep:false, hasOB:false, hasBreakout:true };
}

// ─── CONFIDENCE SCORER ─────────────────────────────────────────────────────
function scoreSignal(sig, regime, rd, sess) {
  let score = 0;
  const rm = { ICT_SMC:['TREND','REVERSAL'], VWAP_SCALP:['MEAN_REVERSION','TREND'], COMP_EXPAND:['EXPANSION','COMPRESSION'] }[sig.strategy]||[];
  score += rm.includes(regime) ? 20 : rm.length ? 8 : 0;
  score += sig.biasAligned ? 15 : 0;
  score += ((sig.action==='Buy'&&rd.priceDist<0.5)||(sig.action==='Sell'&&rd.priceDist>-0.5)) ? 10 : 5;
  score += rd.volRatio>1.5?15:rd.volRatio>1.2?10:rd.volRatio>0.8?5:0;
  score += (sig.hasLiqSweep||sig.hasBreakout)?15:sig.hasOB?8:0;
  score += sig.expectedRR>=2.0?10:sig.expectedRR>=1.5?7:3;
  score += sess.prime ? 10 : 5;
  return Math.round(score);
}

// ─── SESSION ───────────────────────────────────────────────────────────────
function getSession() {
  const et = new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
  const m = et.getHours()*60+et.getMinutes();
  if(m>=570&&m<=630) return {name:'NY Open',prime:true};
  if(m>=630&&m<=720) return {name:'NY AM',prime:true};
  if(m>=810&&m<=900) return {name:'NY PM',prime:true};
  if(m>=180&&m<=300) return {name:'London',prime:true};
  return {name:'Overnight',prime:false};
}

// ─── POSITION MANAGEMENT ───────────────────────────────────────────────────
function updatePositions() {
  positions = positions.map(p => {
    if (p.closed) return p;
    const cur = livePrices[p.sym] || p.entry;
    const pnlPts = p.action==='Buy' ? cur-p.entry : p.entry-cur;
    const rVal = p.stopDist>0 ? pnlPts/p.stopDist : 0;
    let curStop = p.curStop || p.stop;
    if (rVal>=1.0 && !p.stopMoved) curStop = p.entry;
    if (rVal>=1.5) {
      const trail = p.action==='Buy' ? cur-p.stopDist*0.75 : cur+p.stopDist*0.75;
      curStop = p.action==='Buy' ? Math.max(curStop,trail) : Math.min(curStop,trail);
    }
    return { ...p, curPrice:cur, curR:rVal, curStop, stopMoved:rVal>=1||p.stopMoved };
  });

  // Check stops and TPs
  positions.forEach(async (p) => {
    if (p.closed) return;
    const cur = p.curPrice || p.entry;
    const pnlPts = p.action==='Buy' ? cur-p.entry : p.entry-cur;

    // Stop hit
    if ((p.action==='Buy'?cur<=(p.curStop||p.stop):cur>=(p.curStop||p.stop))) {
      const pnl = pnlPts * INSTRUMENTS[p.sym].tv * p.contracts;
      await exitPosition(p, 'STOP', cur, pnl);
      return;
    }
    // TP2 hit
    if (!p.tp2Closed && (p.action==='Buy'?cur>=p.tp2:cur<=p.tp2)) {
      const pnl = pnlPts * INSTRUMENTS[p.sym].tv * p.contracts;
      await exitPosition(p, 'TP2', cur, pnl);
    }
  });
}

async function exitPosition(p, reason, closePrice, pnl) {
  const idx = positions.findIndex(x=>x.id===p.id);
  if (idx<0 || positions[idx].closed) return;
  positions[idx] = { ...positions[idx], closed:true, closeReason:reason, closePrice, finalPnl:pnl };
  const sign = pnl>=0?'+':'';
  log('exit', `[${p.paper?'P':'L'}] ${reason} ${p.sym} ${p.action==='Buy'?'L':'S'}: ${p.contracts}c @ ${closePrice?.toFixed(1)} ${sign}$${pnl.toFixed(2)} ${p.curR?.toFixed(2)}R`);
  if (pnl < 0) { dailyLoss += Math.abs(pnl); streak++; } else streak = 0;
  dailyPnl += pnl;
  closedTrades.unshift({ ...positions[idx], ts: new Date().toLocaleTimeString() });
  if (closedTrades.length > 200) closedTrades = closedTrades.slice(0,200);
  if (!p.paper) await closeOrder(p.sym, p.action, p.contracts);
}

async function enterPosition(sig, score) {
  const spec = INSTRUMENTS[sig.sym];
  const balance = CFG.balance;
  const riskDollars = balance * CFG.riskPct;
  const contracts = Math.max(1, Math.floor(riskDollars / Math.max(sig.riskPerContract, 1)));
  const marginReq = contracts * spec.margin;
  const paper = CFG.paperOnly;

  log('buy', `[${paper?'P':'L'}] ${sig.strategy} ${sig.sym} ${sig.action==='Buy'?'LONG':'SHORT'} ${contracts}c @ ${sig.entry?.toFixed(1)} score:${score}`);

  let orderId = null;
  if (!paper) {
    const res = await placeOrder(sig.sym, sig.action, contracts);
    if (!res) return;
    orderId = res.orderId;
  }

  const p = {
    id: `P${Date.now()}`, sym:sig.sym, action:sig.action, strategy:sig.strategy,
    contracts, entry:sig.entry, curPrice:sig.entry, stop:sig.stop, curStop:sig.stop,
    tp1:sig.tp1, tp2:sig.tp2, tp3:sig.tp3, stopDist:sig.stopDist,
    riskPerContract:sig.riskPerContract, closed:false, stopMoved:false,
    paper, curR:0, score, ts:new Date().toLocaleTimeString(), orderId,
    tp1Closed:false, tp2Closed:false, scaleIns:0,
  };
  positions.push(p);
  dailyTrades++;
}

// ─── MASTER SCANNER ────────────────────────────────────────────────────────
function scan() {
  const sess = getSession();
  const openPositions = positions.filter(p=>!p.closed);

  // Risk checks
  if (dailyLoss >= CFG.balance * CFG.maxDailyLossPct) {
    log('scan', `Daily loss limit hit ($${dailyLoss.toFixed(0)}) -- no new trades`);
    return;
  }
  if (dailyTrades >= CFG.maxDailyTrades) {
    log('scan', `Max daily trades (${CFG.maxDailyTrades}) -- no new trades`);
    return;
  }
  if (streak >= CFG.maxStreak) {
    log('scan', `Loss streak (${streak}) -- cooling down`);
    return;
  }
  if (openPositions.length >= CFG.maxOpen) {
    log('scan', `Max open positions (${CFG.maxOpen})`);
    return;
  }

  const newSigs = [];
  ACTIVE_SYMS.forEach(sym => {
    const b = bars[sym];
    if (!b || !b.M5 || b.M5.length < 10) return;
    const rd = detectRegime(b.M5, b.M15, b.H1);
    const regime = rd.regime;
    if (regime==='DEAD_ZONE'||regime==='NEWS_SPIKE'||regime==='CHOP') return;

    // ICT/SMC
    if ((regime==='TREND'||regime==='REVERSAL') && !openPositions.find(p=>p.sym===sym&&p.strategy==='ICT_SMC')) {
      const sig = runICT(sym, rd);
      if (sig) { const sc=scoreSignal(sig,regime,rd,sess); newSigs.push({...sig,score:sc,regime}); }
    }
    // VWAP
    if ((regime==='MEAN_REVERSION'||regime==='TREND') && !openPositions.find(p=>p.sym===sym&&p.strategy==='VWAP_SCALP')) {
      const sig = runVWAP(sym, rd);
      if (sig) { const sc=scoreSignal(sig,regime,rd,sess); newSigs.push({...sig,score:sc,regime}); }
    }
    // Compression/Expansion
    if ((regime==='COMPRESSION'||regime==='EXPANSION') && !openPositions.find(p=>p.sym===sym&&p.strategy==='COMP_EXPAND')) {
      const sig = runCE(sym, rd);
      if (sig) { const sc=scoreSignal(sig,regime,rd,sess); newSigs.push({...sig,score:sc,regime}); }
    }
  });

  newSigs.sort((a,b)=>b.score-a.score);
  signals = newSigs.slice(0,10);

  const slots = CFG.maxOpen - openPositions.length;
  let fired = 0;
  for (const sig of newSigs) {
    if (fired >= slots) break;
    if (sig.score < CFG.minScore) { log('scan',`[BLOCKED] ${sig.sym} ${sig.strategy} score:${sig.score}<${CFG.minScore}`); continue; }
    enterPosition(sig, sig.score);
    fired++;
  }

  const regimeStr = ACTIVE_SYMS.map(s=>{const b=bars[s];if(!b||!b.M5)return s+':?';const rd=detectRegime(b.M5,b.M15||[],b.H1||[]);return `${s}:${rd.regime}`;}).join(' ');
  lastScan = { time:new Date().toLocaleTimeString(), regimes:regimeStr, signals:newSigs.length, open:openPositions.length };
  log('scan', `${regimeStr} | ${newSigs.length} signals | ${openPositions.length} open | daily P&L: ${dailyPnl>=0?'+':''}$${dailyPnl.toFixed(2)}`);
}

// ─── HTTP DASHBOARD ────────────────────────────────────────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ICT RegimeAI Bot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#fff;font-family:monospace;font-size:12px}
.header{background:#0a0a0a;border-bottom:1px solid #1e293b;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.logo{color:#10b981;font-weight:bold;font-size:16px}
.badge{padding:4px 8px;border-radius:4px;border:1px solid;font-size:10px;font-weight:bold}
.prices{background:#0a0a0a;border-bottom:1px solid #1e293b;padding:8px 16px;display:flex;gap:16px;flex-wrap:wrap}
.price-item{display:flex;gap:6px;align-items:center}
.section{margin:12px 16px}
.section-title{color:#475569;font-size:10px;text-transform:uppercase;letter-spacing:2px;margin-bottom:8px}
.card{background:#0a0a0a;border:1px solid #1e293b;border-radius:8px;padding:12px;margin-bottom:8px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px}
.stat{background:#111827;border-radius:6px;padding:8px;text-align:center}
.stat-val{font-size:16px;font-weight:bold;margin-top:4px}
.log-entry{padding:4px 0;border-bottom:1px solid #0f172a;display:flex;gap:8px}
.log-ts{color:#334155;min-width:60px}
.green{color:#10b981}.red{color:#ef4444}.yellow{color:#f59e0b}.blue{color:#60a5fa}.cyan{color:#22d3ee}
.position{background:#0a0a0a;border:1px solid #1e293b;border-radius:6px;padding:10px;margin-bottom:6px}
.tag{padding:2px 6px;border-radius:3px;border:1px solid;font-size:9px;font-weight:bold}
</style>
</head>
<body>
<div class="header">
  <span class="logo">ICT RegimeAI Bot</span>
  <div style="display:flex;gap:8px;flex-wrap:wrap" id="status-badges"></div>
</div>
<div class="prices" id="prices"></div>
<div class="section">
  <div class="grid" id="stats"></div>
</div>
<div class="section">
  <div class="section-title">Open Positions</div>
  <div id="positions"></div>
</div>
<div class="section">
  <div class="section-title">Recent Signals</div>
  <div id="signals"></div>
</div>
<div class="section">
  <div class="section-title">Event Log</div>
  <div id="logs"></div>
</div>
<script>
const COL={scan:'#60a5fa',buy:'#10b981',exit:'#a855f7',warn:'#ef4444',system:'#f59e0b',info:'#94a3b8'};
function refresh(){
  fetch('/api/state').then(r=>r.json()).then(d=>{
    // Badges
    document.getElementById('status-badges').innerHTML=
      '<span class="badge" style="color:'+(d.mdConnected?'#10b981':'#ef4444')+';border-color:'+(d.mdConnected?'#10b98144':'#ef444444')+';background:'+(d.mdConnected?'#022c2220':'#2c000020')+'">'+(d.mdConnected?'LIVE':'SIM')+'</span>'+
      '<span class="badge" style="color:'+(d.paperOnly?'#22d3ee':'#ef4444')+';border-color:'+(d.paperOnly?'#22d3ee44':'#ef444444')+';background:'+(d.paperOnly?'#00101020':'#2c000020')+'">'+(d.paperOnly?'PAPER':'LIVE ORDERS')+'</span>'+
      '<span class="badge" style="color:#94a3b8;border-color:#1e293b">'+new Date().toLocaleTimeString()+'</span>';
    // Prices
    document.getElementById('prices').innerHTML=Object.entries(d.prices).map(([s,p])=>
      '<div class="price-item"><span style="color:#60a5fa;font-weight:bold">'+s+'</span><span>'+p?.toFixed?.(s==='MCL'?2:0):'--'+'</span></div>'
    ).join('');
    // Stats
    const stats=[
      ['Balance','$'+d.balance.toFixed(0),'#10b981'],
      ['Daily P&L',(d.dailyPnl>=0?'+':'')+'$'+d.dailyPnl.toFixed(2),d.dailyPnl>=0?'#10b981':'#ef4444'],
      ['Win Rate',d.trades>0?(d.wins/d.trades*100).toFixed(0)+'%':'--',d.trades>0&&d.wins/d.trades>=0.5?'#10b981':'#f59e0b'],
      ['Open',d.openCount+'/'+d.maxOpen,'#94a3b8'],
      ['Today Trades',d.dailyTrades+'/'+d.maxDailyTrades,'#94a3b8'],
      ['Streak',d.streak+'/'+d.maxStreak,d.streak>=d.maxStreak?'#ef4444':'#10b981'],
    ];
    document.getElementById('stats').innerHTML=stats.map(([l,v,c])=>
      '<div class="stat"><div style="color:#475569">'+l+'</div><div class="stat-val" style="color:'+c+'">'+v+'</div></div>'
    ).join('');
    // Positions
    document.getElementById('positions').innerHTML=d.positions.length?d.positions.map(p=>{
      const col=p.curR>=0?'#10b981':'#ef4444';
      return '<div class="position">'+
        '<div style="display:flex;justify-content:space-between;align-items:center">'+
        '<div style="display:flex;gap:6px;align-items:center">'+
        '<span style="color:#60a5fa;font-weight:bold">'+p.sym+'</span>'+
        '<span class="tag" style="color:'+(p.action==='Buy'?'#10b981':'#ef4444')+';border-color:'+(p.action==='Buy'?'#10b98144':'#ef444444')+'">'+(p.action==='Buy'?'LONG':'SHORT')+'</span>'+
        '<span class="tag" style="color:#a78bfa;border-color:#a78bfa44">'+p.strategy.replace('_',' ')+'</span>'+
        (p.paper?'<span class="tag" style="color:#22d3ee;border-color:#22d3ee44">PAPER</span>':'')+
        '</div>'+
        '<span style="font-weight:bold;color:'+col+'">'+(p.curR>=0?'+':'')+p.curR?.toFixed(2)+'R</span>'+
        '</div>'+
        '<div style="color:#64748b;font-size:10px;margin-top:4px">entry '+p.entry?.toFixed(1)+' | stop '+p.curStop?.toFixed(1)+' | score '+p.score+'</div>'+
        '</div>';
    }).join(''):'<div style="color:#475569;text-align:center;padding:16px">No open positions</div>';
    // Signals
    document.getElementById('signals').innerHTML=d.signals.length?d.signals.slice(0,5).map(s=>
      '<div class="card" style="font-size:11px">'+
      '<span style="color:#60a5fa">'+s.sym+'</span> '+
      '<span class="tag" style="color:'+(s.action==='Buy'?'#10b981':'#ef4444')+';border-color:'+(s.action==='Buy'?'#10b98144':'#ef444444')+'">'+(s.action==='Buy'?'LONG':'SHORT')+'</span> '+
      s.strategy.replace('_',' ')+' '+s.regime+' score:<b style="color:'+(s.score>=60?'#10b981':'#f59e0b')+'">'+s.score+'</b>'+
      '</div>'
    ).join(''):'<div style="color:#475569;padding:8px">No signals</div>';
    // Logs
    document.getElementById('logs').innerHTML=d.logs.slice(0,50).map(e=>
      '<div class="log-entry"><span class="log-ts">'+e.ts.slice(11,19)+'</span><span style="color:'+(COL[e.type]||'#94a3b8')+'">'+e.msg+'</span></div>'
    ).join('');
  }).catch(()=>{});
}
refresh();
setInterval(refresh,2000);
</script>
</body>
</html>`;

const server = http.createServer((req, res) => {
  if (req.url === '/api/state') {
    const openPos = positions.filter(p=>!p.closed);
    const wins = closedTrades.filter(t=>t.finalPnl>0).length;
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({
      mdConnected: mdWs && mdWs.readyState === WebSocket.OPEN,
      paperOnly: CFG.paperOnly,
      prices: livePrices,
      balance: CFG.balance,
      dailyPnl, dailyLoss, dailyTrades, streak,
      maxOpen: CFG.maxOpen, maxDailyTrades: CFG.maxDailyTrades, maxStreak: CFG.maxStreak,
      openCount: openPos.length,
      trades: closedTrades.length,
      wins,
      positions: openPos.map(p=>({...p})),
      signals,
      logs: logs.slice(0,100),
      lastScan,
    }));
  } else {
    res.writeHead(200, {'Content-Type':'text/html'});
    res.end(DASHBOARD_HTML);
  }
});

// ─── STARTUP ───────────────────────────────────────────────────────────────
async function start() {
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => log('system', `Dashboard running on port ${PORT}`));

  initBars();
  log('system', `ICT RegimeAI Bot starting -- mode: ${CFG.demo?'DEMO':'LIVE'} -- ${CFG.paperOnly?'PAPER TRADING':'LIVE ORDERS'}`);
  log('system', `Symbols: ${ACTIVE_SYMS.join(', ')} -- Scan every ${CFG.scanSec}s -- Min score: ${CFG.minScore}`);

  if (!CFG.username || !CFG.cid) {
    log('warn', 'No credentials set -- running in simulation mode (set TV_USERNAME, TV_PASSWORD, TV_CID, TV_SEC env vars)');
    startScanLoop();
    return;
  }

  const ok = await authenticate();
  if (ok) {
    connectMD();
    startScanLoop();
  } else {
    log('warn', 'Auth failed -- retrying in 30s...');
    setTimeout(start, 30000);
  }
}

function startScanLoop() {
  if (scanTimer) clearInterval(scanTimer);
  setTimeout(() => {
    scan();
    scanTimer = setInterval(() => {
      updatePositions();
      scan();
    }, CFG.scanSec * 1000);
  }, 2000);
}

// Re-auth every 18 hours (token expires in 24h)
setInterval(async () => {
  if (CFG.username && CFG.cid) {
    log('system', 'Refreshing auth token...');
    const ok = await authenticate();
    if (ok) connectMD();
  }
}, 18 * 60 * 60 * 1000);

start();
