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

// Script content as array to avoid template literal issues
const SCRIPT_LINES = [
  "const API = '';  // same origin",
  "let state = {};",
  "let tab = 'regime';",
  "let localCfg = {",
  "  scanSec:5, minScore:60, riskPct:2, maxDailyLoss:5, maxOpen:4, maxDailyTrades:15, maxStreak:3,",
  "  ictEnabled:true, scalpEnabled:true, ceEnabled:true,",
  "  tp1R:1.0, tp2R:2.0, tp3R:3.5,",
  "  bypassLoss:false, bypassMgn:false,",
  "};",
  "let creds = {username:'',password:'',cid:'',sec:''};",
  "",
  "const REGIME_COLORS = {",
  "  TREND:'#10b981',MEAN_REVERSION:'#22d3ee',EXPANSION:'#f59e0b',",
  "  COMPRESSION:'#a78bfa',REVERSAL:'#f97316',NEWS_SPIKE:'#ef4444',",
  "  DEAD_ZONE:'#475569',CHOP:'#64748b',UNKNOWN:'#334155'",
  "};",
  "const REGIME_DESC = {",
  "  TREND:'Clean directional move -- ICT + strategies active',",
  "  MEAN_REVERSION:'Price pulling back to VWAP -- Scalp active',",
  "  EXPANSION:'Volatility burst -- Compression Expansion active',",
  "  COMPRESSION:'ATR squeeze -- waiting for breakout',",
  "  REVERSAL:'Fake breakout detected -- ICT reversal setups',",
  "  NEWS_SPIKE:'Extreme volume spike -- all strategies paused',",
  "  DEAD_ZONE:'Low ATR + low volume -- no quality setups',",
  "  CHOP:'Indecisive price action -- waiting for direction',",
  "  UNKNOWN:'Analyzing market...',",
  "};",
  "const LOG_COLORS = {scan:'#60a5fa',buy:'#10b981',exit:'#a855f7',warn:'#ef4444',system:'#f59e0b',info:'#94a3b8'};",
  "const STRAT_COLORS = {ICT_SMC:'#60a5fa',VWAP_SCALP:'#22d3ee',COMP_EXPAND:'#a78bfa'};",
  "",
  "function setTab(t) {",
  "  tab = t;",
  "  document.querySelectorAll('.tab').forEach((el,i)=>{",
  "    el.classList.toggle('active', el.textContent.toLowerCase().startsWith(t));",
  "  });",
  "  render();",
  "}",
  "",
  "function toggleBypass(type) {",
  "  if(type==='loss') localCfg.bypassLoss=!localCfg.bypassLoss;",
  "  if(type==='mgn') localCfg.bypassMgn=!localCfg.bypassMgn;",
  "  updateBypasses();",
  "}",
  "",
  "function updateBypasses() {",
  "  const lb = document.getElementById('bypass-loss');",
  "  const mb = document.getElementById('bypass-mgn');",
  "  if(lb) { lb.style.color=localCfg.bypassLoss?'#f59e0b':'#475569'; lb.style.borderColor=localCfg.bypassLoss?'#f59e0b55':'#334155'; lb.style.background=localCfg.bypassLoss?'#f59e0b18':'transparent'; lb.textContent=localCfg.bypassLoss?'LOSS OFF':'LOSS ON'; }",
  "  if(mb) { mb.style.color=localCfg.bypassMgn?'#a78bfa':'#475569'; mb.style.borderColor=localCfg.bypassMgn?'#a78bfa55':'#334155'; mb.style.background=localCfg.bypassMgn?'#a78bfa18':'transparent'; mb.textContent=localCfg.bypassMgn?'MGN OFF':'MGN ON'; }",
  "}",
  "",
  "function tag(text, col) {",
  "  return `<span class=\"tag\" style=\"color:${col};border-color:${col}44;background:${col}12\">${text}</span>`;",
  "}",
  "",
  "function scoreRing(score) {",
  "  const col = score>=80?'#10b981':score>=60?'#f59e0b':'#ef4444';",
  "  const dash = score*1.38;",
  "  return `<div style=\"position:relative;width:44px;height:44px;display:inline-flex;align-items:center;justify-content:center\">",
  "    <svg width=\"44\" height=\"44\" viewBox=\"0 0 44 44\">",
  "      <circle cx=\"22\" cy=\"22\" r=\"18\" fill=\"none\" stroke=\"#1e293b\" stroke-width=\"4\"/>",
  "      <circle cx=\"22\" cy=\"22\" r=\"18\" fill=\"none\" stroke=\"${col}\" stroke-width=\"4\" stroke-dasharray=\"${dash} 113\" stroke-linecap=\"round\" transform=\"rotate(-90 22 22)\"/>",
  "    </svg>",
  "    <span style=\"position:absolute;color:${col};font-size:9px;font-weight:bold\">${score}</span>",
  "  </div>`;",
  "}",
  "",
  "function render() {",
  "  const s = state;",
  "  const c = s.config || {};",
  "",
  "  // Update header",
  "  const regime = s.lastScan?.regimes?.split(' ')?.[0]?.split(':')?.[1] || 'UNKNOWN';",
  "  const rc = REGIME_COLORS[regime]||'#64748b';",
  "  const rb = document.getElementById('regime-badge');",
  "  if(rb) { rb.textContent=regime.replace('_',' '); rb.style.color=rc; rb.style.borderColor=rc+'44'; rb.style.background=rc+'12'; }",
  "  const sb = document.getElementById('sess-badge');",
  "  if(sb) sb.textContent = s.session||'--';",
  "  const bd = document.getElementById('bal-display');",
  "  if(bd) bd.textContent = s.balance?'$'+s.balance.toFixed(0):'$--';",
  "",
  "  // Prices bar",
  "  const pb = document.getElementById('prices-bar');",
  "  if(pb && s.prices) {",
  "    const ICOL = {MNQ:'#60a5fa',MES:'#10b981',MYM:'#f59e0b',MCL:'#f97316',MGC:'#fbbf24'};",
  "    pb.innerHTML = Object.entries(s.prices).map(([sym,p])=>",
  "      `<div style=\"display:flex;gap:5px;align-items:center\">",
  "        <span class=\"price-sym\" style=\"color:${ICOL[sym]||'#94a3b8'}\">${sym}</span>",
  "        <span style=\"color:#cbd5e1\">${p?(sym==='MCL'?p.toFixed(2):p.toFixed(0)):'--'}</span>",
  "        ${(s.positions||[]).find(pos=>!pos.closed&&pos.sym===sym)?'<span style=\"color:#10b981;font-size:9px\">●</span>':''}",
  "      </div>`",
  "    ).join('') +",
  "    `<div class=\"ml-auto\" style=\"margin-left:auto;display:flex;align-items:center;gap:5px\">",
  "      <div style=\"width:6px;height:6px;border-radius:50%;background:${s.mdConnected?'#10b981':'#475569'}\"></div>",
  "      <span style=\"color:${s.mdConnected?'#10b981':'#475569'};font-size:9px\">${s.mdConnected?'LIVE':'SIM'}</span>",
  "    </div>`;",
  "  }",
  "",
  "  // Sig/pos counts",
  "  const sigCount = document.getElementById('sig-count');",
  "  if(sigCount) sigCount.textContent = s.signals?.length?`[${s.signals.length}]`:'';",
  "  const posCount = document.getElementById('pos-count');",
  "  const openPos = (s.positions||[]).filter(p=>!p.closed);",
  "  if(posCount) posCount.textContent = openPos.length?`(${openPos.length})`:'';",
  "",
  "  const el = document.getElementById('content');",
  "  if(!el) return;",
  "",
  "  if(tab==='regime') el.innerHTML = renderRegime(s);",
  "  else if(tab==='signals') el.innerHTML = renderSignals(s);",
  "  else if(tab==='positions') el.innerHTML = renderPositions(s);",
  "  else if(tab==='trades') el.innerHTML = renderTrades(s);",
  "  else if(tab==='stats') el.innerHTML = renderStats(s);",
  "  else if(tab==='settings') el.innerHTML = renderSettings(s);",
  "  else if(tab==='connect') el.innerHTML = renderConnect(s);",
  "  else if(tab==='log') el.innerHTML = renderLog(s);",
  "}",
  "",
  "function renderRegime(s) {",
  "  const regime = s.lastScan?.regimes?.split(' ')?.[0]?.split(':')?.[1] || 'UNKNOWN';",
  "  const rc = REGIME_COLORS[regime]||'#64748b';",
  "  const openPos = (s.positions||[]).filter(p=>!p.closed);",
  "  const dailyTarget = (s.balance||10000)*0.02;",
  "  const dailyPct = Math.min(100,Math.max(0,((s.dailyPnl||0)/dailyTarget)*100));",
  "  const winRate = s.trades>0?((s.wins/s.trades)*100).toFixed(0):'--';",
  "",
  "  return `",
  "  <div class=\"card\">",
  "    <div style=\"display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px\">",
  "      <div>",
  "        <div style=\"color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px;margin-bottom:6px\">Current Market Regime</div>",
  "        <div class=\"regime-badge\" style=\"border-color:${rc}44;background:${rc}12;display:inline-flex\">",
  "          <div class=\"regime-dot\" style=\"background:${rc}\"></div>",
  "          <span style=\"color:${rc};font-weight:bold;font-size:11px\">${regime.replace('_',' ')}</span>",
  "        </div>",
  "      </div>",
  "      <div style=\"text-align:right\">",
  "        <div style=\"color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px;margin-bottom:6px\">Trade Permission</div>",
  "        <div style=\"padding:6px 12px;border-radius:6px;font-weight:bold;font-size:10px;background:${s.trading?'#022c2220':'#2c000020'};color:${s.trading?'#10b981':'#ef4444'};border:1px solid ${s.trading?'#10b98144':'#ef444444'}\">",
  "          ${s.trading?'OK TRADING ALLOWED':'X TRADING BLOCKED'}",
  "        </div>",
  "      </div>",
  "    </div>",
  "    <div style=\"color:#64748b;font-size:10px;margin-top:8px\">${REGIME_DESC[regime]||'Analyzing...'}</div>",
  "  </div>",
  "",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Daily P&L Target</div>",
  "    <div style=\"display:flex;justify-content:space-between;margin-bottom:6px\">",
  "      <span style=\"color:#475569;font-size:9px\">Progress to $${dailyTarget.toFixed(0)} target</span>",
  "      <span style=\"font-weight:bold;font-size:10px;color:${(s.dailyPnl||0)>=0?'#10b981':'#ef4444'}\">${(s.dailyPnl||0)>=0?'+':''}$${(s.dailyPnl||0).toFixed(2)}</span>",
  "    </div>",
  "    <div class=\"progress-bar\"><div class=\"progress-fill\" style=\"width:${dailyPct}%;background:${dailyPct>=100?'#10b981':dailyPct>50?'#f59e0b':'#60a5fa'}\"></div></div>",
  "    <div class=\"grid4\" style=\"margin-top:10px\">",
  "      ${[['Bal','$'+(s.balance||0).toFixed(0),'#10b981'],['W%',winRate+'%',parseFloat(winRate)>=50?'#10b981':'#f59e0b'],['Open',openPos.length+'/'+(s.maxOpen||4),'#94a3b8'],['Trades',(s.dailyTrades||0)+'/'+(s.maxDailyTrades||15),'#94a3b8']].map(([l,v,c])=>`",
  "        <div class=\"stat-card\"><div class=\"stat-label\">${l}</div><div class=\"stat-val\" style=\"color:${c}\">${v}</div></div>",
  "      `).join('')}",
  "    </div>",
  "  </div>",
  "",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Risk Meters</div>",
  "    ${[",
  "      ['Daily Loss',localCfg.bypassLoss?0:(s.dailyLoss||0),(s.balance||10000)*0.05,'#ef4444','red'],",
  "      ['Margin Used',(s.marginUsed||0),(s.balance||10000)*0.12,'#a78bfa','purple'],",
  "      ['Streak',localCfg.bypassLoss?0:(s.streak||0),(s.maxStreak||3),'#22d3ee','cyan'],",
  "    ].map(([l,v,mx,col])=>{",
  "      const p=Math.min(100,(v/Math.max(mx,0.01))*100);",
  "      return `<div style=\"margin-bottom:10px\">",
  "        <div style=\"display:flex;justify-content:space-between;color:#64748b;font-size:9px;margin-bottom:3px\">",
  "          <span>${l}</span><span>${typeof v==='number'?v.toFixed(1):v} / ${typeof mx==='number'?mx.toFixed(1):mx}</span>",
  "        </div>",
  "        <div class=\"progress-bar\"><div class=\"progress-fill\" style=\"width:${p}%;background:${p>80?'#ef4444':p>55?'#f59e0b':col}\"></div></div>",
  "      </div>`;",
  "    }).join('')}",
  "  </div>",
  "",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Bot Status</div>",
  "    <div style=\"display:flex;align-items:center;gap:8px;margin-bottom:10px\">",
  "      <div style=\"width:10px;height:10px;border-radius:50%;background:${s.scanning?'#10b981':'#334155'}\"></div>",
  "      <span style=\"color:${s.scanning?'#10b981':'#64748b'};font-weight:bold;font-size:11px\">${s.scanning?'SCANNING LIVE':'SCANNER OFF'}</span>",
  "      ${s.scanning?`<span style=\"color:#475569;font-size:9px;margin-left:auto\">every ${s.config?.scanSec||5}s</span>`:''}",
  "    </div>",
  "    ${s.lastScan?`<div style=\"color:#94a3b8;font-size:10px\">Last: <span style=\"color:#fff\">${s.lastScan.time}</span> &nbsp; Regimes: <span style=\"color:#f59e0b\">${s.lastScan.regimes}</span> &nbsp; Signals: <span style=\"color:#10b981\">${s.lastScan.signals}</span></div>`:''}",
  "  </div>`;",
  "}",
  "",
  "function renderSignals(s) {",
  "  const sigs = s.signals||[];",
  "  if(!sigs.length) return `<div style=\"text-align:center;padding:40px;color:#475569\">${s.scanning?'Scanning -- regime engine running':'Bot not scanning'}</div>`;",
  "  return sigs.map(sig=>{",
  "    const col=sig.action==='Buy'?'#10b981':'#ef4444';",
  "    const sc=sig.score||0;",
  "    const blocked=sc<(s.config?.minScore||60);",
  "    return `<div style=\"border:1px solid ${blocked?'#33415588':col+'33'};background:${blocked?'#0a0a0a':col+'08'};border-radius:10px;padding:12px;margin-bottom:8px\">",
  "      <div style=\"display:flex;justify-content:space-between;align-items:flex-start\">",
  "        <div>",
  "          <div style=\"display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:4px\">",
  "            <span style=\"color:${sig.sym==='MNQ'?'#60a5fa':'#10b981'};font-weight:bold;font-size:13px\">${sig.sym}</span>",
  "            ${tag(sig.action==='Buy'?'LONG':'SHORT',col)}",
  "            ${tag((sig.strategy||'').replace('_',' '),STRAT_COLORS[sig.strategy]||'#94a3b8')}",
  "            ${tag(sig.regime||'',REGIME_COLORS[sig.regime]||'#64748b')}",
  "            ${blocked?tag('BLOCKED','#ef4444'):''}",
  "          </div>",
  "          <div style=\"color:#64748b;font-size:9px\">entry ${sig.entry?.toFixed(1)} · stop ${sig.stop?.toFixed(1)} · R:R ${sig.expectedRR}:1</div>",
  "        </div>",
  "        ${scoreRing(sc)}",
  "      </div>",
  "      ${blocked?`<div style=\"color:#ef4444;font-size:9px;text-align:center;margin-top:6px\">Score ${sc} below minimum ${s.config?.minScore||60}</div>`:'<div style=\"color:#22d3ee;font-size:9px;text-align:center;margin-top:6px\">Auto-executing if risk checks pass</div>'}",
  "    </div>`;",
  "  }).join('');",
  "}",
  "",
  "function renderPositions(s) {",
  "  const open=(s.positions||[]).filter(p=>!p.closed);",
  "  const totalPnl=open.reduce((sum,p)=>{",
  "    const cur=s.prices?.[p.sym]||p.entry;",
  "    const pts=p.action==='Buy'?cur-p.entry:p.entry-cur;",
  "    const tv={MNQ:2,MES:5,MYM:0.5,MCL:100,MGC:10}[p.sym]||2;",
  "    return sum+pts*tv*p.contracts;",
  "  },0);",
  "",
  "  return `<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:10px\">",
  "    <span style=\"color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px\">Open (${open.length})</span>",
  "    <span style=\"font-weight:bold;font-size:11px;color:${totalPnl>=0?'#10b981':'#ef4444'}\">${totalPnl>=0?'+':''}$${totalPnl.toFixed(2)}</span>",
  "  </div>",
  "  ${open.length?open.map(p=>{",
  "    const cur=s.prices?.[p.sym]||p.entry;",
  "    const pts=p.action==='Buy'?cur-p.entry:p.entry-cur;",
  "    const tv={MNQ:2,MES:5,MYM:0.5,MCL:100,MGC:10}[p.sym]||2;",
  "    const pnl=pts*tv*p.contracts;",
  "    const rVal=p.stopDist>0?pts/p.stopDist:0;",
  "    const green=pnl>=0;",
  "    return `<div class=\"pos-card\" style=\"border-color:${green?'#10b98133':'#ef444433'};background:${green?'#022c2215':'#2c000015'}\">",
  "      <div style=\"display:flex;justify-content:space-between;align-items:flex-start\">",
  "        <div>",
  "          <div style=\"display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:4px\">",
  "            <span style=\"color:#60a5fa;font-weight:bold;font-size:12px\">${p.sym}</span>",
  "            ${tag(p.action==='Buy'?'LONG':'SHORT',p.action==='Buy'?'#10b981':'#ef4444')}",
  "            ${tag((p.strategy||'').replace('_',' '),STRAT_COLORS[p.strategy]||'#94a3b8')}",
  "            ${p.paper?tag('PAPER','#22d3ee'):''}",
  "            ${p.stopMoved?tag('BE','#f59e0b'):''}",
  "          </div>",
  "          <div style=\"color:#64748b;font-size:9px\">${p.contracts}c · entry ${p.entry?.toFixed(1)} · stop ${(p.curStop||p.stop)?.toFixed(1)} · score ${p.score}</div>",
  "        </div>",
  "        <div style=\"text-align:right\">",
  "          <div style=\"font-weight:bold;font-size:13px;color:${green?'#10b981':'#ef4444'}\">${green?'+':''}$${pnl.toFixed(2)}</div>",
  "          <div style=\"color:#64748b;font-size:9px\">${rVal>=0?'+':''}${rVal.toFixed(2)}R</div>",
  "        </div>",
  "      </div>",
  "      <div style=\"display:grid;grid-template-columns:repeat(5,1fr);gap:4px;margin-top:8px;font-size:9px\">",
  "        ${[['SL',(p.curStop||p.stop),'#ef4444'],['E',p.entry,'#94a3b8'],['TP1',p.tp1,p.tp1Closed?'#10b981':'#475569'],['TP2',p.tp2,p.tp2Closed?'#10b981':'#475569'],['TP3',p.tp3,'#475569']].map(([l,v,c])=>`",
  "          <div style=\"text-align:center;background:#0f172a;padding:4px;border-radius:4px\">",
  "            <div style=\"color:#334155\">${l}</div>",
  "            <div style=\"color:${c};font-weight:bold\">${v?.toFixed(1)||'-'}</div>",
  "          </div>`).join('')}",
  "      </div>",
  "      <div style=\"height:3px;background:#1e293b;border-radius:2px;margin-top:8px;overflow:hidden\">",
  "        <div style=\"height:100%;width:${Math.min(100,Math.max(0,(rVal/4)*100))}%;background:${green?'#10b981':'#ef4444'};border-radius:2px\"></div>",
  "      </div>",
  "    </div>`;",
  "  }).join(''):'<div style=\"text-align:center;padding:40px;color:#475569\">No open positions</div>'}`;",
  "}",
  "",
  "function renderTrades(s) {",
  "  const closed=s.closedTrades||[];",
  "  const totalPnl=closed.reduce((sum,t)=>sum+(t.finalPnl||0),0);",
  "  return `<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:10px\">",
  "    <span style=\"color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px\">Closed (${closed.length})</span>",
  "    <span style=\"font-weight:bold;font-size:11px;color:${totalPnl>=0?'#10b981':'#ef4444'}\">${totalPnl>=0?'+':''}$${totalPnl.toFixed(2)}</span>",
  "  </div>",
  "  ${closed.length?closed.map(t=>`",
  "    <div style=\"background:#0a0a0a;border:1px solid #1e293b;border-radius:6px;padding:8px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center\">",
  "      <div>",
  "        <div style=\"display:flex;gap:4px;flex-wrap:wrap;margin-bottom:3px\">",
  "          ${tag(t.closeReason||'CLOSE',(t.finalPnl||0)>=0?'#10b981':'#ef4444')}",
  "          ${tag((t.strategy||'').replace('_',' '),STRAT_COLORS[t.strategy]||'#94a3b8')}",
  "          ${t.paper?tag('P','#22d3ee'):''}",
  "        </div>",
  "        <div style=\"color:#94a3b8;font-size:9px\"><span style=\"color:#60a5fa\">${t.sym}</span> ${t.contracts}c · ${t.entry?.toFixed(1)}→${t.closePrice?.toFixed(1)}</div>",
  "        <div style=\"color:#475569;font-size:9px\">${t.ts||''} · ${t.curR?.toFixed(2)||'0.00'}R</div>",
  "      </div>",
  "      <div style=\"font-weight:bold;color:${(t.finalPnl||0)>=0?'#10b981':'#ef4444'}\">${(t.finalPnl||0)>=0?'+':''}$${(t.finalPnl||0).toFixed(2)}</div>",
  "    </div>`).join(''):'<div style=\"text-align:center;padding:40px;color:#475569\">No closed trades yet</div>'}`;",
  "}",
  "",
  "function renderStats(s) {",
  "  const wins=s.wins||0, total=s.trades||0;",
  "  const wr=total>0?(wins/total*100).toFixed(0)+'%':'--';",
  "  return `<div class=\"grid2\" style=\"margin-bottom:10px\">",
  "    ${[['Total P&L',(s.totalPnl||0)>=0?'+$'+(s.totalPnl||0).toFixed(2):'$'+(s.totalPnl||0).toFixed(2),(s.totalPnl||0)>=0?'#10b981':'#ef4444'],",
  "      ['Win Rate',wr,parseFloat(wr)>=50?'#10b981':'#f59e0b'],",
  "      ['Trades',total,'#94a3b8'],",
  "      ['Daily P&L',(s.dailyPnl||0)>=0?'+$'+(s.dailyPnl||0).toFixed(2):'$'+(s.dailyPnl||0).toFixed(2),(s.dailyPnl||0)>=0?'#10b981':'#ef4444']",
  "    ].map(([l,v,c])=>`<div class=\"stat-card\"><div class=\"stat-label\">${l}</div><div class=\"stat-val\" style=\"color:${c}\">${v}</div></div>`).join('')}",
  "  </div>",
  "  ${s.statsByStrat&&Object.keys(s.statsByStrat).length?`<div class=\"card\"><div class=\"card-title\">By Strategy</div>",
  "    ${Object.entries(s.statsByStrat).map(([k,v])=>`",
  "      <div style=\"display:flex;justify-content:space-between;padding:6px;background:#111827;border-radius:4px;margin-bottom:4px\">",
  "        <span style=\"color:${STRAT_COLORS[k]||'#94a3b8'}\">${k.replace('_',' ')}</span>",
  "        <span style=\"color:${v.pnl>=0?'#10b981':'#ef4444'};font-weight:bold\">${v.pnl>=0?'+':''}$${v.pnl.toFixed(2)} (${v.trades})</span>",
  "      </div>`).join('')}",
  "  </div>`:''}`;",
  "}",
  "",
  "function renderSettings(s) {",
  "  const c=localCfg;",
  "  function sl(label,key,min,max,step,fmt) {",
  "    return `<div class=\"slider-row\">",
  "      <span class=\"slider-label\">${label}</span>",
  "      <input type=\"range\" class=\"slider\" min=\"${min}\" max=\"${max}\" step=\"${step}\" value=\"${c[key]}\" oninput=\"localCfg['${key}']=+this.value;document.getElementById('sv_${key}').textContent='${fmt?'':''}'+(${fmt?fmt.replace('v','this.value'):'this.value'})\" onchange=\"saveCfg()\">",
  "      <span class=\"slider-val\" id=\"sv_${key}\">${fmt?eval(fmt.replace('v',c[key])):c[key]}</span>",
  "    </div>`;",
  "  }",
  "  function tog(label,key) {",
  "    return `<div class=\"toggle-row\" onclick=\"localCfg['${key}']=!localCfg['${key}'];render();saveCfg()\">",
  "      <div class=\"toggle-track\" style=\"background:${c[key]?'#10b981':'#334155'}\"><div class=\"toggle-thumb\" style=\"transform:translateX(${c[key]?'13px':'2px'})\"></div></div>",
  "      <span style=\"color:#94a3b8;font-size:9px\">${label}</span>",
  "    </div>`;",
  "  }",
  "  return `<div class=\"card\">",
  "    <div class=\"card-title\">Strategy Toggles</div>",
  "    ${tog('ICT/SMC -- order blocks, FVG, liquidity sweeps','ictEnabled')}",
  "    ${tog('VWAP Scalp -- mean reversion, EMA, momentum','scalpEnabled')}",
  "    ${tog('Compression Expansion -- volatility breakout','ceEnabled')}",
  "  </div>",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Risk & Limits</div>",
  "    ${sl('Risk per trade %','riskPct',0.5,10,0.5,'v+\"%\"')}",
  "    ${sl('Max daily loss %','maxDailyLoss',1,10,0.5,'v+\"%\"')}",
  "    ${sl('Max open trades','maxOpen',1,10,1,'')}",
  "    ${sl('Max daily trades','maxDailyTrades',1,30,1,'')}",
  "    ${sl('Max loss streak','maxStreak',2,6,1,'')}",
  "  </div>",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Entry Quality</div>",
  "    ${sl('Min score (0-100)','minScore',40,95,5,'')}",
  "    ${sl('Scan interval (sec)','scanSec',5,60,5,'')}",
  "  </div>",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Exit Targets (R multiples)</div>",
  "    ${sl('TP1 (50% close)','tp1R',0.5,2,0.25,'\"+\" + v + \"R\"')}",
  "    ${sl('TP2 (remainder)','tp2R',1,5,0.25,'\"+\" + v + \"R\"')}",
  "  </div>",
  "  <button class=\"btn btn-green\" style=\"width:100%;padding:12px\" onclick=\"saveCfg()\">SAVE SETTINGS</button>`;",
  "}",
  "",
  "function renderConnect(s) {",
  "  return `<div class=\"card\" style=\"border-color:${s.mdConnected?'#10b98144':'#1e293b'}\">",
  "    <div style=\"display:flex;align-items:center;justify-content:space-between\">",
  "      <div class=\"connect-status\">",
  "        <div class=\"connect-dot\" style=\"background:${s.mdConnected?'#10b981':'#475569'}\"></div>",
  "        <span style=\"font-weight:bold;font-size:11px;color:${s.mdConnected?'#10b981':'#64748b'}\">${s.mdConnected?'LIVE FEED ACTIVE':'SIMULATED DATA'}</span>",
  "      </div>",
  "    </div>",
  "    <div style=\"color:#64748b;font-size:9px;margin-top:6px\">${s.mdConnected?'Real-time Tradovate tick data streaming':'Bot running on simulated price data'}</div>",
  "  </div>",
  "  <div class=\"card\" style=\"border-color:#f59e0b44\">",
  "    <div style=\"color:#f59e0b;font-weight:bold;font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px\">Tradovate Credentials</div>",
  "    <div style=\"color:#fde68a;font-size:9px;margin-bottom:10px\">Set these as Environment Variables in Render dashboard → ict-regime-bot → Environment</div>",
  "    ${[['TV_USERNAME','Email / Username','text'],['TV_PASSWORD','Password','password'],['TV_CID','CID (integer)','text'],['TV_SEC','Secret UUID','password']].map(([k,l,t])=>`",
  "      <div style=\"margin-bottom:8px\">",
  "        <span style=\"color:#64748b;font-size:9px\">${l} <span style=\"color:#475569\">(env: ${k})</span></span>",
  "        <input type=\"${t}\" placeholder=\"${l}\" oninput=\"creds['${k.toLowerCase().replace('tv_','')}']= this.value\" style=\"border-color:${s.credentials?.[k]?'#10b981':'#334155'}\">",
  "      </div>`).join('')}",
  "    <div style=\"display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px\">",
  "      <div>",
  "        <span style=\"color:#64748b;font-size:9px\">Mode</span>",
  "        <select onchange=\"localCfg.demo=this.value==='demo'\" style=\"background:#1e293b;border:1px solid #334155;color:#fff;padding:8px;border-radius:6px;font-family:monospace;font-size:11px;width:100%;margin-top:4px\">",
  "          <option value=\"live\" ${!localCfg.demo?'selected':''}>Live Account</option>",
  "          <option value=\"demo\" ${localCfg.demo?'selected':''}>Demo Account</option>",
  "        </select>",
  "      </div>",
  "      <div>",
  "        <span style=\"color:#64748b;font-size:9px\">Trading Mode</span>",
  "        <select onchange=\"localCfg.paperOnly=this.value==='paper'\" style=\"background:#1e293b;border:1px solid #334155;color:#fff;padding:8px;border-radius:6px;font-family:monospace;font-size:11px;width:100%;margin-top:4px\">",
  "          <option value=\"paper\" ${localCfg.paperOnly!==false?'selected':''}>Paper Trading</option>",
  "          <option value=\"live\" ${localCfg.paperOnly===false?'selected':''}>Live Orders (!)</option>",
  "        </select>",
  "      </div>",
  "    </div>",
  "    <div style=\"background:#140d00;border:1px solid #f59e0b22;border-radius:6px;padding:8px;margin-top:10px;color:#fde68a;font-size:9px\">",
  "      To update credentials: Go to dashboard.render.com → ict-regime-bot → Environment → add/update the TV_ variables → Save Changes. The bot will restart automatically.",
  "    </div>",
  "  </div>",
  "  <div class=\"card\">",
  "    <div class=\"card-title\">Active Contracts</div>",
  "    ${Object.entries(s.contracts||{MNQ:'MNQM6',MES:'MESM6',MYM:'MYMM6',MCL:'MCLN6',MGC:'MGCM6'}).map(([sym,contract])=>`",
  "      <div style=\"display:flex;justify-content:space-between;padding:5px 8px;background:#111827;border-radius:4px;margin-bottom:4px;font-size:9px\">",
  "        <span style=\"color:#60a5fa;font-weight:bold\">${sym}</span>",
  "        <span style=\"color:${s.mdConnected?'#10b981':'#64748b'}\">${contract}</span>",
  "        <span style=\"color:${s.mdConnected?'#10b981':'#475569'}\">${s.mdConnected?'STREAMING':'--'}</span>",
  "      </div>`).join('')}",
  "    <div style=\"color:#475569;font-size:9px;margin-top:6px\">June 2026 front month. Rolls to September (U6) ~June 20.</div>",
  "  </div>`;",
  "}",
  "",
  "function renderLog(s) {",
  "  const logs=s.logs||[];",
  "  return `<div style=\"display:flex;justify-content:space-between;align-items:center;margin-bottom:8px\">",
  "    <span style=\"color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px\">Events (${logs.length})</span>",
  "  </div>",
  "  ${logs.length?logs.slice(0,100).map(e=>`",
  "    <div class=\"log-row\">",
  "      <span class=\"log-ts\">${e.ts?.slice(11,19)||''}</span>",
  "      <span style=\"color:${LOG_COLORS[e.type]||'#94a3b8'}\">${e.msg}</span>",
  "    </div>`).join(''):'<div style=\"text-align:center;padding:40px;color:#475569\">No events yet</div>'}`;",
  "}",
  "",
  "function saveCfg() {",
  "  fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(localCfg)}).catch(()=>{});",
  "}",
  "",
  "async function fetchState() {",
  "  try {",
  "    const r = await fetch('/api/state');",
  "    state = await r.json();",
  "    if(state.config) {",
  "      Object.assign(localCfg, {",
  "        scanSec:state.config.scanSec||5,",
  "        minScore:state.config.minScore||60,",
  "        riskPct:(state.config.riskPct||0.02)*100,",
  "        maxDailyLoss:(state.config.maxDailyLossPct||0.05)*100,",
  "        maxOpen:state.config.maxOpen||4,",
  "        maxDailyTrades:state.config.maxDailyTrades||15,",
  "        maxStreak:state.config.maxStreak||3,",
  "      });",
  "    }",
  "    render();",
  "  } catch(e) {}",
  "}",
  "",
  "fetchState();",
  "setInterval(fetchState, 2000);",
];
const SCRIPT_JS = SCRIPT_LINES.join("\n");

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>ICT RegimeAI Bot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#000;color:#fff;font-family:monospace;font-size:12px}
.header{background:#0a0a0a;border-bottom:1px solid #1e293b;padding:10px 14px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;position:sticky;top:0;z-index:40}
.logo{color:#10b981;font-weight:bold;font-size:15px;letter-spacing:1px}
.badge{padding:3px 7px;border-radius:4px;border:1px solid;font-size:9px;font-weight:bold;cursor:pointer}
.prices-bar{background:#0a0a0a;border-bottom:1px solid #1e293b;padding:6px 14px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}
.price-sym{font-weight:bold;font-size:11px}
.tabs{display:flex;gap:4px;padding:8px 14px;overflow-x:auto;background:#000;border-bottom:1px solid #0f172a}
.tab{padding:6px 12px;border-radius:6px;font-size:9px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;cursor:pointer;color:#64748b;white-space:nowrap}
.tab.active{background:#10b981;color:#fff}
.content{padding:12px 14px;max-width:900px;margin:0 auto}
.card{background:#0a0a0a;border:1px solid #1e293b;border-radius:10px;padding:12px;margin-bottom:10px}
.card-title{color:#475569;font-size:9px;text-transform:uppercase;letter-spacing:2px;margin-bottom:10px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}
.stat-card{background:#111827;border-radius:6px;padding:10px;text-align:center}
.stat-label{color:#475569;font-size:9px;margin-bottom:4px}
.stat-val{font-size:15px;font-weight:bold}
.btn{padding:8px 14px;border-radius:8px;border:1px solid;font-family:monospace;font-size:10px;font-weight:bold;cursor:pointer;transition:all 0.15s}
.btn-green{background:#022c2220;color:#10b981;border-color:#10b98133}
.btn-green:hover{background:#10b98130}
.btn-red{background:#2c000020;color:#ef4444;border-color:#ef444433}
.btn-red:hover{background:#ef444430}
.btn-blue{background:#001a3a20;color:#60a5fa;border-color:#60a5fa33}
.btn-blue:hover{background:#60a5fa30}
.btn-gray{background:#1e293b;color:#94a3b8;border-color:#334155}
.mode-btn{padding:10px;border-radius:8px;border:1px solid;cursor:pointer;text-align:left;transition:all 0.15s}
.pos-card{border:1px solid #1e293b;border-radius:8px;padding:10px;margin-bottom:8px}
.tag{padding:2px 6px;border-radius:3px;border:1px solid;font-size:9px;font-weight:bold}
.log-row{padding:3px 0;border-bottom:1px solid #0f172a;display:flex;gap:8px;font-size:10px}
.log-ts{color:#334155;min-width:55px;flex-shrink:0}
.slider-row{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.slider-label{color:#64748b;font-size:9px;width:140px;flex-shrink:0}
.slider{flex:1;height:4px;border-radius:2px;background:#1e293b;accent-color:#10b981}
.slider-val{color:#fff;font-size:9px;font-weight:bold;width:45px;text-align:right;flex-shrink:0}
.toggle-row{display:flex;align-items:center;gap:8px;margin-bottom:8px;cursor:pointer}
.toggle-track{width:28px;height:16px;border-radius:8px;position:relative;transition:background 0.2s;flex-shrink:0}
.toggle-thumb{position:absolute;top:2px;width:12px;height:12px;background:#fff;border-radius:50%;transition:transform 0.2s}
.regime-badge{display:flex;align-items:center;gap:6px;padding:6px 10px;border-radius:8px;border:1px solid}
.regime-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.progress-bar{height:6px;border-radius:3px;background:#1e293b;overflow:hidden;margin-top:4px}
.progress-fill{height:100%;border-radius:3px;transition:width 0.5s}
.score-ring{position:relative;display:inline-flex;align-items:center;justify-content:center}
.connect-status{display:flex;align-items:center;gap:6px}
.connect-dot{width:10px;height:10px;border-radius:50%}
input[type=text],input[type=password],input[type=number]{background:#1e293b;border:1px solid #334155;color:#fff;padding:8px 10px;border-radius:6px;font-family:monospace;font-size:11px;width:100%;margin-top:4px}
input[type=text]:focus,input[type=password]:focus,input[type=number]:focus{outline:none;border-color:#10b981}
.input-label{color:#64748b;font-size:9px;margin-top:8px;display:block}
@keyframes pu{0%,100%{opacity:1}50%{opacity:0.3}}
.pulse{animation:pu 1.4s ease-in-out infinite}
</style>
</head>
<body>

<div class="header">
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
    <span class="logo">ICT RegimeAI</span>
    <span class="badge pulse" id="mode-badge" style="color:#22d3ee;border-color:#22d3ee44;background:#22d3ee12">PAPER AUTO</span>
    <span class="badge" id="regime-badge" style="color:#64748b;border-color:#33415544;background:#11111144">UNKNOWN</span>
    <span class="badge" id="sess-badge" style="color:#94a3b8;border-color:#1e293b;background:#0a0a0a">--</span>
  </div>
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
    <button class="badge" id="bypass-loss" onclick="toggleBypass('loss')" style="color:#475569;border-color:#334155;background:transparent">LOSS ON</button>
    <button class="badge" id="bypass-mgn" onclick="toggleBypass('mgn')" style="color:#475569;border-color:#334155;background:transparent">MGN ON</button>
    <div style="text-align:right">
      <div style="color:#475569;font-size:9px">BALANCE</div>
      <div style="color:#10b981;font-weight:bold;font-size:14px" id="bal-display">$--</div>
    </div>
  </div>
</div>

<div class="prices-bar" id="prices-bar">
  <span style="color:#475569;font-size:9px">Loading prices...</span>
</div>

<div class="tabs">
  <div class="tab active" onclick="setTab('regime')">Regime</div>
  <div class="tab" onclick="setTab('signals')">Signals <span id="sig-count"></span></div>
  <div class="tab" onclick="setTab('positions')">Positions <span id="pos-count"></span></div>
  <div class="tab" onclick="setTab('trades')">Trades</div>
  <div class="tab" onclick="setTab('stats')">Stats</div>
  <div class="tab" onclick="setTab('settings')">Settings</div>
  <div class="tab" onclick="setTab('connect')">Connect</div>
  <div class="tab" onclick="setTab('log')">Log</div>
</div>

<div class="content" id="content"></div>

<script>
` + SCRIPT_JS + `
</script>
</body>
</html>
`;

const server = http.createServer((req, res) => {
  if (req.url === '/api/state') {
    const openPos = positions.filter(p=>!p.closed);
    const wins = closedTrades.filter(t=>t.finalPnl>0).length;
    const totalPnl = closedTrades.reduce((s,t)=>s+(t.finalPnl||0),0);
    const statsByStrat = {};
    closedTrades.forEach(t=>{
      if(!statsByStrat[t.strategy]) statsByStrat[t.strategy]={pnl:0,trades:0,wins:0};
      statsByStrat[t.strategy].pnl += t.finalPnl||0;
      statsByStrat[t.strategy].trades++;
      if((t.finalPnl||0)>0) statsByStrat[t.strategy].wins++;
    });
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({
      mdConnected: mdWs && mdWs.readyState === WebSocket.OPEN,
      paperOnly: CFG.paperOnly,
      prices: livePrices,
      balance: CFG.balance,
      dailyPnl, dailyLoss, dailyTrades, streak,
      totalPnl,
      maxOpen: CFG.maxOpen,
      maxDailyTrades: CFG.maxDailyTrades,
      maxStreak: CFG.maxStreak,
      openCount: openPos.length,
      trades: closedTrades.length,
      wins,
      statsByStrat,
      positions: openPos,
      closedTrades: closedTrades.slice(0,50),
      signals,
      logs: logs.slice(0,100),
      lastScan,
      scanning: !!scanTimer,
      trading: !!(scanTimer && dailyLoss < CFG.balance*CFG.maxDailyLossPct && streak < CFG.maxStreak && dailyTrades < CFG.maxDailyTrades),
      session: getSession().name,
      marginUsed: openPos.reduce((s,p)=>{const sp=INSTRUMENTS[p.sym];return s+(sp?sp.margin*p.contracts:0);},0),
      contracts: CONTRACT_MAP,
      config: {
        scanSec: CFG.scanSec,
        minScore: CFG.minScore,
        riskPct: CFG.riskPct,
        maxDailyLossPct: CFG.maxDailyLossPct,
        maxOpen: CFG.maxOpen,
        maxDailyTrades: CFG.maxDailyTrades,
        maxStreak: CFG.maxStreak,
        paperOnly: CFG.paperOnly,
      },
      credentials: {
        TV_USERNAME: !!CFG.username,
        TV_CID: !!CFG.cid,
      }
    }));
    return;
  }

  if (req.url === '/api/config' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const cfg = JSON.parse(body);
        if (cfg.scanSec) CFG.scanSec = parseInt(cfg.scanSec);
        if (cfg.minScore !== undefined) CFG.minScore = parseInt(cfg.minScore);
        if (cfg.riskPct !== undefined) CFG.riskPct = parseFloat(cfg.riskPct) / 100;
        if (cfg.maxDailyLoss !== undefined) CFG.maxDailyLossPct = parseFloat(cfg.maxDailyLoss) / 100;
        if (cfg.maxOpen !== undefined) CFG.maxOpen = parseInt(cfg.maxOpen);
        if (cfg.maxDailyTrades !== undefined) CFG.maxDailyTrades = parseInt(cfg.maxDailyTrades);
        if (cfg.maxStreak !== undefined) CFG.maxStreak = parseInt(cfg.maxStreak);
        // Restart scan loop with new interval
        if (cfg.scanSec) {
          clearInterval(scanTimer);
          scanTimer = setInterval(() => { updatePositions(); scan(); }, CFG.scanSec * 1000);
        }
        log('system', `Config updated: scanSec=${CFG.scanSec} minScore=${CFG.minScore} riskPct=${(CFG.riskPct*100).toFixed(1)}%`);
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(JSON.stringify({ok:true}));
      } catch(e) {
        res.writeHead(400, {'Content-Type':'application/json'});
        res.end(JSON.stringify({error:e.message}));
      }
    });
    return;
  }

  if (req.url === '/health') {
    res.writeHead(200, {'Content-Type':'application/json'});
    res.end(JSON.stringify({status:'ok'}));
    return;
  }

  res.writeHead(200, {'Content-Type':'text/html'});
  res.end(DASHBOARD_HTML);
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

// Re-auth every 18 hours
setInterval(async () => {
  if (CFG.username && CFG.cid) {
    log('system', 'Refreshing auth token...');
    const ok = await authenticate();
    if (ok) connectMD();
  }
}, 18 * 60 * 60 * 1000);

start();
