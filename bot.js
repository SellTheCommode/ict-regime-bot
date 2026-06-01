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

// ─── EXTRA STATE ───────────────────────────────────────────────────────────
let sigLog = [];      // all signals (accepted + rejected)
let tradeMode = 'paper-auto';
let tradingBudget = null;
let statsByRegime = {};
let statsBySession = {};

// ─── HTTP DASHBOARD ────────────────────────────────────────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>ICT RegimeAI</title>
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://cdn.tailwindcss.com"></script>
<style>
* { box-sizing: border-box; }
body { margin:0; padding:0; background:#000; color:#fff; font-family:monospace; }
input[type=range]::-webkit-slider-thumb { cursor:pointer; }
.sb::-webkit-scrollbar { display:none; }
@keyframes pu { 0%,100%{opacity:1} 50%{opacity:0.3} }
.pu { animation:pu 1.4s ease-in-out infinite; }
@keyframes si { from{transform:translateY(-6px);opacity:0} to{transform:translateY(0);opacity:1} }
.si { animation:si 0.25s ease-out; }
</style>
</head>
<body>
<div id="root">
  <div style="position:fixed;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px;background:#000">
    <div style="color:#10b981;font-size:20px;font-weight:bold;letter-spacing:2px">ICT RegimeAI</div>
    <div style="color:#475569;font-size:11px">Loading...</div>
  </div>
</div>
<script>
(function(){
const {useState,useEffect,useRef,useCallback} = React;

// ── CONSTANTS ──────────────────────────────────────────────────────────────
const INSTRUMENTS = {
  MNQ:{name:"Micro Nasdaq",tv:2,ts:0.25,margin:40},
  MES:{name:"Micro S&P",tv:5,ts:0.25,margin:40},
  MYM:{name:"Micro Dow",tv:0.5,ts:1,margin:30},
  MCL:{name:"Micro Crude",tv:100,ts:0.01,margin:50},
  MGC:{name:"Micro Gold",tv:10,ts:0.1,margin:50},
};
const ICOL={MNQ:"#60a5fa",MES:"#10b981",MYM:"#f59e0b",MCL:"#f97316",MGC:"#fbbf24"};
const REGIME_COLOR={TREND:"#10b981",MEAN_REVERSION:"#22d3ee",EXPANSION:"#f59e0b",COMPRESSION:"#a78bfa",REVERSAL:"#f97316",NEWS_SPIKE:"#ef4444",DEAD_ZONE:"#475569",CHOP:"#64748b"};
const REGIME_DESC={TREND:"Clean directional move -- ICT + ORB active",MEAN_REVERSION:"Price pulling back to VWAP -- Scalp active",EXPANSION:"Volatility burst -- Compression Expansion active",COMPRESSION:"ATR squeeze -- waiting for breakout",REVERSAL:"Fake breakout detected -- ICT reversal setups only",NEWS_SPIKE:"Extreme volume spike -- all strategies paused",DEAD_ZONE:"Low ATR + low volume + tiny candles -- no quality setups available",CHOP:"Indecisive price action -- waiting for direction"};
const STRAT_COLORS={ICT_SMC:"#60a5fa",VWAP_SCALP:"#22d3ee",ORB_SWEEP:"#f59e0b",COMP_EXPAND:"#a78bfa"};
const MODES=[{k:"paper",l:"[P] Paper",sub:"watch only",c:"#38bdf8"},{k:"paper-auto",l:"[P][!] Paper Auto",sub:"auto fake money",c:"#22d3ee"},{k:"manual",l:"[M] Manual",sub:"approve each",c:"#f59e0b"},{k:"full-auto",l:"[!] Full Auto",sub:"live orders [!]",c:"#ef4444"}];

// ── UI HELPERS ─────────────────────────────────────────────────────────────
const Tag=({col,children,sm})=>React.createElement("span",{className:"font-mono font-bold rounded border "+(sm?"text-[7px] px-1 py-0":"text-[8px] px-1.5 py-0.5"),style:{color:col,borderColor:col+"55",backgroundColor:col+"15"}},children);

const SL=({label,val,min,max,step,fmt,set})=>React.createElement("div",{className:"flex items-center gap-2"},
  React.createElement("span",{className:"text-[8px] font-mono shrink-0",style:{color:"#64748b",width:140}},label),
  React.createElement("input",{type:"range",min,max,step,value:val,onChange:e=>set(+e.target.value),className:"flex-1 h-1 rounded-full bg-slate-700 accent-emerald-500"}),
  React.createElement("span",{className:"text-[8px] font-mono font-bold text-right shrink-0 text-white",style:{width:40}},fmt?fmt(val):val));

const Tog=({val,set,label})=>React.createElement("button",{onClick:()=>set(!val),className:"flex items-center gap-2 w-full text-left"},
  React.createElement("div",{className:"relative rounded-full shrink-0",style:{width:28,height:16,backgroundColor:val?"#10b981":"#334155"}},
    React.createElement("div",{className:"absolute top-0.5 w-3 h-3 bg-white rounded-full shadow transition-transform",style:{transform:val?"translateX(13px)":"translateX(2px)"}})),
  React.createElement("span",{className:"text-[8px] font-mono",style:{color:"#94a3b8"}},label));

const GBar=({v,max,label,col="#10b981"})=>{const p=Math.min(100,(v/Math.max(max,.01))*100);return React.createElement("div",null,
  React.createElement("div",{className:"flex justify-between text-[7px] font-mono mb-0.5",style:{color:"#64748b"}},
    React.createElement("span",null,label),
    React.createElement("span",null,typeof v==="number"&&typeof max==="number"?v.toFixed(1)+"/"+max.toFixed(1):"")),
  React.createElement("div",{className:"h-1.5 rounded-full overflow-hidden bg-slate-800"},
    React.createElement("div",{className:"h-full rounded-full transition-all duration-500",style:{width:p+"%",backgroundColor:p>80?"#ef4444":p>55?"#f59e0b":col}})));};

const RegimeBadge=({regime})=>{const col=REGIME_COLOR[regime]||"#64748b";return React.createElement("div",{className:"flex items-center gap-1.5 px-2 py-1 rounded-lg",style:{backgroundColor:col+"18",border:"1px solid "+col+"44"}},
  React.createElement("div",{className:"w-2 h-2 rounded-full",style:{backgroundColor:col}}),
  React.createElement("span",{className:"text-[9px] font-bold font-mono",style:{color:col}},regime.replace(/_/g," ")));};

const ScoreRing=({score})=>{const col=score>=80?"#10b981":score>=60?"#f59e0b":"#ef4444";return React.createElement("div",{style:{position:"relative",width:52,height:52,display:"flex",alignItems:"center",justifyContent:"center"}},
  React.createElement("svg",{width:"52",height:"52",viewBox:"0 0 52 52"},
    React.createElement("circle",{cx:"26",cy:"26",r:"22",fill:"none",stroke:"#1e293b",strokeWidth:"4"}),
    React.createElement("circle",{cx:"26",cy:"26",r:"22",fill:"none",stroke:col,strokeWidth:"4",strokeDasharray:score*1.38+" 138",strokeLinecap:"round",transform:"rotate(-90 26 26)"})),
  React.createElement("div",{style:{position:"absolute",color:col,fontSize:10,fontWeight:"bold",fontFamily:"monospace"}},score));};

// ── MAIN APP ───────────────────────────────────────────────────────────────
function App() {
  const [tab,setTab]=useState("regime");
  const [mode,setMode]=useState("paper-auto");
  const [state,setState]=useState({});
  const [cfg,setCfg]=useState({ictEnabled:true,scalpEnabled:true,orbEnabled:true,ceEnabled:true,scaleInEnabled:true,requireKillzone:false,riskPct:0.030,scalpRiskPct:0.015,maxStopPct:0.005,maxDailyLossPct:0.05,maxOpen:6,maxStreak:3,maxDailyTrades:15,dailyTarget:0.02,minScore:60,tp1R:1.0,tp2R:2.0,tp3R:3.5,trailAfterR:1.5,scanSec:20});
  const [bypassLoss,setBypassLoss]=useState(false);
  const [bypassMgn,setBypassMgn]=useState(false);
  const [bypassTrd,setBypassTrd]=useState(false);
  const [bypassStk,setBypassStk]=useState(false);
  const [creds,setCreds]=useState({username:"",password:"",cid:"",sec:""});
  const [alwaysOn,setAlwaysOn]=useState(false);
  const [tradingBudget,setTradingBudget]=useState(null);
  const [showBal,setShowBal]=useState(false);
  const [newBal,setNewBal]=useState("");
  const [showBudget,setShowBudget]=useState(false);
  const [newBudget,setNewBudget]=useState("");
  const [showFA,setShowFA]=useState(false);
  const [cfgChanged,setCfgChanged]=useState(false);
  const [logs,setLogs]=useState([]);

  const s = state;
  const bal = {available: s.balance||10000, total: s.balance||10000};
  const rs = {dailyLoss:s.dailyLoss||0, streak:s.streak||0, dailyTrades:s.dailyTrades||0, dailyPnl:s.dailyPnl||0, exp:s.marginUsed||0, open:(s.positions||[]).filter(p=>!p.closed).length};
  const active = (s.positions||[]).filter(p=>!p.closed);
  const stats = {trades:s.trades||0, wins:s.wins||0, pnl:s.totalPnl||0, byStrat:s.statsByStrat||{}, byRegime:s.statsByRegime||{}, bySess:s.statsBySession||{}};
  const signals = s.signals||[];
  const tradeLog = s.sigLog||[];
  const closedTrades = s.closedTrades||[];
  const regimes = s.regimes||{};
  const firstRegime = s.firstRegime||null;
  const curRegime = (s.lastScan&&s.lastScan.regimes?s.lastScan.regimes.split(" ")[0].split(":")[1]:"") || "UNKNOWN";
  const routes = s.routes||{};
  const session = {name:s.session||"--",color:"#94a3b8",prime:false};
  const livePrices = s.prices||{};
  const mdConnected = s.mdConnected||false;
  const isAuto = mode==="paper-auto"||mode==="full-auto";
  const isPaper = mode==="paper"||mode==="paper-auto";
  const scanning = s.scanning||false;
  const dailyTarget = bal.total * cfg.dailyTarget;
  const dailyPct = dailyTarget>0?((rs.dailyPnl||0)/dailyTarget*100):0;
  const winRate = stats.trades>0?(stats.wins/stats.trades*100).toFixed(0):"--";
  const totalPnl = active.reduce((sum,p)=>{const cur=livePrices[p.sym]||p.entry;const pts=p.action==="Buy"?cur-p.entry:p.entry-cur;const tv=(INSTRUMENTS[p.sym]||{tv:2}).tv;return sum+pts*tv*p.contracts;},0);

  // Fetch state every 2s
  useEffect(()=>{
    const fetch2 = async()=>{
      try{
        const r=await fetch("/api/state");
        const d=await r.json();
        setState(d);
        if(d.config){
          setCfg(c=>({...c,scanSec:d.config.scanSec||20,minScore:d.config.minScore||60,maxOpen:d.config.maxOpen||6,maxDailyTrades:d.config.maxDailyTrades||15,maxStreak:d.config.maxStreak||3}));
        }
        if(d.tradeMode) setMode(d.tradeMode);
      }catch(e){}
    };
    fetch2();
    const t=setInterval(fetch2,2000);
    return ()=>clearInterval(t);
  },[]);

  const saveCfg=(extra={})=>{
    fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...cfg,...extra,mode})}).catch(()=>{});
    setCfgChanged(false);
  };

  const su=(k,v)=>{setCfg(c=>({...c,[k]:v}));setCfgChanged(true);};

  const resetBalance=()=>{
    const v=parseFloat(newBal);
    if(!v||v<100)return;
    fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({resetBalance:v})}).catch(()=>{});
    setShowBal(false);setNewBal("");
  };

  const doConnect=async()=>{
    if(!creds.username||!creds.password||!creds.cid||!creds.sec)return;
    try{
      const r=await fetch("/api/connect",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(creds)});
      const d=await r.json();
      if(d.ok) setLogs(l=>[{type:"system",msg:"Connected to Tradovate",ts:new Date().toLocaleTimeString(),id:Math.random()},...l]);
      else setLogs(l=>[{type:"warn",msg:"Connect failed: "+(d.error||"unknown"),ts:new Date().toLocaleTimeString(),id:Math.random()},...l]);
    }catch(e){setLogs(l=>[{type:"warn",msg:"Connect error: "+e.message,ts:new Date().toLocaleTimeString(),id:Math.random()},...l]);}
  };

  const modeObj = MODES.find(b=>b.k===mode)||MODES[1];

  return React.createElement("div",{className:"min-h-screen text-slate-100",style:{backgroundColor:"#000000",fontFamily:"monospace"}},
    React.createElement("style",null,"@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&display=swap');"),

    // ── FULL AUTO CONFIRM ──
    showFA&&React.createElement("div",{className:"fixed inset-0 bg-black/85 backdrop-blur-sm z-50 flex items-center justify-center p-4"},
      React.createElement("div",{className:"rounded-2xl p-5 max-w-sm w-full space-y-3",style:{backgroundColor:"#0d1117",border:"1px solid #ef444455"}},
        React.createElement("div",{className:"text-red-400 font-bold font-mono text-sm"},"[!] FULL AUTO -- LIVE MONEY"),
        React.createElement("div",{className:"rounded-lg p-3 text-[9px] font-mono text-red-200 space-y-1",style:{backgroundColor:"#2c000040",border:"1px solid #ef444430"}},
          React.createElement("p",null,"- Real futures orders on Tradovate."),
          React.createElement("p",null,"- Regime engine + 4 strategies + confidence scoring all live."),
          React.createElement("p",null,"- Losses cannot be undone.")),
        React.createElement("input",{id:"fatxt",className:"w-full rounded-lg px-3 py-2 text-sm font-mono text-white focus:outline-none",placeholder:"Type: I ACCEPT THE RISK",style:{backgroundColor:"#1e293b",border:"1px solid #475569"}}),
        React.createElement("div",{className:"flex gap-2"},
          React.createElement("button",{onClick:()=>{if(document.getElementById("fatxt").value.toUpperCase()==="I ACCEPT THE RISK"){setMode("full-auto");setShowFA(false);saveCfg({mode:"full-auto"});}},className:"flex-1 font-bold font-mono py-2 rounded-lg text-xs text-white",style:{backgroundColor:"#ef4444"}},"ENABLE"),
          React.createElement("button",{onClick:()=>setShowFA(false),className:"flex-1 font-mono py-2 rounded-lg text-xs text-slate-300",style:{backgroundColor:"#1e293b"}},"CANCEL")))),

    // ── SET BALANCE MODAL ──
    showBal&&React.createElement("div",{className:"fixed inset-0 bg-black/85 backdrop-blur-sm z-50 flex items-center justify-center p-4"},
      React.createElement("div",{className:"rounded-2xl p-5 max-w-xs w-full space-y-3",style:{backgroundColor:"#0d1117",border:"1px solid #10b98155"}},
        React.createElement("div",{className:"font-bold font-mono text-sm",style:{color:"#10b981"}},"[$] Set Paper Balance"),
        React.createElement("div",{className:"text-[8px] font-mono",style:{color:"#94a3b8"}},"Resets all positions, P&L, and trade history."),
        React.createElement("input",{value:newBal,onChange:e=>setNewBal(e.target.value),type:"number",placeholder:"e.g. 25000",className:"w-full rounded-lg px-3 py-2 text-sm font-mono text-white focus:outline-none",style:{backgroundColor:"#1e293b",border:"1px solid "+(newBal?"#10b981":"#334155")}}),
        React.createElement("div",{className:"flex gap-2"},
          React.createElement("button",{onClick:resetBalance,className:"flex-1 font-bold font-mono py-2 rounded-lg text-sm",style:{backgroundColor:"#10b981",color:"white"}},"SET"),
          React.createElement("button",{onClick:()=>{setShowBal(false);setNewBal("");},className:"flex-1 font-mono py-2 rounded-lg text-xs text-slate-300",style:{backgroundColor:"#1e293b"}},"CANCEL")))),

    // ── BUDGET MODAL ──
    showBudget&&React.createElement("div",{className:"fixed inset-0 bg-black/85 backdrop-blur-sm z-50 flex items-center justify-center p-4"},
      React.createElement("div",{className:"rounded-2xl p-5 max-w-xs w-full space-y-3",style:{backgroundColor:"#0d1117",border:"1px solid #60a5fa55"}},
        React.createElement("div",{className:"font-bold font-mono text-sm",style:{color:"#60a5fa"}},"Trading Budget Cap"),
        React.createElement("div",{className:"text-[8px] font-mono",style:{color:"#94a3b8"}},"Set how much of your balance the app is allowed to trade with."),
        React.createElement("input",{value:newBudget,onChange:e=>setNewBudget(e.target.value),type:"number",placeholder:"e.g. 500",className:"w-full rounded-lg px-3 py-2 text-sm font-mono text-white focus:outline-none",style:{backgroundColor:"#1e293b",border:"1px solid "+(newBudget?"#60a5fa":"#334155")}}),
        React.createElement("div",{className:"flex gap-2"},
          React.createElement("button",{onClick:()=>{const v=parseFloat(newBudget);if(!v||v<10)return;setTradingBudget(v);setNewBudget("");setShowBudget(false);saveCfg({tradingBudget:v});},className:"flex-1 font-bold font-mono py-2 rounded-lg text-sm",style:{backgroundColor:"#60a5fa",color:"white"}},"SET BUDGET"),
          React.createElement("button",{onClick:()=>{setTradingBudget(null);setNewBudget("");setShowBudget(false);saveCfg({tradingBudget:null});},className:"flex-1 font-mono py-2 rounded-lg text-xs",style:{backgroundColor:"#1e293b",color:"#94a3b8"}},"REMOVE CAP")),
        React.createElement("button",{onClick:()=>{setShowBudget(false);setNewBudget("");},className:"w-full font-mono py-1.5 rounded-lg text-xs text-slate-500",style:{backgroundColor:"transparent"}},"CANCEL"))),

    // ── HEADER ─────────────────────────────────────────────────────────────
    React.createElement("div",{className:"border-b sticky top-0 z-40",style:{backgroundColor:"#000000",borderColor:"#1e293b"}},
      React.createElement("div",{className:"max-w-4xl mx-auto px-3 py-2 flex items-center justify-between"},
        React.createElement("div",{className:"flex items-center gap-2 flex-wrap"},
          React.createElement("span",{className:"text-sm font-bold"},
            React.createElement("span",{style:{color:"#10b981"}},"ICT"),
            React.createElement("span",{className:"font-light ml-1",style:{color:"#475569"}},"RegimeAI")),
          React.createElement("span",{className:"text-[8px] font-bold px-2 py-0.5 rounded border pu",style:{color:modeObj.c,borderColor:modeObj.c+"55",backgroundColor:modeObj.c+"12"}},
            isAuto&&scanning&&React.createElement("span",{className:"pu mr-1"},"*"),
            mode.toUpperCase().replace("-"," ")),
          curRegime!=="UNKNOWN"&&React.createElement(RegimeBadge,{regime:curRegime}),
          React.createElement("span",{className:"text-[7px] font-mono px-1.5 py-0.5 rounded",style:{color:session.color||"#94a3b8",backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},session.name)),
        React.createElement("div",{className:"flex items-center gap-2"},
          React.createElement("button",{onClick:()=>{setBypassLoss(v=>!v);saveCfg({bypassLoss:!bypassLoss});},className:"text-[8px] font-bold font-mono px-2 py-1 rounded-lg border transition-all",style:{color:bypassLoss?"#f59e0b":"#475569",borderColor:bypassLoss?"#f59e0b55":"#334155",backgroundColor:bypassLoss?"#f59e0b18":"transparent"}},bypassLoss?"LOSS OFF":"LOSS ON"),
          React.createElement("button",{onClick:()=>{setBypassMgn(v=>!v);saveCfg({bypassMgn:!bypassMgn});},className:"text-[8px] font-bold font-mono px-2 py-1 rounded-lg border transition-all",style:{color:bypassMgn?"#a78bfa":"#475569",borderColor:bypassMgn?"#a78bfa55":"#334155",backgroundColor:bypassMgn?"#a78bfa18":"transparent"}},bypassMgn?"MGN OFF":"MGN ON"),
          React.createElement("button",{onClick:()=>{setBypassTrd(v=>!v);saveCfg({bypassTrd:!bypassTrd});},className:"text-[8px] font-bold font-mono px-2 py-1 rounded-lg border transition-all",style:{color:bypassTrd?"#22d3ee":"#475569",borderColor:bypassTrd?"#22d3ee55":"#334155",backgroundColor:bypassTrd?"#22d3ee18":"transparent"}},bypassTrd?"TRD OFF":"TRD ON"),
          React.createElement("button",{onClick:()=>{setBypassStk(v=>!v);saveCfg({bypassStk:!bypassStk});},className:"text-[8px] font-bold font-mono px-2 py-1 rounded-lg border transition-all",style:{color:bypassStk?"#f97316":"#475569",borderColor:bypassStk?"#f9731655":"#334155",backgroundColor:bypassStk?"#f9731618":"transparent"}},bypassStk?"STK OFF":"STK ON"),
          React.createElement("div",{className:"text-right"},
            React.createElement("div",{className:"text-[7px]",style:{color:"#475569"}},"BAL"+(tradingBudget?" / BUDGET":"")),
            React.createElement("div",{className:"text-sm font-bold",style:{color:"#10b981"}},"$"+bal.available.toFixed(0),
              tradingBudget&&React.createElement("span",{style:{color:"#60a5fa",fontSize:10}}," / $"+tradingBudget.toLocaleString()))))),

      // prices bar
      React.createElement("div",{className:"border-t px-3 py-1 flex gap-3 overflow-x-auto sb",style:{borderColor:"#1e293b"}},
        Object.entries(livePrices).map(([sym,p])=>React.createElement("div",{key:sym,className:"flex items-center gap-1 shrink-0"},
          React.createElement("span",{className:"text-[7px] font-bold",style:{color:ICOL[sym]||"#94a3b8"}},sym),
          React.createElement("span",{className:"text-[7px]",style:{color:"#cbd5e1"}},p?(sym==="MCL"?p.toFixed(2):p.toFixed(0)):"--"),
          active.find(pos=>pos.sym===sym)&&React.createElement("span",{className:"text-[6px] ml-0.5",style:{color:STRAT_COLORS[active.find(pos=>pos.sym===sym)?.strategy]||"#14b8a6"}},"*"))),
        React.createElement("div",{className:"ml-auto shrink-0 flex items-center gap-1"},
          React.createElement("div",{className:"w-1.5 h-1.5 rounded-full",style:{backgroundColor:mdConnected?"#10b981":"#475569"}}),
          React.createElement("span",{className:"text-[7px] font-mono",style:{color:mdConnected?"#10b981":"#475569"}},mdConnected?"LIVE":"SIM")))),

    // ── TICKER BAR ─────────────────────────────────────────────────────────
    React.createElement("div",{className:"border-b px-3 py-1 text-center",style:{backgroundColor:"#1a0000",borderColor:"#ef444420"}},
      React.createElement("p",{className:"text-[7px] font-mono",style:{color:"#f87171aa"}},
        "4-strategy regime engine . ICT/SMC . VWAP Scalp . ORB Sweep . Compression Expansion . Confidence >=",cfg.minScore,"/100 required")),

    // ── TABS ───────────────────────────────────────────────────────────────
    React.createElement("div",{className:"max-w-4xl mx-auto px-3 pb-24"},
      React.createElement("div",{className:"flex gap-1 py-2 overflow-x-auto sb"},
        ["regime","signals","positions","trades","log_all","stats","settings","connect","syslog"].map(t=>
          React.createElement("button",{key:t,onClick:()=>setTab(t),className:"px-2.5 py-1.5 text-[9px] font-bold rounded-lg uppercase tracking-wider whitespace-nowrap transition-all",style:{backgroundColor:tab===t?"#10b981":"transparent",color:tab===t?"white":"#64748b"}},
            t==="log_all"?"sig log":t==="syslog"?"LOG":t,
            t==="signals"&&signals.length>0?" ["+signals.length+"]":"",
            t==="positions"&&active.length>0?" ("+active.length+")":"))),

      // ── REGIME TAB ─────────────────────────────────────────────────────
      tab==="regime"&&React.createElement("div",{className:"space-y-3"},

        // Mode selector
        React.createElement("div",{className:"rounded-xl p-3 space-y-2.5",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"grid grid-cols-2 gap-1.5"},
            MODES.map(({k,l,sub,c})=>React.createElement("button",{key:k,onClick:()=>{if(k==="full-auto"){setShowFA(true);}else{setMode(k);saveCfg({mode:k});}},className:"p-2 rounded-lg border text-left transition-all",style:{borderColor:mode===k?c:c+"22",backgroundColor:mode===k?c+"18":"transparent",color:mode===k?c:"#64748b"}},
              React.createElement("div",{className:"text-[10px] font-bold"},l),
              React.createElement("div",{className:"text-[8px] mt-0.5 opacity-70"},sub))))),

        // Regime + trade permission
        React.createElement("div",{className:"rounded-xl p-3 space-y-3",style:{backgroundColor:"#0a0a0a",border:"1px solid "+(REGIME_COLOR[curRegime]||"#1e293b")+"44"}},
          React.createElement("div",{className:"flex items-center justify-between gap-2 flex-wrap"},
            React.createElement("div",null,
              React.createElement("div",{className:"text-[7px] font-mono uppercase tracking-widest mb-1",style:{color:"#475569"}},"Current Market Regime"),
              React.createElement(RegimeBadge,{regime:curRegime})),
            React.createElement("div",{className:"text-right"},
              React.createElement("div",{className:"text-[7px] font-mono uppercase tracking-widest mb-1",style:{color:"#475569"}},"Trade Permission"),
              React.createElement("div",{className:"px-3 py-1 rounded-lg font-bold font-mono text-[10px]",style:{backgroundColor:s.trading?"#022c2220":"#2c000020",color:s.trading?"#10b981":"#ef4444",border:"1px solid "+(s.trading?"#10b98144":"#ef444444")}},s.trading?"OK TRADING ALLOWED":"X TRADING BLOCKED"))),
          React.createElement("div",{className:"text-[8px] font-mono",style:{color:"#64748b"}},REGIME_DESC[curRegime]||"Analyzing market..."),

          // Regime factors grid
          firstRegime&&React.createElement("div",{className:"grid grid-cols-2 gap-1.5 text-[7px] font-mono"},
            [["ATR expand",firstRegime.atrExpand!=null?(+firstRegime.atrExpand).toFixed(2)+"x":"--",+firstRegime.atrExpand>1.2?"#10b981":+firstRegime.atrExpand<0.70?"#ef4444":"#f59e0b"],
             ["Vol ratio",firstRegime.volRatio!=null?(+firstRegime.volRatio).toFixed(2)+"x":"--",+firstRegime.volRatio>1.3?"#10b981":+firstRegime.volRatio<0.55?"#ef4444":"#94a3b8"],
             ["EMA align",firstRegime.emaAlign||"--",firstRegime.emaAlign==="bullish"?"#10b981":firstRegime.emaAlign==="bearish"?"#ef4444":"#f59e0b"],
             ["Session",session.name+" (info)",session.color||"#94a3b8"],
            ].map(([l,v,c])=>React.createElement("div",{key:l,className:"flex items-center justify-between px-2 py-1 rounded",style:{backgroundColor:"#111827"}},
              React.createElement("span",{style:{color:"#475569"}},l),
              React.createElement("span",{className:"font-bold",style:{color:c}},v))))),

        // Strategy router
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Strategy Router"),
          Object.entries(routes).map(([k,v])=>React.createElement("div",{key:k,className:"flex items-start gap-2 py-1.5 px-2 rounded",style:{backgroundColor:v.active?"#022c2215":"#111827",border:"1px solid "+(v.active?"#10b98122":"#1e293b")}},
            React.createElement("div",{className:"mt-0.5 w-2 h-2 rounded-full shrink-0",style:{backgroundColor:v.active?"#10b981":"#334155"}}),
            React.createElement("div",{className:"min-w-0 flex-1"},
              React.createElement("div",{className:"text-[8px] font-bold font-mono",style:{color:v.active?STRAT_COLORS[k]||"#10b981":"#475569"}},k.replace(/_/g," ")),
              React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#64748b"}},v.reason||"")))),
          !Object.keys(routes).length&&React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#475569"}},"Analyzing...")),

        // Daily P&L target
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"flex justify-between items-center"},
            React.createElement("span",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Daily P&L Target"),
            React.createElement("span",{className:"text-[8px] font-mono font-bold",style:{color:rs.dailyPnl>=dailyTarget?"#10b981":rs.dailyPnl>0?"#f59e0b":"#ef4444"}},(rs.dailyPnl>=0?"+":"")+"$"+(rs.dailyPnl||0).toFixed(2)+" / $"+dailyTarget.toFixed(0))),
          React.createElement("div",{className:"h-2 rounded-full overflow-hidden bg-slate-800"},
            React.createElement("div",{className:"h-full rounded-full transition-all duration-500",style:{width:Math.min(100,Math.max(0,dailyPct))+"%",backgroundColor:dailyPct>=100?"#10b981":dailyPct>50?"#f59e0b":"#60a5fa"}})),
          React.createElement("div",{className:"grid grid-cols-4 gap-1 text-[7px] font-mono"},
            [["Bal","$"+bal.available.toFixed(0),"#10b981"],["W%",winRate+"%",parseFloat(winRate)>=50?"#10b981":"#f59e0b"],["Open",active.length+"/"+cfg.maxOpen,"#94a3b8"],["Trades",rs.dailyTrades+"/"+cfg.maxDailyTrades,"#94a3b8"]].map(([l,v,c])=>React.createElement("div",{key:l,className:"text-center py-1 rounded",style:{backgroundColor:"#111827"}},
              React.createElement("div",{style:{color:"#475569"}},l),
              React.createElement("div",{className:"font-bold",style:{color:c}},v))))),

        // Risk meters
        React.createElement("div",{className:"rounded-xl p-3 space-y-2.5",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Risk Meters"),
          React.createElement(GBar,{v:bypassLoss?0:rs.dailyLoss,max:bal.total*cfg.maxDailyLossPct,label:"Daily Loss ("+(cfg.maxDailyLossPct*100).toFixed(0)+"% cap = $"+(bal.total*cfg.maxDailyLossPct).toFixed(0)+")"+(bypassLoss?" [BYPASSED]":"")}),
          React.createElement(GBar,{v:bypassMgn?0:rs.exp,max:bal.total*0.12,label:"Margin Used (12% cap)"+(bypassMgn?" [BYPASSED]":""),col:"#a78bfa"}),
          React.createElement(GBar,{v:bypassTrd?0:rs.dailyTrades,max:cfg.maxDailyTrades,label:"Trades Today ("+cfg.maxDailyTrades+" cap)"+(bypassTrd?" [BYPASSED]":""),col:"#22d3ee"}),
          React.createElement("div",{className:"grid grid-cols-2 gap-1 text-[7px] font-mono"},
            [["Streak: "+rs.streak+"/"+cfg.maxStreak+(bypassStk?" [BYPASSED]":""),bypassStk?"#f97316":rs.streak>=cfg.maxStreak?"#ef4444":"#10b981"],["Score gate: "+cfg.minScore+"/100",signals.length>0&&signals[0].score>=cfg.minScore?"#10b981":"#f59e0b"]].map(([l,c])=>React.createElement("div",{key:l,className:"px-2 py-1 rounded text-center",style:{backgroundColor:"#111827",color:c}},l)))),

        // Instruments
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Instruments"),
          React.createElement("div",{className:"flex gap-1 flex-wrap"},
            Object.keys(INSTRUMENTS).map(sym=>React.createElement("span",{key:sym,className:"px-2 py-1 rounded border text-[8px] font-bold font-mono",style:{borderColor:ICOL[sym]+"88",color:ICOL[sym],backgroundColor:"#ffffff0a"}},sym)))),

        // Account compatibility
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Account Compatibility (Intraday Margins)"),
          React.createElement("div",{className:"space-y-1"},
            Object.entries(INSTRUMENTS).map(([sym,spec])=>{const ok=bal.available>=spec.margin;return React.createElement("div",{key:sym,className:"flex items-center justify-between px-2 py-1.5 rounded text-[7px] font-mono",style:{backgroundColor:ok?"#022c2215":"#2c000015",border:"1px solid "+(ok?"#10b98122":"#ef444422")}},
              React.createElement("div",{className:"flex items-center gap-2"},
                React.createElement("div",{className:"w-1.5 h-1.5 rounded-full",style:{backgroundColor:ok?"#10b981":"#ef4444"}}),
                React.createElement("span",{className:"font-bold",style:{color:ICOL[sym]}},sym),
                React.createElement("span",{style:{color:"#64748b"}},spec.name)),
              React.createElement("div",{className:"flex items-center gap-3 text-right"},
                React.createElement("span",{style:{color:"#64748b"}},"margin: ",React.createElement("span",{style:{color:"#94a3b8"}},"$"+spec.margin)),
                React.createElement("span",{style:{color:ok?"#10b981":"#ef4444"}},ok?"OK to trade":"Need $"+spec.margin)));})),
          React.createElement("div",{className:"text-[7px] font-mono pt-1",style:{color:"#475569"}},"Using Tradovate intraday margins (MNQ/MES ~$40-50). Your 3% risk budget: ",React.createElement("span",{style:{color:"white"}},"$"+(bal.available*cfg.riskPct).toFixed(0))," per trade.")),

        // Reset balance button
        React.createElement("button",{onClick:()=>setShowBal(true),className:"w-full font-bold font-mono py-2.5 rounded-xl text-[9px] border",style:{color:"#10b981",borderColor:"#10b98133",backgroundColor:"#022c2215"}},"[$] Reset / Set Paper Balance -> $"+bal.available.toFixed(0)),

        // Trading budget cap
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:tradingBudget?"#0f1a2a":"#0a0a0a",border:"1px solid "+(tradingBudget?"#60a5fa44":"#1e293b")}},
          React.createElement("div",{className:"flex items-center justify-between"},
            React.createElement("div",null,
              React.createElement("div",{className:"text-[9px] font-bold font-mono",style:{color:tradingBudget?"#60a5fa":"#94a3b8"}},"Trading Budget Cap"),
              React.createElement("div",{className:"text-[7px] font-mono mt-0.5",style:{color:"#64748b"}},tradingBudget?"App uses $"+tradingBudget.toLocaleString()+" -- $"+(bal.available-tradingBudget).toFixed(0)+" reserved":"No cap -- app uses full available balance")),
            React.createElement("button",{onClick:()=>setShowBudget(true),className:"text-[8px] font-bold font-mono px-2.5 py-1 rounded-lg border shrink-0",style:{color:"#60a5fa",borderColor:"#60a5fa44",backgroundColor:"#60a5fa12"}},"SET")))),

      // ── SIGNALS TAB ────────────────────────────────────────────────────
      tab==="signals"&&React.createElement("div",{className:"space-y-3"},
        React.createElement("div",{className:"flex items-center justify-between"},
          React.createElement("span",{className:"text-[9px] uppercase tracking-widest",style:{color:"#475569"}},"Signals ("+signals.length+")"),
          s.lastScan&&React.createElement("span",{className:"text-[7px] font-mono",style:{color:"#64748b"}},s.lastScan.time)),
        signals.length===0&&React.createElement("div",{className:"text-center py-12 space-y-2"},
          React.createElement("div",{className:"text-sm",style:{color:"#475569"}},scanning?"Scanning -- regime engine running":"Select Paper Auto on Regime tab"),
          React.createElement("div",{className:"text-[8px] font-mono",style:{color:"#64748b"}},"Regime: "+curRegime+" . Min score: "+cfg.minScore+"/100")),
        signals.map((sig,i)=>{
          const stCol=STRAT_COLORS[sig.strategy]||"#94a3b8";
          const isBull=sig.action==="Buy";
          const col=isBull?"#10b981":"#ef4444";
          const blocked=sig.score<cfg.minScore;
          return React.createElement("div",{key:i,className:"si rounded-xl p-3 space-y-2.5 border",style:{borderColor:blocked?"#33415588":col+"33",backgroundColor:blocked?"#0a0a0a":col+"08"}},
            React.createElement("div",{className:"flex items-start justify-between gap-2"},
              React.createElement("div",{className:"min-w-0 flex-1"},
                React.createElement("div",{className:"flex items-center gap-1.5 flex-wrap"},
                  React.createElement("span",{className:"text-[11px] font-bold font-mono",style:{color:ICOL[sig.sym]||"#94a3b8"}},sig.sym),
                  React.createElement(Tag,{col},isBull?"LONG":"SHORT"),
                  React.createElement(Tag,{col:stCol},sig.strategy.replace(/_/g," ")),
                  React.createElement(Tag,{col:REGIME_COLOR[sig.regime]||"#64748b",sm:true},sig.regime?.replace(/_/g," ")),
                  blocked&&React.createElement(Tag,{col:"#ef4444"},"BLOCKED")),
                React.createElement("div",{className:"text-[7px] mt-0.5",style:{color:"#64748b"}},"entry "+(sig.entry?.toFixed(1)||"--")+" . stop "+(sig.stop?.toFixed(1)||"--")+" . R:R "+(sig.expectedRR||2)+":1")),
              React.createElement("div",{style:{position:"relative",flexShrink:0}},React.createElement(ScoreRing,{score:sig.score||0}))),
            sig.scoreObj?.breakdown&&React.createElement("div",{className:"grid grid-cols-2 gap-1 text-[7px] font-mono"},
              Object.entries(sig.scoreObj.breakdown).map(([k,v])=>React.createElement("div",{key:k,className:"flex justify-between px-1.5 py-0.5 rounded",style:{backgroundColor:"#111827"}},
                React.createElement("span",{style:{color:"#475569"}},k),
                React.createElement("span",{style:{color:"#10b981"}},v)))),
            sig.checklist&&React.createElement("div",{className:"grid grid-cols-2 gap-1"},
              Object.entries(sig.checklist).map(([k,v])=>React.createElement("div",{key:k,className:"flex items-center gap-1 text-[7px] font-mono"},
                React.createElement("span",{style:{color:v.ok?"#10b981":"#334155"}},v.ok?"OK":"o"),
                React.createElement("span",{className:"truncate",style:{color:v.ok?"#94a3b8":"#475569"}},v.label)))),
            blocked&&React.createElement("div",{className:"text-center text-[7px] font-mono",style:{color:"#ef4444"}},"Score "+sig.score+" below minimum "+cfg.minScore+" -- not executed"),
            !blocked&&React.createElement("div",{className:"text-center text-[7px] font-mono",style:{color:"#22d3ee"}},"[!] Auto-executing -- score "+sig.score+"/"+cfg.minScore));})),

      // ── POSITIONS TAB ──────────────────────────────────────────────────
      tab==="positions"&&React.createElement("div",{className:"space-y-2"},
        React.createElement("div",{className:"flex items-center justify-between"},
          React.createElement("span",{className:"text-[9px] uppercase tracking-widest",style:{color:"#475569"}},"Open ("+active.length+")"),
          React.createElement("span",{className:"text-[9px] font-mono font-bold",style:{color:totalPnl>=0?"#10b981":"#ef4444"}},(totalPnl>=0?"+":"")+"$"+totalPnl.toFixed(2))),
        active.length===0&&React.createElement("div",{className:"text-center py-12 text-sm",style:{color:"#475569"}},scanning?"Regime engine scanning...":"Select Paper Auto on Regime tab"),
        active.map(p=>{
          const cur=livePrices[p.sym]||p.entry;
          const pnlPts=p.action==="Buy"?cur-p.entry:p.entry-cur;
          const tv=(INSTRUMENTS[p.sym]||{tv:2}).tv;
          const pnl=pnlPts*tv*p.contracts;
          const rVal=p.stopDist>0?pnlPts/p.stopDist:0;
          const green=pnl>=0;
          const stCol=STRAT_COLORS[p.strategy]||"#94a3b8";
          return React.createElement("div",{key:p.id,className:"si rounded-xl p-3 space-y-2 border",style:{borderColor:green?"#10b98133":"#ef444433",backgroundColor:green?"#022c2215":"#2c000015"}},
            React.createElement("div",{className:"flex items-start justify-between gap-2"},
              React.createElement("div",{className:"min-w-0 flex-1"},
                React.createElement("div",{className:"flex items-center gap-1.5 flex-wrap"},
                  React.createElement("span",{className:"text-[11px] font-bold font-mono",style:{color:ICOL[p.sym]}},p.sym),
                  React.createElement(Tag,{col:p.action==="Buy"?"#10b981":"#ef4444"},p.action==="Buy"?"LONG":"SHORT"),
                  React.createElement(Tag,{col:stCol},p.strategy.replace(/_/g," ")),
                  p.paper&&React.createElement(Tag,{col:"#22d3ee"},"PAPER"),
                  p.stopMoved&&React.createElement(Tag,{col:"#f59e0b"},"BE")),
                React.createElement("div",{className:"text-[7px] mt-0.5",style:{color:"#64748b"}},p.contracts+"c . entry "+(p.entry?.toFixed(1)||"--")+" . stop "+((p.curStop||p.stop)?.toFixed(1)||"--")+" . score "+p.score)),
              React.createElement("div",{className:"text-right shrink-0"},
                React.createElement("div",{className:"text-sm font-bold font-mono",style:{color:green?"#10b981":"#ef4444"}},(green?"+":"")+"$"+pnl.toFixed(2)),
                React.createElement("div",{className:"text-[7px]",style:{color:"#64748b"}},(rVal>=0?"+":"")+rVal.toFixed(2)+"R"))),
            React.createElement("div",{className:"grid grid-cols-5 gap-1 text-[7px] font-mono"},
              [["SL",(p.curStop||p.stop),"#ef4444"],["E",p.entry,"#94a3b8"],["TP1",p.tp1,p.tp1Closed?"#10b981":"#475569"],["TP2",p.tp2,p.tp2Closed?"#10b981":"#475569"],["TP3",p.tp3,"#475569"]].map(([l,v,c])=>React.createElement("div",{key:l,className:"text-center py-0.5 rounded bg-slate-800/60"},
                React.createElement("div",{style:{color:"#475569"}},l),
                React.createElement("div",{className:"font-bold",style:{color:c}},typeof v==="number"?v.toFixed(1):"-")))),
            React.createElement("div",{className:"h-1 rounded-full overflow-hidden bg-slate-800"},
              React.createElement("div",{className:"h-full rounded-full",style:{width:Math.min(100,Math.max(0,(rVal/4)*100))+"%",backgroundColor:green?"#10b981":"#ef4444"}})));})),

      // ── TRADES TAB ─────────────────────────────────────────────────────
      tab==="trades"&&React.createElement("div",{className:"space-y-1.5"},
        React.createElement("div",{className:"flex justify-between"},
          React.createElement("span",{className:"text-[9px] uppercase tracking-widest",style:{color:"#475569"}},"Closed ("+closedTrades.length+")"),
          React.createElement("span",{className:"text-[9px] font-mono font-bold",style:{color:stats.pnl>=0?"#10b981":"#ef4444"}},(stats.pnl>=0?"+":"")+"$"+stats.pnl.toFixed(2))),
        closedTrades.length===0&&React.createElement("div",{className:"text-center py-10 text-sm",style:{color:"#475569"}},"No closed trades yet"),
        closedTrades.map((t,i)=>React.createElement("div",{key:i,className:"rounded-lg px-2.5 py-2 flex items-center justify-between gap-2 text-[8px] font-mono",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"space-y-0.5 min-w-0"},
            React.createElement("div",{className:"flex items-center gap-1 flex-wrap"},
              React.createElement(Tag,{col:t.finalPnl>=0?"#10b981":"#ef4444"},t.closeReason||"CLOSE"),
              React.createElement(Tag,{col:STRAT_COLORS[t.strategy]||"#94a3b8"},t.strategy?.replace(/_/g," ")),
              t.paper&&React.createElement(Tag,{col:"#22d3ee"},"P")),
            React.createElement("div",null,
              React.createElement("span",{style:{color:ICOL[t.sym]}},t.sym)," ",
              React.createElement("span",{style:{color:"#94a3b8"}},t.contracts+"c . "+(t.entry?.toFixed(1)||"--")+"->"+(t.closePrice?.toFixed(1)||"--"))),
            React.createElement("div",{style:{color:"#475569"}},(t.ts||"")+" . "+(t.curR?.toFixed(2)||"0.00")+"R . MAE:"+(t.mae?.toFixed(2)||"--")+"R MFE:"+(t.mfe?.toFixed(2)||"--")+"R")),
          React.createElement("div",{className:"font-bold text-right shrink-0",style:{color:t.finalPnl>=0?"#10b981":"#ef4444"}},(t.finalPnl>=0?"+":"")+"$"+(t.finalPnl||0).toFixed(2))))),

      // ── SIG LOG TAB ────────────────────────────────────────────────────
      tab==="log_all"&&React.createElement("div",{className:"space-y-1.5"},
        React.createElement("div",{className:"flex justify-between items-center"},
          React.createElement("span",{className:"text-[9px] uppercase tracking-widest",style:{color:"#475569"}},"Signal Log ("+tradeLog.length+")"),
          React.createElement("span",{className:"text-[7px] font-mono",style:{color:"#64748b"}},"All signals -- accepted and rejected")),
        tradeLog.length===0&&React.createElement("div",{className:"text-center py-10 text-sm",style:{color:"#475569"}},"No signals logged yet"),
        tradeLog.slice(0,100).map((l,i)=>React.createElement("div",{key:l.id||i,className:"rounded-lg px-2 py-1.5 text-[7px] font-mono space-y-0.5",style:{backgroundColor:"#0a0a0a",border:"1px solid "+(l.status==="ACCEPTED"?"#10b98122":"#ef444422")}},
          React.createElement("div",{className:"flex items-center gap-1 flex-wrap"},
            React.createElement(Tag,{col:l.status==="ACCEPTED"?"#10b981":"#ef4444"},l.status||""),
            React.createElement(Tag,{col:STRAT_COLORS[l.strategy]||"#94a3b8"},l.strategy?.replace(/_/g," ")),
            React.createElement(Tag,{col:REGIME_COLOR[l.regime]||"#64748b",sm:true},l.regime?.replace(/_/g," ")),
            React.createElement("span",{style:{color:"#64748b"}},l.session||""),
            React.createElement("span",{className:"ml-auto font-bold",style:{color:l.score>=(s.config?.minScore||60)?"#10b981":"#ef4444"}},"score:"+l.score)),
          React.createElement("div",{className:"flex gap-2",style:{color:"#64748b"}},
            React.createElement("span",{style:{color:ICOL[l.sym]}},l.sym),
            React.createElement("span",null,(l.action||"")+" @ "+(l.entry?.toFixed(1)||"--")),
            React.createElement("span",null,"SL:"+(l.stop?.toFixed(1)||"--")),
            React.createElement("span",null,"TP:"+(l.tp2?.toFixed(1)||"--")),
            l.result&&React.createElement("span",{style:{color:l.result==="WIN"?"#10b981":"#ef4444"}},"->"+(l.result||"")+" "+(l.rMultiple?.toFixed(2)||"")+"R")),
          l.rejectReason&&React.createElement("div",{style:{color:"#ef4444"}},"X "+l.rejectReason)))),

      // ── STATS TAB ──────────────────────────────────────────────────────
      tab==="stats"&&React.createElement("div",{className:"space-y-3"},
        React.createElement("div",{className:"grid grid-cols-2 gap-2 text-[8px] font-mono"},
          [["Total P&L",(stats.pnl>=0?"+":"")+"$"+stats.pnl.toFixed(2),stats.pnl>=0?"#10b981":"#ef4444"],["Win Rate",winRate+"%",parseFloat(winRate)>=50?"#10b981":"#f59e0b"],["Trades",""+stats.trades,stats.trades>0?"#94a3b8":"#475569"],["Daily P&L",(rs.dailyPnl>=0?"+":"")+"$"+(rs.dailyPnl||0).toFixed(2),rs.dailyPnl>=0?"#10b981":"#ef4444"]].map(([l,v,c])=>React.createElement("div",{key:l,className:"rounded-lg p-2.5",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
            React.createElement("div",{style:{color:"#475569"}},l),
            React.createElement("div",{className:"font-bold text-sm",style:{color:c}},v)))),
        ["By Strategy","By Regime","By Session"].map((title,ti)=>{
          const data=[stats.byStrat,stats.byRegime,stats.bySess][ti]||{};
          const colors=[STRAT_COLORS,REGIME_COLOR,{}][ti];
          return React.createElement("div",{key:title,className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
            React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},title),
            Object.entries(data).map(([k,v])=>React.createElement("div",{key:k,className:"flex items-center justify-between text-[8px] font-mono px-2 py-1.5 rounded",style:{backgroundColor:"#111827"}},
              React.createElement("div",null,
                React.createElement("span",{className:"font-bold",style:{color:colors[k]||"#94a3b8"}},k.replace(/_/g," ")),
                React.createElement("span",{className:"ml-2",style:{color:"#64748b"}},v.trades+" trades")),
              React.createElement("span",{className:"font-bold",style:{color:v.pnl>=0?"#10b981":"#ef4444"}},(v.pnl>=0?"+":"")+"$"+v.pnl.toFixed(2)))),
            !Object.keys(data).length&&React.createElement("div",{className:"text-[8px] font-mono",style:{color:"#475569"}},"No trades yet"));})),

      // ── SETTINGS TAB ───────────────────────────────────────────────────
      tab==="settings"&&React.createElement("div",{className:"space-y-3"},
        React.createElement("div",{className:"rounded-xl p-2.5 text-[8px] font-mono",style:{backgroundColor:"#140d00",border:"1px solid #f59e0b44",color:"#fbbf24aa"}},
          React.createElement("div",{className:"font-bold mb-1",style:{color:"#fde68a"}},"Sizing: ICT $"+(bal.available*cfg.riskPct).toFixed(0)+"/trade . Scalp $"+(bal.available*cfg.scalpRiskPct).toFixed(0)+"/trade . Stop cap "+(cfg.maxStopPct*100).toFixed(1)+"% of price . Score gate "+cfg.minScore+"/100")),
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Strategy Toggles"),
          React.createElement(Tog,{val:cfg.ictEnabled,set:v=>su("ictEnabled",v),label:"ICT/SMC -- order blocks, FVG, liquidity sweeps"}),
          React.createElement(Tog,{val:cfg.scalpEnabled,set:v=>su("scalpEnabled",v),label:"VWAP Scalp -- mean reversion, EMA, momentum"}),
          React.createElement(Tog,{val:cfg.orbEnabled,set:v=>su("orbEnabled",v),label:"ORB Sweep -- NY session opening range breakout"}),
          React.createElement(Tog,{val:cfg.ceEnabled,set:v=>su("ceEnabled",v),label:"Compression Expansion -- volatility squeeze breakout"}),
          React.createElement(Tog,{val:cfg.scaleInEnabled,set:v=>su("scaleInEnabled",v),label:"Scale-in on winning ICT positions"}),
          React.createElement(Tog,{val:cfg.requireKillzone,set:v=>su("requireKillzone",v),label:"Require killzone for all entries"})),
        [
          {title:"Risk & Limits",rows:[
            {l:"ICT risk %",v:(cfg.riskPct*100).toFixed(1),mn:0.5,mx:10,st:0.5,fmt:v=>v+"%",set:v=>su("riskPct",v/100)},
            {l:"Scalp risk %",v:(cfg.scalpRiskPct*100).toFixed(1),mn:0.25,mx:5,st:0.25,fmt:v=>v+"%",set:v=>su("scalpRiskPct",v/100)},
            {l:"Max stop % of price",v:(cfg.maxStopPct*100).toFixed(2),mn:0.1,mx:3,st:0.1,fmt:v=>v+"%",set:v=>su("maxStopPct",v/100)},
            {l:"Max daily loss",v:(cfg.maxDailyLossPct*100).toFixed(0),mn:1,mx:10,st:0.5,fmt:v=>v+"%",set:v=>su("maxDailyLossPct",v/100)},
            {l:"Daily target %",v:(cfg.dailyTarget*100).toFixed(0),mn:0.5,mx:10,st:0.5,fmt:v=>v+"%",set:v=>su("dailyTarget",v/100)},
            {l:"Max open trades",v:cfg.maxOpen,mn:1,mx:10,st:1,fmt:v=>v,set:v=>su("maxOpen",v)},
            {l:"Max daily trades",v:cfg.maxDailyTrades,mn:1,mx:30,st:1,fmt:v=>v,set:v=>su("maxDailyTrades",v)},
            {l:"Max streak",v:cfg.maxStreak,mn:2,mx:6,st:1,fmt:v=>v,set:v=>su("maxStreak",v)},
          ]},
          {title:"Entry Quality",rows:[
            {l:"Min score (0-100)",v:cfg.minScore,mn:40,mx:95,st:5,fmt:v=>v,set:v=>su("minScore",v)},
          ]},
          {title:"Exit Targets (R multiples)",rows:[
            {l:"TP1 -- 50% close",v:cfg.tp1R,mn:0.5,mx:2,st:0.25,fmt:v=>"+"+v+"R",set:v=>su("tp1R",v)},
            {l:"TP2 -- 30% close",v:cfg.tp2R,mn:1,mx:5,st:0.25,fmt:v=>"+"+v+"R",set:v=>su("tp2R",v)},
            {l:"TP3 -- remainder",v:cfg.tp3R,mn:2,mx:8,st:0.5,fmt:v=>"+"+v+"R",set:v=>su("tp3R",v)},
            {l:"Trail starts",v:cfg.trailAfterR,mn:0.75,mx:3,st:0.25,fmt:v=>"+"+v+"R",set:v=>su("trailAfterR",v)},
          ]},
          {title:"Automation",rows:[
            {l:"Scan interval",v:cfg.scanSec,mn:10,mx:120,st:5,fmt:v=>v+"s",set:v=>su("scanSec",v)},
          ]},
        ].map(({title,rows})=>React.createElement("div",{key:title,className:"rounded-xl p-3 space-y-3",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},title),
          rows.map(r=>React.createElement(SL,{key:r.l,label:r.l,val:r.v,min:r.mn,max:r.mx,step:r.st,fmt:r.fmt,set:r.set})))),
        cfgChanged&&React.createElement("button",{onClick:()=>saveCfg(),className:"w-full font-bold font-mono py-2.5 rounded-xl text-sm",style:{backgroundColor:"#10b981",color:"white"}},"SAVE SETTINGS"),
        React.createElement("button",{onClick:()=>setShowBal(true),className:"w-full font-mono py-2 rounded-xl text-[9px] border",style:{color:"#10b981",borderColor:"#10b98133",backgroundColor:"#022c2215"}},"[$] Reset / Set Paper Balance -> $"+bal.available.toFixed(0))),

      // ── CONNECT TAB ────────────────────────────────────────────────────
      tab==="connect"&&React.createElement("div",{className:"space-y-3"},
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid "+(mdConnected?"#10b98144":"#1e293b")}},
          React.createElement("div",{className:"flex items-center justify-between"},
            React.createElement("div",{className:"flex items-center gap-2"},
              React.createElement("div",{className:"w-2.5 h-2.5 rounded-full",style:{backgroundColor:mdConnected?"#10b981":"#475569"}}),
              React.createElement("span",{className:"text-[10px] font-bold font-mono",style:{color:mdConnected?"#10b981":"#64748b"}},mdConnected?"LIVE FEED ACTIVE":"SIMULATED DATA"))),
          React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#64748b"}},mdConnected?"Real-time Tradovate tick data -- bars aggregating from live quotes":"Simulated bars -- connect below to switch to live Tradovate market data")),
        !mdConnected&&React.createElement("div",{className:"rounded-xl p-3 space-y-1",style:{backgroundColor:"#0a0f0a",border:"1px solid #10b98133"}},
          React.createElement("div",{className:"text-[9px] font-bold font-mono",style:{color:"#10b981"}},"Paper Auto works right now without credentials"),
          React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#94a3b8"}},"Credentials only needed to receive live Tradovate market data and place real orders.")),
        React.createElement("div",{className:"rounded-xl p-3 space-y-3",style:{backgroundColor:"#0a0a0a",border:"1px solid #f59e0b44"}},
          React.createElement("div",{className:"text-[9px] font-bold uppercase tracking-widest",style:{color:"#f59e0b"}},"Tradovate Credentials"),
          React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#fde68a"}},"Get CID + SEC: Tradovate app -> Settings -> API Access -> Generate Key"),
          [["username","Username / Email","text","trader@example.com"],["password","Password","password","**********"],["cid","CID (integer)","text","12345"],["sec","Secret (UUID)","password","xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"]].map(([k,l,t,ph])=>React.createElement("div",{key:k,className:"space-y-0.5"},
            React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#64748b"}},l),
            React.createElement("input",{value:creds[k]||"",onChange:e=>setCreds(c=>({...c,[k]:e.target.value})),type:t,placeholder:ph,className:"w-full rounded-lg px-2.5 py-1.5 text-[8px] font-mono text-white focus:outline-none",style:{backgroundColor:"#1e293b",border:"1px solid "+(creds[k]?"#10b981":"#334155")}}))),
          React.createElement("button",{onClick:doConnect,className:"w-full font-bold font-mono py-2.5 rounded-lg text-[9px]",style:{backgroundColor:mdConnected?"#064e3b":"#10b981",color:"white"}},mdConnected?"RECONNECT TO TRADOVATE":"CONNECT -- GET LIVE DATA + ENABLE REAL ORDERS"),
          mdConnected&&React.createElement("div",{className:"text-center text-[7px] font-mono",style:{color:"#10b981"}},"REST connected . MD feed: live")),
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid #1e293b"}},
          React.createElement("div",{className:"text-[8px] uppercase tracking-widest",style:{color:"#475569"}},"Active Contract Map (update quarterly on roll)"),
          React.createElement("div",{className:"space-y-1"},
            Object.entries(s.contracts||{MNQ:"MNQM6",MES:"MESM6",MYM:"MYMM6",MCL:"MCLN6",MGC:"MGCM6"}).map(([sym,contract])=>React.createElement("div",{key:sym,className:"flex justify-between text-[7px] font-mono px-2 py-1 rounded",style:{backgroundColor:"#111827"}},
              React.createElement("span",{style:{color:ICOL[sym]||"#94a3b8"}},sym),
              React.createElement("span",{style:{color:mdConnected?"#10b981":"#64748b"}},contract),
              React.createElement("span",{style:{color:mdConnected?"#10b981":"#475569"}},mdConnected?"STREAMING":"--")))),
          React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#475569"}},"Current front month: June 2026 (M6). Next roll to September (U6) around June 20 2026. Contracts roll 4x/year (Mar/Jun/Sep/Dec)."))),

      // ── LOG TAB ────────────────────────────────────────────────────────
      tab==="syslog"&&React.createElement("div",{className:"space-y-2"},
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:"#0a0a0a",border:"1px solid "+(scanning?"#10b98144":"#1e293b")}},
          React.createElement("div",{className:"flex items-center gap-2"},
            React.createElement("div",{className:"w-2.5 h-2.5 rounded-full",style:{backgroundColor:scanning?"#10b981":"#334155"}}),
            React.createElement("span",{className:"text-[10px] font-bold font-mono",style:{color:scanning?"#10b981":"#64748b"}},scanning?"SCANNING LIVE":"SCANNER OFF"),
            scanning&&React.createElement("span",{className:"text-[7px] font-mono ml-auto",style:{color:"#475569"}},"every "+cfg.scanSec+"s")),
          s.lastScan&&React.createElement("div",{className:"space-y-1 text-[7px] font-mono"},
            React.createElement("div",{style:{color:"#94a3b8"}},"Last: ",React.createElement("span",{style:{color:"white"}},s.lastScan.time)," -- Regime: ",React.createElement("span",{style:{color:"#f59e0b"}},s.lastScan.regimes||"--")," -- Session: ",React.createElement("span",{style:{color:"#94a3b8"}},s.session||"--")),
            React.createElement("div",{style:{color:"#94a3b8"}},"Signals: ",React.createElement("span",{style:{color:signals.length>0?"#10b981":"#f59e0b"}},signals.length)," -- Open: ",React.createElement("span",{style:{color:"white"}},active.length+"/"+cfg.maxOpen))),
          !s.lastScan&&React.createElement("div",{className:"text-[7px] font-mono",style:{color:"#64748b"}},"Select Paper Auto on Regime tab to start")),
        React.createElement("div",{className:"rounded-xl p-3 space-y-2",style:{backgroundColor:alwaysOn?"#0f2a18":"#0a0a0a",border:"1px solid "+(alwaysOn?"#10b98144":"#1e293b")}},
          React.createElement("div",{className:"flex items-center justify-between"},
            React.createElement("div",null,
              React.createElement("div",{className:"text-[9px] font-bold font-mono",style:{color:alwaysOn?"#10b981":"#94a3b8"}},"[24/7] Trade All Sessions"),
              React.createElement("div",{className:"text-[7px] font-mono mt-0.5",style:{color:"#64748b"}},alwaysOn?"ON -- lunch, dead zone, overnight all active":"OFF -- lunch and dead zones blocked")),
            React.createElement("button",{onClick:()=>{setAlwaysOn(v=>!v);saveCfg({alwaysOn:!alwaysOn});},className:"rounded-full transition-colors shrink-0",style:{width:36,height:20,backgroundColor:alwaysOn?"#10b981":"#334155",position:"relative"}},
              React.createElement("div",{className:"absolute top-1 w-3.5 h-3.5 bg-white rounded-full shadow transition-transform",style:{transform:alwaysOn?"translateX(17px)":"translateX(3px)"}}))),
        React.createElement("div",{className:"flex items-center justify-between"},
          React.createElement("span",{className:"text-[9px] uppercase tracking-widest",style:{color:"#475569"}},"Events ("+(s.logs||[]).length+")"),
          React.createElement("button",{onClick:()=>fetch("/api/clearlog",{method:"POST"}),className:"text-[7px] font-mono",style:{color:"#475569"}},"CLEAR")),
        (s.logs||[]).length===0&&React.createElement("div",{className:"text-center py-8 text-sm",style:{color:"#475569"}},"No events yet -- start Paper Auto on Regime tab"),
        (s.logs||[]).map((e,i)=>React.createElement("div",{key:e.id||i,className:"flex gap-2 text-[7px] font-mono py-0.5 border-b",style:{borderColor:"#0f172a"}},
          React.createElement("span",{className:"shrink-0",style:{color:"#334155"}},e.ts?.slice(11,19)||""),
          React.createElement("span",{style:{color:{scan:"#60a5fa",buy:"#10b981",si:"#14b8a6",exit:"#a855f7",warn:"#ef4444",system:"#f59e0b"}[e.type]||"#94a3b8"}},e.msg))))));
}

ReactDOM.createRoot(document.getElementById("root")).render(React.createElement(App));
})();
</script>
</body>
</html>`;

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
      const reg = t.regime||'UNKNOWN';
      if(!statsByRegime[reg]) statsByRegime[reg]={pnl:0,trades:0};
      statsByRegime[reg].pnl += t.finalPnl||0;
      statsByRegime[reg].trades++;
      const sess = t.session||'Other';
      if(!statsBySession[sess]) statsBySession[sess]={pnl:0,trades:0};
      statsBySession[sess].pnl += t.finalPnl||0;
      statsBySession[sess].trades++;
    });
    // Build routes from current signals
    const curRoutes = {};
    if (signals.length) {
      const strats = ['ICT_SMC','VWAP_SCALP','ORB_SWEEP','COMP_EXPAND'];
      strats.forEach(st=>{
        const hasSig = signals.some(s=>s.strategy===st);
        curRoutes[st]={active:hasSig,reason:hasSig?'Active -- signal generated':'No signal this scan'};
      });
    }
    // First regime factors
    let firstRegime = null;
    if (lastScan && lastScan.regimes) {
      const parts = lastScan.regimes.split(' ')[0].split(':');
      if (parts.length===2) {
        const regime = parts[1];
        firstRegime = { regime, atrExpand: null, volRatio: null, emaAlign: null };
      }
    }
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({
      mdConnected: mdWs && mdWs.readyState === WebSocket.OPEN,
      tradeMode,
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
      statsByRegime,
      statsBySession,
      positions: openPos,
      closedTrades: closedTrades.slice(0,50),
      signals,
      sigLog: sigLog.slice(0,200),
      logs: logs.slice(0,100),
      lastScan,
      firstRegime,
      routes: curRoutes,
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
      credentials: { TV_USERNAME: !!CFG.username, TV_CID: !!CFG.cid },
    }));
    return;
  }

  if (req.url === '/api/clearlog' && req.method === 'POST') {
    logs = [];
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ok:true}));
    return;
  }

  if (req.url === '/api/connect' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const c = JSON.parse(body);
        if(c.username) CFG.username = c.username;
        if(c.password) CFG.password = c.password;
        if(c.cid) CFG.cid = parseInt(c.cid);
        if(c.sec) CFG.sec = c.sec;
        log('system', 'Credentials updated via dashboard -- reconnecting...');
        const ok = await authenticate();
        if(ok) connectMD();
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(JSON.stringify({ok}));
      } catch(e) {
        res.writeHead(400); res.end(JSON.stringify({error:e.message}));
      }
    });
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
        if (cfg.riskPct !== undefined) CFG.riskPct = parseFloat(cfg.riskPct);
        if (cfg.maxDailyLossPct !== undefined) CFG.maxDailyLossPct = parseFloat(cfg.maxDailyLossPct);
        if (cfg.maxDailyLoss !== undefined) CFG.maxDailyLossPct = parseFloat(cfg.maxDailyLoss)/100;
        if (cfg.maxOpen !== undefined) CFG.maxOpen = parseInt(cfg.maxOpen);
        if (cfg.maxDailyTrades !== undefined) CFG.maxDailyTrades = parseInt(cfg.maxDailyTrades);
        if (cfg.maxStreak !== undefined) CFG.maxStreak = parseInt(cfg.maxStreak);
        if (cfg.tradeMode !== undefined) tradeMode = cfg.tradeMode;
        if (cfg.resetBalance !== undefined) { CFG.balance = parseFloat(cfg.resetBalance); dailyPnl=0; dailyLoss=0; dailyTrades=0; streak=0; positions=[]; closedTrades=[]; log('system', `Balance reset to $${CFG.balance}`); }
        if (cfg.scanSec) {
          clearInterval(scanTimer);
          scanTimer = setInterval(() => { updatePositions(); scan(); }, CFG.scanSec * 1000);
        }
        log('system', `Config updated`);
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
