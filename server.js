const express = require('express'), http = require('http'), crypto = require('crypto');
const WebSocket = require('ws'), { Pool } = require('pg'), webpush = require('web-push');
const { FINNHUB_KEY, APP_PASS, DATABASE_URL, PORT = 3000 } = process.env;
const APP_USER = process.env.APP_USER || 'mango', SECRET = process.env.SESSION_SECRET || APP_PASS;
if (!APP_PASS || !DATABASE_URL) { console.error('Missing APP_PASS or DATABASE_URL'); process.exit(1); }

let cb = null, fh = null;
const pool = new Pool({ connectionString: DATABASE_URL, ssl: DATABASE_URL.includes('railway.internal') ? false : { rejectUnauthorized: false } });
const STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'TSLA', 'AMZN', 'LMT', 'NOC', 'WMT'];
const CRYPTO = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA'];
const prices = {}, lastSent = {}, firing = new Set();
const DEF = { autoSL: 0, autoTP: 0, defaultBuy: 0, startBalance: 10000, confirmBuy: false, confirmSell: true, pushAlerts: true, pushOrders: true, hideBalances: false, defaultRange: '1D', defaultChart: 'Candles' };
let settings = { ...DEF };
let open = {}, alerts = {}, divOn = true;

// ---------- auth ----------
const sign = e => e + '.' + crypto.createHmac('sha256', SECRET).update(String(e)).digest('hex');
const valid = t => { const e = String(t || '').split('.')[0]; return !!e && sign(e) === t && +e > Date.now(); };
const auth = (q, r, n) => valid((q.headers.authorization || '').replace('Bearer ', '')) ? n() : r.status(401).json({ error: 'auth' });
const W = f => (q, r) => f(q, r).catch(e => r.status(400).json({ error: e.message }));

// ---------- database ----------
async function init() {
  await pool.query(`
    create table if not exists acct(id int primary key, cash numeric not null);
    insert into acct values(1,10000) on conflict do nothing;
    create table if not exists pos(sym text primary key, qty numeric not null, avg numeric not null);
    create table if not exists ord(id serial primary key, sym text, kind text, price numeric);
    create table if not exists trd(id serial primary key, ts timestamptz default now(), sym text, side text, qty numeric, price numeric, why text, pnl numeric);
    create table if not exists snap(ts timestamptz default now(), eq numeric);
    create table if not exists extra(sym text primary key, kind text);
    create table if not exists watch(sym text primary key);
    create table if not exists alert(id serial primary key, sym text, dir text, price numeric, fired boolean default false, ts timestamptz default now());
    create table if not exists divpaid(sym text, ex text, primary key(sym, ex));
    create table if not exists kv(k text primary key, v text);
    create table if not exists push(endpoint text primary key, sub text);
    alter table ord add column if not exists pct numeric default 100;
    alter table ord add column if not exists trail numeric;
    alter table ord add column if not exists peak numeric;
    alter table alert add column if not exists label text;
    alter table trd add column if not exists note text;
    alter table pos add column if not exists since timestamptz default now();`);
}
async function loadOrders() {
  open = {};
  for (const o of (await pool.query('select * from ord')).rows) { o.pct = +o.pct || 100; o.trail = +o.trail; o.peak = Math.max(+o.peak || 0, prices[o.sym]?.p || 0); (open[o.sym] ??= []).push(o); }
}

// ---------- trading ----------
async function trade(sym, side, qty, why) {
  const px = prices[sym]?.p;
  if (!px) throw Error('No live price for ' + sym);
  const c = await pool.connect();
  try {
    await c.query('begin');
    const cash = +(await c.query('select cash from acct where id=1 for update')).rows[0].cash;
    const pos = (await c.query('select * from pos where sym=$1 for update', [sym])).rows[0];
    let pnl = null;
    if (side === 'buy') {
      const cost = qty * px;
      if (cost > cash + 0.01) throw Error('Not enough cash');
      const spend = Math.min(cost, cash), nq = (pos ? +pos.qty : 0) + qty;
      const avg = pos ? (pos.qty * pos.avg + spend) / nq : spend / qty;
      await c.query('update acct set cash=cash-$1 where id=1', [spend]);
      await c.query('insert into pos values($1,$2,$3) on conflict(sym) do update set qty=$2, avg=$3', [sym, nq, avg]);
    } else {
      if (!pos || qty > +pos.qty + 1e-9) throw Error('Not enough to sell');
      qty = Math.min(qty, +pos.qty);
      pnl = (px - pos.avg) * qty;
      await c.query('update acct set cash=cash+$1 where id=1', [qty * px]);
      const left = pos.qty - qty;
      if (left < 1e-9) {
        await c.query('delete from pos where sym=$1', [sym]);
        await c.query('delete from ord where sym=$1', [sym]);
      } else await c.query('update pos set qty=$1 where sym=$2', [left, sym]);
    }
    await c.query('insert into trd(sym,side,qty,price,why,pnl) values($1,$2,$3,$4,$5,$6)', [sym, side, qty, px, why || 'manual', pnl]);
    await c.query('commit');
  } catch (e) { await c.query('rollback'); throw e; } finally { c.release(); }
  await loadOrders();
}

// stop loss / take profit engine: runs on every price tick, server side
async function check(s, p) {
  const list = open[s];
  if (!list || firing.has(s)) return;
  for (const o of list) if (o.kind === 'trail' && p > o.peak) o.peak = p;
  const hit = list.find(o => o.kind === 'trail' ? p <= o.peak * (1 - o.trail / 100) : (o.kind === 'stop' && p <= +o.price) || (o.kind === 'take' && p >= +o.price));
  if (!hit) return;
  firing.add(s);
  try {
    const r = (await pool.query('select qty from pos where sym=$1', [s])).rows[0];
    const label = hit.kind === 'take' ? 'Take profit' : hit.kind === 'trail' ? 'Trailing stop' : 'Stop loss', pct = hit.pct;
    if (r) await trade(s, 'sell', pct >= 100 ? +r.qty : +(r.qty * pct / 100).toFixed(8), label.toLowerCase());
    if (r) sendPush(label + ' hit', `Sold ${pct >= 100 ? 'all' : pct + '% of'} ${s} at $${prices[s].p.toFixed(prices[s].p < 1 ? 5 : 2)}`, 'order');
    if (pct >= 100) await pool.query('delete from ord where sym=$1', [s]); else await pool.query('delete from ord where id=$1', [hit.id]);
    await loadOrders();
  } catch (e) { console.error('order error', e.message); } finally { firing.delete(s); }
}

// ---------- live prices ----------
const app = express(), server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/stream' });
wss.on('connection', (c, q) => { if (!valid(new URL(q.url, 'http://x').searchParams.get('t'))) c.close(); });

function tick(s, p, prev, fromWs) {
  const o = prices[s] || {};
  prices[s] = { p, prev: prev || o.prev, t: Date.now(), wsT: fromWs ? Date.now() : o.wsT };
  check(s, p); alertCheck(s, p);
  const n = Date.now();
  if (n - (lastSent[s] || 0) > 250) {
    lastSent[s] = n;
    const m = JSON.stringify({ s, p, v: prices[s].prev });
    wss.clients.forEach(c => c.readyState === 1 && c.send(m));
  }
}
function coinbase() {
  const ws = cb = new WebSocket('wss://ws-feed.exchange.coinbase.com');
  ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', product_ids: CRYPTO.map(s => s + '-USD'), channels: ['ticker'] })));
  ws.on('message', m => { try { const d = JSON.parse(m); if (d.type === 'ticker' && d.price) tick(d.product_id.split('-')[0], +d.price, +d.open_24h, true); } catch (e) {} });
  ws.on('close', () => setTimeout(coinbase, 3000));
  ws.on('error', () => ws.close());
}
function finnhub() {
  if (!FINNHUB_KEY) return;
  const ws = fh = new WebSocket('wss://ws.finnhub.io?token=' + FINNHUB_KEY);
  ws.on('open', () => STOCKS.forEach(s => ws.send(JSON.stringify({ type: 'subscribe', symbol: s }))));
  ws.on('message', m => { try { const d = JSON.parse(m); if (d.type === 'trade') for (const t of d.data) tick(t.s, t.p, null, true); } catch (e) {} });
  ws.on('close', () => setTimeout(finnhub, 3000));
  ws.on('error', () => ws.close());
}
// REST quotes: previous close for % change, and last price when the market is closed
async function pollStocks() {
  if (!FINNHUB_KEY) return;
  for (const s of [...STOCKS]) {
    if (prices[s]?.wsT > Date.now() - 60000 && prices[s].prev) continue;
    try {
      const q = await (await fetch(`https://finnhub.io/api/v1/quote?symbol=${s}&token=${FINNHUB_KEY}`)).json();
      if (!q.c) continue;
      if (prices[s]?.wsT > Date.now() - 60000) prices[s].prev = q.pc; else tick(s, q.c, q.pc, false);
    } catch (e) {}
  }
}

// ---------- API ----------
app.use(express.json());
app.get('/', (q, r) => r.sendFile(__dirname + '/index.html'));
app.post('/api/login', (q, r) => {
  const { u, p } = q.body || {};
  if (u === APP_USER && p === APP_PASS) return r.json({ token: sign(Date.now() + 180 * 864e5) });
  setTimeout(() => r.status(401).json({ error: 'Wrong username or password' }), 800);
});

app.get('/api/state', auth, W(async (q, r) => {
  const Q = s => pool.query(s).then(x => x.rows);
  const [a, ps, os, ts, sn, ex, wl, al] = await Promise.all([
    Q('select cash from acct'), Q('select * from pos order by sym'), Q('select * from ord'),
    Q('select * from trd order by id desc limit 30'),
    Q("select eq from snap where ts>now()-interval '24 hours' order by ts"), Q('select * from extra'), Q('select * from watch'), Q('select * from alert order by id desc limit 40')]);
  r.json({
    cash: +a[0].cash,
    pos: ps.map(x => ({ sym: x.sym, qty: +x.qty, avg: +x.avg })),
    ord: os.map(x => ({ id: x.id, sym: x.sym, kind: x.kind, price: +x.price, pct: +x.pct, trail: x.trail == null ? null : +x.trail, peak: x.peak == null ? null : +x.peak })),
    trd: ts.map(x => ({ id: x.id, note: x.note, ts: x.ts, sym: x.sym, side: x.side, qty: +x.qty, price: +x.price, why: x.why, pnl: x.pnl == null ? null : +x.pnl })),
    snap: sn.map(x => +x.eq),
    prices: Object.fromEntries(Object.entries(prices).map(([k, v]) => [k, { p: v.p, prev: v.prev }])),
    watch: wl.map(x => x.sym), alerts: al.map(x => ({ id: x.id, sym: x.sym, dir: x.dir, price: +x.price, fired: x.fired, label: x.label })), divOn, settings,
    extra: ex.map(x => ({ sym: x.sym, kind: x.kind })),
    live: !!FINNHUB_KEY
  });
}));

app.post('/api/trade', auth, W(async (q, r) => {
  const { sym, side, usd, pct } = q.body;
  if (!prices[sym]) throw Error('No live price yet for ' + sym);
  let qty;
  if (side === 'buy') qty = +(usd / prices[sym].p).toFixed(8);
  else {
    const p = (await pool.query('select qty from pos where sym=$1', [sym])).rows[0];
    if (!p) throw Error('No position in ' + sym);
    qty = pct >= 100 ? +p.qty : +(p.qty * pct / 100).toFixed(8);
  }
  if (!(qty > 0)) throw Error('Enter a valid amount');
  await trade(sym, side, qty);
  if (side === 'buy') {
    const px = prices[sym].p;
    for (const [k, v, m] of [['stop', settings.autoSL, 1 - settings.autoSL / 100], ['take', settings.autoTP, 1 + settings.autoTP / 100]]) if (v > 0) {
      await pool.query('delete from ord where sym=$1 and kind=$2', [sym, k]);
      await pool.query('insert into ord(sym,kind,price,pct) values($1,$2,$3,100)', [sym, k, px * m]);
    }
    await loadOrders();
  }
  r.json({ ok: 1 });
}));

app.post('/api/order', auth, W(async (q, r) => {
  const { sym, kind, price, trail } = q.body, pct = Math.min(100, Math.max(1, +q.body.pct || 100));
  if (!['stop', 'take', 'trail'].includes(kind)) throw Error('Bad order type');
  if (!(await pool.query('select 1 from pos where sym=$1', [sym])).rows[0]) throw Error('Buy first, then set stop loss or take profit');
  const p = prices[sym]?.p;
  let trig = price;
  if (kind === 'trail') { if (!(trail >= 0.1 && trail <= 50)) throw Error('Trailing distance must be between 0.1% and 50%'); trig = p * (1 - trail / 100); }
  if (kind === 'stop' && !(price < p)) throw Error('Stop loss must be below the current price');
  if (kind === 'take' && !(price > p)) throw Error('Take profit must be above the current price');
  await pool.query('delete from ord where sym=$1 and kind=$2', [sym, kind]);
  await pool.query('insert into ord(sym,kind,price,pct,trail,peak) values($1,$2,$3,$4,$5,$6)', [sym, kind, trig, pct, kind === 'trail' ? trail : null, kind === 'trail' ? p : null]);
  await loadOrders();
  r.json({ ok: 1 });
}));

app.delete('/api/order/:id', auth, W(async (q, r) => {
  await pool.query('delete from ord where id=$1', [q.params.id]);
  await loadOrders();
  r.json({ ok: 1 });
}));

app.post('/api/reset', auth, W(async (q, r) => {
  await pool.query(`delete from pos; delete from ord; delete from trd; delete from snap; update acct set cash=${+settings.startBalance || 10000};`);
  await loadOrders();
  r.json({ ok: 1 });
}));

// ---------- Stage 2: search, stats, history ----------
const cache = {};
const cached = async (k, ttl, f) => { const c = cache[k]; if (c && Date.now() - c.t < ttl) return c.v; const v = await f(); cache[k] = { t: Date.now(), v }; return v; };
const getJ = async u => { const r = await fetch(u, { headers: { 'User-Agent': 'Mozilla/5.0' } }); if (!r.ok) throw Error('Data source error ' + r.status); return r.json(); };
const FH = p => `https://finnhub.io/api/v1/${p}${p.includes('?') ? '&' : '?'}token=${FINNHUB_KEY}`;
const CBX = 'https://api.exchange.coinbase.com';

app.get('/api/search', auth, W(async (q, r) => {
  const t = String(q.query.q || '').trim().toUpperCase();
  if (!t) return r.json([]);
  const prods = await cached('prods', 6e5, async () => (await getJ(CBX + '/products')).filter(p => p.quote_currency === 'USD' && p.status === 'online').map(p => p.base_currency));
  const out = prods.filter(b => b.startsWith(t)).slice(0, 6).map(b => ({ sym: b, name: b + ' (crypto)', kind: 'crypto' }));
  if (FINNHUB_KEY) {
    const d = await getJ(FH('search?q=' + encodeURIComponent(t)));
    for (const x of d.result || []) if (x.type === 'Common Stock' && !x.symbol.includes('.') && out.length < 14) out.push({ sym: x.symbol, name: x.description, kind: 'stock' });
  }
  r.json(out);
}));

async function seed(sym, kind) {
  if (kind === 'crypto') {
    const [t, s] = await Promise.all([getJ(`${CBX}/products/${sym}-USD/ticker`), getJ(`${CBX}/products/${sym}-USD/stats`)]);
    tick(sym, +t.price, +s.open, false);
  } else {
    const d = await getJ(FH('quote?symbol=' + sym));
    if (!d.c) throw Error('No price found for ' + sym);
    tick(sym, d.c, d.pc, false);
  }
}
app.post('/api/track', auth, W(async (q, r) => {
  const { sym, kind } = q.body;
  if (!/^[A-Z0-9]{1,10}$/.test(sym)) throw Error('Bad symbol');
  const list = kind === 'crypto' ? CRYPTO : STOCKS;
  if (!list.includes(sym)) {
    if (kind !== 'crypto' && STOCKS.length >= 30) throw Error('Live stock limit reached (30 on the free plan)');
    await seed(sym, kind);
    list.push(sym);
    await pool.query('insert into extra values($1,$2) on conflict do nothing', [sym, kind]);
    if (kind === 'crypto') { if (cb?.readyState === 1) cb.send(JSON.stringify({ type: 'subscribe', product_ids: [sym + '-USD'], channels: ['ticker'] })); }
    else if (fh?.readyState === 1) fh.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
  }
  r.json({ ok: 1 });
}));

app.get('/api/stats/:sym', auth, W(async (q, r) => {
  const s = q.params.sym;
  r.json(await cached('st' + s, 6e4, async () => {
    if (CRYPTO.includes(s)) {
      const d = await getJ(`${CBX}/products/${s}-USD/stats`);
      return { name: s, rows: [['Open (24h)', +d.open], ['High (24h)', +d.high], ['Low (24h)', +d.low], ['Volume (24h)', +d.volume, 'n'], ['Volume (30d)', +d.volume_30day, 'n']] };
    }
    const [qt, pr, me] = await Promise.all([getJ(FH('quote?symbol=' + s)), getJ(FH('stock/profile2?symbol=' + s)), getJ(FH('stock/metric?symbol=' + s + '&metric=all'))]);
    const m = me.metric || {};
    return { name: pr.name || s, rows: [['Open', qt.o], ['High', qt.h], ['Low', qt.l], ['Previous close', qt.pc], ['52-week high', m['52WeekHigh']], ['52-week low', m['52WeekLow']], ['Market cap', pr.marketCapitalization ? pr.marketCapitalization * 1e6 : null, 'c'], ['P/E (TTM)', m.peTTM, 'n'], ['Beta', m.beta, 'n'], ['Dividend yield', m.dividendYieldIndicatedAnnual, 'p']] };
  }));
}));

const RG = { '1D': { cb: [300, 86400], y: ['1d', '5m'] }, '1W': { cb: [3600, 604800], y: ['5d', '15m'] }, '1M': { cb: [21600, 2592000], y: ['1mo', '1h'] }, '3M': { cb: [86400, 7776000], y: ['3mo', '1d'] } };
app.get('/api/candles/:sym', auth, W(async (q, r) => {
  const s = q.params.sym, rg = RG[q.query.r] ? q.query.r : '1D', g = RG[rg];
  r.json(await cached('cd' + s + rg, 3e4, async () => {
    if (CRYPTO.includes(s)) {
      const end = new Date(), start = new Date(end - g.cb[1] * 1000);
      const d = await getJ(`${CBX}/products/${s}-USD/candles?granularity=${g.cb[0]}&start=${start.toISOString()}&end=${end.toISOString()}`);
      return d.map(x => ({ t: x[0], l: x[1], h: x[2], o: x[3], c: x[4] })).sort((a, b) => a.t - b.t);
    }
    const d = (await getJ(`https://query1.finance.yahoo.com/v8/finance/chart/${s}?range=${g.y[0]}&interval=${g.y[1]}`)).chart.result[0], z = d.indicators.quote[0];
    return d.timestamp.map((t, i) => ({ t, o: z.open[i], h: z.high[i], l: z.low[i], c: z.close[i] })).filter(x => x.c != null && x.o != null);
  }));
}));

// ---------- Stage 3: watchlist, alerts, journal, analytics, dividends ----------
function alertCheck(s, p) {
  for (const a of alerts[s] || []) {
    if (a.fired || !((a.dir === 'above' && p >= +a.price) || (a.dir === 'below' && p <= +a.price))) continue;
    a.fired = true;
    pool.query('update alert set fired=true where id=$1', [a.id]).catch(() => {});
    const m = JSON.stringify({ alert: { sym: s, dir: a.dir, price: +a.price } });
    sendPush('Price alert', `${s} price ${a.dir === 'above' ? 'rose to' : 'fell to'} $${+a.price}`, 'alert');
    wss.clients.forEach(c => c.readyState === 1 && c.send(m));
  }
}
async function loadAlerts() {
  alerts = {};
  for (const a of (await pool.query('select * from alert where not fired')).rows) (alerts[a.sym] ??= []).push(a);
}
app.post('/api/watch', auth, W(async (q, r) => {
  const { sym, on } = q.body;
  if (on) await pool.query('insert into watch values($1) on conflict do nothing', [sym]); else await pool.query('delete from watch where sym=$1', [sym]);
  r.json({ ok: 1 });
}));
app.post('/api/alert', auth, W(async (q, r) => {
  const { sym, price } = q.body, p = prices[sym]?.p;
  if (!p || !(price > 0) || price === p) throw Error('Pick a price different from the current one');
  await pool.query('insert into alert(sym,dir,price,label) values($1,$2,$3,$4)', [sym, price > p ? 'above' : 'below', price, String(q.body.label || '').slice(0, 80)]);
  await loadAlerts(); r.json({ ok: 1 });
}));
app.delete('/api/alert/:id', auth, W(async (q, r) => { await pool.query('delete from alert where id=$1', [q.params.id]); await loadAlerts(); r.json({ ok: 1 }); }));
app.post('/api/note', auth, W(async (q, r) => { await pool.query('update trd set note=$1 where id=$2', [String(q.body.note || '').slice(0, 500), q.body.id]); r.json({ ok: 1 }); }));
app.post('/api/divs', auth, W(async (q, r) => {
  divOn = !!q.body.on;
  await pool.query("insert into kv values('div',$1) on conflict(k) do update set v=$1", [divOn ? 'on' : 'off']);
  r.json({ ok: 1 });
}));
app.get('/api/analytics', auth, W(async (q, r) => {
  const sum = a => a.reduce((x, y) => x + y, 0);
  const t = (await pool.query("select pnl from trd where side='sell'")).rows.map(x => +x.pnl), w = t.filter(x => x > 0), l = t.filter(x => x < 0);
  const dv = +(await pool.query("select coalesce(sum(qty*price),0) s from trd where side='div'")).rows[0].s;
  const eqs = (await pool.query('select ts, eq from snap order by ts')).rows;
  let peak = 0, dd = 0;
  for (const e of eqs) { peak = Math.max(peak, +e.eq); dd = Math.min(dd, (+e.eq - peak) / peak); }
  let spy = null;
  if (eqs.length) try {
    const t0 = new Date(eqs[0].ts), res = (await cached('spy' + t0.getTime(), 36e5, () => getJ(`https://query1.finance.yahoo.com/v8/finance/chart/SPY?period1=${Math.floor(t0 / 1000) - 864000}&period2=${Math.floor(Date.now() / 1000) + 86400}&interval=1d`))).chart.result[0];
    const cl = res.indicators.quote[0].close; let i0 = 0;
    res.timestamp.forEach((x, i) => { if (x * 1000 <= t0 && cl[i] != null) i0 = i; });
    spy = (cl.filter(x => x != null).pop() / cl[i0] - 1) * 100;
  } catch (e) {}
  r.json({ closed: t.length, win: t.length ? w.length / t.length * 100 : null, avgWin: w.length ? sum(w) / w.length : null, avgLoss: l.length ? sum(l) / l.length : null, best: t.length ? Math.max(...t) : null, worst: t.length ? Math.min(...t) : null, realized: sum(t), dividends: dv, maxDD: dd * 100, pf: l.length ? sum(w) / -sum(l) : null, spy });
}));
// dividends: credited when a held stock's ex-dividend date passes (checked every 6 hours)
async function divJob() {
  if (!divOn) return;
  try {
    for (const p of (await pool.query('select * from pos')).rows) {
      if (CRYPTO.includes(p.sym)) continue;
      const d = (await getJ(`https://query1.finance.yahoo.com/v8/finance/chart/${p.sym}?range=3mo&interval=1d&events=div`)).chart.result[0].events?.dividends || {};
      for (const v of Object.values(d)) {
        if (v.date * 1000 < new Date(p.since) || (await pool.query('select 1 from divpaid where sym=$1 and ex=$2', [p.sym, String(v.date)])).rows[0]) continue;
        await pool.query('insert into divpaid values($1,$2)', [p.sym, String(v.date)]);
        await pool.query('update acct set cash=cash+$1 where id=1', [p.qty * v.amount]);
        await pool.query("insert into trd(sym,side,qty,price,why) values($1,'div',$2,$3,'dividend')", [p.sym, p.qty, v.amount]);
      }
    }
  } catch (e) { console.error('dividends', e.message); }
}

// ---------- Stage 3b: push notifications (work when the app is closed) ----------
let pushOn = false, VAPID_PUB = '';
async function initPush() {
  try {
    const g = async k => (await pool.query('select v from kv where k=$1', [k])).rows[0]?.v;
    let pub = await g('vapid_pub'), priv = await g('vapid_priv');
    if (!pub || !priv) {
      ({ publicKey: pub, privateKey: priv } = webpush.generateVAPIDKeys());
      await pool.query("insert into kv values('vapid_pub',$1),('vapid_priv',$2) on conflict(k) do update set v=excluded.v", [pub, priv]);
    }
    webpush.setVapidDetails(process.env.VAPID_EMAIL || 'mailto:mango-app@example.com', pub, priv);
    VAPID_PUB = pub; pushOn = true;
  } catch (e) { console.error('push setup', e.message); }
}
async function sendPush(title, body, kind) {
  if (!pushOn || (kind === 'alert' && !settings.pushAlerts) || (kind === 'order' && !settings.pushOrders)) return;
  try {
    for (const row of (await pool.query('select * from push')).rows) {
      try { await webpush.sendNotification(JSON.parse(row.sub), JSON.stringify({ title, body })); }
      catch (e) {
        if (e.statusCode === 404 || e.statusCode === 410) await pool.query('delete from push where endpoint=$1', [row.endpoint]);
        else console.error('push error', e.statusCode || e.message);
      }
    }
  } catch (e) { console.error('push', e.message); }
}
const SW = `self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(clients.claim()));
self.addEventListener('push',e=>{const d=e.data?e.data.json():{};e.waitUntil(self.registration.showNotification(d.title||'Mango',{body:d.body||''}))});
self.addEventListener('notificationclick',e=>{e.notification.close();e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(l=>l.length?l[0].focus():clients.openWindow('/')))});`;
app.get('/sw.js', (q, r) => r.type('js').send(SW));
app.get('/manifest.json', (q, r) => r.type('application/manifest+json').send(JSON.stringify({ name: 'Mango Paper Trading', short_name: 'Mango', start_url: '/', display: 'standalone', background_color: '#16111f', theme_color: '#16111f' })));
app.get('/api/push/key', auth, (q, r) => r.json({ key: VAPID_PUB, on: pushOn }));
app.post('/api/push/sub', auth, W(async (q, r) => {
  const sub = q.body.sub;
  if (!sub?.endpoint) throw Error('Bad subscription');
  await pool.query('insert into push values($1,$2) on conflict(endpoint) do update set sub=$2', [sub.endpoint, JSON.stringify(sub)]);
  r.json({ ok: 1 });
}));
app.post('/api/push/unsub', auth, W(async (q, r) => { await pool.query('delete from push where endpoint=$1', [q.body.endpoint]); r.json({ ok: 1 }); }));
app.post('/api/push/test', auth, W(async (q, r) => { await sendPush('Mango test', 'Notifications are working 🥭'); r.json({ ok: 1 }); }));

app.post('/api/settings', auth, W(async (q, r) => {
  for (const k of Object.keys(DEF)) {
    if (!(k in q.body) || typeof q.body[k] !== typeof DEF[k]) continue;
    let v = q.body[k];
    if (typeof v === 'number') v = Math.max(k === 'startBalance' ? 100 : 0, Math.min(k.startsWith('auto') ? 90 : 1e9, v || 0));
    settings[k] = v;
  }
  await pool.query("insert into kv values('settings',$1) on conflict(k) do update set v=$1", [JSON.stringify(settings)]);
  r.json({ ok: 1 });
}));

// equity snapshots for the portfolio chart
setInterval(async () => {
  try {
    for (const l of Object.values(open)) for (const o of l) if (o.kind === 'trail') pool.query('update ord set peak=$1 where id=$2', [o.peak, o.id]).catch(() => {});
    const cash = +(await pool.query('select cash from acct')).rows[0].cash;
    const ps = (await pool.query('select * from pos')).rows;
    await pool.query('insert into snap(eq) values($1)', [cash + ps.reduce((t, x) => t + x.qty * (prices[x.sym]?.p || x.avg), 0)]);
  } catch (e) { console.error(e.message); }
}, 30000);

(async () => {
  await init(); await loadOrders(); await loadAlerts(); await initPush();
  try { settings = { ...DEF, ...JSON.parse((await pool.query("select v from kv where k='settings'")).rows[0]?.v || '{}') }; } catch (e) {}
  divOn = (await pool.query("select v from kv where k='div'")).rows[0]?.v !== 'off'; divJob(); setInterval(divJob, 6 * 36e5);
  for (const x of (await pool.query('select * from extra')).rows) { const l = x.kind === 'crypto' ? CRYPTO : STOCKS; if (!l.includes(x.sym)) l.push(x.sym); }
  coinbase(); finnhub(); pollStocks(); setInterval(pollStocks, 60000);
  server.listen(PORT, () => console.log('Mango running on ' + PORT));
})();
