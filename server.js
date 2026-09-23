const express = require('express'), http = require('http'), crypto = require('crypto');
const WebSocket = require('ws'), { Pool } = require('pg');
const { FINNHUB_KEY, APP_PASS, DATABASE_URL, PORT = 3000 } = process.env;
const APP_USER = process.env.APP_USER || 'mango', SECRET = process.env.SESSION_SECRET || APP_PASS;
if (!APP_PASS || !DATABASE_URL) { console.error('Missing APP_PASS or DATABASE_URL'); process.exit(1); }

let cb = null, fh = null;
const pool = new Pool({ connectionString: DATABASE_URL, ssl: DATABASE_URL.includes('railway.internal') ? false : { rejectUnauthorized: false } });
const STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'TSLA', 'AMZN', 'LMT', 'NOC', 'WMT'];
const CRYPTO = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA'];
const prices = {}, lastSent = {}, firing = new Set();
let open = {};

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
    create table if not exists extra(sym text primary key, kind text);`);
}
async function loadOrders() {
  open = {};
  for (const o of (await pool.query('select * from ord')).rows) (open[o.sym] ??= []).push(o);
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
  const hit = list.find(o => (o.kind === 'stop' && p <= +o.price) || (o.kind === 'take' && p >= +o.price));
  if (!hit) return;
  firing.add(s);
  try {
    const r = (await pool.query('select qty from pos where sym=$1', [s])).rows[0];
    if (r) await trade(s, 'sell', +r.qty, hit.kind === 'stop' ? 'stop loss' : 'take profit');
    await pool.query('delete from ord where sym=$1', [s]);
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
  check(s, p);
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
  const [a, ps, os, ts, sn, ex] = await Promise.all([
    Q('select cash from acct'), Q('select * from pos order by sym'), Q('select * from ord'),
    Q('select * from trd order by id desc limit 30'),
    Q("select eq from snap where ts>now()-interval '24 hours' order by ts"), Q('select * from extra')]);
  r.json({
    cash: +a[0].cash,
    pos: ps.map(x => ({ sym: x.sym, qty: +x.qty, avg: +x.avg })),
    ord: os.map(x => ({ id: x.id, sym: x.sym, kind: x.kind, price: +x.price })),
    trd: ts.map(x => ({ ts: x.ts, sym: x.sym, side: x.side, qty: +x.qty, price: +x.price, why: x.why, pnl: x.pnl == null ? null : +x.pnl })),
    snap: sn.map(x => +x.eq),
    prices: Object.fromEntries(Object.entries(prices).map(([k, v]) => [k, { p: v.p, prev: v.prev }])),
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
  r.json({ ok: 1 });
}));

app.post('/api/order', auth, W(async (q, r) => {
  const { sym, kind, price } = q.body;
  if (!['stop', 'take'].includes(kind)) throw Error('Bad order type');
  if (!(await pool.query('select 1 from pos where sym=$1', [sym])).rows[0]) throw Error('Buy first, then set stop loss or take profit');
  const p = prices[sym]?.p;
  if (kind === 'stop' && !(price < p)) throw Error('Stop loss must be below the current price');
  if (kind === 'take' && !(price > p)) throw Error('Take profit must be above the current price');
  await pool.query('delete from ord where sym=$1 and kind=$2', [sym, kind]);
  await pool.query('insert into ord(sym,kind,price) values($1,$2,$3)', [sym, kind, price]);
  await loadOrders();
  r.json({ ok: 1 });
}));

app.delete('/api/order/:id', auth, W(async (q, r) => {
  await pool.query('delete from ord where id=$1', [q.params.id]);
  await loadOrders();
  r.json({ ok: 1 });
}));

app.post('/api/reset', auth, W(async (q, r) => {
  await pool.query('delete from pos; delete from ord; delete from trd; delete from snap; update acct set cash=10000;');
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

// equity snapshots for the portfolio chart
setInterval(async () => {
  try {
    const cash = +(await pool.query('select cash from acct')).rows[0].cash;
    const ps = (await pool.query('select * from pos')).rows;
    await pool.query('insert into snap(eq) values($1)', [cash + ps.reduce((t, x) => t + x.qty * (prices[x.sym]?.p || x.avg), 0)]);
  } catch (e) { console.error(e.message); }
}, 30000);

(async () => {
  await init(); await loadOrders();
  for (const x of (await pool.query('select * from extra')).rows) { const l = x.kind === 'crypto' ? CRYPTO : STOCKS; if (!l.includes(x.sym)) l.push(x.sym); }
  coinbase(); finnhub(); pollStocks(); setInterval(pollStocks, 60000);
  server.listen(PORT, () => console.log('Mango running on ' + PORT));
})();
