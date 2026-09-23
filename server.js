const express = require('express'), http = require('http'), crypto = require('crypto');
const WebSocket = require('ws'), { Pool } = require('pg');
const { FINNHUB_KEY, APP_PASS, DATABASE_URL, PORT = 3000 } = process.env;
const APP_USER = process.env.APP_USER || 'mango', SECRET = process.env.SESSION_SECRET || APP_PASS;
if (!APP_PASS || !DATABASE_URL) { console.error('Missing APP_PASS or DATABASE_URL'); process.exit(1); }

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
    create table if not exists snap(ts timestamptz default now(), eq numeric);`);
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
  const ws = new WebSocket('wss://ws-feed.exchange.coinbase.com');
  ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', product_ids: CRYPTO.map(s => s + '-USD'), channels: ['ticker'] })));
  ws.on('message', m => { try { const d = JSON.parse(m); if (d.type === 'ticker' && d.price) tick(d.product_id.split('-')[0], +d.price, +d.open_24h, true); } catch (e) {} });
  ws.on('close', () => setTimeout(coinbase, 3000));
  ws.on('error', () => ws.close());
}
function finnhub() {
  if (!FINNHUB_KEY) return;
  const ws = new WebSocket('wss://ws.finnhub.io?token=' + FINNHUB_KEY);
  ws.on('open', () => STOCKS.forEach(s => ws.send(JSON.stringify({ type: 'subscribe', symbol: s }))));
  ws.on('message', m => { try { const d = JSON.parse(m); if (d.type === 'trade') for (const t of d.data) tick(t.s, t.p, null, true); } catch (e) {} });
  ws.on('close', () => setTimeout(finnhub, 3000));
  ws.on('error', () => ws.close());
}
// REST quotes: previous close for % change, and last price when the market is closed
async function pollStocks() {
  if (!FINNHUB_KEY) return;
  for (const s of STOCKS) {
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
  const [a, ps, os, ts, sn] = await Promise.all([
    Q('select cash from acct'), Q('select * from pos order by sym'), Q('select * from ord'),
    Q('select * from trd order by id desc limit 30'),
    Q("select eq from snap where ts>now()-interval '24 hours' order by ts")]);
  r.json({
    cash: +a[0].cash,
    pos: ps.map(x => ({ sym: x.sym, qty: +x.qty, avg: +x.avg })),
    ord: os.map(x => ({ id: x.id, sym: x.sym, kind: x.kind, price: +x.price })),
    trd: ts.map(x => ({ ts: x.ts, sym: x.sym, side: x.side, qty: +x.qty, price: +x.price, why: x.why, pnl: x.pnl == null ? null : +x.pnl })),
    snap: sn.map(x => +x.eq),
    prices: Object.fromEntries(Object.entries(prices).map(([k, v]) => [k, { p: v.p, prev: v.prev }])),
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
  coinbase(); finnhub(); pollStocks(); setInterval(pollStocks, 20000);
  server.listen(PORT, () => console.log('Mango running on ' + PORT));
})();
