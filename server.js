'use strict';
/*
 * Mango Paper Trading — server
 * One file on purpose: easy to paste into GitHub from a phone.
 * Sections: core · database · market hours · prices · data sources · trading · notifications
 *           · analytics · games · API · jobs · app files · startup
 */
const http = require('http'), crypto = require('crypto'), zlib = require('zlib'), fs = require('fs'), path = require('path');
const WebSocket = require('ws'), { Pool } = require('pg'), webpush = require('web-push');

const ENV = process.env;
const FINNHUB_KEY = (ENV.FINNHUB_KEY || '').trim();
const APP_PASS = ENV.APP_PASS, DATABASE_URL = ENV.DATABASE_URL;
const PORT = +ENV.PORT || 3000;
const APP_USER = (ENV.APP_USER || 'mango').trim();
const SECRET = ENV.SESSION_SECRET || APP_PASS;
const WS_MAX = Math.max(1, +ENV.FINNHUB_WS_MAX || 30);
if (!APP_PASS || !DATABASE_URL) {
  console.error('Missing APP_PASS or DATABASE_URL. Add them in Railway → your app → Variables.');
  process.exit(1);
}
const BOOT = Date.now();
const stamp = () => new Date().toISOString().slice(5, 19).replace('T', ' ');
const log = (...a) => console.log(stamp(), ...a);
const warn = (...a) => console.warn(stamp(), '⚠', ...a);

// ---------- small helpers ----------
const sleep = ms => new Promise(r => setTimeout(r, ms));
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const num = (v, d = 0) => (v === null || v === undefined || v === '' || !Number.isFinite(+v)) ? d : +v;
const round = (v, d = 8) => { const f = 10 ** d; return Math.round(v * f) / f; };
const money = v => (v < 0 ? '-' : '') + '$' + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: Math.abs(v) < 1 ? 5 : 2 });
const pct = v => (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
const pxStr = v => '$' + (+v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: v < 1 ? 6 : 2 });
const SYM_RE = /^[A-Z0-9][A-Z0-9.\-]{0,11}$/;
const cleanSym = s => { s = String(s || '').trim().toUpperCase(); return SYM_RE.test(s) ? s : null; };
const tsMs = d => d ? new Date(d).getTime() : null;
const debounce = (fn, ms) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };

// ---------- tiny HTTP framework ----------
class HttpError extends Error { constructor(status, msg) { super(msg); this.status = status; } }
const fail = (msg, status = 400) => { throw new HttpError(status, msg); };
const routes = [];
function route(method, pattern, ...handlers) {
  const keys = [];
  const re = new RegExp('^' + pattern.replace(/:(\w+)/g, (_, k) => (keys.push(k), '([^/]+)')) + '/?$');
  routes.push({ method, re, keys, handlers });
}
const GET = (p, ...h) => route('GET', p, ...h), POST = (p, ...h) => route('POST', p, ...h);
const PATCH = (p, ...h) => route('PATCH', p, ...h), DEL = (p, ...h) => route('DELETE', p, ...h);

const CSP = [
  "default-src 'self'", "script-src 'self' 'unsafe-inline'", "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src 'self' https://fonts.gstatic.com data:", "img-src 'self' data: blob: https:", "connect-src 'self' ws: wss:",
  "worker-src 'self'", "manifest-src 'self'", "frame-ancestors 'none'", "base-uri 'none'", "form-action 'self'",
].join('; ');
function baseHeaders(res) {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'same-origin');
  res.setHeader('X-Frame-Options', 'DENY');
}
function send(req, res, status, body, type = 'application/json; charset=utf-8', extra = {}) {
  if (res.headersSent) return;
  let buf = Buffer.isBuffer(body) ? body : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body));
  const headers = { 'Content-Type': type, 'Cache-Control': 'no-store', ...extra };
  if (buf.length > 1200 && /gzip/.test(req.headers['accept-encoding'] || '') && /json|text|javascript|svg|csv/.test(type) && !extra['Content-Encoding']) {
    buf = zlib.gzipSync(buf, { level: 6 }); headers['Content-Encoding'] = 'gzip'; headers['Vary'] = 'Accept-Encoding';
  }
  headers['Content-Length'] = buf.length;
  res.writeHead(status, headers); res.end(req.method === 'HEAD' ? undefined : buf);
}
function readBody(req, limit = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > limit) { reject(new HttpError(413, 'That upload is too big')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => {
      const s = Buffer.concat(chunks).toString('utf8');
      if (!s) return resolve({});
      try { resolve(JSON.parse(s)); } catch (e) { reject(new HttpError(400, 'The request was not valid JSON')); }
    });
    req.on('error', reject);
  });
}
const clientIp = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
let staticHandler = null; // set in the app files section
const server = http.createServer(async (req, res) => {
  baseHeaders(res);
  let u; try { u = new URL(req.url, 'http://x'); } catch (e) { return send(req, res, 400, { error: 'Bad URL' }); }
  req.path = u.pathname; req.query = Object.fromEntries(u.searchParams);
  const method = req.method === 'HEAD' ? 'GET' : req.method;
  const r = routes.find(r => r.method === method && r.re.test(req.path));
  if (!r) {
    if (method === 'GET' && staticHandler && staticHandler(req, res)) return;
    return send(req, res, 404, { error: 'Not found' });
  }
  const m = req.path.match(r.re); req.params = {};
  r.keys.forEach((k, i) => { try { req.params[k] = decodeURIComponent(m[i + 1]); } catch (e) { req.params[k] = m[i + 1]; } });
  try {
    req.body = ['POST', 'PATCH', 'PUT', 'DELETE'].includes(method) ? await readBody(req) : {};
    let out;
    for (const h of r.handlers) { out = await h(req, res); if (res.headersSent) return; }
    if (out === undefined) out = { ok: true };
    send(req, res, 200, out);
  } catch (e) {
    const status = e.status || 500;
    if (status >= 500) warn(req.method, req.path, e.stack || e.message);
    send(req, res, status, { error: status >= 500 ? 'Something went wrong on the server. Try again in a moment.' : e.message });
  }
});

// =====================================================================
// Database
// =====================================================================
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: /railway\.internal|localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
  max: 8,
});
pool.on('error', e => warn('database', e.message));
const Q = (sql, params) => pool.query(sql, params);
const rows = async (sql, params) => (await pool.query(sql, params)).rows;
const row = async (sql, params) => (await pool.query(sql, params)).rows[0];
async function tx(fn) {
  const c = await pool.connect();
  try { await c.query('begin'); const r = await fn(c); await c.query('commit'); return r; }
  catch (e) { try { await c.query('rollback'); } catch (_) {} throw e; }
  finally { c.release(); }
}
const kvGet = async (k, d = null) => { const r = await row('select v from kv where k=$1', [k]); return r ? r.v : d; };
const kvSet = (k, v) => Q('insert into kv values($1,$2) on conflict(k) do update set v=excluded.v', [k, String(v)]);

// The original app's tables. Everything uses "if not exists" so old databases are left as they are.
const BASE_SCHEMA = `
  create table if not exists kv(k text primary key, v text);
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
  create table if not exists push(endpoint text primary key, sub text);
  alter table ord add column if not exists pct numeric default 100;
  alter table ord add column if not exists trail numeric;
  alter table ord add column if not exists peak numeric;
  alter table alert add column if not exists label text;
  create table if not exists lim(id serial primary key, sym text, side text, price numeric, amt numeric, ts timestamptz default now());
  create table if not exists seasons(id serial primary key, ts timestamptz default now(), started timestamptz, start_bal numeric, final_bal numeric, trades int, wins int, closed int);
  alter table trd add column if not exists note text;
  alter table pos add column if not exists since timestamptz default now();
`;

// Each step runs once, in its own transaction, and is recorded in kv 'schema'.
const MIGRATIONS = [
  // 1 — wallets, and a wallet id on everything that belongs to a wallet
  async c => {
    const s = await c.query("select v from kv where k='settings'");
    let start = 10000; try { start = +JSON.parse(s.rows[0]?.v || '{}').startBalance || 10000; } catch (e) {}
    await c.query(`create table if not exists wallets(
      id serial primary key, name text not null, emoji text not null default '🥭', color text not null default 'mango',
      cash numeric not null default 0, start_bal numeric not null default 10000, created timestamptz not null default now(),
      season_start timestamptz not null default now(), archived boolean not null default false, sort int not null default 0)`);
    await c.query(`insert into wallets(id,name,emoji,color,cash,start_bal,created,season_start)
      select 1,'Main','🥭','mango',a.cash,$1,
        coalesce((select min(ts) from trd),(select min(ts) from snap),now()),
        coalesce((select min(ts) from snap),(select min(ts) from trd),now())
      from acct a where a.id=1 and not exists(select 1 from wallets)`, [start]);
    await c.query(`select setval(pg_get_serial_sequence('wallets','id'), greatest((select coalesce(max(id),1) from wallets),1))`);
    await c.query(`
      alter table pos add column if not exists wid int not null default 1;
      alter table pos drop constraint if exists pos_pkey;
      alter table pos add primary key (wid, sym);
      alter table ord add column if not exists wid int not null default 1;
      alter table ord add column if not exists created timestamptz default now();
      alter table lim add column if not exists wid int not null default 1;
      alter table lim add column if not exists kind text not null default 'limit';
      alter table lim add column if not exists units text not null default 'auto';
      alter table lim add column if not exists sl numeric;
      alter table lim add column if not exists tp numeric;
      alter table lim add column if not exists expires timestamptz;
      alter table trd add column if not exists wid int not null default 1;
      alter table trd add column if not exists fee numeric not null default 0;
      alter table trd add column if not exists tags text;
      alter table snap add column if not exists wid int not null default 1;
      create index if not exists snap_wid_ts on snap(wid, ts);
      create index if not exists trd_wid_id on trd(wid, id);
      alter table divpaid add column if not exists wid int not null default 1;
      alter table divpaid drop constraint if exists divpaid_pkey;
      alter table divpaid add primary key (wid, sym, ex);
      alter table seasons add column if not exists wid int not null default 1;
      alter table seasons add column if not exists name text;
      alter table alert add column if not exists kind text not null default 'price';
      alter table alert add column if not exists pct numeric;
      alter table alert add column if not exists rep boolean not null default false;
      alter table alert add column if not exists armed boolean not null default true;
      alter table alert add column if not exists last_fired timestamptz;
      alter table watch add column if not exists pos int not null default 0;
      alter table watch add column if not exists created timestamptz default now();
      alter table push add column if not exists created timestamptz default now();
      alter table push add column if not exists ua text;
      create table if not exists meta(sym text primary key, kind text, name text, logo text, industry text, cap numeric, cg text, exchange text, updated timestamptz);
      create table if not exists notifs(id serial primary key, ts timestamptz not null default now(), kind text not null, title text not null, body text, sym text, wid int, url text, read boolean not null default false);
      create index if not exists notifs_ts on notifs(ts);
      create table if not exists preds(id serial primary key, sym text not null, dir text not null, start_px numeric not null, start_ts timestamptz not null default now(), end_ts timestamptz not null, end_px numeric, result text, points int not null default 0);
      create table if not exists achv(key text primary key, ts timestamptz not null default now());
      create table if not exists prices(sym text primary key, p numeric not null, prev numeric, ts timestamptz not null);
    `);
    // Old trades that changed cash count as deposits; old dividend rows keep working as they are.
    // Existing user-added symbols get a meta row so names survive.
    await c.query(`insert into meta(sym,kind) select sym,kind from extra on conflict do nothing`);
  },
];

async function migrate() {
  await Q(BASE_SCHEMA);
  let v = +(await kvGet('schema', '0')) || 0;
  for (let i = v; i < MIGRATIONS.length; i++) {
    log(`Upgrading database to version ${i + 1}…`);
    await tx(async c => { await MIGRATIONS[i](c); await c.query("insert into kv values('schema',$1) on conflict(k) do update set v=excluded.v", [String(i + 1)]); });
  }
  if (v < MIGRATIONS.length) log('Database is up to date (version ' + MIGRATIONS.length + ')');
}

// =====================================================================
// Settings (shared by every device you use)
// =====================================================================
const DEF = {
  // trading
  feeFlat: 0, feePct: 0, slippage: 0, maxPos: 0, reservePct: 0, wholeShares: false,
  closedTrading: 'fill', rthOrders: true, autoSL: 0, autoTP: 0, defaultBuy: 0,
  confirmStyle: 'hold', confirmBuy: false, confirmSell: true, cashAPY: 0, startBalance: 10000,
  // notifications
  pushAlerts: true, pushOrders: true, notifySummary: true, notifyMoves: true, moveThreshold: 5,
  notifyAchv: true, notifyGames: true, quietStart: '', quietEnd: '',
  // look and feel
  hideBalances: false, showCents: true, compactNums: false, defaultRange: '1D', defaultChart: 'Candles',
  theme: 'mango', accent: 'mango', plColors: 'classic', roundedNums: true, reduceMotion: false, celebrate: true,
  defaultTab: 'portfolio', posMetric: 'day', posSort: 'value', compactRows: false, sparklines: true, defaultWallet: 1,
  chartGrid: true, chartVolume: true, indicators: '',
  // features (every one can be switched off)
  f_watch: true, f_alerts: true, f_analytics: true, f_journal: true, f_news: true, f_earnings: true,
  f_heatmap: true, f_book: true, f_fng: true, f_games: true, f_preds: true, f_achv: true, f_leader: true,
  f_divs: true, f_ext: true, f_movers: true, f_insights: true,
};
const LIMITS = {
  feeFlat: [0, 1000], feePct: [0, 10], slippage: [0, 5], maxPos: [0, 100], reservePct: [0, 100], autoSL: [0, 90], autoTP: [0, 1000],
  defaultBuy: [0, 1e9], cashAPY: [0, 100], startBalance: [100, 1e9], moveThreshold: [1, 50], defaultWallet: [1, 1e9],
};
const CHOICES = {
  closedTrading: ['fill', 'queue', 'block'], confirmStyle: ['hold', 'tap', 'none'], defaultRange: ['1D', '1W', '1M', '3M', '1Y', 'ALL', '5Y'],
  defaultChart: ['Candles', 'Line'], theme: ['mango', 'midnight', 'light', 'auto'], accent: ['mango', 'leaf', 'sky', 'grape', 'rose'],
  plColors: ['classic', 'colorblind', 'reversed'], defaultTab: ['portfolio', 'market', 'activity', 'games'],
  posMetric: ['day', 'total', 'value', 'pl'], posSort: ['value', 'day', 'total', 'name'],
};
let S = { ...DEF };
async function loadSettings() {
  try { S = { ...DEF, ...JSON.parse(await kvGet('settings', '{}')) }; } catch (e) { S = { ...DEF }; }
  // The old app kept the dividend switch on its own; fold it into settings once.
  if (await kvGet('div') === 'off') { S.f_divs = false; await kvSet('settings', JSON.stringify(S)); await Q("delete from kv where k='div'"); }
}
async function saveSettings(patch) {
  for (const [k, v0] of Object.entries(patch || {})) {
    if (!(k in DEF)) continue;
    let v = v0;
    if (typeof DEF[k] === 'number') { v = num(v, DEF[k]); if (LIMITS[k]) v = clamp(v, ...LIMITS[k]); }
    else if (typeof DEF[k] === 'boolean') v = v === true || v === 'true';
    else { v = String(v ?? '').slice(0, 200); if (CHOICES[k] && !CHOICES[k].includes(v)) continue; if ((k === 'quietStart' || k === 'quietEnd') && v && !/^\d\d:\d\d$/.test(v)) continue; }
    S[k] = v;
  }
  await kvSet('settings', JSON.stringify(S));
}

// =====================================================================
// US stock market hours (NYSE calendar, computed — no API needed)
// =====================================================================
const ET_FMT = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false, weekday: 'short' });
const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
function etParts(t = Date.now()) {
  const o = {}; for (const p of ET_FMT.formatToParts(new Date(t))) o[p.type] = p.value;
  return { y: +o.year, m: +o.month, d: +o.day, h: (+o.hour) % 24, mi: +o.minute, s: +o.second, wd: WD.indexOf(o.weekday) };
}
const etDate = (t = Date.now()) => { const p = etParts(t); return `${p.y}-${String(p.m).padStart(2, '0')}-${String(p.d).padStart(2, '0')}`; };
// Wall-clock New York time → UTC milliseconds (handles daylight saving).
function etToUtc(y, m, d, h = 0, mi = 0) {
  let t = Date.UTC(y, m - 1, d, h + 5, mi);
  for (let i = 0; i < 3; i++) { const p = etParts(t); const diff = (Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi) - Date.UTC(y, m - 1, d, h, mi)); if (!diff) break; t -= diff; }
  return t;
}
const dow = (y, m, d) => new Date(Date.UTC(y, m - 1, d)).getUTCDay();
const addDays = (y, m, d, n) => { const t = new Date(Date.UTC(y, m - 1, d + n)); return [t.getUTCFullYear(), t.getUTCMonth() + 1, t.getUTCDate()]; };
const nthWeekday = (y, m, wd, n) => { const first = dow(y, m, 1); return 1 + ((wd - first + 7) % 7) + (n - 1) * 7; };
const lastWeekday = (y, m, wd) => { const days = new Date(Date.UTC(y, m, 0)).getUTCDate(); const last = dow(y, m, days); return days - ((last - wd + 7) % 7); };
function easter(y) {
  const a = y % 19, b = Math.floor(y / 100), c = y % 100, d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30, i = Math.floor(c / 4), k = c % 4, l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31), day = ((h + l - 7 * m + 114) % 31) + 1; return [month, day];
}
const holidayCache = new Map();
function holidays(y) {
  if (holidayCache.has(y)) return holidayCache.get(y);
  const set = new Map(); const key = (m, d) => `${m}-${d}`;
  const observed = (m, d, name, allowFri = true) => {
    const w = dow(y, m, d);
    if (w === 6) { if (allowFri) { const [, mm, dd] = addDays(y, m, d, -1); set.set(key(mm, dd), name); } }
    else if (w === 0) { const [, mm, dd] = addDays(y, m, d, 1); set.set(key(mm, dd), name); }
    else set.set(key(m, d), name);
  };
  observed(1, 1, "New Year's Day", false);
  set.set(key(1, nthWeekday(y, 1, 1, 3)), 'Martin Luther King Jr. Day');
  set.set(key(2, nthWeekday(y, 2, 1, 3)), "Presidents' Day");
  const [em, ed] = easter(y); const [, gm, gd] = addDays(y, em, ed, -2); set.set(key(gm, gd), 'Good Friday');
  set.set(key(5, lastWeekday(y, 5, 1)), 'Memorial Day');
  if (y >= 2022) observed(6, 19, 'Juneteenth');
  observed(7, 4, 'Independence Day');
  set.set(key(9, nthWeekday(y, 9, 1, 1)), 'Labor Day');
  set.set(key(11, nthWeekday(y, 11, 4, 4)), 'Thanksgiving');
  observed(12, 25, 'Christmas');
  // New Year's Day of next year landing on a Saturday is not made up on Dec 31.
  holidayCache.set(y, set); return set;
}
const holidayName = (y, m, d) => holidays(y).get(`${m}-${d}`) || null;
const isTradingDay = (y, m, d) => { const w = dow(y, m, d); return w !== 0 && w !== 6 && !holidayName(y, m, d); };
function earlyClose(y, m, d) {
  if (!isTradingDay(y, m, d)) return false;
  const tg = nthWeekday(y, 11, 4, 4);
  if (m === 11 && d === tg + 1) return true;                       // day after Thanksgiving
  if (m === 12 && d === 24) return true;                            // Christmas Eve
  if (m === 7 && d === 3 && !holidayName(y, 7, 3)) { const w = dow(y, 7, 4); return w >= 2 && w <= 5; } // day before July 4th
  return false;
}
function nextTradingDay(y, m, d) { for (let i = 1; i < 15; i++) { const [yy, mm, dd] = addDays(y, m, d, i); if (isTradingDay(yy, mm, dd)) return [yy, mm, dd]; } return addDays(y, m, d, 1); }
let marketMemo = { t: 0, v: null };
function market(t = Date.now()) {
  if (Math.abs(t - marketMemo.t) < 1000 && marketMemo.v) return marketMemo.v;
  const p = etParts(t), trading = isTradingDay(p.y, p.m, p.d), mins = p.h * 60 + p.mi;
  const closeMin = earlyClose(p.y, p.m, p.d) ? 13 * 60 : 16 * 60;
  let state = 'closed';
  if (trading) { if (mins >= 240 && mins < 570) state = 'pre'; else if (mins >= 570 && mins < closeMin) state = 'open'; else if (mins >= closeMin && mins < 1200) state = 'post'; }
  const ov = ENV.MARKET_OVERRIDE; if (ov === 'open' || ov === 'closed' || ov === 'pre' || ov === 'post') state = ov;
  let opensAt = null, closesAt = null;
  if (state === 'open') closesAt = etToUtc(p.y, p.m, p.d, Math.floor(closeMin / 60), closeMin % 60);
  else {
    let [y, m, d] = [p.y, p.m, p.d];
    if (!(trading && mins < 570)) [y, m, d] = nextTradingDay(p.y, p.m, p.d);
    opensAt = etToUtc(y, m, d, 9, 30);
  }
  const hol = holidayName(p.y, p.m, p.d);
  const v = { state, open: state === 'open', opensAt, closesAt, holiday: hol, early: closeMin < 960 && trading, date: etDate(t) };
  marketMemo = { t, v }; return v;
}
// When does the current (or next) regular session end? Used for "until the close" games and day orders.
function sessionClose(t = Date.now()) {
  const mk = market(t); if (mk.closesAt) return mk.closesAt;
  const p = etParts(mk.opensAt); const c = earlyClose(p.y, p.m, p.d) ? 13 : 16;
  return etToUtc(p.y, p.m, p.d, c, 0);
}

// =====================================================================
// Assets and live prices
// =====================================================================
const NAMES = {
  AAPL: 'Apple', MSFT: 'Microsoft', NVDA: 'NVIDIA', GOOGL: 'Alphabet', AMZN: 'Amazon', META: 'Meta Platforms', TSLA: 'Tesla',
  AVGO: 'Broadcom', JPM: 'JPMorgan Chase', V: 'Visa', WMT: 'Walmart', LLY: 'Eli Lilly', XOM: 'Exxon Mobil', COST: 'Costco',
  NFLX: 'Netflix', AMD: 'AMD', LMT: 'Lockheed Martin', NOC: 'Northrop Grumman', DIS: 'Disney', KO: 'Coca-Cola',
  SPY: 'S&P 500 ETF', QQQ: 'Nasdaq 100 ETF', DIA: 'Dow Jones ETF', IWM: 'Russell 2000 ETF',
  BTC: 'Bitcoin', ETH: 'Ethereum', SOL: 'Solana', XRP: 'XRP', DOGE: 'Dogecoin', ADA: 'Cardano', AVAX: 'Avalanche',
  LINK: 'Chainlink', LTC: 'Litecoin', DOT: 'Polkadot',
};
const SECTORS = {
  AAPL: 'Technology', MSFT: 'Technology', NVDA: 'Semiconductors', GOOGL: 'Communication', AMZN: 'Retail', META: 'Communication',
  TSLA: 'Automobiles', AVGO: 'Semiconductors', JPM: 'Banking', V: 'Financial Services', WMT: 'Retail', LLY: 'Pharmaceuticals',
  XOM: 'Energy', COST: 'Retail', NFLX: 'Media', AMD: 'Semiconductors', LMT: 'Aerospace & Defense', NOC: 'Aerospace & Defense',
  DIS: 'Media', KO: 'Beverages',
};
const STOCK_DEFAULT = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'AVGO', 'JPM', 'V', 'WMT', 'LLY', 'XOM', 'COST', 'NFLX', 'AMD', 'LMT', 'NOC', 'DIS', 'KO'];
const INDEXES = ['SPY', 'QQQ', 'DIA', 'IWM'];
const CRYPTO_DEFAULT = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'LTC', 'DOT'];
const CG_IDS = { BTC: 'bitcoin', ETH: 'ethereum', SOL: 'solana', XRP: 'ripple', DOGE: 'dogecoin', ADA: 'cardano', AVAX: 'avalanche-2', LINK: 'chainlink', LTC: 'litecoin', DOT: 'polkadot' };

const assets = new Map(); // sym -> { sym, kind, name, logo, industry, cap, cg, exchange }
function addAsset(sym, kind, info = {}) {
  let a = assets.get(sym);
  if (!a) { a = { sym, kind, name: NAMES[sym] || sym, logo: null, industry: SECTORS[sym] || null, cap: null, cg: CG_IDS[sym] || null, exchange: null }; assets.set(sym, a); }
  for (const [k, v] of Object.entries(info)) if (v !== null && v !== undefined && v !== '') a[k] = v;
  if (kind) a.kind = kind;
  return a;
}
STOCK_DEFAULT.forEach(s => addAsset(s, 'stock')); INDEXES.forEach(s => addAsset(s, 'stock', { industry: 'Index fund' })); CRYPTO_DEFAULT.forEach(s => addAsset(s, 'crypto', { industry: 'Crypto' }));
const kindOf = s => assets.get(s)?.kind || (CRYPTO_DEFAULT.includes(s) ? 'crypto' : 'stock');
const isCrypto = s => kindOf(s) === 'crypto';
async function loadAssets() {
  for (const r of await rows('select m.*, e.kind ekind from meta m left join extra e on e.sym=m.sym')) {
    addAsset(r.sym, r.ekind || r.kind || kindOf(r.sym), { name: r.name, logo: r.logo, industry: r.industry, cap: r.cap == null ? null : +r.cap, cg: r.cg, exchange: r.exchange });
  }
  for (const r of await rows('select * from extra')) addAsset(r.sym, r.kind);
}
async function saveMeta(sym) {
  const a = assets.get(sym); if (!a) return;
  await Q(`insert into meta(sym,kind,name,logo,industry,cap,cg,exchange,updated) values($1,$2,$3,$4,$5,$6,$7,$8,now())
    on conflict(sym) do update set kind=excluded.kind,name=excluded.name,logo=excluded.logo,industry=excluded.industry,cap=excluded.cap,cg=excluded.cg,exchange=excluded.exchange,updated=now()`,
    [sym, a.kind, a.name, a.logo, a.industry, a.cap, a.cg, a.exchange]).catch(e => warn('meta save', e.message));
}

// ---------- the live price book ----------
const prices = new Map();   // sym -> { p, prev, t, src, wsT, qT, hi, lo, open }
const spark = new Map();    // sym -> [[minute, close], ...] for the last 24 hours
const dirty = new Set();
const feeds = { crypto: { ok: false, last: 0, err: null }, stocks: { ok: false, last: 0, err: null, ws: 0, polled: 0 } };
let engineHook = () => {};
function tick(sym, p, { prev, src = 'ws', hi, lo, open } = {}) {
  if (!(p > 0) || !Number.isFinite(p)) return;
  let q = prices.get(sym);
  if (!q) { q = { p, prev: null, t: 0, src }; prices.set(sym, q); }
  q.p = p; q.t = Date.now(); q.src = src;
  if (prev > 0) q.prev = prev;
  if (src === 'ws') q.wsT = q.t;
  if (hi > 0) q.hi = hi; else if (!q.hi || p > q.hi) q.hi = p;
  if (lo > 0) q.lo = lo; else if (!q.lo || p < q.lo) q.lo = p;
  if (open > 0) q.open = open;
  sparkPush(sym, p, q.t);
  dirty.add(sym);
  try { engineHook(sym, p, src); } catch (e) { warn('engine', e.message); }
}
function sparkPush(sym, p, t) {
  const m = Math.floor(t / 60000); let a = spark.get(sym);
  if (!a) { a = []; spark.set(sym, a); }
  const last = a[a.length - 1];
  if (last && last[0] === m) last[1] = p; else if (!last || m > last[0]) { a.push([m, p]); if (a.length > 1500) a.splice(0, a.length - 1440); }
}
function sparkFor(sym, points = 48) {
  const a = spark.get(sym); if (!a || a.length < 2) return null;
  const since = Math.floor(Date.now() / 60000) - 1440;
  const recent = a.filter(x => x[0] >= since); if (recent.length < 2) return null;
  if (recent.length <= points) return recent.map(x => round(x[1], 8));
  const out = [], step = (recent.length - 1) / (points - 1);
  for (let i = 0; i < points; i++) out.push(round(recent[Math.round(i * step)][1], 8));
  return out;
}
const priceOf = s => prices.get(s)?.p || null;
const fresh = (s, maxAge = 5 * 60e3) => { const q = prices.get(s); return !!q && q.src !== 'cache' && Date.now() - q.t < maxAge; };

async function loadPriceCache() {
  for (const r of await rows('select sym,p,prev,ts from prices')) prices.set(r.sym, { p: +r.p, prev: r.prev == null ? null : +r.prev, t: tsMs(r.ts), src: 'cache' });
}
async function savePriceCache() {
  const list = [...prices.entries()].filter(([, q]) => q.p > 0).map(([sym, q]) => ({ sym, p: q.p, prev: q.prev, ts: new Date(q.t || Date.now()).toISOString() }));
  if (!list.length) return;
  await Q(`insert into prices(sym,p,prev,ts) select sym,p,prev,ts from json_to_recordset($1::json) as x(sym text, p numeric, prev numeric, ts timestamptz)
    on conflict(sym) do update set p=excluded.p, prev=excluded.prev, ts=excluded.ts`, [JSON.stringify(list)]).catch(e => warn('price cache', e.message));
}

// ---------- which symbols matter right now ----------
const focus = new Map(); // sym -> last time someone looked at it
let userSyms = { pos: new Set(), orders: new Set(), alerts: new Set(), watch: new Set(), extra: new Set() };
async function refreshUserSyms() {
  const [p, o, l, a, w, e, pr] = await Promise.all([
    rows('select distinct p.sym from pos p join wallets w on w.id=p.wid where not w.archived'), rows('select distinct sym from ord'), rows('select distinct sym from lim'),
    rows('select distinct sym from alert where not fired'), rows('select sym from watch'), rows('select sym, kind from extra'), rows('select distinct sym from preds where result is null')]);
  userSyms = { pos: new Set(p.map(x => x.sym)), orders: new Set([...o, ...l].map(x => x.sym)), alerts: new Set([...a, ...pr].map(x => x.sym)), watch: new Set(w.map(x => x.sym)), extra: new Set(e.map(x => x.sym)) };
  for (const x of e) addAsset(x.sym, x.kind);
  for (const s of [...userSyms.pos, ...userSyms.orders, ...userSyms.alerts, ...userSyms.watch]) if (!assets.has(s)) addAsset(s, kindOf(s));
  cbSync(); fhSync();
}
function allTracked() { return new Set([...assets.keys()]); }
function stockPriority() {
  const now = Date.now(), out = [], seen = new Set();
  const add = s => { if (!seen.has(s) && kindOf(s) === 'stock') { seen.add(s); out.push(s); } };
  [...focus.entries()].filter(([, t]) => now - t < 3 * 60e3).sort((a, b) => b[1] - a[1]).forEach(([s]) => add(s));
  userSyms.pos.forEach(add); userSyms.orders.forEach(add); userSyms.alerts.forEach(add); add('SPY'); userSyms.watch.forEach(add);
  INDEXES.forEach(add); STOCK_DEFAULT.forEach(add); userSyms.extra.forEach(add); [...assets.keys()].forEach(add);
  return out;
}

// ---------- Coinbase (crypto, free, no key) ----------
let cbws = null, cbSubs = new Set(), cbBackoff = 1000, cbBad = new Set();
function cbConnect() {
  let ws;
  try { ws = cbws = new WebSocket('wss://ws-feed.exchange.coinbase.com'); } catch (e) { feeds.crypto.err = e.message; return setTimeout(cbConnect, cbBackoff); }
  ws.on('open', () => { cbBackoff = 1000; feeds.crypto.ok = true; feeds.crypto.err = null; cbSubs = new Set(); cbSync(); });
  ws.on('message', m => {
    let d; try { d = JSON.parse(m); } catch (e) { return; }
    if (d.type === 'ticker' && d.price) {
      const s = d.product_id.split('-')[0]; feeds.crypto.last = Date.now();
      tick(s, +d.price, { prev: +d.open_24h, hi: +d.high_24h, lo: +d.low_24h, open: +d.open_24h });
    } else if (d.type === 'error') {
      const bad = /([A-Z0-9]+)-USD/.exec(d.reason || d.message || '');
      if (bad) { cbBad.add(bad[1]); cbSubs.delete(bad[1]); }
      feeds.crypto.err = d.reason || d.message;
    }
  });
  ws.on('close', () => { feeds.crypto.ok = false; if (cbws === ws) setTimeout(cbConnect, cbBackoff = Math.min(cbBackoff * 2, 60000)); });
  ws.on('error', e => { feeds.crypto.err = e.message; try { ws.terminate(); } catch (_) {} });
}
function cbSync() {
  if (!cbws || cbws.readyState !== 1) return;
  const want = [...allTracked()].filter(s => isCrypto(s) && !cbBad.has(s));
  const add = want.filter(s => !cbSubs.has(s));
  for (const s of add) { cbws.send(JSON.stringify({ type: 'subscribe', product_ids: [s + '-USD'], channels: ['ticker'] })); cbSubs.add(s); }
}

// ---------- Finnhub (stocks, needs a free key) ----------
let fhws = null, fhSubs = new Set(), fhBackoff = 2000;
function fhConnect() {
  if (!FINNHUB_KEY) return;
  let ws;
  try { ws = fhws = new WebSocket('wss://ws.finnhub.io?token=' + encodeURIComponent(FINNHUB_KEY)); } catch (e) { return setTimeout(fhConnect, fhBackoff); }
  ws.on('open', () => { fhBackoff = 2000; feeds.stocks.ok = true; feeds.stocks.err = null; fhSubs = new Set(); fhSync(); });
  ws.on('message', m => {
    let d; try { d = JSON.parse(m); } catch (e) { return; }
    if (d.type === 'trade' && Array.isArray(d.data)) {
      feeds.stocks.last = Date.now();
      const latest = {}; for (const t of d.data) if (!latest[t.s] || t.t >= latest[t.s].t) latest[t.s] = t;
      for (const t of Object.values(latest)) tick(t.s, +t.p);
    } else if (d.type === 'error') feeds.stocks.err = d.msg || 'Finnhub error';
  });
  ws.on('close', (code) => { feeds.stocks.ok = false; if (fhws === ws) setTimeout(fhConnect, fhBackoff = Math.min(fhBackoff * 2, 120000)); });
  ws.on('error', e => { feeds.stocks.err = e.message; try { ws.terminate(); } catch (_) {} });
}
function fhSync() {
  if (!fhws || fhws.readyState !== 1) return;
  const want = new Set(stockPriority().slice(0, WS_MAX));
  for (const s of fhSubs) if (!want.has(s)) { fhws.send(JSON.stringify({ type: 'unsubscribe', symbol: s })); fhSubs.delete(s); }
  for (const s of want) if (!fhSubs.has(s)) { fhws.send(JSON.stringify({ type: 'subscribe', symbol: s })); fhSubs.add(s); }
  feeds.stocks.ws = fhSubs.size;
}

// Finnhub's free plan allows about 60 requests a minute. Everything goes through this queue.
const fhQueue = []; let fhTokens = 20, fhLast = Date.now(), fhPauseUntil = 0;
function fhApi(pathq, priority = 1) {
  if (!FINNHUB_KEY) return Promise.reject(new HttpError(503, 'Add FINNHUB_KEY in Railway to turn on stock data'));
  return new Promise((resolve, reject) => { fhQueue.push({ pathq, priority, resolve, reject, t: Date.now() }); fhQueue.sort((a, b) => a.priority - b.priority || a.t - b.t); });
}
setInterval(async () => {
  const now = Date.now(); fhTokens = Math.min(20, fhTokens + (now - fhLast) * 55 / 60000); fhLast = now;
  if (now < fhPauseUntil) return;
  while (fhTokens >= 1 && fhQueue.length) {
    const j = fhQueue.shift(); fhTokens -= 1;
    if (now - j.t > 30000) { j.reject(new HttpError(503, 'Stock data is busy right now. Try again shortly.')); continue; }
    const url = `https://finnhub.io/api/v1/${j.pathq}${j.pathq.includes('?') ? '&' : '?'}token=${encodeURIComponent(FINNHUB_KEY)}`;
    fetch(url, { signal: AbortSignal.timeout(9000) }).then(async r => {
      if (r.status === 429) { fhPauseUntil = Date.now() + 8000; fhQueue.unshift(j); return; }
      if (r.status === 401 || r.status === 403) return j.reject(new HttpError(502, 'Finnhub refused the request (this data may not be on the free plan)'));
      if (!r.ok) return j.reject(new HttpError(502, 'Finnhub error ' + r.status));
      j.resolve(await r.json());
    }).catch(e => j.reject(new HttpError(502, 'Could not reach Finnhub')));
  }
}, 120);

// Quote poller: fills prices for stocks that the live stream isn't covering, plus the previous close.
let pollCursor = 0;
async function pollOneQuote() {
  if (!FINNHUB_KEY || fhQueue.length > 6) return;
  const mk = market(), now = Date.now();
  const list = stockPriority(); if (!list.length) return;
  const nonLive = list.filter(s => !(prices.get(s)?.wsT > now - 90e3)).length;
  const liveEvery = Math.max(25e3, nonLive * 2500);
  for (let i = 0; i < list.length; i++) {
    const s = list[(pollCursor + i) % list.length], q = prices.get(s), age = now - (q?.qT || 0);
    let due;
    if (!q || !q.prev || q.src === 'cache') due = age > 15e3;
    else if (mk.state === 'open') due = (q.wsT > now - 90e3) ? age > 30 * 60e3 : age > liveEvery;
    else due = age > (mk.state === 'closed' ? 30 : 10) * 60e3;
    if (!due) continue;
    pollCursor = (pollCursor + i + 1) % list.length;
    if (q) q.qT = now; else prices.set(s, { p: 0, prev: null, t: 0, src: 'none', qT: now });
    try {
      const d = await fhApi('quote?symbol=' + encodeURIComponent(s), 2);
      if (!(d && d.c > 0)) return;
      feeds.stocks.polled = Date.now();
      const cur = prices.get(s);
      if (cur && cur.wsT > Date.now() - 90e3) { cur.prev = d.pc || cur.prev; cur.hi = d.h || cur.hi; cur.lo = d.l || cur.lo; cur.open = d.o || cur.open; cur.qT = Date.now(); dirty.add(s); }
      else { tick(s, d.c, { prev: d.pc, hi: d.h, lo: d.l, open: d.o, src: 'rest' }); prices.get(s).qT = Date.now(); }
    } catch (e) { /* try again later */ }
    return;
  }
}

// =====================================================================
// Market data sources (cached so the free plans are never overwhelmed)
// =====================================================================
const cache = new Map();
// Cached with "stale while refreshing": a slightly old answer comes back instantly while a new one loads.
async function cached(key, ttl, fn) {
  const c = cache.get(key), now = Date.now();
  const refresh = () => {
    const ent = cache.get(key) || {};
    if (ent.p) return ent.p;
    const p = (async () => fn())().then(v => { cache.set(key, { t: Date.now(), v, ttl }); return v; }, e => {
      const cur = cache.get(key);
      if (cur && cur.v !== undefined) { cache.set(key, { t: Date.now() - ttl + 30000, v: cur.v, ttl }); return cur.v; }
      cache.delete(key); throw e;
    });
    cache.set(key, { ...ent, p }); return p;
  };
  if (c && c.v !== undefined) {
    if (now - c.t < ttl) return c.v;
    if (now - c.t < ttl * 10) { refresh().catch(() => {}); return c.v; }
  }
  return refresh();
}
setInterval(() => { const now = Date.now(); for (const [k, c] of cache) if (c.t && now - c.t > (c.ttl || 0) * 10 + 3600e3) cache.delete(k); }, 30 * 60e3);
async function getJ(url, { timeout = 9000, headers = {} } = {}) {
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Mango Paper Trading)', Accept: 'application/json', ...headers }, signal: AbortSignal.timeout(timeout) });
  if (!r.ok) throw new HttpError(502, `Data source error ${r.status}`);
  return r.json();
}
const yahooSym = s => isCrypto(s) ? s + '-USD' : s.replace('.', '-');
async function yahooChart(sym, qs) {
  let last;
  for (const host of ['query1', 'query2']) {
    try {
      const d = await getJ(`https://${host}.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSym(sym))}?${qs}`);
      const r = d?.chart?.result?.[0]; if (!r) throw new HttpError(502, 'No chart data');
      return r;
    } catch (e) { last = e; }
  }
  throw last;
}
const CBX = 'https://api.exchange.coinbase.com';
const CG = 'https://api.coingecko.com/api/v3';

// ---------- candles ----------
const RANGES = {
  '1D': { y: 'range=1d&interval=5m', cb: [300, 86400], secs: 86400 },
  '1W': { y: 'range=5d&interval=15m', cb: [3600, 7 * 86400], secs: 7 * 86400 },
  '1M': { y: 'range=1mo&interval=60m', cb: [21600, 30 * 86400], secs: 30 * 86400 },
  '3M': { y: 'range=3mo&interval=1d', cb: [86400, 90 * 86400], secs: 90 * 86400 },
  '1Y': { y: 'range=1y&interval=1d', cb: [86400, 365 * 86400], secs: 365 * 86400 },
  '5Y': { y: 'range=5y&interval=1wk', cb: null, secs: 5 * 365 * 86400 },
  'ALL': { y: 'range=max&interval=1mo', cb: null, secs: 30 * 365 * 86400 },
};
async function coinbaseCandles(sym, gran, span) {
  const end = Date.now(), out = [];
  const per = gran * 1000 * 290;
  for (let s = end - span * 1000; s < end; s += per) {
    const e = Math.min(end, s + per);
    const d = await getJ(`${CBX}/products/${sym}-USD/candles?granularity=${gran}&start=${new Date(s).toISOString()}&end=${new Date(e).toISOString()}`);
    if (Array.isArray(d)) for (const x of d) out.push({ t: x[0], l: +x[1], h: +x[2], o: +x[3], c: +x[4], v: +x[5] });
  }
  const seen = new Set();
  return out.filter(x => !seen.has(x.t) && seen.add(x.t)).sort((a, b) => a.t - b.t);
}
function yahooToCandles(r) {
  const z = r.indicators?.quote?.[0] || {}, ts = r.timestamp || [];
  const out = [];
  for (let i = 0; i < ts.length; i++) {
    const c = z.close?.[i], o = z.open?.[i];
    if (c == null || o == null) continue;
    out.push({ t: ts[i], o, h: z.high?.[i] ?? Math.max(o, c), l: z.low?.[i] ?? Math.min(o, c), c, v: z.volume?.[i] || 0 });
  }
  return out;
}
async function candles(sym, range) {
  const g = RANGES[range] || RANGES['1D'];
  return cached(`cd:${sym}:${range}`, range === '1D' ? 45e3 : range === '1W' ? 5 * 60e3 : 30 * 60e3, async () => {
    let list = null, prevClose = null, src = null, errors = [];
    if (isCrypto(sym) && g.cb) {
      try { list = await coinbaseCandles(sym, g.cb[0], g.cb[1]); src = 'coinbase'; } catch (e) { errors.push(e.message); }
    }
    if (!list || list.length < 2) {
      try {
        const r = await yahooChart(sym, g.y);
        list = yahooToCandles(r); src = 'yahoo';
        prevClose = r.meta?.chartPreviousClose ?? r.meta?.previousClose ?? null;
      } catch (e) { errors.push(e.message); }
    }
    if ((!list || list.length < 2) && isCrypto(sym) && !g.cb) {
      try { list = await coinbaseCandles(sym, 86400, Math.min(g.secs, 3 * 365 * 86400)); src = 'coinbase'; } catch (e) { errors.push(e.message); }
    }
    if (!list || list.length < 2) {
      // Last resort: the minute-by-minute prices this server has recorded itself.
      const a = spark.get(sym);
      if (a && a.length > 5 && range === '1D') { list = a.map(x => ({ t: x[0] * 60, o: x[1], h: x[1], l: x[1], c: x[1], v: 0 })); src = 'live'; }
      else throw new HttpError(502, 'Price history is not available for ' + sym + ' right now');
    }
    if (range === '1D' && isCrypto(sym)) prevClose = list[0]?.o ?? null;
    if (range === '1D' && !isCrypto(sym) && list.length) {
      // Keep only the latest session for the one-day view.
      const lastDay = etDate(list[list.length - 1].t * 1000); list = list.filter(x => etDate(x.t * 1000) === lastDay);
    }
    return { candles: list, prevClose, src };
  });
}
// Fill the little sparkline history after a restart so lists aren't empty.
const sparkTried = new Map(); const sparkQueue = [];
function wantSpark(sym) {
  const a = spark.get(sym); if (a && a.length >= 20) return;
  const t = sparkTried.get(sym) || 0; if (Date.now() - t < 20 * 60e3 || sparkQueue.includes(sym)) return;
  sparkTried.set(sym, Date.now()); sparkQueue.push(sym);
}
setInterval(async () => {
  const sym = sparkQueue.shift(); if (!sym) return;
  try {
    const { candles: cs } = await candles(sym, '1D');
    const a = spark.get(sym) || []; const have = new Set(a.map(x => x[0]));
    for (const c of cs) { const m = Math.floor(c.t / 60); if (!have.has(m)) a.push([m, c.c]); }
    a.sort((x, y) => x[0] - y[0]); spark.set(sym, a.slice(-1440));
  } catch (e) { /* quiet */ }
}, 1500);

// ---------- search ----------
async function coinbaseProducts() {
  return cached('cb:products', 6 * 3600e3, async () => {
    const d = await getJ(CBX + '/products');
    return d.filter(p => p.quote_currency === 'USD' && p.status === 'online' && !p.trading_disabled).map(p => p.base_currency);
  });
}
async function search(q) {
  const t = String(q || '').trim(); if (!t) return [];
  const T = t.toUpperCase(), out = [], seen = new Set();
  const push = x => { if (!seen.has(x.sym)) { seen.add(x.sym); out.push(x); } };
  for (const a of assets.values()) if (a.sym === T || a.sym.startsWith(T) || (a.name || '').toUpperCase().includes(T)) push({ sym: a.sym, name: a.name, kind: a.kind, logo: a.logo });
  await Promise.all([
    coinbaseProducts().then(list => {
      for (const b of list) if (b === T || (T.length >= 2 && b.startsWith(T))) push({ sym: b, name: NAMES[b] || b, kind: 'crypto' });
    }).catch(() => {}),
    FINNHUB_KEY ? cached('fhs:' + T, 3600e3, () => fhApi('search?q=' + encodeURIComponent(t), 0)).then(d => {
      for (const x of d.result || []) if (/Common Stock|ETP|ETF|ADR/.test(x.type || '') && !x.symbol.includes('.') && !x.symbol.includes(':')) push({ sym: x.symbol, name: titleCase(x.description), kind: 'stock' });
    }).catch(() => {}) : null,
  ]);
  const score = x => (x.sym === T ? 0 : x.sym.startsWith(T) ? 1 : 2) + (assets.has(x.sym) ? -0.5 : 0) + x.sym.length / 100;
  return out.sort((a, b) => score(a) - score(b)).slice(0, 20);
}
const exchangeName = e => { e = String(e || ''); if (/NASDAQ/i.test(e)) return 'Nasdaq'; if (/ARCA/i.test(e)) return 'NYSE Arca'; if (/AMERICAN|AMEX/i.test(e)) return 'NYSE American'; if (/NEW YORK STOCK EXCHANGE|NYSE/i.test(e)) return 'NYSE'; if (/CBOE|BATS/i.test(e)) return 'Cboe'; if (/OTC/i.test(e)) return 'OTC markets'; return titleCase(e.replace(/,.*$/, '')); };
const titleCase = s => String(s || '').toLowerCase().replace(/\b([a-z])/g, c => c.toUpperCase()).replace(/\b(Inc|Corp|Co|Ltd|Plc|Etf|Adr)\b/g, w => w === 'Etf' || w === 'Adr' ? w.toUpperCase() : w);
async function trackAsset(sym, kind) {
  sym = cleanSym(sym); if (!sym) fail('That symbol does not look right');
  kind = kind === 'crypto' ? 'crypto' : 'stock';
  if (!assets.has(sym) || kindOf(sym) !== kind) addAsset(sym, kind);
  if (!fresh(sym, 10 * 60e3)) {
    if (kind === 'crypto') {
      const [t, s] = await Promise.all([getJ(`${CBX}/products/${sym}-USD/ticker`), getJ(`${CBX}/products/${sym}-USD/stats`)]).catch(() => fail(sym + ' is not available on Coinbase in US dollars', 404));
      tick(sym, +t.price, { prev: +s.open, hi: +s.high, lo: +s.low, open: +s.open, src: 'rest' });
    } else {
      const d = await fhApi('quote?symbol=' + encodeURIComponent(sym), 0);
      if (!(d && d.c > 0)) fail('No price found for ' + sym, 404);
      tick(sym, d.c, { prev: d.pc, hi: d.h, lo: d.l, open: d.o, src: 'rest' }); prices.get(sym).qT = Date.now();
    }
  }
  if (!STOCK_DEFAULT.includes(sym) && !CRYPTO_DEFAULT.includes(sym) && !INDEXES.includes(sym)) await Q('insert into extra(sym,kind) values($1,$2) on conflict(sym) do update set kind=excluded.kind', [sym, kind]);
  focus.set(sym, Date.now());
  ensureMeta(sym).catch(() => {});
  await refreshUserSyms();
  return assets.get(sym);
}

// ---------- names, logos, sectors ----------
const metaTried = new Map();
async function ensureMeta(sym) {
  const a = assets.get(sym); if (!a) return null;
  if (a.logo && a.industry && (a.cap || a.kind === 'crypto')) return a;
  if (Date.now() - (metaTried.get(sym) || 0) < 6 * 3600e3) return a;
  metaTried.set(sym, Date.now());
  if (a.kind === 'stock' && FINNHUB_KEY) {
    const p = await cached('prof:' + sym, 24 * 3600e3, () => fhApi('stock/profile2?symbol=' + encodeURIComponent(sym), 1)).catch(() => null);
    if (p && p.name) { if (!NAMES[sym]) a.name = p.name.replace(/[,\s]+(Inc|Corp|Corporation|Co|Ltd|Plc|Holdings|Class [A-C])\.?$/i, '').trim() || a.name; a.logo = p.logo || a.logo; a.industry = p.finnhubIndustry || a.industry; a.cap = p.marketCapitalization ? p.marketCapitalization * 1e6 : a.cap; a.exchange = p.exchange ? exchangeName(p.exchange) : a.exchange; }
  } else if (a.kind === 'crypto') {
    const id = await cgId(sym).catch(() => null);
    if (id) { a.cg = id; const m = await cgMarkets([id]).catch(() => null); const x = m?.[0]; if (x) { a.logo = x.image || a.logo; a.cap = x.market_cap || a.cap; if (!NAMES[sym]) a.name = x.name || a.name; } }
  }
  await saveMeta(sym); return a;
}
async function cgId(sym) {
  const a = assets.get(sym); if (a?.cg) return a.cg; if (CG_IDS[sym]) return CG_IDS[sym];
  const d = await cached('cgs:' + sym, 7 * 86400e3, () => getJ(`${CG}/search?query=${encodeURIComponent(sym)}`));
  const c = (d.coins || []).filter(x => (x.symbol || '').toUpperCase() === sym).sort((a, b) => (a.market_cap_rank || 1e9) - (b.market_cap_rank || 1e9))[0];
  return c?.id || null;
}
async function cgMarkets(ids) {
  const key = ids.slice().sort().join(',');
  return cached('cgm:' + key, 5 * 60e3, () => getJ(`${CG}/coins/markets?vs_currency=usd&ids=${encodeURIComponent(key)}&price_change_percentage=7d,30d,1y`));
}

// ---------- one asset's details ----------
async function assetDetails(sym) {
  const a = assets.get(sym) || addAsset(sym, kindOf(sym));
  focus.set(sym, Date.now()); fhSync();
  await ensureMeta(sym).catch(() => {});
  const q = prices.get(sym) || {};
  const out = { sym, kind: a.kind, name: a.name, logo: a.logo, industry: a.industry, exchange: a.exchange, stats: [], about: null, links: [], ext: null };
  const stat = (label, value, fmt = 'money', hint) => { if (value !== null && value !== undefined && value !== '' && !(typeof value === 'number' && !Number.isFinite(value))) out.stats.push({ label, value, fmt, hint }); };
  if (a.kind === 'crypto') {
    const [st, cgm] = await Promise.all([
      cached('cbs:' + sym, 30e3, () => getJ(`${CBX}/products/${sym}-USD/stats`)).catch(() => null),
      (async () => { const id = await cgId(sym); return id ? (await cgMarkets([id]))[0] : null; })().catch(() => null),
    ]);
    stat('Open (24h)', st ? +st.open : q.open); stat('High (24h)', st ? +st.high : q.hi); stat('Low (24h)', st ? +st.low : q.lo);
    if (cgm) {
      stat('Market cap', cgm.market_cap, 'cap'); stat('Rank', cgm.market_cap_rank ? '#' + cgm.market_cap_rank : null, 'text');
      stat('Volume (24h)', cgm.total_volume, 'cap'); stat('Circulating supply', cgm.circulating_supply, 'big');
      stat('Max supply', cgm.max_supply, 'big'); stat('All-time high', cgm.ath, 'money', cgm.ath_date ? 'Reached ' + new Date(cgm.ath_date).toLocaleDateString('en-US', { month: 'short', year: 'numeric' }) : null);
      stat('From all-time high', cgm.ath_change_percentage, 'pct'); stat('7-day change', cgm.price_change_percentage_7d_in_currency, 'pct');
      stat('30-day change', cgm.price_change_percentage_30d_in_currency, 'pct'); stat('1-year change', cgm.price_change_percentage_1y_in_currency, 'pct');
    } else if (st) stat('Volume (24h)', +st.volume * (q.p || 0), 'cap');
    try {
      const id = await cgId(sym);
      if (id) {
        const info = await cached('cgi:' + id, 24 * 3600e3, () => getJ(`${CG}/coins/${id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false`));
        const en = (info.description?.en || '').replace(/<[^>]+>/g, '').trim();
        if (en) out.about = en.split(/(?<=\.)\s+/).slice(0, 4).join(' ');
        if (info.links?.homepage?.[0]) out.links.push({ label: 'Website', url: info.links.homepage[0] });
        if (info.genesis_date) stat('Launched', info.genesis_date.slice(0, 4), 'text');
        if (info.categories?.length) out.tags = info.categories.filter(Boolean).slice(0, 4);
      }
    } catch (e) {}
  } else {
    const [prof, met, ych] = await Promise.all([
      FINNHUB_KEY ? cached('prof:' + sym, 24 * 3600e3, () => fhApi('stock/profile2?symbol=' + encodeURIComponent(sym), 0)).catch(() => null) : null,
      FINNHUB_KEY ? cached('met:' + sym, 6 * 3600e3, () => fhApi('stock/metric?symbol=' + encodeURIComponent(sym) + '&metric=all', 0)).catch(() => null) : null,
      S.f_ext ? cached('ext:' + sym, 60e3, () => yahooChart(sym, 'range=1d&interval=2m&includePrePost=true')).catch(() => null) : null,
    ]);
    const m = met?.metric || {};
    stat('Open', q.open); stat('Day high', q.hi); stat('Day low', q.lo); stat('Previous close', q.prev);
    stat('52-week high', m['52WeekHigh'] ?? ych?.meta?.fiftyTwoWeekHigh); stat('52-week low', m['52WeekLow'] ?? ych?.meta?.fiftyTwoWeekLow);
    stat('Market cap', prof?.marketCapitalization ? prof.marketCapitalization * 1e6 : a.cap, 'cap');
    stat('Volume', ych?.meta?.regularMarketVolume, 'big'); stat('Avg volume (10 day)', m['10DayAverageTradingVolume'] ? m['10DayAverageTradingVolume'] * 1e6 : null, 'big');
    stat('P/E ratio', m.peTTM ?? m.peBasicExclExtraTTM, 'num', 'Price compared to the last year of earnings'); stat('EPS (TTM)', m.epsTTM ?? m.epsBasicExclExtraItemsTTM, 'money');
    stat('Dividend yield', m.dividendYieldIndicatedAnnual ?? m.currentDividendYieldTTM, 'pct0'); stat('Beta', m.beta, 'num', 'How much it moves compared to the whole market');
    stat('52-week change', m['52WeekPriceReturnDaily'], 'pct'); stat('Profit margin', m.netProfitMarginTTM, 'pct0'); stat('Revenue growth (TTM)', m.revenueGrowthTTMYoy, 'pct');
    if (prof) {
      if (prof.weburl) out.links.push({ label: 'Website', url: prof.weburl });
      stat('Exchange', prof.exchange ? exchangeName(prof.exchange) : null, 'text'); stat('Listed since', prof.ipo?.slice(0, 4), 'text');
      stat('Country', prof.country, 'text'); stat('Shares outstanding', prof.shareOutstanding ? prof.shareOutstanding * 1e6 : null, 'big');
      out.about = `${a.name} is a ${prof.finnhubIndustry ? prof.finnhubIndustry.toLowerCase() + ' ' : ''}company${prof.country ? ' based in ' + (prof.country === 'US' ? 'the United States' : prof.country) : ''}, listed on the ${prof.exchange ? exchangeName(prof.exchange) : 'stock market'}${prof.ipo ? ' since ' + prof.ipo.slice(0, 4) : ''}.`;
    }
    // Pre-market / after-hours price, when the regular session is closed.
    if (ych && S.f_ext && market().state !== 'open') {
      const z = ych.indicators?.quote?.[0]?.close || [], ts = ych.timestamp || [], reg = ych.meta?.currentTradingPeriod?.regular;
      let lastI = -1; for (let i = ts.length - 1; i >= 0; i--) if (z[i] != null) { lastI = i; break; }
      if (lastI >= 0 && reg && (ts[lastI] > reg.end || ts[lastI] < reg.start)) {
        const base = ts[lastI] > reg.end ? (ych.meta.regularMarketPrice || q.p) : (ych.meta.chartPreviousClose || q.prev);
        out.ext = { price: z[lastI], label: ts[lastI] > reg.end ? 'After hours' : 'Pre-market', t: ts[lastI] * 1000, change: base ? (z[lastI] / base - 1) * 100 : null };
      }
    }
  }
  return out;
}
async function assetInsights(sym) {
  if (isCrypto(sym)) return {};
  return cached('ins:' + sym, 6 * 3600e3, async () => {
    const today = etDate(), later = etDate(Date.now() + 100 * 86400e3);
    const [rec, eps, cal, peers, divs] = await Promise.all([
      fhApi('stock/recommendation?symbol=' + encodeURIComponent(sym), 1).catch(() => null),
      fhApi('stock/earnings?symbol=' + encodeURIComponent(sym), 1).catch(() => null),
      fhApi(`calendar/earnings?from=${today}&to=${later}&symbol=${encodeURIComponent(sym)}`, 1).catch(() => null),
      fhApi('stock/peers?symbol=' + encodeURIComponent(sym), 1).catch(() => null),
      yahooChart(sym, 'range=5y&interval=3mo&events=div').then(r => Object.values(r.events?.dividends || {})).catch(() => null),
    ]);
    const next = (cal?.earningsCalendar || []).filter(x => x.date >= today).sort((a, b) => a.date.localeCompare(b.date))[0] || null;
    return {
      ratings: Array.isArray(rec) && rec[0] ? { period: rec[0].period, strongBuy: rec[0].strongBuy, buy: rec[0].buy, hold: rec[0].hold, sell: rec[0].sell, strongSell: rec[0].strongSell } : null,
      earnings: Array.isArray(eps) ? eps.slice(0, 6).reverse().map(x => ({ period: x.period, actual: x.actual, estimate: x.estimate, surprise: x.surprisePercent, q: x.quarter, y: x.year })) : [],
      next: next ? { date: next.date, hour: next.hour, eps: next.epsEstimate, revenue: next.revenueEstimate } : null,
      peers: Array.isArray(peers) ? peers.filter(p => p !== sym && !p.includes('.')).slice(0, 8) : [],
      dividends: (divs || []).sort((a, b) => b.date - a.date).slice(0, 12).map(d => ({ date: d.date * 1000, amount: d.amount })),
    };
  });
}
async function orderBook(sym) {
  if (!isCrypto(sym)) return null;
  return cached('book:' + sym, 4000, async () => {
    const d = await getJ(`${CBX}/products/${sym}-USD/book?level=2`);
    const g = (side, n = 12) => (d[side] || []).slice(0, n).map(x => [+x[0], +x[1]]);
    const bids = g('bids'), asks = g('asks');
    return { bids, asks, spread: asks[0] && bids[0] ? asks[0][0] - bids[0][0] : null };
  });
}
const newsItem = x => ({ id: x.id, t: x.datetime * 1000, title: x.headline, summary: (x.summary || '').slice(0, 280), source: x.source, url: x.url, image: x.image || null, related: x.related || null });
async function assetNews(sym) {
  if (isCrypto(sym)) {
    const all = await marketNews('crypto').catch(() => []);
    const name = (assets.get(sym)?.name || sym).toLowerCase();
    const hit = all.filter(n => (n.title + ' ' + n.summary).toLowerCase().includes(name) || new RegExp(`\\b${sym}\\b`).test(n.title + ' ' + n.summary));
    return hit.slice(0, 12);
  }
  return cached('news:' + sym, 15 * 60e3, async () => {
    const d = await fhApi(`company-news?symbol=${encodeURIComponent(sym)}&from=${etDate(Date.now() - 7 * 86400e3)}&to=${etDate()}`, 1);
    const seen = new Set();
    return (Array.isArray(d) ? d : []).filter(x => x.headline && !seen.has(x.headline) && seen.add(x.headline)).slice(0, 15).map(newsItem);
  });
}
async function marketNews(cat = 'general') {
  return cached('mnews:' + cat, 10 * 60e3, async () => {
    const d = await fhApi('news?category=' + (cat === 'crypto' ? 'crypto' : 'general'), 1);
    return (Array.isArray(d) ? d : []).slice(0, 30).map(newsItem);
  });
}
async function earningsWeek() {
  return cached('earn:week', 3 * 3600e3, async () => {
    const from = etDate(), to = etDate(Date.now() + 8 * 86400e3);
    const d = await fhApi(`calendar/earnings?from=${from}&to=${to}`, 1);
    return (d?.earningsCalendar || []).map(x => ({ sym: x.symbol, date: x.date, hour: x.hour, eps: x.epsEstimate, revenue: x.revenueEstimate }));
  });
}
async function fearGreed() {
  return cached('fng', 30 * 60e3, async () => {
    const d = await getJ('https://api.alternative.me/fng/?limit=8');
    return (d.data || []).map(x => ({ value: +x.value, label: x.value_classification, t: +x.timestamp * 1000 }));
  });
}
async function cryptoGlobal() {
  return cached('cgglobal', 15 * 60e3, async () => {
    const d = (await getJ(`${CG}/global`)).data;
    return { btcDom: d.market_cap_percentage?.btc, ethDom: d.market_cap_percentage?.eth, cap: d.total_market_cap?.usd, capChange: d.market_cap_change_percentage_24h_usd };
  });
}

// =====================================================================
// Wallets and trading
// =====================================================================
const qtyDp = sym => isCrypto(sym) ? 8 : 6;
async function walletRow(wid, c = null) {
  const r = (await (c || pool).query('select * from wallets where id=$1', [wid])).rows[0];
  if (!r) fail('That wallet does not exist', 404);
  return r;
}
async function activeWalletId(req) {
  const w = +(req.query.w || req.body?.w || S.defaultWallet || 1);
  const r = await row('select id from wallets where id=$1', [w]);
  if (r) return r.id;
  return (await row('select id from wallets where not archived order by sort, id limit 1'))?.id || 1;
}
const walletJson = w => ({ id: w.id, name: w.name, emoji: w.emoji, color: w.color, cash: +w.cash, start: +w.start_bal, created: tsMs(w.created), seasonStart: tsMs(w.season_start), archived: w.archived, sort: w.sort });
async function positions(wid, c = null) {
  return ((await (c || pool).query('select * from pos where wid=$1 order by sym', [wid])).rows).map(p => ({ sym: p.sym, qty: +p.qty, avg: +p.avg, since: tsMs(p.since) }));
}
const marketValue = pos => pos.reduce((t, x) => t + x.qty * (priceOf(x.sym) || x.avg), 0);

const firing = new Set();
const fillCbs = []; // functions to run after every trade
// The heart of the app: buys and sells, with every rule from Settings applied.
async function execTrade(wid, sym, side, o = {}) {
  sym = cleanSym(sym); if (!sym) fail('Pick something to trade');
  if (side !== 'buy' && side !== 'sell') fail('Choose buy or sell');
  const q = prices.get(sym); const base = o.px || q?.p;
  if (!(base > 0)) fail(`There's no live price for ${sym} yet. Try again in a few seconds.`);
  const slip = S.slippage / 100, px = side === 'buy' ? base * (1 + slip) : base * (1 - slip);
  const dp = qtyDp(sym), stockWhole = S.wholeShares && !isCrypto(sym);
  const fill = await tx(async c => {
    const w = (await c.query('select * from wallets where id=$1 for update', [wid])).rows[0];
    if (!w) fail('That wallet does not exist', 404);
    if (w.archived) fail('That wallet is archived. Restore it first.');
    const cash = +w.cash;
    const pr = (await c.query('select * from pos where wid=$1 and sym=$2 for update', [wid, sym])).rows[0];
    const pos = pr ? { qty: +pr.qty, avg: +pr.avg } : null;
    let qty = num(o.qty), pnl = null, fee = 0, newCash = cash, closed = false;
    if (side === 'buy') {
      if (!(qty > 0) && o.usd > 0) qty = o.usd / px;
      qty = stockWhole ? Math.floor(qty + 1e-9) : round(qty, dp);
      if (!(qty > 0)) fail(stockWhole ? `That's less than one share of ${sym}. Whole shares only is on in Settings.` : 'Enter an amount to buy');
      const cost = qty * px; fee = round(S.feeFlat + cost * S.feePct / 100, 6);
      if (cost + fee > cash + 0.005) fail(`Not enough cash. This needs ${money(cost + fee)}${fee ? ' including the ' + money(fee) + ' fee' : ''} and you have ${money(cash)}.`);
      const all = (await c.query('select sym, qty, avg from pos where wid=$1', [wid])).rows.map(x => ({ sym: x.sym, qty: +x.qty, avg: +x.avg }));
      const equity = cash + marketValue(all);
      if (S.maxPos > 0 && ((pos ? pos.qty : 0) + qty) * px > equity * S.maxPos / 100 + 0.01) fail(`One position can be at most ${S.maxPos}% of this wallet (${money(equity * S.maxPos / 100)}). Change this in Settings → Trading.`);
      if (S.reservePct > 0 && cash - cost - fee < equity * S.reservePct / 100 - 0.01) fail(`That would leave less than your ${S.reservePct}% cash reserve (${money(equity * S.reservePct / 100)}).`);
      const nq = (pos ? pos.qty : 0) + qty;
      const avg = ((pos ? pos.qty * pos.avg : 0) + cost + fee) / nq;
      newCash = Math.max(0, round(cash - cost - fee, 6));
      await c.query('update wallets set cash=$1 where id=$2', [newCash, wid]);
      await c.query(`insert into pos(wid,sym,qty,avg,since) values($1,$2,$3,$4,now()) on conflict(wid,sym) do update set qty=$3, avg=$4`, [wid, sym, nq, avg]);
    } else {
      if (!pos) fail(`You don't own any ${sym} in this wallet`);
      if (!(qty > 0) && o.pct > 0) qty = o.pct >= 100 ? pos.qty : pos.qty * Math.min(100, o.pct) / 100;
      if (!(qty > 0) && o.usd > 0) qty = o.usd / px;
      if (qty >= pos.qty * 0.99999 || qty > pos.qty) qty = pos.qty;
      qty = qty === pos.qty ? qty : (stockWhole && Number.isInteger(pos.qty) ? Math.floor(qty) : round(qty, dp));
      if (!(qty > 0)) fail('Enter an amount to sell');
      const proceeds = qty * px; fee = round(S.feeFlat + proceeds * S.feePct / 100, 6);
      pnl = round((px - pos.avg) * qty - fee, 6);
      newCash = round(cash + Math.max(0, proceeds - fee), 6);
      await c.query('update wallets set cash=$1 where id=$2', [newCash, wid]);
      const left = round(pos.qty - qty, dp);
      if (left <= 0) {
        closed = true;
        await c.query('delete from pos where wid=$1 and sym=$2', [wid, sym]);
        await c.query('delete from ord where wid=$1 and sym=$2', [wid, sym]);
        await c.query("delete from lim where wid=$1 and sym=$2 and side='sell'", [wid, sym]);
      } else await c.query('update pos set qty=$1 where wid=$2 and sym=$3', [left, wid, sym]);
    }
    const t = (await c.query('insert into trd(wid,sym,side,qty,price,why,pnl,fee,note) values($1,$2,$3,$4,$5,$6,$7,$8,$9) returning id, ts',
      [wid, sym, side, qty, px, o.why || 'manual', pnl, fee, o.note ? String(o.note).slice(0, 500) : null])).rows[0];
    return { id: t.id, ts: tsMs(t.ts), wid, sym, side, qty, price: px, fee, pnl, pnlPct: pnl != null && pos ? pnl / (pos.avg * qty) * 100 : null, cash: newCash, closed, stale: q?.src === 'cache' || (q && Date.now() - q.t > 10 * 60e3) };
  });
  for (const cb of fillCbs) try { await cb(fill); } catch (e) { warn('after trade', e.message); }
  return fill;
}

// ---------- automatic orders (stop loss, take profit, trailing stop, limit, stop, queued) ----------
const condOrders = new Map(), limOrders = new Map();
async function loadOrders() {
  condOrders.clear(); limOrders.clear();
  for (const r of await rows('select * from ord order by id')) {
    const x = { id: r.id, wid: r.wid, sym: r.sym, kind: r.kind, price: +r.price, pct: +r.pct || 100, trail: r.trail == null ? null : +r.trail, peak: r.peak == null ? null : +r.peak, created: tsMs(r.created) };
    if (x.kind === 'trail') x.peak = Math.max(x.peak || 0, priceOf(x.sym) || 0);
    (condOrders.get(x.sym) || condOrders.set(x.sym, []).get(x.sym)).push(x);
  }
  for (const r of await rows('select * from lim order by id')) {
    const x = { id: r.id, wid: r.wid, sym: r.sym, side: r.side, kind: r.kind || 'limit', price: +r.price, amt: +r.amt, units: r.units || 'auto', sl: r.sl == null ? null : +r.sl, tp: r.tp == null ? null : +r.tp, expires: tsMs(r.expires), ts: tsMs(r.ts) };
    (limOrders.get(x.sym) || limOrders.set(x.sym, []).get(x.sym)).push(x);
  }
}
const orderJson = o => ({ ...o, stop: o.kind === 'trail' ? o.peak * (1 - o.trail / 100) : o.price });
async function addCondOrder(wid, sym, kind, { price, pct = 100, trail = null }) {
  const p = priceOf(sym); if (!p) fail('No live price yet for ' + sym);
  if (!(await row('select 1 from pos where wid=$1 and sym=$2', [wid, sym]))) fail(`Buy ${sym} first, then add a stop loss or take profit`);
  pct = clamp(num(pct, 100), 1, 100);
  if (kind === 'trail') { trail = num(trail); if (!(trail >= 0.1 && trail <= 50)) fail('The trailing distance must be between 0.1% and 50%'); price = p * (1 - trail / 100); }
  else if (kind === 'stop') { if (!(price > 0 && price < p)) fail(`A stop loss has to be below the current price (${pxStr(p)})`); }
  else if (kind === 'take') { if (!(price > p)) fail(`A take profit has to be above the current price (${pxStr(p)})`); }
  else fail('Unknown order type');
  const same = kind === 'take' ? ['take'] : ['stop', 'trail'];
  if (pct >= 100) await Q('delete from ord where wid=$1 and sym=$2 and kind = any($3::text[])', [wid, sym, same]);
  const r = await row('insert into ord(wid,sym,kind,price,pct,trail,peak) values($1,$2,$3,$4,$5,$6,$7) returning id', [wid, sym, kind, price, pct, kind === 'trail' ? trail : null, kind === 'trail' ? p : null]);
  await loadOrders(); return r.id;
}
// After a buy: attach the stop loss / take profit that came with it.
async function attachBracket(wid, sym, fillPx, b = {}) {
  const out = [];
  try {
    if (b.trail > 0) out.push(await addCondOrder(wid, sym, 'trail', { trail: b.trail }));
    else if (b.slPx > 0 && b.slPx < fillPx) out.push(await addCondOrder(wid, sym, 'stop', { price: b.slPx }));
    else if (b.sl > 0) out.push(await addCondOrder(wid, sym, 'stop', { price: fillPx * (1 - Math.min(90, b.sl) / 100) }));
    if (b.tpPx > fillPx) out.push(await addCondOrder(wid, sym, 'take', { price: b.tpPx }));
    else if (b.tp > 0) out.push(await addCondOrder(wid, sym, 'take', { price: fillPx * (1 + b.tp / 100) }));
  } catch (e) { warn('bracket', e.message); }
  return out;
}
async function addLimit(wid, sym, o) {
  const p = priceOf(sym); if (!p) fail('No live price yet for ' + sym);
  const side = o.side === 'sell' ? 'sell' : 'buy', kind = ['limit', 'stop', 'moo'].includes(o.kind) ? o.kind : 'limit';
  const price = num(o.price), amt = num(o.amount ?? o.amt);
  let units = o.units || (side === 'buy' ? 'usd' : 'pct');
  if (!['usd', 'qty', 'pct'].includes(units)) units = side === 'buy' ? 'usd' : 'pct';
  if (side === 'buy' && units === 'pct') units = 'usd';
  if (!(amt > 0)) fail('Enter how much to ' + side);
  if (kind !== 'moo' && !(price > 0)) fail('Enter the trigger price');
  if (kind === 'limit' && side === 'buy' && price >= p) fail(`A buy limit has to be below the current price (${pxStr(p)}). To buy higher, use a stop order.`);
  if (kind === 'limit' && side === 'sell' && price <= p) fail(`A sell limit has to be above the current price (${pxStr(p)}). To sell lower, use a stop loss.`);
  if (kind === 'stop' && side === 'buy' && price <= p) fail(`A buy stop has to be above the current price (${pxStr(p)})`);
  if (kind === 'stop' && side === 'sell' && price >= p) fail(`A sell stop has to be below the current price (${pxStr(p)})`);
  if (side === 'sell' && !(await row('select 1 from pos where wid=$1 and sym=$2', [wid, sym]))) fail(`You don't own any ${sym} in this wallet`);
  let expires = null;
  if (o.expires === 'day') expires = isCrypto(sym) ? Date.now() + 86400e3 : sessionClose();
  else if (o.expires && o.expires !== 'gtc' && Number.isFinite(tsMs(o.expires))) expires = tsMs(o.expires);
  const r = await row('insert into lim(wid,sym,side,kind,price,amt,units,sl,tp,expires) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) returning id',
    [wid, sym, side, kind, kind === 'moo' ? p : price, units === 'pct' ? Math.min(100, amt) : amt, units, num(o.sl) || null, num(o.tp) || null, expires ? new Date(expires) : null]);
  await loadOrders(); await refreshUserSyms(); return r.id;
}

// Can automatic orders for this stock fire right now?
function canFire(sym, src) {
  if (isCrypto(sym)) return src !== 'cache';
  if (src === 'cache') return false;
  const st = market().state;
  if (st === 'open') return true;
  return !S.rthOrders && src === 'ws';
}
engineHook = (sym, p, src) => {
  for (const o of condOrders.get(sym) || []) if (o.kind === 'trail' && p > (o.peak || 0)) { o.peak = p; o.dirty = true; }
  if (canFire(sym, src)) { checkCond(sym, p); checkLims(sym, p); }
  if (src !== 'cache') checkAlerts(sym, p);
};
async function checkCond(sym, p) {
  const list = condOrders.get(sym); if (!list || !list.length) return;
  const hit = list.find(o => !firing.has(o.wid + ':' + sym) && (o.kind === 'trail' ? p <= o.peak * (1 - o.trail / 100) : o.kind === 'stop' ? p <= o.price : o.kind === 'take' ? p >= o.price : false));
  if (!hit) return;
  const key = hit.wid + ':' + sym; firing.add(key);
  try {
    const label = hit.kind === 'take' ? 'Take profit' : hit.kind === 'trail' ? 'Trailing stop' : 'Stop loss';
    const pos = await row('select qty from pos where wid=$1 and sym=$2', [hit.wid, sym]);
    if (!pos) { await Q('delete from ord where id=$1', [hit.id]); await loadOrders(); return; }
    const f = await execTrade(hit.wid, sym, 'sell', { pct: hit.pct, why: label.toLowerCase() });
    if (!f.closed) await Q('delete from ord where id=$1', [hit.id]);
    await loadOrders();
    const w = await row('select name, emoji from wallets where id=$1', [hit.wid]);
    await notify('order', `${label} hit · ${sym}`, `Sold ${hit.pct >= 100 ? 'all' : hit.pct + '% of'} your ${sym} (${+f.qty.toFixed(6)}) at ${pxStr(f.price)}. ${f.pnl >= 0 ? 'Profit' : 'Loss'}: ${money(f.pnl)} (${pct(f.pnlPct || 0)}).${walletCount > 1 ? ' Wallet: ' + w.emoji + ' ' + w.name : ''}`, { sym, wid: hit.wid, data: { kind: 'fill', fill: f, label } });
  } catch (e) {
    warn('order', sym, e.message);
    await Q('delete from ord where id=$1', [hit.id]).catch(() => {}); await loadOrders();
    await notify('order', `Couldn't sell ${sym}`, `Your ${hit.kind === 'take' ? 'take profit' : 'stop loss'} triggered but the sale failed: ${e.message}`, { sym, wid: hit.wid });
  } finally { firing.delete(key); }
}
async function checkLims(sym, p) {
  const list = limOrders.get(sym); if (!list || !list.length) return;
  const now = Date.now();
  const hit = list.find(l => !firing.has('L' + l.id) && (!l.expires || l.expires > now) && (
    l.kind === 'moo' ? true :
    l.kind === 'stop' ? (l.side === 'buy' ? p >= l.price : p <= l.price) :
    (l.side === 'buy' ? p <= l.price : p >= l.price)));
  if (!hit) return;
  firing.add('L' + hit.id);
  const label = hit.kind === 'moo' ? 'Queued order' : hit.kind === 'stop' ? 'Stop order' : 'Limit order';
  try {
    const del = await row('delete from lim where id=$1 returning id', [hit.id]); await loadOrders();
    if (!del) return;
    const o = { why: label.toLowerCase() };
    if (hit.side === 'buy') { if (hit.units === 'qty') o.qty = hit.amt; else o.usd = hit.amt; }
    else { if (hit.units === 'qty') o.qty = hit.amt; else if (hit.units === 'usd') o.usd = hit.amt; else o.pct = hit.amt; }
    const f = await execTrade(hit.wid, sym, hit.side, o);
    if (hit.side === 'buy' && (hit.sl || hit.tp)) await attachBracket(hit.wid, sym, f.price, { sl: hit.sl, tp: hit.tp });
    await notify('order', `${label} filled · ${sym}`, `${hit.side === 'buy' ? 'Bought' : 'Sold'} ${+f.qty.toFixed(6)} ${sym} at ${pxStr(f.price)} (${money(f.qty * f.price)}).${f.pnl != null ? ` ${f.pnl >= 0 ? 'Profit' : 'Loss'}: ${money(f.pnl)}.` : ''}`, { sym, wid: hit.wid, data: { kind: 'fill', fill: f, label } });
  } catch (e) {
    await notify('order', `${label} not filled · ${sym}`, `It triggered at ${pxStr(p)} but couldn't go through: ${e.message}`, { sym, wid: hit.wid });
  } finally { firing.delete('L' + hit.id); }
}
async function expireOrders() {
  const gone = await rows('delete from lim where expires is not null and expires < now() returning *');
  if (!gone.length) return;
  await loadOrders();
  for (const l of gone) await notify('order', `Order expired · ${l.sym}`, `Your ${l.kind === 'stop' ? 'stop' : 'limit'} ${l.side} order at ${pxStr(+l.price)} expired without filling.`, { sym: l.sym, wid: l.wid, push: false });
}

// ---------- price alerts ----------
const alertsBy = new Map();
async function loadAlerts() {
  alertsBy.clear();
  for (const a of await rows('select * from alert where not fired order by id')) {
    const x = { id: a.id, sym: a.sym, kind: a.kind || 'price', dir: a.dir, price: +a.price, pct: a.pct == null ? null : +a.pct, rep: a.rep, armed: a.armed !== false, last: tsMs(a.last_fired), label: a.label };
    (alertsBy.get(x.sym) || alertsBy.set(x.sym, []).get(x.sym)).push(x);
  }
}
function checkAlerts(sym, p) {
  const list = alertsBy.get(sym); if (!list) return;
  for (const a of list) {
    if (a.kind === 'daymove') {
      const prev = prices.get(sym)?.prev; if (!prev || !a.pct) continue;
      const ch = (p / prev - 1) * 100;
      if (Math.abs(ch) >= a.pct && etDate(a.last || 0) !== etDate()) {
        a.last = Date.now();
        Q('update alert set last_fired=now() where id=$1', [a.id]).catch(() => {});
        notify('alert', `${sym} is ${ch >= 0 ? 'up' : 'down'} ${Math.abs(ch).toFixed(1)}% today`, `${sym} is at ${pxStr(p)}, ${pct(ch)} since the last close. You asked to know about moves over ${a.pct}%.${a.label ? ' Note: ' + a.label : ''}`, { sym });
      }
      continue;
    }
    const crossed = a.dir === 'above' ? p >= a.price : p <= a.price;
    if (!a.armed) { // repeating alerts wait until the price moves back first
      const back = a.dir === 'above' ? p < a.price * 0.997 : p > a.price * 1.003;
      if (back) { a.armed = true; Q('update alert set armed=true where id=$1', [a.id]).catch(() => {}); }
      continue;
    }
    if (!crossed) continue;
    a.armed = false; a.last = Date.now();
    if (a.rep) Q('update alert set armed=false, last_fired=now() where id=$1', [a.id]).catch(() => {});
    else { Q('update alert set fired=true, armed=false, last_fired=now() where id=$1', [a.id]).catch(() => {}); list.splice(list.indexOf(a), 1); }
    notify('alert', `${sym} ${a.dir === 'above' ? 'rose to' : 'fell to'} ${pxStr(a.price)}`, `${sym} is now ${pxStr(p)} per ${isCrypto(sym) ? 'coin' : 'share'}.${a.label ? ' ' + a.label : ''}${a.rep ? ' This alert repeats.' : ''}`, { sym, data: { kind: 'alert' } });
    break;
  }
}

// =====================================================================
// Live connections, notifications and push
// =====================================================================
let walletCount = 1;
const wss = new WebSocket.Server({ noServer: true, maxPayload: 64 * 1024 });
function broadcast(msg) {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) { try { c.send(s); } catch (e) {} }
}
// Prices go out in small batches a couple of times a second instead of on every trade.
setInterval(() => {
  if (!dirty.size || !wss.clients.size) { dirty.clear(); return; }
  const d = {};
  for (const s of dirty) { const q = prices.get(s); if (q && q.p > 0) d[s] = [round(q.p, 8), q.prev ? round(q.prev, 8) : null, q.t]; }
  dirty.clear();
  broadcast({ t: 'px', d });
}, 400);

let pushOn = false, VAPID_PUB = '';
async function initPush() {
  try {
    let pub = await kvGet('vapid_pub'), priv = await kvGet('vapid_priv');
    if (!pub || !priv) {
      ({ publicKey: pub, privateKey: priv } = webpush.generateVAPIDKeys());
      await Q("insert into kv values('vapid_pub',$1),('vapid_priv',$2) on conflict(k) do update set v=excluded.v", [pub, priv]);
    }
    webpush.setVapidDetails(ENV.VAPID_EMAIL || 'mailto:mango-app@example.com', pub, priv);
    VAPID_PUB = pub; pushOn = true;
  } catch (e) { warn('push setup', e.message); }
}
const PUSH_SETTING = { alert: 'pushAlerts', order: 'pushOrders', summary: 'notifySummary', move: 'notifyMoves', achv: 'notifyAchv', game: 'notifyGames' };
function quietNow() {
  if (!S.quietStart || !S.quietEnd) return false;
  const p = etParts(), m = p.h * 60 + p.mi, [a, b] = [S.quietStart, S.quietEnd].map(x => { const [h, mi] = x.split(':').map(Number); return h * 60 + mi; });
  return a < b ? (m >= a && m < b) : (m >= a || m < b);
}
async function unreadCount() { return +(await row('select count(*) n from notifs where not read')).n; }
async function sendPush(title, body, extra = {}) {
  if (!pushOn) return 0;
  let n = 0;
  const badge = await unreadCount().catch(() => null);
  for (const r of await rows('select * from push')) {
    try { await webpush.sendNotification(JSON.parse(r.sub), JSON.stringify({ title, body, badge, ...extra }), { TTL: 6 * 3600, urgency: extra.urgency || 'high' }); n++; }
    catch (e) {
      if (e.statusCode === 404 || e.statusCode === 410) await Q('delete from push where endpoint=$1', [r.endpoint]);
      else warn('push', e.statusCode || '', e.body || e.message);
    }
  }
  return n;
}
// Every notification lands in the in-app inbox; push is sent too unless that kind is switched off.
async function notify(kind, title, body, o = {}) {
  let id = null;
  try {
    const r = await row('insert into notifs(kind,title,body,sym,wid,url) values($1,$2,$3,$4,$5,$6) returning id, ts', [kind, title, body, o.sym || null, o.wid || null, o.url || null]);
    id = r.id;
    broadcast({ t: 'evt', e: 'notif', n: { id, kind, title, body, sym: o.sym || null, wid: o.wid || null, ts: tsMs(r.ts), read: false }, data: o.data || null });
  } catch (e) { warn('inbox', e.message); }
  const setting = PUSH_SETTING[kind];
  if (o.push !== false && (!setting || S[setting]) && !quietNow()) {
    const url = o.url || (o.sym ? '/#/asset/' + encodeURIComponent(o.sym) : '/#/inbox');
    sendPush(title, body, { url, tag: o.tag || `${kind}-${id || Date.now()}`, id }).catch(e => warn('push', e.message));
  }
  return id;
}

// =====================================================================
// Portfolio history and analytics
// =====================================================================
const lastSnap = new Map(); // wid -> { t, v }
async function snapshot(wid) {
  const w = await row('select id, cash, archived from wallets where id=$1', [wid]); if (!w || w.archived) return;
  const v = +w.cash + marketValue(await positions(wid));
  const ls = lastSnap.get(wid);
  if (ls && Math.abs(ls.v - v) < 0.005 && Date.now() - ls.t < 5 * 60e3) return;
  await Q('insert into snap(wid, ts, eq) values($1, now(), $2)', [wid, round(v, 4)]);
  lastSnap.set(wid, { t: Date.now(), v });
}
async function snapshotAll() { for (const w of await rows('select id from wallets where not archived')) await snapshot(w.id).catch(e => warn('snapshot', e.message)); }
// Keep every point for 2 days, one per 15 minutes for 5 weeks, then one every 2 hours.
async function thinSnapshots() {
  await Q(`delete from snap s using (
      select ctid from (select ctid, row_number() over (partition by wid, date_bin('15 minutes', ts, timestamptz '2000-01-01') order by ts desc) rn
        from snap where ts < now() - interval '2 days' and ts >= now() - interval '35 days') x where rn > 1) d where s.ctid = d.ctid`);
  await Q(`delete from snap s using (
      select ctid from (select ctid, row_number() over (partition by wid, date_bin('2 hours', ts, timestamptz '2000-01-01') order by ts desc) rn
        from snap where ts < now() - interval '35 days') x where rn > 1) d where s.ctid = d.ctid`);
}
const RANGE_MS = { '1D': 86400e3, '1W': 7 * 86400e3, '1M': 30 * 86400e3, '3M': 90 * 86400e3, '1Y': 365 * 86400e3 };
function downsample(pts, n = 320) {
  if (pts.length <= n) return pts;
  const t0 = pts[0][0], t1 = pts[pts.length - 1][0], step = (t1 - t0) / (n - 1), out = [pts[0]];
  let b = 1;
  for (let i = 1; i < pts.length - 1; i++) {
    const bucket = Math.floor((pts[i][0] - t0) / step);
    if (bucket >= b) { out.push(pts[i]); b = bucket + 1; }
  }
  out.push(pts[pts.length - 1]); return out;
}
async function cashFlows(wid, since) {
  return (await rows("select ts, price from trd where wid=$1 and side='cash' and ts >= $2 order by ts", [wid, new Date(since)])).map(r => [tsMs(r.ts), +r.price]);
}
async function equitySeries(wid, range = '1D', bench = null) {
  const w = await walletRow(wid);
  const created = Math.max(tsMs(w.season_start) || 0, 0);
  const since = range === 'ALL' ? created : Math.max(Date.now() - (RANGE_MS[range] || 86400e3), created);
  const raw = (await rows('select ts, eq from snap where wid=$1 and ts >= $2 order by ts', [wid, new Date(since)])).map(r => [tsMs(r.ts), +r.eq]);
  const before = await row('select ts, eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [wid, new Date(since)]);
  if (before && range !== 'ALL') raw.unshift([since, +before.eq]);
  const live = +w.cash + marketValue(await positions(wid));
  raw.push([Date.now(), round(live, 4)]);
  if (raw.length === 1) raw.unshift([since, +w.start_bal]);
  const pts = downsample(raw);
  const flows = await cashFlows(wid, pts[0][0]);
  const netFlow = flows.reduce((t, f) => t + f[1], 0);
  const first = pts[0][1], change = live - first - netFlow;
  const out = { range, points: pts, first, last: live, change, changePct: first ? change / first * 100 : 0, flows };
  if (bench) {
    try {
      const sym = cleanSym(bench) || 'SPY';
      let series;
      if (range === '1D' && spark.get(sym)?.length > 30) series = spark.get(sym).map(x => [x[0] * 60e3, x[1]]);
      else {
        const span = Date.now() - pts[0][0];
        const r = span <= 86400e3 * 1.1 ? '1D' : span <= 7 * 86400e3 ? '1W' : span <= 30 * 86400e3 ? '1M' : span <= 90 * 86400e3 ? '3M' : span <= 365 * 86400e3 ? '1Y' : '5Y';
        series = (await candles(sym, r)).candles.map(c => [c.t * 1000, c.c]);
      }
      const t0 = pts[0][0];
      let base = null; for (const s of series) { if (s[0] <= t0) base = s[1]; else break; }
      if (base == null) base = series[0]?.[1];
      const lp = priceOf(sym);
      const b = series.filter(s => s[0] >= t0).map(s => [s[0], round(first * s[1] / base, 4)]);
      if (lp) b.push([Date.now(), round(first * lp / base, 4)]);
      if (b.length > 1) out.bench = { sym, points: downsample(b, 200), changePct: (b[b.length - 1][1] / first - 1) * 100 };
    } catch (e) { out.bench = null; }
  }
  return out;
}
// Today's move, using each position's previous close (what brokers show as "Today").
function dayChange(pos) {
  const today = etDate(); let ch = 0, base = 0;
  for (const p of pos) {
    const q = prices.get(p.sym); if (!q?.p) continue;
    const ref = p.since && etDate(p.since) === today ? p.avg : (q.prev || p.avg);
    ch += p.qty * (q.p - ref); base += p.qty * ref;
  }
  return { change: ch, pct: base ? ch / base * 100 : 0 };
}
function diversity(pos) {
  const vals = pos.map(p => p.qty * (priceOf(p.sym) || p.avg)).filter(v => v > 0), tot = vals.reduce((a, b) => a + b, 0);
  if (vals.length < 2 || !tot) return 0;
  const hhi = vals.reduce((a, v) => a + (v / tot) ** 2, 0);
  return (1 - hhi) / (1 - 1 / vals.length) * 100;
}
function protectedPct(wid, pos) {
  const tot = marketValue(pos); if (!tot) return 0;
  let prot = 0;
  for (const p of pos) if ((condOrders.get(p.sym) || []).some(o => o.wid === wid && (o.kind === 'stop' || o.kind === 'trail'))) prot += p.qty * (priceOf(p.sym) || p.avg);
  return prot / tot * 100;
}
async function benchReturn(sym, since) {
  const span = Date.now() - since; if (span < 3600e3) return 0;
  const r = span <= 86400e3 ? '1D' : span <= 7 * 86400e3 ? '1W' : span <= 30 * 86400e3 ? '1M' : span <= 90 * 86400e3 ? '3M' : span <= 365 * 86400e3 ? '1Y' : '5Y';
  const cs = (await candles(sym, r)).candles; if (!cs.length) return null;
  let base = null; for (const c of cs) { if (c.t * 1000 <= since) base = c.c; else break; }
  if (base == null) base = cs[0].o;
  const now = priceOf(sym) || cs[cs.length - 1].c;
  return (now / base - 1) * 100;
}
async function analytics(wid) {
  const w = await walletRow(wid), pos = await positions(wid);
  const trades = (await rows('select * from trd where wid=$1 order by id', [wid])).map(t => ({ id: t.id, ts: tsMs(t.ts), sym: t.sym, side: t.side, qty: +t.qty, price: +t.price, pnl: t.pnl == null ? null : +t.pnl, fee: +t.fee || 0, why: t.why, note: t.note }));
  const sells = trades.filter(t => t.side === 'sell' && t.pnl != null);
  const wins = sells.filter(t => t.pnl > 0), losses = sells.filter(t => t.pnl < 0);
  const sum = a => a.reduce((x, y) => x + y, 0);
  const equity = +w.cash + marketValue(pos), invested = marketValue(pos);
  const unrealized = sum(pos.map(p => p.qty * ((priceOf(p.sym) || p.avg) - p.avg)));
  const realized = sum(sells.map(t => t.pnl));
  const dividends = sum(trades.filter(t => t.side === 'div').map(t => t.qty * t.price));
  const interest = sum(trades.filter(t => t.side === 'int').map(t => t.price));
  const fees = sum(trades.map(t => t.fee));
  const deposits = sum(trades.filter(t => t.side === 'cash').map(t => t.price));
  // streaks
  let streak = 0, bestStreak = 0, worstStreak = 0, ls = 0;
  for (const t of sells) { if (t.pnl > 0) { streak++; ls = 0; } else { ls++; streak = 0; } bestStreak = Math.max(bestStreak, streak); worstStreak = Math.max(worstStreak, ls); }
  // holding time (first in, first out)
  const lots = {}; let holdSum = 0, holdQty = 0, longest = 0;
  for (const t of trades) {
    if (t.side === 'buy') (lots[t.sym] ||= []).push({ q: t.qty, t: t.ts });
    else if (t.side === 'sell') {
      let left = t.qty; const L = lots[t.sym] || [];
      while (left > 1e-12 && L.length) { const lot = L[0], used = Math.min(lot.q, left); holdSum += used * (t.ts - lot.t); holdQty += used; longest = Math.max(longest, t.ts - lot.t); lot.q -= used; left -= used; if (lot.q <= 1e-12) L.shift(); }
    }
  }
  for (const p of pos) if (p.since) longest = Math.max(longest, Date.now() - p.since);
  // per symbol
  const bySym = {};
  for (const t of trades) {
    if (!t.sym || t.side === 'cash' || t.side === 'int') continue;
    const s = bySym[t.sym] ||= { sym: t.sym, trades: 0, realized: 0, dividends: 0, bought: 0, sold: 0 };
    if (t.side === 'buy') { s.trades++; s.bought += t.qty * t.price; }
    if (t.side === 'sell') { s.trades++; s.sold += t.qty * t.price; s.realized += t.pnl || 0; }
    if (t.side === 'div') s.dividends += t.qty * t.price;
  }
  for (const p of pos) { const s = bySym[p.sym] ||= { sym: p.sym, trades: 0, realized: 0, dividends: 0, bought: 0, sold: 0 }; s.value = p.qty * (priceOf(p.sym) || p.avg); s.unrealized = p.qty * ((priceOf(p.sym) || p.avg) - p.avg); }
  const symbols = Object.values(bySym).map(s => ({ ...s, total: s.realized + s.dividends + (s.unrealized || 0) })).sort((a, b) => b.total - a.total);
  // daily closes → returns, volatility, Sharpe, calendar
  const days = (await rows(`select to_char(ts at time zone 'America/New_York','YYYY-MM-DD') d, (array_agg(eq order by ts desc))[1] eq from snap where wid=$1 and ts >= $2 group by 1 order by 1`, [wid, w.season_start])).map(r => [r.d, +r.eq]);
  const today = etDate(); if (!days.length || days[days.length - 1][0] !== today) days.push([today, equity]); else days[days.length - 1][1] = equity;
  const flowByDay = {}; for (const t of trades.filter(t => t.side === 'cash')) { const d = etDate(t.ts); flowByDay[d] = (flowByDay[d] || 0) + t.price; }
  // start_bal already includes later deposits, so the money you began the season with is start_bal minus them.
  const daily = []; let prev = +w.start_bal - sum(Object.values(flowByDay));
  for (const [d, v] of days) { const base = prev + (flowByDay[d] || 0); const ch = v - base; daily.push({ d, v, ch, pct: base ? ch / base * 100 : 0 }); prev = v; }
  const rets = daily.slice(1).map(x => x.pct / 100);
  const mean = rets.length ? sum(rets) / rets.length : 0;
  const sd = rets.length > 1 ? Math.sqrt(sum(rets.map(r => (r - mean) ** 2)) / (rets.length - 1)) : 0;
  const allSnaps = (await rows('select eq from snap where wid=$1 and ts >= $2 order by ts', [wid, w.season_start])).map(r => +r.eq).concat(equity);
  let peak = 0, maxDD = 0; for (const v of allSnaps) { peak = Math.max(peak, v); if (peak) maxDD = Math.min(maxDD, (v - peak) / peak); }
  const curDD = peak ? Math.min(0, (equity - peak) / peak) : 0;
  const bestDay = daily.slice(1).reduce((a, x) => !a || x.ch > a.ch ? x : a, null), worstDay = daily.slice(1).reduce((a, x) => !a || x.ch < a.ch ? x : a, null);
  // allocation
  const alloc = pos.map(p => ({ sym: p.sym, kind: kindOf(p.sym), sector: assets.get(p.sym)?.industry || (isCrypto(p.sym) ? 'Crypto' : 'Other'), value: p.qty * (priceOf(p.sym) || p.avg) })).sort((a, b) => b.value - a.value);
  const group = key => { const g = {}; for (const a of alloc) g[a[key]] = (g[a[key]] || 0) + a.value; return Object.entries(g).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value); };
  // benchmarks since the season started
  const since = tsMs(w.season_start);
  const [spy, qqq, btc] = await Promise.all(['SPY', 'QQQ', 'BTC'].map(s => benchReturn(s, since).catch(() => null)));
  const ret = (equity / +w.start_bal - 1) * 100;
  const best = sells.reduce((a, t) => !a || t.pnl > a.pnl ? t : a, null), worst = sells.reduce((a, t) => !a || t.pnl < a.pnl ? t : a, null);
  return {
    wallet: walletJson(w), equity, invested, cash: +w.cash, start: +w.start_bal, ret, pl: equity - +w.start_bal,
    realized, unrealized, dividends, interest, fees, deposits,
    closed: sells.length, trades: trades.filter(t => t.side === 'buy' || t.side === 'sell').length,
    win: sells.length ? wins.length / sells.length * 100 : null, avgWin: wins.length ? sum(wins.map(t => t.pnl)) / wins.length : null,
    avgLoss: losses.length ? sum(losses.map(t => t.pnl)) / losses.length : null, pf: losses.length ? sum(wins.map(t => t.pnl)) / -sum(losses.map(t => t.pnl)) : (wins.length ? Infinity : null),
    expectancy: sells.length ? realized / sells.length : null,
    best: best ? { sym: best.sym, pnl: best.pnl, ts: best.ts } : null, worst: worst ? { sym: worst.sym, pnl: worst.pnl, ts: worst.ts } : null,
    streak, bestStreak, worstStreak, avgHold: holdQty ? holdSum / holdQty : null, longestHold: longest || null,
    maxDD: maxDD * 100, curDD: curDD * 100, volatility: sd * Math.sqrt(365) * 100, sharpe: sd > 0 && rets.length >= 5 ? mean / sd * Math.sqrt(365) : null,
    bestDay, worstDay, daily: daily.slice(-400), symbols,
    alloc, byKind: group('kind'), bySector: group('sector'), diversity: diversity(pos), protected: protectedPct(wid, pos),
    bench: { SPY: spy, QQQ: qqq, BTC: btc }, seasonStart: since,
    whys: trades.reduce((o, t) => (o[t.why] = (o[t.why] || 0) + 1, o), {}),
  };
}

// =====================================================================
// Games: Bull vs Bear, achievements, challenges, levels, leaderboard
// =====================================================================
const PRED_MINS = [15, 60, 240, 1440];
async function newPrediction(sym, dir, mins) {
  sym = cleanSym(sym); if (!sym) fail('Pick something to predict');
  if (dir !== 'up' && dir !== 'down') fail('Choose bull (up) or bear (down)');
  const p = priceOf(sym); if (!p || !fresh(sym, 20 * 60e3)) fail(`No fresh price for ${sym} right now. Try a crypto, or wait for the market to open.`);
  if (!isCrypto(sym) && market().state !== 'open') fail(`The stock market is closed, so ${sym}'s price won't move. Try a crypto instead, or come back when the market opens.`);
  let end;
  if (mins === 'close') { if (isCrypto(sym)) fail('"Until the close" only works for stocks'); end = sessionClose(); }
  else { mins = +mins; if (!PRED_MINS.includes(mins)) fail('Pick a time: 15 minutes, 1 hour, 4 hours or 1 day'); end = Date.now() + mins * 60e3; }
  if (!isCrypto(sym) && end > sessionClose() && mins !== 1440) end = sessionClose();
  const active = await rows('select sym from preds where result is null');
  if (active.length >= 10) fail('You can have 10 predictions running at once');
  if (active.some(a => a.sym === sym)) fail(`You already have a prediction running on ${sym}`);
  const r = await row('insert into preds(sym,dir,start_px,end_ts) values($1,$2,$3,$4) returning id', [sym, dir, p, new Date(end)]);
  await refreshUserSyms();
  return r.id;
}
async function resolvePredictions() {
  for (const pr of await rows('select * from preds where result is null and end_ts <= now() order by end_ts')) {
    const p = priceOf(pr.sym); if (!p) continue;
    const ch = (p / +pr.start_px - 1) * 100;
    const res = Math.abs(ch) < 0.01 ? 'push' : ((ch > 0) === (pr.dir === 'up') ? 'win' : 'loss');
    const streakRows = await rows("select result from preds where result in ('win','loss') order by end_ts desc limit 20");
    let streak = 0; for (const s of streakRows) { if (s.result === 'win') streak++; else break; }
    const points = res === 'win' ? 10 + Math.min(20, streak * 2) : 0;
    await Q('update preds set end_px=$1, result=$2, points=$3 where id=$4', [p, res, points, pr.id]);
    await notify('game', res === 'win' ? `🐂🐻 You called it on ${pr.sym}!` : res === 'loss' ? `${pr.sym} went the other way` : `${pr.sym} didn't move`,
      `You picked ${pr.dir === 'up' ? 'up (bull)' : 'down (bear)'} at ${pxStr(+pr.start_px)}. It ended at ${pxStr(p)} (${pct(ch)}).${points ? ` +${points} points.` : ''}`, { sym: pr.sym, url: '/#/games' });
    broadcast({ t: 'evt', e: 'pred' });
  }
}
async function predStats() {
  const all = await rows('select * from preds order by start_ts desc limit 200');
  const done = all.filter(p => p.result === 'win' || p.result === 'loss');
  let streak = 0; for (const p of done) { if (p.result === 'win') streak++; else break; }
  let best = 0, cur = 0; for (const p of done.slice().reverse()) { cur = p.result === 'win' ? cur + 1 : 0; best = Math.max(best, cur); }
  const wins = done.filter(p => p.result === 'win').length;
  return {
    active: all.filter(p => !p.result).map(p => ({ id: p.id, sym: p.sym, dir: p.dir, start: +p.start_px, startTs: tsMs(p.start_ts), end: tsMs(p.end_ts) })),
    history: all.filter(p => p.result).slice(0, 30).map(p => ({ id: p.id, sym: p.sym, dir: p.dir, start: +p.start_px, endPx: +p.end_px, result: p.result, points: p.points, end: tsMs(p.end_ts) })),
    wins, losses: done.length - wins, accuracy: done.length ? wins / done.length * 100 : null, streak, best,
    points: all.reduce((t, p) => t + (p.points || 0), 0),
  };
}

// ---------- achievements (checked on the server, unlocked once, forever) ----------
const ACHIEVEMENTS = [
  ['first_trade', '🥭', 'First bite', 'Make your first trade', c => c.trades >= 1],
  ['trades_10', '🔟', 'Getting the hang of it', 'Make 10 trades', c => c.trades >= 10],
  ['trades_50', '🧺', 'Regular', 'Make 50 trades', c => c.trades >= 50],
  ['trades_100', '💯', 'Centurion', 'Make 100 trades', c => c.trades >= 100],
  ['trades_500', '🏭', 'Trading machine', 'Make 500 trades', c => c.trades >= 500],
  ['first_win', '💰', 'In the green', 'Sell something for a profit', c => c.wins >= 1],
  ['big_win', '🚀', 'Moonshot', 'Sell a position up 25% or more', c => c.bigWin],
  ['streak_5', '🔥', 'On fire', 'Five profitable sales in a row', c => c.bestStreak >= 5],
  ['streak_10', '☄️', 'Unstoppable', 'Ten profitable sales in a row', c => c.bestStreak >= 10],
  ['stop_hit', '🛡️', 'Safety net', 'Have a stop loss protect you', c => c.why['stop loss'] > 0],
  ['take_hit', '🎯', 'Bullseye', 'Hit a take profit', c => c.why['take profit'] > 0],
  ['trail_hit', '🧗', 'Trail blazer', 'Have a trailing stop sell for you', c => c.why['trailing stop'] > 0],
  ['limit_fill', '🧲', 'Patient buyer', 'Get a limit order filled', c => c.why['limit order'] > 0],
  ['diverse_5', '🌈', 'Diversified', 'Hold 5 different things at once', c => c.maxHeld >= 5],
  ['diverse_10', '🗂️', 'Collector', 'Hold 10 different things at once', c => c.maxHeld >= 10],
  ['both', '🪙', 'Best of both', 'Hold a stock and a crypto at the same time', c => c.both],
  ['dividend', '💵', 'Paid to wait', 'Receive a dividend', c => c.divs > 0],
  ['up_10', '📈', 'Up 10%', 'Grow a wallet by 10%', c => c.bestRet >= 10],
  ['up_25', '🌴', 'Up 25%', 'Grow a wallet by 25%', c => c.bestRet >= 25],
  ['up_100', '💎', 'Doubled', 'Double a wallet', c => c.bestRet >= 100],
  ['hold_30', '🧘', 'Diamond hands', 'Hold one position for 30 days', c => c.longestHold >= 30 * 86400e3],
  ['day_trader', '⚡', 'Busy day', 'Make 5 trades in one day', c => c.maxDay >= 5],
  ['night_owl', '🦉', 'Night owl', 'Trade between midnight and 5 AM', c => c.night],
  ['weekend', '🏖️', 'Weekend warrior', 'Trade on a Saturday or Sunday', c => c.weekend],
  ['wallets', '👛', 'Many pockets', 'Create a second wallet', c => c.wallets >= 2],
  ['whale', '🐋', 'Whale', 'Make a single trade worth $10,000 or more', c => c.whale],
  ['pred_win', '🔮', 'Fortune teller', 'Win a Bull vs Bear prediction', c => c.predWins >= 1],
  ['pred_10', '🧙', 'Oracle', 'Win 10 Bull vs Bear predictions', c => c.predWins >= 10],
  ['pred_streak', '🐂', 'Called it', 'Win 5 predictions in a row', c => c.predBest >= 5],
  ['journal', '✍️', 'Thoughtful trader', 'Write notes on 5 trades', c => c.notes >= 5],
  ['alert', '🔔', 'Heads up', 'Set a price alert', c => c.alerts >= 1],
  ['protected', '🦺', 'Fully protected', 'Have a stop loss on every position (3 or more)', c => c.fullyProtected],
];
let achvBusy = false;
const checkAchievementsSoon = debounce(() => checkAchievements().catch(e => warn('achievements', e.message)), 4000);
async function achievementContext() {
  const trd = await rows("select wid, ts, sym, side, qty, price, pnl, why, note from trd where side in ('buy','sell','div') order by id");
  const c = { trades: 0, wins: 0, bigWin: false, bestStreak: 0, why: {}, maxHeld: 0, both: false, divs: 0, bestRet: 0, longestHold: 0, maxDay: 0, night: false, weekend: false, wallets: 0, whale: false, notes: 0, alerts: 0, fullyProtected: false, predWins: 0, predBest: 0 };
  const perDay = {}; let streak = 0;
  for (const t of trd) {
    if (t.side === 'div') { c.divs++; continue; }
    c.trades++; c.why[t.why] = (c.why[t.why] || 0) + 1; if (t.note) c.notes++;
    const ts = tsMs(t.ts), p = etParts(ts); const d = etDate(ts) + ':' + t.wid; perDay[d] = (perDay[d] || 0) + 1;
    if (p.h < 5) c.night = true; if (p.wd === 0 || p.wd === 6) c.weekend = true;
    if (+t.qty * +t.price >= 10000) c.whale = true;
    if (t.side === 'sell' && t.pnl != null) {
      if (+t.pnl > 0) { c.wins++; streak++; c.bestStreak = Math.max(c.bestStreak, streak); } else streak = 0;
      const cost = +t.qty * +t.price - +t.pnl; if (cost > 0 && +t.pnl / cost >= 0.25) c.bigWin = true;
    }
  }
  c.maxDay = Math.max(0, ...Object.values(perDay));
  const ws = await rows('select * from wallets'); c.wallets = ws.length;
  for (const w of ws) {
    const pos = await positions(w.id);
    c.maxHeld = Math.max(c.maxHeld, pos.length);
    if (pos.some(p => isCrypto(p.sym)) && pos.some(p => !isCrypto(p.sym))) c.both = true;
    for (const p of pos) if (p.since) c.longestHold = Math.max(c.longestHold, Date.now() - p.since);
    if (!w.archived) c.bestRet = Math.max(c.bestRet, ((+w.cash + marketValue(pos)) / +w.start_bal - 1) * 100);
    if (pos.length >= 3 && protectedPct(w.id, pos) >= 99.9) c.fullyProtected = true;
  }
  c.alerts = +(await row('select count(*) n from alert')).n;
  const ps = await predStats(); c.predWins = ps.wins; c.predBest = ps.best;
  return c;
}
async function checkAchievements() {
  if (achvBusy) return; achvBusy = true;
  try {
    const have = new Set((await rows('select key from achv')).map(r => r.key));
    const firstRun = !(await kvGet('achv_init'));
    const c = await achievementContext();
    for (const [key, icon, name, desc, test] of ACHIEVEMENTS) {
      if (have.has(key) || !test(c)) continue;
      await Q('insert into achv(key) values($1) on conflict do nothing', [key]);
      // The first time this runs, past achievements unlock quietly instead of sending a pile of notifications.
      if (!firstRun) await notify('achv', `${icon} Achievement unlocked: ${name}`, desc + '.', { url: '/#/games', data: { kind: 'achv', key } });
    }
    if (firstRun) await kvSet('achv_init', '1');
  } finally { achvBusy = false; }
}
async function achievementList() {
  const have = new Map((await rows('select key, ts from achv')).map(r => [r.key, tsMs(r.ts)]));
  return ACHIEVEMENTS.map(([key, icon, name, desc]) => ({ key, icon, name, desc, at: have.get(key) || null }));
}

// ---------- levels ----------
const LEVELS = ['Seedling', 'Sprout', 'Sapling', 'Blossom', 'Green mango', 'Ripening', 'Golden', 'Juicy', 'Sweet spot', 'Orchard keeper', 'Mango master', 'Tropical legend'];
async function xpInfo() {
  const t = await row(`select count(*) filter (where side in ('buy','sell')) trades, count(*) filter (where side='sell' and pnl>0) wins, count(*) filter (where note is not null and note<>'') notes from trd`);
  const a = +(await row('select count(*) n from achv')).n;
  const p = await row(`select coalesce(sum(points),0) pts, count(*) filter (where result='win') w from preds`);
  const xp = +t.trades * 3 + +t.wins * 8 + +t.notes * 4 + a * 40 + +p.pts + +p.w * 5;
  let level = 1, need = 100, floor = 0;
  while (xp >= floor + need && level < 99) { floor += need; level++; need = 100 + (level - 1) * 60; }
  return { xp, level, title: LEVELS[Math.min(LEVELS.length - 1, Math.floor((level - 1) / 2))], into: xp - floor, need };
}

// ---------- challenges ----------
const weekStart = () => { const p = etParts(); const back = (p.wd + 6) % 7; const [y, m, d] = addDays(p.y, p.m, p.d, -back); return etToUtc(y, m, d, 0, 0); };
const WEEKLY = [
  ['w_trades', 'Make 5 trades', c => c.wTrades, 5], ['w_wins', 'Close 2 trades for a profit', c => c.wWins, 2],
  ['w_limit', 'Get a limit order filled', c => c.wLimit, 1], ['w_preds', 'Win 3 predictions', c => c.wPreds, 3],
  ['w_crypto', 'Trade a crypto', c => c.wCrypto, 1], ['w_new', 'Buy something you have never owned', c => c.wNew, 1],
  ['w_protect', 'Add a stop loss to a position', c => c.wStops, 1], ['w_notes', 'Write notes on 2 trades', c => c.wNotes, 2],
  ['w_green', 'End a day in profit', c => c.wGreenDays, 1], ['w_alerts', 'Set 2 price alerts', c => c.wAlerts, 2],
];
async function challenges(wid, a) {
  const ws = weekStart(), weekNo = Math.floor(ws / (7 * 86400e3));
  const pick = []; for (let i = 0; pick.length < 4 && i < 40; i++) { const x = WEEKLY[(weekNo * 7 + i * 3) % WEEKLY.length]; if (!pick.includes(x)) pick.push(x); }
  const wt = await rows('select * from trd where wid=$1 and ts >= $2', [wid, new Date(ws)]);
  const earlier = new Set((await rows('select distinct sym from trd where wid=$1 and ts < $2 and side=$3', [wid, new Date(ws), 'buy'])).map(r => r.sym));
  const c = {
    wTrades: wt.filter(t => t.side === 'buy' || t.side === 'sell').length, wWins: wt.filter(t => t.side === 'sell' && +t.pnl > 0).length,
    wLimit: wt.filter(t => t.why === 'limit order').length, wCrypto: wt.filter(t => (t.side === 'buy' || t.side === 'sell') && isCrypto(t.sym)).length ? 1 : 0,
    wNew: wt.filter(t => t.side === 'buy' && !earlier.has(t.sym)).length ? 1 : 0, wNotes: wt.filter(t => t.note).length,
    wPreds: +(await row("select count(*) n from preds where result='win' and end_ts >= $1", [new Date(ws)])).n,
    wStops: +(await row("select count(*) n from ord where wid=$1 and kind in ('stop','trail') and created >= $2", [wid, new Date(ws)])).n,
    wAlerts: +(await row('select count(*) n from alert where ts >= $1', [new Date(ws)])).n,
    wGreenDays: (a.daily || []).filter(d => tsMs(d.d + 'T12:00:00Z') >= ws && d.ch > 0).length,
  };
  const weekly = pick.map(([key, name, f, goal]) => ({ key, name, value: Math.min(goal, f(c)), goal, done: f(c) >= goal }));
  const spy = a.bench?.SPY;
  const season = [
    { key: 's_spy', name: 'Beat the S&P 500 this season', value: spy != null && a.ret > spy ? 1 : 0, goal: 1, hint: spy != null ? `You ${pct(a.ret)} vs S&P ${pct(spy)}` : null },
    { key: 's_10', name: 'Grow this wallet 10%', value: clamp(a.ret, 0, 10), goal: 10, unit: '%' },
    { key: 's_25t', name: 'Make 25 trades', value: Math.min(25, a.trades), goal: 25 },
    { key: 's_div', name: 'Hold 5 different things at once', value: Math.min(5, a.alloc.length), goal: 5 },
    { key: 's_dd', name: 'Keep your worst drop under 5% (after 10 trades)', value: a.trades >= 10 && a.maxDD > -5 ? 1 : 0, goal: 1 },
    { key: 's_wr', name: 'Win 60% of your sales (at least 5)', value: a.closed >= 5 && a.win >= 60 ? 1 : 0, goal: 1 },
  ].map(x => ({ ...x, done: x.value >= x.goal }));
  return { weekly, season, weekEnds: ws + 7 * 86400e3 };
}
async function leaderboard(activeWid, a) {
  const out = [];
  for (const w of await rows('select * from wallets where not archived order by sort, id')) {
    const v = +w.cash + marketValue(await positions(w.id));
    out.push({ type: 'wallet', id: w.id, name: w.name, emoji: w.emoji, ret: (v / +w.start_bal - 1) * 100, value: v, since: tsMs(w.season_start), me: w.id === activeWid });
  }
  for (const [sym, name, emoji] of [['SPY', 'S&P 500', '🇺🇸'], ['QQQ', 'Nasdaq 100', '💻'], ['BTC', 'Bitcoin', '₿']]) if (a.bench?.[sym] != null) out.push({ type: 'bench', name, emoji, sym, ret: a.bench[sym], since: a.seasonStart });
  for (const s of await rows('select s.*, w.name wname, w.emoji from seasons s left join wallets w on w.id=s.wid order by (s.final_bal/nullif(s.start_bal,0)) desc nulls last limit 12')) {
    out.push({ type: 'season', id: s.id, name: (s.name || ((s.wname || 'Wallet') + ' season')), emoji: s.emoji || '🏁', ret: (+s.final_bal / +s.start_bal - 1) * 100, value: +s.final_bal, since: tsMs(s.started), ended: tsMs(s.ts), trades: s.trades, wins: s.wins, closed: s.closed });
  }
  return out.sort((x, y) => y.ret - x.ret);
}

// =====================================================================
// Login
// =====================================================================
let tokenSalt = '';
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const sign = e => e + '.' + crypto.createHmac('sha256', SECRET).update(String(e) + tokenSalt).digest('hex');
const TOKEN_DAYS = 180;
const newToken = () => sign(Date.now() + TOKEN_DAYS * 864e5);
function tokenValid(t) {
  t = String(t || ''); const e = t.split('.')[0];
  if (!/^\d+$/.test(e) || +e < Date.now()) return false;
  return safeEq(sign(e), t);
}
const tokenExpiry = t => +String(t).split('.')[0] || 0;
async function passwordOk(p) {
  const h = await kvGet('pass_hash');
  if (h) { const [salt, hash] = h.split(':'); return safeEq(crypto.scryptSync(String(p), salt, 32).toString('hex'), hash); }
  return safeEq(String(p), APP_PASS);
}
const tries = new Map();
function rateLimit(ip) {
  const now = Date.now(), list = (tries.get(ip) || []).filter(t => now - t < 10 * 60e3);
  tries.set(ip, list);
  if (list.length >= 8) fail(`Too many tries. Wait ${Math.ceil((list[0] + 10 * 60e3 - now) / 60e3)} minutes and try again.`, 429);
  list.push(now);
}
function auth(req) {
  const t = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!tokenValid(t)) fail('Please log in again', 401);
  req.token = t;
}

POST('/api/login', async req => {
  const ip = clientIp(req); rateLimit(ip);
  const { u, p } = req.body || {};
  const ok = safeEq(String(u || '').trim().toLowerCase(), APP_USER.toLowerCase()) && await passwordOk(p || '');
  if (!ok) { await sleep(600); fail('That username or password is wrong', 401); }
  tries.delete(ip);
  return { token: newToken() };
});
POST('/api/password', auth, async req => {
  const { current, next } = req.body;
  if (!(await passwordOk(current || ''))) fail('Your current password is wrong');
  if (String(next || '').length < 6) fail('Use at least 6 characters for the new password');
  const salt = crypto.randomBytes(12).toString('hex');
  await kvSet('pass_hash', salt + ':' + crypto.scryptSync(String(next), salt, 32).toString('hex'));
  return { ok: true };
});
POST('/api/logout-all', auth, async () => {
  tokenSalt = crypto.randomBytes(12).toString('hex'); await kvSet('token_salt', tokenSalt);
  for (const c of wss.clients) try { c.close(4001, 'logged out'); } catch (e) {}
  return { token: newToken() };
});
GET('/api/health', () => ({ ok: true, up: Math.round((Date.now() - BOOT) / 1000), crypto: feeds.crypto.ok, stocks: FINNHUB_KEY ? feeds.stocks.ok : null }));

// =====================================================================
// App state
// =====================================================================
const assetsJson = syms => {
  const o = {};
  for (const s of syms) { const a = assets.get(s); if (a) o[s] = { k: a.kind === 'crypto' ? 'c' : 's', n: a.name, l: a.logo || null, i: a.industry || null, c: a.cap || null }; }
  return o;
};
const pricesJson = () => { const o = {}; for (const [s, q] of prices) if (q.p > 0) o[s] = [round(q.p, 8), q.prev ? round(q.prev, 8) : null, q.t, q.src === 'cache' ? 1 : 0, q.hi || null, q.lo || null]; return o; };
const tradeJson = t => ({ id: t.id, ts: tsMs(t.ts), sym: t.sym, side: t.side, qty: +t.qty, price: +t.price, why: t.why, pnl: t.pnl == null ? null : +t.pnl, fee: +t.fee || 0, note: t.note, tags: t.tags, wid: t.wid });
const alertJson = a => ({ id: a.id, sym: a.sym, kind: a.kind || 'price', dir: a.dir, price: +a.price, pct: a.pct == null ? null : +a.pct, rep: a.rep, fired: a.fired, armed: a.armed, label: a.label, ts: tsMs(a.ts), last: tsMs(a.last_fired) });
async function walletsSummary() {
  const ws = await rows('select * from wallets order by archived, sort, id');
  walletCount = ws.filter(w => !w.archived).length;
  const out = [];
  for (const w of ws) { const pos = await positions(w.id); const v = +w.cash + marketValue(pos); out.push({ ...walletJson(w), value: v, ret: (v / +w.start_bal - 1) * 100, day: dayChange(pos), count: pos.length }); }
  return out;
}
GET('/api/state', auth, async req => {
  const wid = await activeWalletId(req);
  const [wallets, pos, trd, watch, alerts, unread, counts, preds, seasons] = await Promise.all([
    walletsSummary(), positions(wid), rows('select * from trd where wid=$1 order by id desc limit 40', [wid]),
    rows('select sym from watch order by pos, created, sym'), rows('select * from alert order by fired, id desc limit 100'), unreadCount(),
    row(`select (select count(*) from trd where wid=$1 and side in ('buy','sell')) trades, (select count(*) from trd where wid=$1) events`, [wid]),
    rows('select id, sym, dir, start_px, start_ts, end_ts from preds where result is null order by end_ts'), rows('select count(*) n from seasons where wid=$1', [wid]),
  ]);
  const w = wallets.find(x => x.id === wid);
  const ord = [...condOrders.values()].flat().filter(o => o.wid === wid).map(orderJson);
  const lims = [...limOrders.values()].flat().filter(l => l.wid === wid);
  const syms = new Set([...STOCK_DEFAULT, ...INDEXES, ...CRYPTO_DEFAULT, ...assets.keys()]);
  const sp = {}, mine = [...pos.map(p => p.sym), ...watch.map(x => x.sym), ...userSyms.extra];
  for (const s of new Set([...mine, ...syms])) { const a = sparkFor(s); if (a) sp[s] = a; else wantSpark(s); }
  for (const s of new Set(mine)) ensureMeta(s).catch(() => {});
  const eq = await equitySeries(wid, '1D').catch(() => null);
  const out = {
    v: BUILD, now: Date.now(), market: market(), user: APP_USER,
    wallet: w, wallets, pos, ord, lims, trd: trd.map(tradeJson), watch: watch.map(x => x.sym), alerts: alerts.map(alertJson),
    preds: preds.map(p => ({ id: p.id, sym: p.sym, dir: p.dir, start: +p.start_px, startTs: tsMs(p.start_ts), end: tsMs(p.end_ts) })),
    assets: assetsJson(syms), prices: pricesJson(), spark: sp, eq1d: eq,
    lists: { stocks: STOCK_DEFAULT, indexes: INDEXES, crypto: CRYPTO_DEFAULT, extra: [...userSyms.extra] },
    settings: S, unread, counts: { trades: +counts.trades, events: +counts.events, conditional: ord.length + lims.length, seasons: +seasons[0].n },
    stats: { day: dayChange(pos), diversity: diversity(pos), protected: protectedPct(wid, pos), invested: marketValue(pos) },
    feeds: { crypto: { ok: feeds.crypto.ok, last: feeds.crypto.last }, stocks: { ok: feeds.stocks.ok, last: feeds.stocks.last, key: !!FINNHUB_KEY, live: feeds.stocks.ws } },
    push: { key: VAPID_PUB, on: pushOn },
  };
  if (tokenExpiry(req.token) - Date.now() < (TOKEN_DAYS - 30) * 864e5) out.token = newToken(); // keeps you logged in while you use the app
  return out;
});
GET('/api/equity', auth, async req => equitySeries(await activeWalletId(req), String(req.query.r || '1D').toUpperCase(), req.query.bench || null));

// =====================================================================
// Assets
// =====================================================================
GET('/api/search', auth, req => search(req.query.q));
POST('/api/track', auth, async req => { const a = await trackAsset(req.body.sym, req.body.kind); return { sym: a.sym, kind: a.kind, name: a.name }; });
GET('/api/candles/:sym', auth, async req => {
  const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol');
  focus.set(sym, Date.now());
  const r = String(req.query.r || '1D').toUpperCase();
  return candles(sym, RANGES[r] ? r : '1D');
});
GET('/api/asset/:sym', auth, async req => { const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol'); return assetDetails(sym); });
GET('/api/asset/:sym/insights', auth, async req => { const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol'); return assetInsights(sym); });
GET('/api/asset/:sym/news', auth, async req => { const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol'); return assetNews(sym); });
GET('/api/asset/:sym/book', auth, async req => { const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol'); return orderBook(sym); });
GET('/api/asset/:sym/history', auth, async req => {
  const sym = cleanSym(req.params.sym), wid = await activeWalletId(req);
  return (await rows('select * from trd where wid=$1 and sym=$2 order by id desc limit 50', [wid, sym])).map(tradeJson);
});
GET('/api/market', auth, async () => {
  const [fng, glob] = await Promise.all([S.f_fng ? fearGreed().catch(() => null) : null, S.f_fng ? cryptoGlobal().catch(() => null) : null]);
  const heat = [...STOCK_DEFAULT, ...[...userSyms.extra].filter(s => !isCrypto(s))].map(s => { ensureMeta(s).catch(() => {}); const a = assets.get(s); return { sym: s, sector: a?.industry || 'Other', cap: a?.cap || null }; });
  return { fng, global: glob, heat, market: market() };
});
GET('/api/news', auth, async req => {
  const cat = req.query.cat || 'general';
  if (cat === 'mine') {
    const syms = [...new Set([...userSyms.pos, ...userSyms.watch])].filter(s => !isCrypto(s)).slice(0, 6);
    const lists = await Promise.all(syms.map(s => assetNews(s).then(l => l.map(n => ({ ...n, sym: s }))).catch(() => [])));
    const seen = new Set(); return lists.flat().filter(n => !seen.has(n.title) && seen.add(n.title)).sort((a, b) => b.t - a.t).slice(0, 40);
  }
  return marketNews(cat === 'crypto' ? 'crypto' : 'general');
});
GET('/api/earnings', auth, async () => {
  const all = await earningsWeek();
  const mine = new Set([...userSyms.pos, ...userSyms.watch, ...STOCK_DEFAULT, ...userSyms.extra]);
  const pick = all.filter(x => mine.has(x.sym));
  const big = all.filter(x => !mine.has(x.sym) && x.revenue > 0).sort((a, b) => b.revenue - a.revenue).slice(0, 25);
  return [...pick.map(x => ({ ...x, mine: userSyms.pos.has(x.sym) || userSyms.watch.has(x.sym) })), ...big].sort((a, b) => a.date.localeCompare(b.date) || (b.revenue || 0) - (a.revenue || 0));
});

// =====================================================================
// Trading
// =====================================================================
POST('/api/trade', auth, async req => {
  const wid = await activeWalletId(req), b = req.body;
  const sym = cleanSym(b.sym); if (!sym) fail('Pick something to trade');
  if (!assets.has(sym)) await trackAsset(sym, b.kind);
  const side = b.side === 'sell' ? 'sell' : 'buy';
  const o = { note: b.note, why: 'manual' };
  const mode = b.mode || (b.usd ? 'usd' : b.pct ? 'pct' : 'qty'), amount = num(b.amount ?? b.usd ?? b.pct ?? b.qty);
  if (!(amount > 0)) fail('Enter an amount');
  if (mode === 'usd') o.usd = amount; else if (mode === 'pct') o.pct = amount; else o.qty = amount;
  if (!isCrypto(sym) && market().state !== 'open') {
    if (S.closedTrading === 'block') fail(`The stock market is closed. It opens ${new Date(market().opensAt).toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: '2-digit' })} New York time.`);
    if (S.closedTrading === 'queue') {
      const id = await addLimit(wid, sym, { side, kind: 'moo', amount, units: mode === 'qty' ? 'qty' : mode === 'pct' ? 'pct' : 'usd', sl: b.sl, tp: b.tp });
      return { queued: true, id };
    }
  }
  const f = await execTrade(wid, sym, side, o);
  let bracket = [];
  if (side === 'buy') {
    const br = { sl: b.sl === undefined ? S.autoSL : num(b.sl), tp: b.tp === undefined ? S.autoTP : num(b.tp), slPx: num(b.slPx), tpPx: num(b.tpPx), trail: num(b.trail) };
    bracket = await attachBracket(wid, sym, f.price, br);
  }
  return { fill: f, bracket };
});
POST('/api/order', auth, async req => {
  const wid = await activeWalletId(req), b = req.body, sym = cleanSym(b.sym);
  const id = await addCondOrder(wid, sym, b.kind, { price: num(b.price), pct: b.pct, trail: b.trail });
  await refreshUserSyms(); return { id };
});
PATCH('/api/order/:id', auth, async req => {
  const o = await row('select * from ord where id=$1', [+req.params.id]); if (!o) fail('That order is gone', 404);
  const b = req.body, p = priceOf(o.sym);
  if (o.kind === 'trail') { const t = num(b.trail, +o.trail); if (!(t >= 0.1 && t <= 50)) fail('The trailing distance must be between 0.1% and 50%'); await Q('update ord set trail=$1, pct=$2 where id=$3', [t, clamp(num(b.pct, +o.pct), 1, 100), o.id]); }
  else {
    const price = num(b.price, +o.price);
    if (o.kind === 'stop' && !(price < p)) fail(`A stop loss has to be below the current price (${pxStr(p)})`);
    if (o.kind === 'take' && !(price > p)) fail(`A take profit has to be above the current price (${pxStr(p)})`);
    await Q('update ord set price=$1, pct=$2 where id=$3', [price, clamp(num(b.pct, +o.pct), 1, 100), o.id]);
  }
  await loadOrders(); return { ok: true };
});
DEL('/api/order/:id', auth, async req => { await Q('delete from ord where id=$1', [+req.params.id]); await loadOrders(); await refreshUserSyms(); });
POST('/api/limit', auth, async req => {
  const wid = await activeWalletId(req), sym = cleanSym(req.body.sym); if (!sym) fail('Pick something');
  if (!assets.has(sym)) await trackAsset(sym, req.body.kind_asset);
  return { id: await addLimit(wid, sym, req.body) };
});
DEL('/api/limit/:id', auth, async req => { await Q('delete from lim where id=$1', [+req.params.id]); await loadOrders(); await refreshUserSyms(); });
GET('/api/orders', auth, async req => {
  const wid = req.query.all ? null : await activeWalletId(req);
  const ord = [...condOrders.values()].flat().filter(o => !wid || o.wid === wid).map(orderJson);
  const lims = [...limOrders.values()].flat().filter(l => !wid || l.wid === wid);
  return { ord, lims };
});

// ---------- alerts and watchlist ----------
POST('/api/alert', auth, async req => {
  const b = req.body, sym = cleanSym(b.sym); if (!sym) fail('Pick something');
  if (!assets.has(sym)) await trackAsset(sym, b.kind_asset);
  const p = priceOf(sym), kind = b.kind === 'daymove' ? 'daymove' : 'price';
  if (kind === 'daymove') { const v = num(b.pct); if (!(v >= 0.5 && v <= 50)) fail('Pick a move between 0.5% and 50%'); await Q('insert into alert(sym,kind,dir,price,pct,rep,label) values($1,$2,$3,$4,$5,true,$6)', [sym, kind, 'both', p || 0, v, String(b.label || '').slice(0, 80) || null]); }
  else {
    const price = num(b.price);
    if (!p) fail('No live price yet for ' + sym);
    if (!(price > 0) || Math.abs(price / p - 1) < 0.0001) fail('Pick a price different from the current one');
    await Q('insert into alert(sym,kind,dir,price,rep,label) values($1,$2,$3,$4,$5,$6)', [sym, kind, price > p ? 'above' : 'below', price, !!b.rep, String(b.label || '').slice(0, 80) || null]);
  }
  await loadAlerts(); await refreshUserSyms(); checkAchievementsSoon(); return { ok: true };
});
PATCH('/api/alert/:id', auth, async req => {
  const a = await row('select * from alert where id=$1', [+req.params.id]); if (!a) fail('That alert is gone', 404);
  if (req.body.rearm) { const p = priceOf(a.sym) || +a.price; await Q('update alert set fired=false, armed=true, dir=$1 where id=$2', [+a.price > p ? 'above' : 'below', a.id]); }
  if ('rep' in req.body) await Q('update alert set rep=$1 where id=$2', [!!req.body.rep, a.id]);
  await loadAlerts(); return { ok: true };
});
DEL('/api/alert/:id', auth, async req => { await Q('delete from alert where id=$1', [+req.params.id]); await loadAlerts(); await refreshUserSyms(); });
DEL('/api/alerts/fired', auth, async () => { await Q('delete from alert where fired'); });
POST('/api/watch', auth, async req => {
  const sym = cleanSym(req.body.sym); if (!sym) fail('Pick something');
  if (req.body.on) { if (!assets.has(sym)) await trackAsset(sym, req.body.kind); await Q('insert into watch(sym,pos) values($1,(select coalesce(max(pos),0)+1 from watch)) on conflict do nothing', [sym]); }
  else await Q('delete from watch where sym=$1', [sym]);
  await refreshUserSyms(); return { ok: true };
});
POST('/api/watch/order', auth, async req => {
  const list = (req.body.syms || []).map(cleanSym).filter(Boolean);
  for (let i = 0; i < list.length; i++) await Q('update watch set pos=$1 where sym=$2', [i, list[i]]);
});

// ---------- history and journal ----------
GET('/api/activity', auth, async req => {
  const wid = await activeWalletId(req), lim = clamp(num(req.query.limit, 50), 1, 200);
  const where = ['wid=$1'], p = [wid];
  if (req.query.before) { p.push(+req.query.before); where.push(`id < $${p.length}`); }
  const types = { buy: "side='buy'", sell: "side='sell'", auto: "why <> 'manual' and side in ('buy','sell')", div: "side in ('div','int')", cash: "side='cash'", notes: "note is not null and note <> ''" };
  if (types[req.query.type]) where.push(types[req.query.type]);
  if (req.query.q) { p.push('%' + String(req.query.q).toUpperCase() + '%'); where.push(`(upper(sym) like $${p.length} or upper(coalesce(note,'')) like $${p.length} or upper(coalesce(tags,'')) like $${p.length})`); }
  p.push(lim);
  return (await rows(`select * from trd where ${where.join(' and ')} order by id desc limit $${p.length}`, p)).map(tradeJson);
});
POST('/api/note', auth, async req => {
  const id = +req.body.id; const note = String(req.body.note || '').slice(0, 1000) || null;
  const tags = String(req.body.tags || '').split(',').map(t => t.trim().toLowerCase().replace(/[^a-z0-9 \-]/g, '')).filter(Boolean).slice(0, 8).join(',') || null;
  await Q('update trd set note=$1, tags=$2 where id=$3', [note, tags, id]); checkAchievementsSoon();
});
GET('/api/analytics', auth, async req => analytics(await activeWalletId(req)));
GET('/api/dividends', auth, async req => {
  const wid = await activeWalletId(req), pos = await positions(wid);
  const got = (await rows("select * from trd where wid=$1 and side in ('div','int') order by id desc", [wid])).map(tradeJson);
  const holdings = [];
  for (const p of pos.filter(p => !isCrypto(p.sym))) {
    const ins = await assetInsights(p.sym).catch(() => ({}));
    const divs = ins.dividends || [], yr = divs.filter(d => d.date > Date.now() - 370 * 86400e3);
    const annual = yr.reduce((t, d) => t + d.amount, 0);
    holdings.push({ sym: p.sym, qty: p.qty, annual, yield: priceOf(p.sym) ? annual / priceOf(p.sym) * 100 : null, yoc: p.avg ? annual / p.avg * 100 : null, est: annual * p.qty, last: divs[0] || null, freq: yr.length });
  }
  return { received: got, total: got.filter(t => t.side === 'div').reduce((t, x) => t + x.qty * x.price, 0), interest: got.filter(t => t.side === 'int').reduce((t, x) => t + x.price, 0), holdings: holdings.sort((a, b) => b.est - a.est), on: S.f_divs };
});

// =====================================================================
// Wallets
// =====================================================================
const COLORS = ['mango', 'leaf', 'sky', 'grape', 'rose', 'slate'];
GET('/api/wallets', auth, () => walletsSummary());
POST('/api/wallets', auth, async req => {
  const b = req.body, name = String(b.name || '').trim().slice(0, 30) || 'New wallet';
  const start = clamp(num(b.start, 10000), 100, 1e9);
  if (+(await row('select count(*) n from wallets')).n >= 25) fail('You can have up to 25 wallets');
  const r = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort) values($1,$2,$3,$4,$4,(select coalesce(max(sort),0)+1 from wallets)) returning *`,
    [name, String(b.emoji || '💼').slice(0, 8), COLORS.includes(b.color) ? b.color : 'sky', start]);
  await snapshot(r.id); checkAchievementsSoon();
  return walletJson(r);
});
PATCH('/api/wallets/:id', auth, async req => {
  const w = await walletRow(+req.params.id), b = req.body;
  if (b.archived === true) { const n = +(await row('select count(*) n from wallets where not archived and id<>$1', [w.id])).n; if (!n) fail("You can't archive your only wallet"); }
  await Q('update wallets set name=$1, emoji=$2, color=$3, archived=$4, sort=$5 where id=$6', [
    b.name !== undefined ? (String(b.name).trim().slice(0, 30) || w.name) : w.name, b.emoji !== undefined ? String(b.emoji).slice(0, 8) : w.emoji,
    COLORS.includes(b.color) ? b.color : w.color, b.archived !== undefined ? !!b.archived : w.archived, b.sort !== undefined ? num(b.sort) : w.sort, w.id]);
  if (b.archived === true && S.defaultWallet === w.id) await saveSettings({ defaultWallet: (await row('select id from wallets where not archived order by sort, id limit 1')).id });
  await refreshUserSyms(); return walletJson(await walletRow(w.id));
});
async function resetWallet(wid, start) {
  const w = await walletRow(wid);
  const pos = await positions(wid), v = +w.cash + marketValue(pos);
  const tr = await row("select count(*) n, count(*) filter (where side='sell') closed, count(*) filter (where side='sell' and pnl>0) wins from trd where wid=$1 and side in ('buy','sell')", [wid]);
  await tx(async c => {
    if (+tr.n > 0) await c.query('insert into seasons(wid,started,start_bal,final_bal,trades,wins,closed,name) values($1,$2,$3,$4,$5,$6,$7,$8)', [wid, w.season_start, w.start_bal, v, +tr.n, +tr.wins, +tr.closed, `${w.name} · ${new Date(tsMs(w.season_start)).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`]);
    for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid']) await c.query(`delete from ${t} where wid=$1`, [wid]);
    await c.query('update wallets set cash=$1, start_bal=$1, season_start=now() where id=$2', [start, wid]);
  });
  lastSnap.delete(wid); await loadOrders(); await snapshot(wid); await refreshUserSyms();
}
POST('/api/wallets/:id/reset', auth, async req => { const w = await walletRow(+req.params.id); await resetWallet(w.id, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
POST('/api/reset', auth, async req => { const wid = await activeWalletId(req); const w = await walletRow(wid); await resetWallet(wid, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
DEL('/api/wallets/:id', auth, async req => {
  const w = await walletRow(+req.params.id);
  if (+(await row('select count(*) n from wallets where id<>$1', [w.id])).n === 0) fail("You can't delete your only wallet");
  await tx(async c => { for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid', 'seasons']) await c.query(`delete from ${t} where wid=$1`, [w.id]); await c.query('delete from wallets where id=$1', [w.id]); });
  if (S.defaultWallet === w.id) await saveSettings({ defaultWallet: (await row('select id from wallets order by archived, sort, id limit 1')).id });
  await loadOrders(); await refreshUserSyms();
});
// Money in and out. Deposits don't count as profit: the starting balance moves with them.
async function moveCash(c, wid, delta, why) {
  const w = (await c.query('select * from wallets where id=$1 for update', [wid])).rows[0]; if (!w) fail('That wallet does not exist', 404);
  const next = +w.cash + delta; if (next < -0.005) fail(`${w.name} only has ${money(+w.cash)} in cash`);
  await c.query('update wallets set cash=$1, start_bal=greatest(start_bal+$2, 1) where id=$3', [Math.max(0, next), delta, wid]);
  await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,'','cash',1,$2,$3)", [wid, delta, why]);
}
POST('/api/cash', auth, async req => {
  const wid = await activeWalletId(req), a = num(req.body.amount), act = req.body.action;
  if (!(a >= 0) || a > 1e9) fail('Enter a valid amount');
  await tx(async c => {
    const w = (await c.query('select cash from wallets where id=$1', [wid])).rows[0];
    const delta = act === 'add' ? a : act === 'withdraw' ? -a : a - +w.cash;
    if (Math.abs(delta) < 0.005) return;
    await moveCash(c, wid, delta, act === 'add' ? 'deposit' : act === 'withdraw' ? 'withdrawal' : 'balance set');
  });
  await snapshot(wid);
});
POST('/api/wallets/transfer', auth, async req => {
  const from = +req.body.from, to = +req.body.to, a = num(req.body.amount);
  if (from === to) fail('Pick two different wallets'); if (!(a > 0)) fail('Enter an amount');
  await tx(async c => { await moveCash(c, from, -a, 'transfer out'); await moveCash(c, to, a, 'transfer in'); });
  await snapshot(from); await snapshot(to);
});

// =====================================================================
// Settings, notifications, games, data
// =====================================================================
POST('/api/settings', auth, async req => { await saveSettings(req.body); broadcast({ t: 'evt', e: 'settings' }); return { settings: S }; });
GET('/api/notifs', auth, async req => (await rows('select * from notifs order by id desc limit $1', [clamp(num(req.query.limit, 60), 1, 200)])).map(n => ({ id: n.id, ts: tsMs(n.ts), kind: n.kind, title: n.title, body: n.body, sym: n.sym, wid: n.wid, url: n.url, read: n.read })));
POST('/api/notifs/read', auth, async req => { if (req.body.id) await Q('update notifs set read=true where id=$1', [+req.body.id]); else await Q('update notifs set read=true where not read'); return { unread: await unreadCount() }; });
DEL('/api/notifs', auth, async () => { await Q('delete from notifs'); });
GET('/api/push/key', auth, () => ({ key: VAPID_PUB, on: pushOn }));
POST('/api/push/sub', auth, async req => {
  const sub = req.body.sub; if (!sub?.endpoint || !/^https:\/\//.test(sub.endpoint)) fail('That notification setup looks wrong');
  await Q('insert into push(endpoint,sub,ua) values($1,$2,$3) on conflict(endpoint) do update set sub=$2, ua=$3', [sub.endpoint, JSON.stringify(sub), String(req.headers['user-agent'] || '').slice(0, 200)]);
});
POST('/api/push/unsub', auth, async req => { await Q('delete from push where endpoint=$1', [String(req.body.endpoint || '')]); });
GET('/api/push/devices', auth, async () => (await rows('select endpoint, ua, created from push order by created')).map(d => ({ endpoint: d.endpoint, ua: d.ua, created: tsMs(d.created) })));
POST('/api/push/test', auth, async () => { const n = await sendPush('Mango test 🥭', 'Notifications are working. You will hear about orders, alerts and more here.', { url: '/#/inbox', tag: 'test' }); if (!n) fail('No devices have notifications turned on yet'); return { sent: n }; });
GET('/api/games', auth, async req => {
  const wid = await activeWalletId(req), a = await analytics(wid);
  const [preds, achv, xp, ch, lb] = await Promise.all([predStats(), achievementList(), xpInfo(), challenges(wid, a), leaderboard(wid, a)]);
  return { preds, achv, xp, challenges: ch, leaderboard: lb, ret: a.ret };
});
POST('/api/preds', auth, async req => { const id = await newPrediction(req.body.sym, req.body.dir, req.body.mins); return { id }; });
DEL('/api/preds/:id', auth, async req => { await Q('delete from preds where id=$1 and result is null and start_ts > now() - interval \'2 minutes\'', [+req.params.id]); });
GET('/api/status', auth, async () => ({
  version: BUILD, up: Date.now() - BOOT, market: market(), finnhubKey: !!FINNHUB_KEY, wsMax: WS_MAX,
  feeds: { crypto: { ...feeds.crypto, subs: cbSubs.size }, stocks: { ...feeds.stocks, subs: [...fhSubs] } },
  queue: fhQueue.length, tracked: assets.size, clients: wss.clients.size, db: { schema: +(await kvGet('schema', '0')), snaps: +(await row('select count(*) n from snap')).n },
}));

// ---------- backup ----------
const BACKUP_TABLES = ['wallets', 'pos', 'ord', 'lim', 'trd', 'snap', 'extra', 'watch', 'alert', 'divpaid', 'seasons', 'preds', 'achv', 'meta'];
GET('/api/export', auth, async (req, res) => {
  const wid = await activeWalletId(req);
  if (req.query.fmt === 'csv') {
    const list = await rows('select * from trd where wid=$1 order by id', [wid]);
    const esc = v => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    const lines = ['Date,Type,Symbol,Quantity,Price,Total,Fee,Profit/Loss,Reason,Note,Tags'];
    for (const t of list) lines.push([new Date(tsMs(t.ts)).toISOString(), t.side, t.sym, +t.qty, +t.price, t.side === 'cash' ? +t.price : +t.qty * +t.price, +t.fee || 0, t.pnl == null ? '' : +t.pnl, t.why, t.note, t.tags].map(esc).join(','));
    return send(req, res, 200, lines.join('\n'), 'text/csv; charset=utf-8', { 'Content-Disposition': `attachment; filename="mango-trades-${etDate()}.csv"` });
  }
  const out = { app: 'mango', version: 2, exported: new Date().toISOString(), settings: S, tables: {} };
  for (const t of BACKUP_TABLES) out.tables[t] = await rows(`select * from ${t}`);
  send(req, res, 200, JSON.stringify(out), 'application/json; charset=utf-8', { 'Content-Disposition': `attachment; filename="mango-backup-${etDate()}.json"` });
});
POST('/api/import', auth, async req => {
  const b = req.body?.backup; if (!b || b.app !== 'mango' || !b.tables?.wallets?.length) fail('That file is not a Mango backup');
  if (req.body.confirm !== 'RESTORE') fail('Type RESTORE to confirm');
  await tx(async c => {
    for (const t of BACKUP_TABLES) await c.query(`delete from ${t}`);
    for (const t of BACKUP_TABLES) {
      const list = b.tables[t] || []; if (!list.length) continue;
      const cols = (await c.query(`select column_name from information_schema.columns where table_name=$1`, [t])).rows.map(r => r.column_name);
      for (let i = 0; i < list.length; i += 500) {
        const chunk = list.slice(i, i + 500).map(r => Object.fromEntries(Object.entries(r).filter(([k]) => cols.includes(k))));
        const keys = cols.filter(k => chunk.some(r => k in r));
        await c.query(`insert into ${t}(${keys.join(',')}) select ${keys.join(',')} from json_populate_recordset(null::${t}, $1::json)`, [JSON.stringify(chunk)]);
      }
    }
    for (const t of ['wallets', 'ord', 'lim', 'trd', 'alert', 'seasons', 'preds', 'notifs']) await c.query(`select setval(pg_get_serial_sequence('${t}','id'), greatest((select coalesce(max(id),1) from ${t}),1))`);
  });
  if (b.settings) await saveSettings(b.settings);
  lastSnap.clear(); await loadAssets(); await loadOrders(); await loadAlerts(); await refreshUserSyms();
  return { ok: true };
});

// =====================================================================
// Background jobs
// =====================================================================
fillCbs.push(async f => {
  lastSnap.delete(f.wid); snapshot(f.wid).catch(() => {});
  broadcast({ t: 'evt', e: 'fill', wid: f.wid, sym: f.sym });
  checkAchievementsSoon();
  refreshUserSymsSoon();
});
const refreshUserSymsSoon = debounce(() => refreshUserSyms().catch(e => warn('symbols', e.message)), 1500);
async function savePeaks() {
  for (const list of condOrders.values()) for (const o of list) if (o.kind === 'trail' && o.dirty) { o.dirty = false; await Q('update ord set peak=$1 where id=$2', [o.peak, o.id]).catch(() => {}); }
}
async function divJob() {
  if (!S.f_divs) return;
  for (const p of await rows('select p.* from pos p join wallets w on w.id=p.wid where not w.archived')) {
    if (isCrypto(p.sym)) continue;
    let ev; try { ev = Object.values((await yahooChart(p.sym, 'range=3mo&interval=1d&events=div')).events?.dividends || {}); } catch (e) { continue; }
    for (const v of ev) {
      const ex = v.date * 1000;
      if (ex < tsMs(p.since) || ex > Date.now() || !(v.amount > 0)) continue;
      const got = await row('insert into divpaid(wid,sym,ex) values($1,$2,$3) on conflict do nothing returning sym', [p.wid, p.sym, String(v.date)]);
      if (!got) continue;
      const amt = +p.qty * v.amount;
      await tx(async c => {
        await c.query('update wallets set cash=cash+$1 where id=$2', [amt, p.wid]);
        await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,$2,'div',$3,$4,'dividend')", [p.wid, p.sym, +p.qty, v.amount]);
      });
      await notify('div', `💵 Dividend from ${p.sym}`, `You received ${money(amt)} (${pxStr(v.amount)} a share on ${+(+p.qty).toFixed(4)} shares).`, { sym: p.sym, wid: p.wid });
      checkAchievementsSoon();
    }
  }
}
async function daily() {
  const p = etParts(), today = etDate(), mk = market(), mins = p.h * 60 + p.mi;
  // interest on cash, once a day
  if (S.cashAPY > 0 && mins >= 5 && (await kvGet('job_interest')) !== today) {
    await kvSet('job_interest', today);
    for (const w of await rows('select * from wallets where not archived and cash > 0')) {
      const amt = round(+w.cash * S.cashAPY / 100 / 365, 4); if (amt < 0.0001) continue;
      await tx(async c => { await c.query('update wallets set cash=cash+$1 where id=$2', [amt, w.id]); await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,'','int',1,$2,'interest')", [w.id, amt]); });
    }
  }
  // end-of-day summary after the stock market closes
  if (S.notifySummary && isTradingDay(p.y, p.m, p.d) && mins >= (earlyClose(p.y, p.m, p.d) ? 13 * 60 + 5 : 16 * 60 + 5) && (await kvGet('job_summary')) !== today) {
    await kvSet('job_summary', today);
    const lines = [];
    for (const w of await rows('select * from wallets where not archived order by sort, id')) {
      const pos = await positions(w.id); if (!pos.length) continue;
      const d = dayChange(pos);
      const movers = pos.map(x => ({ sym: x.sym, ch: prices.get(x.sym)?.prev ? (priceOf(x.sym) / prices.get(x.sym).prev - 1) * 100 : 0 })).sort((a, b) => b.ch - a.ch);
      lines.push(`${w.emoji} ${w.name}: ${d.change >= 0 ? '+' : '-'}${money(Math.abs(d.change)).replace('-', '')} (${pct(d.pct)})${movers[0] ? ` · best ${movers[0].sym} ${pct(movers[0].ch)}` : ''}`);
    }
    if (lines.length) await notify('summary', '🔔 Market closed', lines.join('\n'), { url: '/#/portfolio' });
  }
  // big moves on things you own
  if (S.notifyMoves) {
    const sent = new Set(JSON.parse((await kvGet('job_moves')) || '{}')[today] || []);
    let changed = false;
    const held = await rows('select p.sym, sum(p.qty) qty from pos p join wallets w on w.id=p.wid where not w.archived group by p.sym');
    for (const h of held) {
      const q = prices.get(h.sym); if (!q?.prev || q.src === 'cache' || sent.has(h.sym)) continue;
      if (!isCrypto(h.sym) && mk.state !== 'open') continue;
      const ch = (q.p / q.prev - 1) * 100;
      if (Math.abs(ch) < S.moveThreshold) continue;
      sent.add(h.sym); changed = true;
      await notify('move', `${h.sym} is ${ch >= 0 ? 'up' : 'down'} ${Math.abs(ch).toFixed(1)}% today ${ch >= 0 ? '🚀' : '📉'}`, `It's at ${pxStr(q.p)}. Your ${+(+h.qty).toFixed(6)} ${isCrypto(h.sym) ? h.sym : 'shares'} ${ch >= 0 ? 'gained' : 'lost'} about ${money(Math.abs(+h.qty * (q.p - q.prev)))} today.`, { sym: h.sym });
    }
    if (changed) await kvSet('job_moves', JSON.stringify({ [today]: [...sent] }));
  }
}
let lastMarketState = null;
function startJobs() {
  const every = (ms, fn, name) => setInterval(() => Promise.resolve().then(fn).catch(e => warn(name, e.message)), ms);
  every(60e3, snapshotAll, 'snapshots'); setTimeout(() => snapshotAll().catch(() => {}), 15e3);
  every(30e3, savePeaks, 'peaks');
  every(60e3, savePriceCache, 'prices');
  every(10e3, resolvePredictions, 'predictions');
  every(30e3, expireOrders, 'expiry');
  every(1200, pollOneQuote, 'quotes');
  every(60e3, daily, 'daily');
  every(30e3, () => { fhSync(); const st = market().state; if (st !== lastMarketState) { lastMarketState = st; broadcast({ t: 'evt', e: 'market', market: market() }); } }, 'market');
  every(5 * 60e3, refreshUserSyms, 'symbols');
  every(6 * 3600e3, divJob, 'dividends'); setTimeout(() => divJob().catch(() => {}), 90e3);
  every(3600e3, async () => { await thinSnapshots(); await Q('delete from notifs where id not in (select id from notifs order by id desc limit 500)'); }, 'cleanup');
  every(10 * 60e3, checkAchievements, 'achievements'); setTimeout(() => checkAchievements().catch(e => warn('achievements', e.message)), 20e3);
  // Feed watchdogs: reconnect if a stream goes quiet.
  every(30e3, () => {
    if (cbws && cbws.readyState === 1 && feeds.crypto.last && Date.now() - feeds.crypto.last > 120e3) { warn('crypto feed went quiet, reconnecting'); cbws.terminate(); }
    if (fhws && fhws.readyState === 1 && market().state === 'open' && fhSubs.size && Date.now() - Math.max(feeds.stocks.last, BOOT) > 6 * 60e3) { warn('stock feed went quiet, reconnecting'); feeds.stocks.last = Date.now(); fhws.terminate(); }
  }, 'watchdog');
}

// =====================================================================
// App files: the page, icons, manifest, service worker
// =====================================================================
function png(w, h, px) {
  const raw = Buffer.alloc((w * 4 + 1) * h);
  for (let y = 0; y < h; y++) { raw[y * (w * 4 + 1)] = 0; px.copy(raw, y * (w * 4 + 1) + 1, y * w * 4, (y + 1) * w * 4); }
  const T = []; for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; T[n] = c >>> 0; }
  const crc = b => { let c = -1; for (const x of b) c = T[(c ^ x) & 255] ^ (c >>> 8); return (c ^ -1) >>> 0; };
  const chunk = (t, d) => { const b = Buffer.concat([Buffer.from(t), d]), l = Buffer.alloc(4), k = Buffer.alloc(4); l.writeUInt32BE(d.length); k.writeUInt32BE(crc(b)); return Buffer.concat([l, b, k]); };
  const ih = Buffer.alloc(13); ih.writeUInt32BE(w, 0); ih.writeUInt32BE(h, 4); ih[8] = 8; ih[9] = 6;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ih), chunk('IDAT', zlib.deflateSync(raw, { level: 9 })), chunk('IEND', Buffer.alloc(0))]);
}
// The mango icon, drawn with math so there are no image files to upload.
function drawIcon(size, { scale = 1, mono = false, bg = true } = {}) {
  const px = Buffer.alloc(size * size * 4), SS = 4;
  const rot = (x, y, a) => [x * Math.cos(a) - y * Math.sin(a), x * Math.sin(a) + y * Math.cos(a)];
  const mix = (a, b, t) => a.map((v, i) => v + (b[i] - v) * t);
  const sm = (e0, e1, x) => { const t = clamp((x - e0) / (e1 - e0), 0, 1); return t * t * (3 - 2 * t); };
  const segD = (px_, py, ax, ay, bx, by) => { const dx = bx - ax, dy = by - ay, t = clamp(((px_ - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy), 0, 1); return Math.hypot(px_ - ax - t * dx, py - ay - t * dy); };
  const LINE = [[0.315, 0.64], [0.415, 0.545], [0.49, 0.6], [0.64, 0.445]];
  function sample(u, v) {
    // to icon space, centered and scaled
    let x = (u - 0.5) / scale + 0.5, y = (v - 0.5) / scale + 0.5;
    let col = bg ? mix([44, 26, 62], [17, 11, 27], v) : null, a = bg ? 1 : 0;
    if (bg) { const g = Math.max(0, 1 - Math.hypot(u - 0.5, v - 0.55) / 0.5); col = mix(col, [255, 140, 50], g * g * 0.22); }
    // mango body: a tilted egg
    let [mx, my] = rot(x - 0.49, y - 0.56, 0.52); my *= 1 + mx * 0.35;
    const body = Math.hypot(mx / 0.285, my / 0.355) - 1;
    const bodyA = sm(0.012, -0.012, body * 0.3);
    if (bodyA > 0) {
      let c;
      if (mono) c = [255, 255, 255];
      else {
        const t = clamp((x - 0.25) * 0.9 + (y - 0.3) * 0.9, 0, 1);
        c = t < 0.5 ? mix([255, 214, 70], [255, 160, 40], t / 0.5) : mix([255, 160, 40], [250, 82, 86], (t - 0.5) / 0.5);
        const hl = Math.max(0, 1 - Math.hypot((x - 0.39) / 0.11, (y - 0.42) / 0.07)); c = mix(c, [255, 250, 225], hl * 0.55);
        const rim = sm(-0.05, 0.0, body * 0.3); c = mix(c, [200, 60, 70], rim * 0.25);
        // rising chart line
        let d = 1; for (let i = 0; i < LINE.length - 1; i++) d = Math.min(d, segD(x, y, ...LINE[i], ...LINE[i + 1]));
        const [ex, ey] = LINE[LINE.length - 1];
        d = Math.min(d, segD(x, y, ex, ey, ex - 0.075, ey + 0.012), segD(x, y, ex, ey, ex - 0.012, ey + 0.075));
        c = mix(c, [255, 255, 255], sm(0.028, 0.02, d));
      }
      col = col ? mix(col, c, bodyA) : c; a = Math.max(a, bodyA);
    }
    // stem
    const st = segD(x, y, 0.575, 0.245, 0.605, 0.175) - 0.017;
    const stA = sm(0.004, -0.004, st);
    if (stA > 0) { const c = mono ? [255, 255, 255] : [96, 58, 34]; col = col ? mix(col, c, stA) : c; a = Math.max(a, stA); }
    // leaf: two overlapping circles
    let [lx, ly] = rot(x - 0.715, y - 0.175, -0.42);
    const leaf = Math.max(Math.hypot(lx, ly - 0.11) - 0.15, Math.hypot(lx, ly + 0.11) - 0.15);
    const leafA = sm(0.006, -0.006, leaf);
    if (leafA > 0) {
      let c = mono ? [255, 255, 255] : mix([92, 230, 150], [34, 160, 98], clamp((lx + 0.1) * 4, 0, 1));
      if (!mono) c = mix(c, [24, 120, 74], sm(0.012, 0.004, Math.abs(ly)) * (Math.abs(lx) < 0.085 ? 0.6 : 0));
      col = col ? mix(col, c, leafA) : c; a = Math.max(a, leafA);
    }
    return [col || [0, 0, 0], a];
  }
  for (let j = 0; j < size; j++) for (let i = 0; i < size; i++) {
    let r = 0, g = 0, b = 0, al = 0;
    for (let sy = 0; sy < SS; sy++) for (let sx = 0; sx < SS; sx++) {
      const [c, a] = sample((i + (sx + 0.5) / SS) / size, (j + (sy + 0.5) / SS) / size);
      r += c[0] * a; g += c[1] * a; b += c[2] * a; al += a;
    }
    const o = (j * size + i) * 4, n = SS * SS;
    px[o + 3] = Math.round(al / n * 255);
    if (al > 0) { px[o] = Math.round(r / al); px[o + 1] = Math.round(g / al); px[o + 2] = Math.round(b / al); }
  }
  return png(size, size, px);
}
const ICONS = {};
const icon = (name, fn) => ICONS[name] || (ICONS[name] = fn());
const FAVICON = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#FFD646"/><stop offset=".5" stop-color="#FFA028"/><stop offset="1" stop-color="#FA5256"/></linearGradient></defs><rect width="64" height="64" rx="14" fill="#1E1430"/><ellipse cx="31" cy="36" rx="18" ry="22" transform="rotate(30 31 36)" fill="url(#g)"/><path d="M20 41l6.5-6 5 3.5 9.5-10" fill="none" stroke="#fff" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"/><path d="M37 28.5h4.5V33" fill="none" stroke="#fff" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"/><ellipse cx="45" cy="11.5" rx="9" ry="4.6" transform="rotate(-24 45 11.5)" fill="#3BD68A"/></svg>`;
let BUILD = 'dev', INDEX = null, INDEX_GZ = null;
function loadIndex() {
  const file = path.join(__dirname, 'index.html');
  let html = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '<!doctype html><title>Mango</title><p>index.html is missing next to server.js.</p>';
  let srv = ''; try { srv = fs.readFileSync(__filename, 'utf8'); } catch (e) {}
  BUILD = crypto.createHash('sha1').update(html).update(srv).digest('hex').slice(0, 10);
  html = html.replace(/__BUILD__/g, BUILD);
  INDEX = Buffer.from(html); INDEX_GZ = zlib.gzipSync(INDEX, { level: 9 });
}
const SW = () => `// Mango service worker ${BUILD}
const V = 'mango-${BUILD}';
self.addEventListener('install', e => { self.skipWaiting(); e.waitUntil(caches.open(V).then(c => c.addAll(['/', '/icon-192.png', '/favicon.svg'])).catch(() => {})); });
self.addEventListener('activate', e => e.waitUntil((async () => { for (const k of await caches.keys()) if (k !== V && k.startsWith('mango-')) await caches.delete(k); await self.clients.claim(); })()));
self.addEventListener('fetch', e => {
  const r = e.request; if (r.method !== 'GET') return;
  const u = new URL(r.url);
  if (u.origin === location.origin && (u.pathname === '/' || u.pathname === '/index.html')) {
    e.respondWith((async () => {
      const c = await caches.open(V);
      try { const net = await Promise.race([fetch(r), new Promise((_, no) => setTimeout(() => no(new Error('slow')), 4500))]); if (net.ok) c.put('/', net.clone()); return net; }
      catch (err) { return (await c.match('/')) || fetch(r); }
    })());
    return;
  }
  if (/fonts\\.(googleapis|gstatic)\\.com$/.test(u.hostname) || (u.origin === location.origin && /\\.(png|svg)$/.test(u.pathname))) {
    e.respondWith(caches.open(V).then(async c => { const hit = await c.match(r); if (hit) return hit; const net = await fetch(r); if (net.ok || net.type === 'opaque') c.put(r, net.clone()); return net; }));
  }
});
self.addEventListener('push', e => {
  let d = {}; try { d = e.data ? e.data.json() : {}; } catch (err) { d = { title: 'Mango', body: e.data && e.data.text() }; }
  const jobs = [self.registration.showNotification(d.title || 'Mango', { body: d.body || '', icon: '/icon-192.png', badge: '/badge.png', tag: d.tag, data: { url: d.url || '/' }, renotify: !!d.tag })];
  if (typeof d.badge === 'number' && self.navigator.setAppBadge) jobs.push(d.badge > 0 ? self.navigator.setAppBadge(d.badge) : self.navigator.clearAppBadge());
  e.waitUntil(Promise.all(jobs).catch(() => {}));
});
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil((async () => {
    const list = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of list) { try { await c.focus(); c.postMessage({ nav: url }); return; } catch (err) {} }
    return self.clients.openWindow(url);
  })());
});`;
staticHandler = (req, res) => {
  const p = req.path;
  if (p === '/' || p === '/index.html') {
    const etag = '"' + BUILD + '"';
    if (req.headers['if-none-match'] === etag) { res.writeHead(304, { ETag: etag }); res.end(); return true; }
    const gz = /gzip/.test(req.headers['accept-encoding'] || '');
    send(req, res, 200, gz ? INDEX_GZ : INDEX, 'text/html; charset=utf-8', { 'Cache-Control': 'no-cache', ETag: etag, 'Content-Security-Policy': CSP, ...(gz ? { 'Content-Encoding': 'gzip', Vary: 'Accept-Encoding' } : {}) });
    return true;
  }
  const img = (buf, type = 'image/png') => { send(req, res, 200, buf, type, { 'Cache-Control': 'public, max-age=86400' }); return true; };
  if (p === '/sw.js') { send(req, res, 200, SW(), 'text/javascript; charset=utf-8', { 'Cache-Control': 'no-cache', 'Service-Worker-Allowed': '/' }); return true; }
  if (p === '/icon.png' || p === '/apple-touch-icon.png' || p === '/apple-touch-icon-precomposed.png') return img(icon('180', () => drawIcon(180)));
  if (p === '/icon-192.png') return img(icon('192', () => drawIcon(192)));
  if (p === '/icon-512.png') return img(icon('512', () => drawIcon(512)));
  if (p === '/icon-maskable.png') return img(icon('m512', () => drawIcon(512, { scale: 0.78 })));
  if (p === '/badge.png') return img(icon('badge', () => drawIcon(96, { mono: true, bg: false })));
  if (p === '/favicon.svg' || p === '/favicon.ico') return img(Buffer.from(FAVICON), 'image/svg+xml');
  if (p === '/manifest.json' || p === '/manifest.webmanifest') {
    send(req, res, 200, {
      name: 'Mango Paper Trading', short_name: 'Mango', id: '/', start_url: '/', scope: '/', display: 'standalone', orientation: 'any',
      background_color: '#120C1C', theme_color: '#120C1C', description: 'Paper trading with real market prices',
      icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png' }, { src: '/icon-maskable.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }],
      shortcuts: [{ name: 'Market', url: '/#/market' }, { name: 'Activity', url: '/#/activity' }, { name: 'Games', url: '/#/games' }],
    }, 'application/manifest+json', { 'Cache-Control': 'public, max-age=3600' });
    return true;
  }
  if (p === '/robots.txt') { send(req, res, 200, 'User-agent: *\nDisallow: /\n', 'text/plain'); return true; }
  return false;
};

// =====================================================================
// Live connection for prices and events
// =====================================================================
server.on('upgrade', (req, sock, head) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch (e) { return sock.destroy(); }
  if (u.pathname !== '/stream') return sock.destroy();
  if (!tokenValid(u.searchParams.get('t'))) { sock.write('HTTP/1.1 401 Unauthorized\r\n\r\n'); return sock.destroy(); }
  if (wss.clients.size >= 30) { sock.write('HTTP/1.1 503 Busy\r\n\r\n'); return sock.destroy(); }
  wss.handleUpgrade(req, sock, head, ws => {
    ws.alive = true;
    ws.on('pong', () => { ws.alive = true; });
    ws.on('message', m => {
      let d; try { d = JSON.parse(m); } catch (e) { return; }
      if (d.t === 'ping') { try { ws.send('{"t":"pong"}'); } catch (e) {} }
      if (d.t === 'focus' && cleanSym(d.sym)) { focus.set(cleanSym(d.sym), Date.now()); fhSync(); }
    });
    try { ws.send(JSON.stringify({ t: 'hello', v: BUILD, market: market() })); } catch (e) {}
  });
});
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; try { c.ping(); } catch (e) {} } }, 30e3);

// =====================================================================
// Startup
// =====================================================================
async function shutdown(sig) {
  log(`Shutting down (${sig})…`);
  try { await Promise.race([Promise.all([savePriceCache(), savePeaks()]), sleep(3000)]); } catch (e) {}
  try { server.close(); } catch (e) {}
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM')); process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', e => warn('unhandled', e && (e.stack || e.message || e)));
(async () => {
  loadIndex();
  await migrate();
  tokenSalt = (await kvGet('token_salt', '')) || '';
  await loadSettings(); await loadAssets(); await loadPriceCache(); await loadOrders(); await loadAlerts(); await initPush();
  await refreshUserSyms(); await walletsSummary();
  server.listen(PORT, () => log(`🥭 Mango is running on port ${PORT} · build ${BUILD} · ${FINNHUB_KEY ? 'stocks on' : 'stocks off (no FINNHUB_KEY)'}`));
  cbConnect(); fhConnect(); startJobs();
})().catch(e => { console.error('Mango could not start:', e); process.exit(1); });
