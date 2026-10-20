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
  // 2 — real-market rules: broker profiles, account types, tax lots, settlement, transfers, splits, richer orders and fills
  async c => {
    await c.query(`
      alter table wallets add column if not exists profile text not null default 'robinhood';
      alter table wallets add column if not exists acct text not null default 'instant';
      alter table wallets add column if not exists gold boolean not null default false;
      alter table wallets add column if not exists drip boolean not null default false;
      alter table wallets add column if not exists lot_method text not null default 'fifo';
      alter table wallets add column if not exists crypto_route text not null default 'mm';
      alter table wallets add column if not exists margin_max numeric;
      alter table wallets add column if not exists broker text;
      alter table wallets add column if not exists margin_accrued numeric not null default 0;
      alter table wallets add column if not exists call_amt numeric not null default 0;
      alter table wallets add column if not exists call_since timestamptz;
      alter table trd add column if not exists fees text;
      alter table trd add column if not exists bid numeric;
      alter table trd add column if not exists ask numeric;
      alter table trd add column if not exists otype text;
      alter table trd add column if not exists tif text;
      alter table trd add column if not exists sess text;
      alter table trd add column if not exists settle timestamptz;
      alter table trd add column if not exists liq text;
      alter table trd add column if not exists via text;
      alter table trd add column if not exists ext_id text;
      create unique index if not exists trd_ext on trd(ext_id);
      alter table lim add column if not exists lpx numeric;
      alter table lim add column if not exists tif text not null default 'gtc';
      alter table lim add column if not exists sess text not null default 'regular';
      alter table lim add column if not exists rested boolean not null default true;
      alter table lim add column if not exists trig boolean not null default false;
      alter table lim add column if not exists why text;
      alter table ord add column if not exists expires timestamptz;
      create table if not exists lots(id serial primary key, wid int not null, sym text not null, qty numeric not null, basis numeric not null, ts timestamptz not null, trd_id int, wash_used numeric not null default 0);
      create index if not exists lots_w on lots(wid, sym);
      create table if not exists realized(id serial primary key, wid int not null, trd_id int, sym text not null, qty numeric not null, proceeds numeric not null, basis numeric not null,
        acquired timestamptz not null, sold timestamptz not null, term text not null, crypto boolean not null default false, wash numeric not null default 0, wash_qty numeric not null default 0);
      create index if not exists realized_w on realized(wid, sold);
      create table if not exists settle(id serial primary key, wid int not null, amount numeric not null, settles timestamptz not null, trd_id int);
      create table if not exists xfer(id serial primary key, wid int not null, kind text not null, amount numeric not null, instant numeric not null default 0, status text not null default 'pending',
        created timestamptz not null default now(), avail timestamptz, done timestamptz, method text, fee numeric not null default 0);
      create table if not exists splits(wid int not null, sym text not null, ex text not null, primary key(wid, sym, ex));
    `);
    // Open GTC orders get the brokers' 90-day limit, counted from today so nothing disappears right away.
    await c.query("update ord set expires = now() + interval '90 days' where expires is null");
    await c.query("update lim set expires = now() + interval '90 days' where expires is null and kind <> 'moo'");
    await backfillLots(c);
  },
  // 3: bots that trade by themselves, and their decision journal
  async c => {
    await c.query(`
      create table if not exists bots(id serial primary key, name text not null, wid int not null, strategy text not null, params text, syms text not null, mandate text,
        status text not null default 'running', created timestamptz not null default now(), last_run timestamptz, last_day text, peak numeric, day_start numeric, day_date text,
        state text, summary text, killed_reason text);
      create table if not exists bot_log(id serial primary key, bot_id int not null, ts timestamptz not null default now(), kind text not null, sym text, title text not null, body text, data text);
      create index if not exists bot_log_b on bot_log(bot_id, ts desc);
    `);
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
  notifyAchv: true, notifyGames: true, pushBots: true, quietStart: '', quietEnd: '',
  // look and feel
  hideBalances: false, showCents: true, compactNums: false, defaultRange: '1D', defaultChart: 'Candles',
  theme: 'mango', accent: 'mango', plColors: 'classic', roundedNums: true, reduceMotion: false, celebrate: true,
  defaultTab: 'portfolio', posMetric: 'day', posSort: 'value', compactRows: false, sparklines: true, defaultWallet: 1,
  chartGrid: true, chartVolume: true, indicators: '',
  // features (every one can be switched off)
  f_watch: true, f_alerts: true, f_analytics: true, f_journal: true, f_news: true, f_earnings: true,
  f_heatmap: true, f_book: true, f_fng: true, f_games: true, f_preds: true, f_achv: true, f_leader: true,
  f_divs: true, f_ext: true, f_movers: true, f_insights: true, f_taxes: true, f_statements: true, f_bots: true, f_buzz: true,
};
const LIMITS = {
  feeFlat: [0, 1000], feePct: [0, 10], slippage: [0, 5], maxPos: [0, 100], reservePct: [0, 100], autoSL: [0, 90], autoTP: [0, 1000],
  defaultBuy: [0, 1e9], cashAPY: [0, 100], startBalance: [100, 1e9], moveThreshold: [1, 50], defaultWallet: [1, 1e9],
};
const CHOICES = {
  closedTrading: ['fill', 'queue', 'block'], confirmStyle: ['hold', 'tap', 'none'], defaultRange: ['1D', '1W', '1M', '3M', '1Y', 'ALL', '5Y'],
  defaultChart: ['Candles', 'Line'], theme: ['mango', 'midnight', 'light', 'auto'], accent: ['mango', 'leaf', 'sky', 'grape', 'rose'],
  plColors: ['classic', 'colorblind', 'reversed'], defaultTab: ['portfolio', 'market', 'bots', 'activity', 'games'],
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
  const [p, o, l, a, w, e, pr, bt] = await Promise.all([
    rows('select distinct p.sym from pos p join wallets w on w.id=p.wid where not w.archived'), rows('select distinct sym from ord'), rows('select distinct sym from lim'),
    rows('select distinct sym from alert where not fired'), rows('select sym from watch'), rows('select sym, kind from extra'), rows('select distinct sym from preds where result is null'), rows("select syms from bots where status<>'killed'").catch(() => [])]);
  userSyms = { pos: new Set(p.map(x => x.sym)), orders: new Set([...o, ...l].map(x => x.sym).concat(bt.flatMap(x => { try { return JSON.parse(x.syms); } catch (e) { return []; } }))), alerts: new Set([...a, ...pr].map(x => x.sym)), watch: new Set(w.map(x => x.sym)), extra: new Set(e.map(x => x.sym)) };
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
      const q = prices.get(s); if (q && +d.best_bid > 0 && +d.best_ask > 0) { q.bid = +d.best_bid; q.ask = +d.best_ask; q.bqT = Date.now(); }
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
      if (mk.state !== 'open' && cur) cur.close = d.c;
      if (cur && (cur.wsT > Date.now() - 90e3 || (mk.state !== 'open' && cur.extT > Date.now() - 15 * 60e3))) { cur.prev = d.pc || cur.prev; cur.hi = d.h || cur.hi; cur.lo = d.l || cur.lo; cur.open = d.o || cur.open; cur.qT = Date.now(); dirty.add(s); }
      else { tick(s, d.c, { prev: d.pc, hi: d.h, lo: d.l, open: d.o, src: 'rest' }); prices.get(s).qT = Date.now(); }
    } catch (e) { /* try again later */ }
    return;
  }
}

// Pre-market, after-hours and overnight prices for the stocks that matter (Yahoo includes extended trading).
let extCursor = 0;
async function pollExtended() {
  const st = market().state; if (st !== 'pre' && st !== 'post') return; // Yahoo has pre-market and after-hours trades, not overnight ones
  const now = Date.now();
  const want = [...new Set([...userSyms.pos, ...userSyms.orders, ...[...focus.entries()].filter(([, t]) => now - t < 3 * 60e3).map(([s]) => s)])].filter(s => kindOf(s) === 'stock').slice(0, 12);
  if (!want.length) return;
  const sym = want[extCursor++ % want.length];
  try {
    const r = await yahooChart(sym, 'range=1d&interval=1m&includePrePost=true');
    const z = r.indicators?.quote?.[0]?.close || [], ts = r.timestamp || [];
    for (let i = ts.length - 1; i >= 0; i--) if (z[i] != null) {
      const t = ts[i] * 1000, q = prices.get(sym);
      if (q && now - t < 20 * 60e3 && t > (q.extT || 0) && !(q.wsT > t)) { tick(sym, z[i], { src: 'ext' }); prices.get(sym).extT = t; }
      break;
    }
  } catch (e) { /* quiet */ }
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
    const met = await cached('met:' + sym, 6 * 3600e3, () => fhApi('stock/metric?symbol=' + encodeURIComponent(sym) + '&metric=all', 2)).catch(() => null);
    if (met?.metric?.['10DayAverageTradingVolume']) a.adv = met.metric['10DayAverageTradingVolume'] * 1e6;
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
    if (m['10DayAverageTradingVolume']) a.adv = m['10DayAverageTradingVolume'] * 1e6;
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
// Real-market rules: broker profiles, sessions, quotes, fees, settlement, margin
// Sources (checked Oct 2026): SEC fee rate advisory 2026-2, FINRA SR-FINRA-2026-021 (TAF pause),
// Robinhood "Trading fees", crypto fee schedule, margin rates; Alpaca crypto fees and margin docs.
// =====================================================================
const PROFILES = {
  robinhood: {
    key: 'robinhood', name: 'Robinhood rules', pre: 7 * 60, post: 20 * 60, overnight: true, gtcDays: 90, minUsd: 1,
    instant: { std: 1000, gold: 5000 }, achDays: 3, wdDays: 2, goldFee: 5, goldFree: 1000, maintBase: 25,
    marginRates: [[50e3, 5.25], [100e3, 5.05], [1e6, 4.75], [10e6, 4.5], [50e6, 4.45], [Infinity, 4.2]],
    // exchange routing tiers by 30-day crypto volume: [up to, taker %, maker %]
    cryptoTiers: [[10e3, 0.95, 0.5], [50e3, 0.75, 0.35], [250e3, 0.25, 0.125], [500e3, 0.15, 0.075], [1e6, 0.125, 0.06], [5e6, 0.10, 0.04], [10e6, 0.04, 0.02], [25e6, 0.03, 0.01], [Infinity, 0.03, 0]],
  },
  alpaca: {
    key: 'alpaca', name: 'Alpaca rules', pre: 4 * 60, post: 20 * 60, overnight: false, gtcDays: 90, minUsd: 1,
    instant: { std: 0, gold: 0 }, achDays: 3, wdDays: 2, goldFee: 0, goldFree: 0, maintBase: 30,
    marginRates: [[Infinity, 6.5]],
    cryptoTiers: [[100e3, 0.25, 0.15], [500e3, 0.22, 0.12], [1e6, 0.20, 0.10], [10e6, 0.18, 0.08], [25e6, 0.15, 0.05], [50e6, 0.13, 0.02], [100e6, 0.12, 0.02], [Infinity, 0.10, 0]],
  },
  practice: { key: 'practice', name: 'Practice rules', pre: 4 * 60, post: 20 * 60, overnight: false, gtcDays: 0, minUsd: 0, instant: { std: Infinity, gold: Infinity }, achDays: 0, wdDays: 0, goldFee: 0, goldFree: 0, maintBase: 25, marginRates: [[Infinity, 0]], cryptoTiers: [] },
};
const profOf = w => PROFILES[w?.profile] || PROFILES.robinhood;
const isReal = w => !!w && w.profile !== 'practice';

// Regulatory fees on US stock sales, with the dates they took effect.
const REG = {
  sec: [['2024-05-22', 27.80e-6], ['2025-05-14', 0], ['2026-04-04', 20.60e-6]],               // per dollar sold
  taf: [['2024-01-01', 0.000166, 8.30], ['2026-01-01', 0.000195, 9.79], ['2026-10-01', 0, 0], ['2027-01-01', 0.000195, 9.79]], // per share sold, max per trade
  cat: 0.000003,                                                                                  // per share, both sides
};
const rateAt = (table, date) => { let r = table[0]; for (const x of table) if (date >= x[0]) r = x; return r; };
const cents = v => Math.round(v * 100 + 1e-9) / 100;
const ceilCents = v => v <= 0 ? 0 : Math.ceil(v * 100 - 1e-7) / 100;
function stockFees(prof, side, shares, notional, date = etDate()) {
  const lines = [];
  const [, secRate] = rateAt(REG.sec, date), [, tafRate, tafMax] = rateAt(REG.taf, date);
  let sec = 0, taf = 0, cat = 0;
  if (prof.key === 'robinhood') {
    if (side === 'sell' && notional > 500) sec = ceilCents(notional * secRate);
    if (side === 'sell' && shares > 50) taf = Math.min(tafMax, cents(shares * tafRate));
    const c = shares * REG.cat; cat = c < 0.01 ? 0 : cents(c);
  } else {
    if (side === 'sell') { sec = ceilCents(notional * secRate); taf = Math.min(tafMax, ceilCents(shares * tafRate)); }
    cat = ceilCents(shares * REG.cat);
  }
  if (side === 'sell') {
    lines.push({ k: 'sec', label: 'SEC fee', amt: sec, note: secRate ? `$${(secRate * 1e6).toFixed(2)} per $1M sold${prof.key === 'robinhood' ? ', $500 or less is free' : ''}` : 'Rate is $0 right now' });
    lines.push({ k: 'taf', label: 'FINRA trading activity fee', amt: taf, note: tafRate ? `$${tafRate} a share, max $${tafMax}${prof.key === 'robinhood' ? ', 50 shares or less is free' : ''}` : 'Paused by FINRA until Jan 1, 2027' });
  }
  lines.push({ k: 'cat', label: 'CAT fee', amt: cat, note: `$${REG.cat} a share${prof.key === 'robinhood' ? ', under 1¢ is free' : ''}` });
  lines.push({ k: 'comm', label: 'Commission', amt: 0, note: 'Free' });
  return { lines, total: cents(sec + taf + cat) };
}
function cryptoTier(prof, vol30) { const t = prof.cryptoTiers.find(x => vol30 < x[0]) || prof.cryptoTiers[prof.cryptoTiers.length - 1]; return t ? { taker: t[1], maker: t[2] } : { taker: 0, maker: 0 }; }
function cryptoFees(prof, w, notional, liq, vol30) {
  if (prof.key === 'robinhood' && (w.crypto_route || 'mm') === 'mm') return { lines: [{ k: 'comm', label: 'Commission', amt: 0, note: 'Market maker routing: no fee, you pay the spread' }], total: 0, rate: 0 };
  const t = cryptoTier(prof, vol30), rate = liq === 'maker' ? t.maker : t.taker;
  const amt = ceilCents(notional * rate / 100);
  return { lines: [{ k: 'crypto', label: `${liq === 'maker' ? 'Maker' : 'Taker'} fee`, amt, note: `${rate}% at your 30-day volume of ${money(vol30)}` }], total: amt, rate };
}
async function cryptoVolume30(wid, c = pool) {
  const r = (await c.query("select coalesce(sum(qty*price),0) v from trd where wid=$1 and side in ('buy','sell') and ts > now() - interval '30 days' and sym = any($2::text[])", [wid, [...assets.values()].filter(a => a.kind === 'crypto').map(a => a.sym)])).rows[0];
  return +r.v || 0;
}

// ---------- trading sessions ----------
// regular 9:30–4:00 (1:00 on early-close days), pre-market, after hours (until 8 PM, or 5 PM on early days),
// and Robinhood's 24 Hour Market overnight (8 PM–7 AM, Sunday to Thursday night) for stocks and ETFs.
const SESSION_LABEL = { regular: 'Regular hours', pre: 'Pre-market', post: 'After hours', overnight: '24 Hour Market', closed: 'Closed' };
function sessionAt(prof, t = Date.now()) {
  const ov = ENV.MARKET_OVERRIDE;
  if (ov === 'open') return 'regular'; if (ov === 'closed') return 'closed'; if (ov === 'pre' || ov === 'post' || ov === 'overnight') return ov;
  const p = etParts(t), mins = p.h * 60 + p.mi, trading = isTradingDay(p.y, p.m, p.d);
  const early = trading && earlyClose(p.y, p.m, p.d), close = early ? 13 * 60 : 16 * 60, postEnd = early ? 17 * 60 : prof.post;
  const [ty, tm, td] = addDays(p.y, p.m, p.d, 1);
  if (trading && mins >= 570 && mins < close) return 'regular';
  if (trading && mins >= prof.pre && mins < 570) return 'pre';
  if (trading && mins >= close && mins < postEnd) return 'post';
  if (prof.overnight && ((mins >= 20 * 60 && isTradingDay(ty, tm, td)) || (mins < prof.pre && trading))) return 'overnight';
  return 'closed';
}
// When does the session the order belongs to end? Used for day orders.
function sessionEnd(prof, ext, t = Date.now()) {
  const mk = market(t); const base = mk.state === 'open' ? t : mk.opensAt;
  const endOn = (y, m, d) => { const early = earlyClose(y, m, d); return ext ? etToUtc(y, m, d, early ? 17 : Math.floor(prof.post / 60), 0) : etToUtc(y, m, d, early ? 13 : 16, 0); };
  let p = etParts(base), end = endOn(p.y, p.m, p.d);
  if (end < t + 60e3) { const [y, m, d] = nextTradingDay(p.y, p.m, p.d); end = endOn(y, m, d); }
  return end;
}

// ---------- settlement (T+1) ----------
// Settlement skips weekends, market holidays and the two bank holidays the market stays open for.
function bankOnlyHoliday(y, m, d) {
  if (m === 10 && d === nthWeekday(y, 10, 1, 2)) return true;                          // Columbus Day
  const w = dow(y, 11, 11); if (m === 11 && ((d === 11 && w !== 0 && w !== 6) || (d === 12 && w === 1))) return true; // Veterans Day
  return false;
}
const isSettleDay = (y, m, d) => isTradingDay(y, m, d) && !bankOnlyHoliday(y, m, d);
function tradeDate(prof, t = Date.now()) {
  const p = etParts(t); let [y, m, d] = [p.y, p.m, p.d];
  if (!isTradingDay(y, m, d) || p.h >= 20) [y, m, d] = nextTradingDay(y, m, d);
  return [y, m, d];
}
function addSettleDays([y, m, d], n) { let out = [y, m, d]; for (let i = 0; i < n;) { out = addDays(...out, 1); if (isSettleDay(...out)) i++; } return out; }
const settleTime = (prof, t = Date.now()) => { const sd = addSettleDays(tradeDate(prof, t), 1); return etToUtc(sd[0], sd[1], sd[2], 6, 0); };
const businessDaysLater = (n, t = Date.now()) => { const p = etParts(t); const sd = addSettleDays([p.y, p.m, p.d], n); return etToUtc(sd[0], sd[1], sd[2], 9, 0); };

// ---------- quotes: bid, ask and how prices fill ----------
const TICKS = { stockBig: 0.01, stockSmall: 0.0001 };
const tickOf = (sym, p) => isCrypto(sym) ? (p >= 1000 ? 0.01 : p >= 1 ? 0.0001 : 1e-8) : (p >= 1 ? TICKS.stockBig : TICKS.stockSmall);
const floorTo = (v, t) => Math.floor(v / t + 1e-9) * t, ceilTo = (v, t) => Math.ceil(v / t - 1e-9) * t;
function modelSpreadBps(sym, sess) {
  if (isCrypto(sym)) return sym === 'BTC' || sym === 'ETH' ? 1 : 4;
  const a = assets.get(sym), cap = a?.cap || 0;
  let bps = INDEXES.includes(sym) ? 0.3 : cap >= 200e9 ? 1 : cap >= 50e9 ? 1.5 : cap >= 10e9 ? 3 : cap >= 2e9 ? 8 : cap >= 300e6 ? 20 : cap > 0 ? 50 : 6;
  if (sess === 'pre' || sess === 'post') bps *= 5; else if (sess === 'overnight') bps *= 10;
  return bps;
}
function quoteOf(sym, sess = 'regular') {
  const q = prices.get(sym); if (!q?.p) return null;
  const now = Date.now(), fresh_ = q.bqT && now - q.bqT < (isCrypto(sym) ? 90e3 : 30e3);
  if (fresh_ && q.bid > 0 && q.ask >= q.bid && (q.ask - q.bid) / q.p < 0.05 && Math.abs((q.bid + q.ask) / 2 / q.p - 1) < 0.02)
    return { bid: q.bid, ask: q.ask, mid: (q.bid + q.ask) / 2, last: q.p, src: isCrypto(sym) ? 'Coinbase' : 'Alpaca', live: true };
  const t = tickOf(sym, q.p), half = Math.max(t / 2, q.p * modelSpreadBps(sym, sess) / 1e4 / 2);
  const dp = t >= 0.01 ? 2 : t >= 0.0001 ? 4 : 8, bid = round(floorTo(q.p - half, t), dp), ask = round(Math.max(bid + t, ceilTo(q.p + half, t)), dp);
  return { bid, ask, mid: (bid + ask) / 2, last: q.p, src: 'estimated', live: false };
}
// Square-root market impact: big orders compared with daily volume move the price against you.
function impactOf(sym, shares, px) {
  const adv = assets.get(sym)?.adv; if (!adv || !shares) return 0;
  return px * 0.02 * Math.sqrt(Math.min(1, shares / adv));
}
// Crypto market orders walk Coinbase's live order book level by level.
async function bookDepth(sym) {
  return cached('depth:' + sym, 2500, async () => {
    const d = await getJ(`${CBX}/products/${sym}-USD/book?level=2`, { timeout: 4000 });
    return { bids: (d.bids || []).slice(0, 60).map(x => [+x[0], +x[1]]), asks: (d.asks || []).slice(0, 60).map(x => [+x[0], +x[1]]), t: Date.now() };
  });
}
async function walkBook(sym, side, { qty, usd }) {
  let b; try { b = await bookDepth(sym); } catch (e) { return null; }
  const levels = side === 'buy' ? b.asks : b.bids; if (!levels.length) return null;
  let gotQ = 0, spent = 0;
  for (const [p, s] of levels) {
    if (qty > 0) { const take = Math.min(s, qty - gotQ); gotQ += take; spent += take * p; if (gotQ >= qty - 1e-12) break; }
    else { const take = Math.min(s * p, usd - spent); spent += take; gotQ += take / p; if (spent >= usd - 1e-9) break; }
  }
  const filled = qty > 0 ? gotQ >= qty - 1e-12 : spent >= usd - 1e-6;
  if (!filled) { // ran past the levels we can see: price the rest at the deepest level plus a little
    const last = levels[levels.length - 1][0] * (side === 'buy' ? 1.002 : 0.998);
    if (qty > 0) { spent += (qty - gotQ) * last; gotQ = qty; } else { gotQ += (usd - spent) / last; spent = usd; }
  }
  return { px: spent / gotQ, levels: levels.length, top: levels[0][0], deep: !filled };
}

// ---------- margin ----------
function maintRate(prof, sym, px) {
  if (isCrypto(sym)) return 100;
  if (prof.key === 'alpaca') return px < 2.5 ? 100 : px < 6 ? 50 : 30;
  return px < 3 ? 100 : px < 5 ? 50 : prof.maintBase;
}
const marginable = (sym, px) => !isCrypto(sym) && px >= 5;
function marginRate(prof, debit) { const t = prof.marginRates.find(x => debit <= x[0]) || prof.marginRates[prof.marginRates.length - 1]; return t[1]; }

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
const walletJson = w => ({
  id: w.id, name: w.name, emoji: w.emoji, color: w.color, cash: +w.cash, start: +w.start_bal, created: tsMs(w.created), seasonStart: tsMs(w.season_start),
  archived: w.archived, sort: w.sort, profile: w.profile || 'robinhood', acct: w.acct || 'instant', gold: !!w.gold, drip: !!w.drip, lotMethod: w.lot_method || 'fifo',
  cryptoRoute: w.crypto_route || 'mm', marginMax: w.margin_max == null ? null : +w.margin_max, broker: w.broker || null, brokerEnv: w.broker ? (BROKER?.env || null) : null,
  call: +w.call_amt > 0 ? { amount: +w.call_amt, since: tsMs(w.call_since) } : null,
});
async function positions(wid, c = null) {
  return ((await (c || pool).query('select * from pos where wid=$1 order by sym', [wid])).rows).map(p => ({ sym: p.sym, qty: +p.qty, avg: +p.avg, since: tsMs(p.since) }));
}
const marketValue = pos => pos.reduce((t, x) => t + x.qty * (priceOf(x.sym) || x.avg), 0);
// Profile, account type and broker link for every wallet, kept in memory for the order engine.
const walletMeta = new Map();
async function loadWalletMeta() { walletMeta.clear(); for (const w of await rows('select * from wallets')) walletMeta.set(w.id, w); }

// ---------- buying power, holds and the account picture ----------
function holdsFor(wid, pos, exclude = null) {
  const out = { cash: 0, shares: {} };
  for (const list of limOrders.values()) for (const l of list) {
    if (l.wid !== wid || l.id === exclude) continue;
    const px = priceOf(l.sym) || l.price;
    if (l.side === 'buy') {
      const unitPx = l.kind === 'stoplimit' ? l.lpx : l.kind === 'limit' ? l.price : px * 1.05;
      out.cash += l.units === 'qty' ? l.amt * unitPx : l.amt;
    } else {
      const p = pos.find(x => x.sym === l.sym); if (!p) continue;
      const q = l.units === 'qty' ? l.amt : l.units === 'usd' ? l.amt / px : p.qty * Math.min(100, l.amt) / 100;
      out.shares[l.sym] = (out.shares[l.sym] || 0) + q;
    }
  }
  return out;
}
async function accountInfo(wid, { c = pool, w = null, pos = null, exclude = null } = {}) {
  w = w || await walletRow(wid, c); pos = pos || await positions(wid, c);
  const prof = profOf(w), real = isReal(w), cash = +w.cash;
  const unsettled = real ? +(await c.query('select coalesce(sum(amount),0) s from settle where wid=$1 and settles > now()', [wid])).rows[0].s : 0;
  const nextSettle = real && unsettled > 0 ? tsMs((await c.query('select min(settles) t from settle where wid=$1 and settles > now()', [wid])).rows[0].t) : null;
  const pending = real ? +(await c.query("select coalesce(sum(amount-instant),0) s from xfer where wid=$1 and kind='deposit' and status='pending'", [wid])).rows[0].s : 0;
  const holds = real ? holdsFor(wid, pos, exclude) : { cash: 0, shares: {} };
  let lmv = 0, lmvM = 0, maint = 0;
  for (const p of pos) { const px = priceOf(p.sym) || p.avg, v = p.qty * px; lmv += v; if (marginable(p.sym, px)) lmvM += v; maint += v * maintRate(prof, p.sym, px) / 100; }
  const equity = cash + lmv, debit = Math.max(0, -cash), avail = cash - holds.cash;
  let bp, bpCrypto, withdrawable, marginOn = false;
  if (w.broker && BROKER?.account) { // the broker's own numbers
    const a = BROKER.account;
    return { profile: 'alpaca', acct: w.acct, broker: true, cash, settled: cash, unsettled: 0, nextSettle: null, pendingDeposits: 0, holdsCash: 0, holdsShares: {}, lmv, lmvMarginable: lmvM, equity: +a.equity || equity, debit, maint: +a.maintenance_margin || maint, excess: (+a.equity || equity) - (+a.maintenance_margin || maint),
      bp: +a.buying_power || 0, bpCrypto: +a.non_marginable_buying_power || Math.max(0, cash), withdrawable: +a.cash_withdrawable || 0, marginOn: +a.multiplier > 1, marginRate: 0, accrued: 0, call: 0, daytrades: +a.daytrade_count || 0, status: a.status };
  }
  if (!real) { bp = bpCrypto = Math.max(0, cash); withdrawable = Math.max(0, cash); }
  else if (w.acct === 'cash') { bp = bpCrypto = Math.max(0, cash - unsettled - holds.cash); withdrawable = bp; }
  else if (w.acct === 'margin' && equity >= 2000) {
    marginOn = true;
    const regT = lmvM + 2 * avail, maintCap = (equity - maint) / (prof.maintBase / 100);
    const cap = w.margin_max == null ? Infinity : avail + +w.margin_max;
    bp = Math.max(0, Math.min(regT, maintCap, cap)); bpCrypto = Math.max(0, avail);
    withdrawable = debit > 0 ? 0 : Math.max(0, Math.min(cash - unsettled - holds.cash, equity - maint));
  } else { bp = bpCrypto = Math.max(0, avail); withdrawable = Math.max(0, cash - unsettled - holds.cash); }
  return {
    profile: prof.key, acct: w.acct, cash, settled: cash - unsettled, unsettled, nextSettle, pendingDeposits: pending, holdsCash: holds.cash, holdsShares: holds.shares,
    lmv, lmvMarginable: lmvM, equity, debit, maint, excess: equity - maint, bp, bpCrypto, withdrawable, marginOn,
    marginRate: debit > 0 ? marginRate(prof, debit) : marginRate(prof, 0), accrued: +w.margin_accrued || 0, call: +w.call_amt > 0 ? +w.call_amt : 0,
  };
}

// ---------- planning an order: price, quantity, fees, checks ----------
// Used for the live preview on the trade screen and again at the moment an order fills.
async function planOrder(c, w, pos, acct, sym, side, o = {}) {
  const prof = profOf(w), crypto = isCrypto(sym), sess = crypto ? 'regular' : sessionAt(prof);
  const q = prices.get(sym), quote = quoteOf(sym, sess);
  if (!quote) fail(`There's no live price for ${sym} yet. Try again in a few seconds.`);
  const p = pos.find(x => x.sym === sym), dp = qtyDp(sym), unit = crypto ? sym : 'shares';
  let qty = num(o.qty), px = o.px || null, via = o.px ? 'limit' : 'quote', deep = false;
  if (side === 'sell') {
    if (!p) fail(`You don't own any ${sym} in this wallet`);
    if (!(qty > 0) && o.pct > 0) qty = o.pct >= 100 ? p.qty : p.qty * Math.min(100, o.pct) / 100;
  }
  if (!px) {
    if (crypto && (qty > 0 || o.usd > 0)) { const wb = await walkBook(sym, side, qty > 0 ? { qty } : { usd: o.usd }); if (wb) { px = wb.px; via = 'book'; deep = wb.deep; } }
    if (!px) {
      px = side === 'buy' ? quote.ask : quote.bid;
      if (!crypto) { const est = qty > 0 ? qty : (o.usd || 0) / px; const imp = impactOf(sym, est, px); if (imp) { px = side === 'buy' ? px + imp : Math.max(tickOf(sym, px), px - imp); via = 'quote+impact'; } }
    }
  }
  if (!(qty > 0) && o.usd > 0) qty = o.usd / px;
  const whole = !crypto && (S.wholeShares || sess === 'overnight');
  if (side === 'buy') qty = whole ? Math.floor(qty + 1e-9) : Math.floor(qty * 10 ** dp + 1e-6) / 10 ** dp;
  else {
    if (qty >= p.qty * 0.99999 || qty > p.qty) qty = p.qty;
    else qty = whole && Number.isInteger(p.qty) ? Math.floor(qty) : Math.floor(qty * 10 ** dp + 1e-6) / 10 ** dp;
  }
  if (!(qty > 0)) fail(whole ? (sess === 'overnight' ? 'The 24 Hour Market only trades whole shares' : `That's less than one share of ${sym}`) : `Enter an amount to ${side}`);
  const notional = qty * px;
  if (prof.minUsd && !o.drip && !o.auto && notional < prof.minUsd - 1e-9 && !(side === 'sell' && qty === p?.qty)) fail(`The smallest order is ${money(prof.minUsd)}`);
  if (side === 'sell' && !o.force) {
    const held = acct.holdsShares[sym] || 0, avail = p.qty - held;
    if (qty > avail + 1e-9) fail(held > 0 ? `${+held.toFixed(6)} of your ${sym} ${crypto ? '' : 'shares '}are held for open sell orders. You can sell up to ${+Math.max(0, avail).toFixed(6)} ${unit}, or cancel an order first.` : `You only have ${+p.qty.toFixed(6)} ${unit}`);
  }
  const vol30 = crypto ? await cryptoVolume30(w.id, c) : 0;
  const fees = crypto ? cryptoFees(prof, w, notional, o.liq || 'taker', vol30) : stockFees(prof, side, qty, notional);
  const cashAfter = side === 'buy' ? acct.cash - notional - fees.total : acct.cash + notional - fees.total;
  if (side === 'buy') {
    const cashOnly = crypto || !marginable(sym, px), need = notional + fees.total, bp = cashOnly ? acct.bpCrypto : acct.bp;
    if (need > bp + 0.005) {
      let why = `Not enough buying power. This needs ${money(need)} and you have ${money(bp)}.`;
      if (acct.acct === 'cash' && acct.unsettled > 0) why += ` ${money(acct.unsettled)} from recent sales settles ${acct.nextSettle ? new Date(acct.nextSettle).toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long' }) : 'soon'}.`;
      if (acct.holdsCash > 0) why += ` ${money(acct.holdsCash)} is held for open buy orders.`;
      if (cashOnly && acct.marginOn) why += crypto ? ' Crypto can only be bought with cash, not margin.' : ' Stocks under $5 can only be bought with cash, not margin.';
      fail(why);
    }
    const equity = acct.equity;
    if (S.maxPos > 0 && !o.bot && ((p ? p.qty : 0) + qty) * px > equity * S.maxPos / 100 + 0.01) fail(`One position can be at most ${S.maxPos}% of this wallet (${money(equity * S.maxPos / 100)}). Change this in Settings → Trading.`);
    if (S.reservePct > 0 && !o.bot && cashAfter < equity * S.reservePct / 100 - 0.01) fail(`That would leave less than your ${S.reservePct}% cash reserve (${money(equity * S.reservePct / 100)}).`);
  }
  const pnl = side === 'sell' ? (px - p.avg) * qty - fees.total : null;
  return {
    sym, side, qty, px, notional, fees, quote, sess, via, deep, pnl, cashAfter, liq: o.liq || 'taker',
    settle: !crypto && side === 'sell' ? settleTime(prof) : null, borrow: side === 'buy' && cashAfter < 0 ? -cashAfter : 0,
  };
}

const firing = new Set();
const fillCbs = []; // functions to run after every trade
// The heart of the app: buys and sells, with every rule applied.
async function execTrade(wid, sym, side, o = {}) {
  sym = cleanSym(sym); if (!sym) fail('Pick something to trade');
  if (side !== 'buy' && side !== 'sell') fail('Choose buy or sell');
  const fill = await tx(async c => {
    const w = (await c.query('select * from wallets where id=$1 for update', [wid])).rows[0];
    if (!w) fail('That wallet does not exist', 404);
    if (w.broker) fail('This wallet is connected to your broker, so orders go through the broker.');
    if (w.archived) fail('That wallet is archived. Restore it first.');
    if (!isReal(w)) return practiceTrade(c, w, sym, side, o);
    await c.query('select 1 from pos where wid=$1 and sym=$2 for update', [wid, sym]);
    const pos = await positions(wid, c), acct = await accountInfo(wid, { c, w, pos, exclude: o.limId });
    const pl = await planOrder(c, w, pos, acct, sym, side, o);
    const p = pos.find(x => x.sym === sym), prof = profOf(w);
    const newCash = round(pl.cashAfter, 6);
    let closed = false;
    await c.query('update wallets set cash=$1 where id=$2', [newCash, wid]);
    if (side === 'buy') {
      const nq = round((p ? p.qty : 0) + pl.qty, qtyDp(sym));
      const avg = ((p ? p.qty * p.avg : 0) + pl.notional + pl.fees.total) / nq;
      await c.query(`insert into pos(wid,sym,qty,avg,since) values($1,$2,$3,$4,now()) on conflict(wid,sym) do update set qty=$3, avg=$4`, [wid, sym, nq, avg]);
    } else {
      const left = round(p.qty - pl.qty, qtyDp(sym));
      if (left <= 0) {
        closed = true;
        await c.query('delete from pos where wid=$1 and sym=$2', [wid, sym]);
        await c.query('delete from ord where wid=$1 and sym=$2', [wid, sym]);
        await c.query("delete from lim where wid=$1 and sym=$2 and side='sell'", [wid, sym]);
      } else await c.query('update pos set qty=$1 where wid=$2 and sym=$3', [left, wid, sym]);
    }
    const feeMap = Object.fromEntries(pl.fees.lines.map(l => [l.k, l.amt]));
    const t = (await c.query(`insert into trd(wid,sym,side,qty,price,why,pnl,fee,note,fees,bid,ask,otype,tif,sess,settle,liq,via)
      values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) returning id, ts`,
      [wid, sym, side, pl.qty, pl.px, o.why || 'manual', pl.pnl == null ? null : round(pl.pnl, 6), pl.fees.total, o.note ? String(o.note).slice(0, 500) : null,
        JSON.stringify(feeMap), pl.quote.bid, pl.quote.ask, o.otype || 'market', o.tif || null, pl.sess, pl.settle ? new Date(pl.settle) : null, pl.liq, pl.via])).rows[0];
    let tax = null;
    if (side === 'buy') await addLot(c, wid, sym, pl.qty, (pl.notional + pl.fees.total) / pl.qty, t.id);
    else tax = await consumeLots(c, w, sym, pl.qty, pl.notional - pl.fees.total, t.id);
    if (pl.settle) await c.query('insert into settle(wid,amount,settles,trd_id) values($1,$2,$3,$4)', [wid, round(pl.notional - pl.fees.total, 6), new Date(pl.settle), t.id]);
    return {
      id: t.id, ts: tsMs(t.ts), wid, sym, side, qty: pl.qty, price: pl.px, fee: pl.fees.total, fees: pl.fees.lines, pnl: pl.pnl, pnlPct: pl.pnl != null && p ? pl.pnl / (p.avg * pl.qty) * 100 : null,
      cash: newCash, closed, bid: pl.quote.bid, ask: pl.quote.ask, quoteSrc: pl.quote.src, sess: pl.sess, settle: pl.settle, via: pl.via, borrow: pl.borrow, tax, profile: prof.key,
      stale: prices.get(sym)?.src === 'cache',
    };
  });
  for (const cb of fillCbs) try { await cb(fill); } catch (e) { warn('after trade', e.message); }
  return fill;
}
// The original simple engine, kept for wallets on practice rules.
async function practiceTrade(c, w, sym, side, o) {
  const wid = w.id, q = prices.get(sym); const base = o.px || q?.p;
  if (!(base > 0)) fail(`There's no live price for ${sym} yet. Try again in a few seconds.`);
  const slip = S.slippage / 100, px = o.px ? o.px : side === 'buy' ? base * (1 + slip) : base * (1 - slip);
  const dp = qtyDp(sym), stockWhole = S.wholeShares && !isCrypto(sym);
  const cash = +w.cash;
  const pr = (await c.query('select * from pos where wid=$1 and sym=$2 for update', [wid, sym])).rows[0];
  const pos = pr ? { qty: +pr.qty, avg: +pr.avg } : null;
  let qty = num(o.qty), pnl = null, fee = 0, newCash = cash, closed = false, tax = null;
  if (side === 'buy') {
    if (!(qty > 0) && o.usd > 0) qty = o.usd / px;
    qty = stockWhole ? Math.floor(qty + 1e-9) : round(qty, dp);
    if (!(qty > 0)) fail(stockWhole ? `That's less than one share of ${sym}. Whole shares only is on in Settings.` : 'Enter an amount to buy');
    const cost = qty * px; fee = round(S.feeFlat + cost * S.feePct / 100, 6);
    if (cost + fee > cash + 0.005) fail(`Not enough cash. This needs ${money(cost + fee)}${fee ? ' including the ' + money(fee) + ' fee' : ''} and you have ${money(cash)}.`);
    const all = (await c.query('select sym, qty, avg from pos where wid=$1', [wid])).rows.map(x => ({ sym: x.sym, qty: +x.qty, avg: +x.avg }));
    const equity = cash + marketValue(all);
    if (S.maxPos > 0 && !o.bot && ((pos ? pos.qty : 0) + qty) * px > equity * S.maxPos / 100 + 0.01) fail(`One position can be at most ${S.maxPos}% of this wallet (${money(equity * S.maxPos / 100)}). Change this in Settings → Trading.`);
    if (S.reservePct > 0 && !o.bot && cash - cost - fee < equity * S.reservePct / 100 - 0.01) fail(`That would leave less than your ${S.reservePct}% cash reserve (${money(equity * S.reservePct / 100)}).`);
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
  const t = (await c.query('insert into trd(wid,sym,side,qty,price,why,pnl,fee,note,otype,tif) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) returning id, ts',
    [wid, sym, side, qty, px, o.why || 'manual', pnl, fee, o.note ? String(o.note).slice(0, 500) : null, o.otype || 'market', o.tif || null])).rows[0];
  if (side === 'buy') await addLot(c, wid, sym, qty, (qty * px + fee) / qty, t.id);
  else tax = await consumeLots(c, w, sym, qty, qty * px - fee, t.id);
  return { id: t.id, ts: tsMs(t.ts), wid, sym, side, qty, price: px, fee, fees: fee ? [{ k: 'custom', label: 'Fee', amt: fee, note: 'From your settings' }] : [], pnl, pnlPct: pnl != null && pos ? pnl / (pos.avg * qty) * 100 : null, cash: newCash, closed, stale: q?.src === 'cache' || (q && Date.now() - q.t > 10 * 60e3), tax, profile: 'practice' };
}

// ---------- automatic orders (stop loss, take profit, trailing stop, limit, stop, stop limit, queued) ----------
const condOrders = new Map(), limOrders = new Map();
async function loadOrders() {
  condOrders.clear(); limOrders.clear();
  await loadWalletMeta();
  for (const r of await rows('select * from ord order by id')) {
    const x = { id: r.id, wid: r.wid, sym: r.sym, kind: r.kind, price: +r.price, pct: +r.pct || 100, trail: r.trail == null ? null : +r.trail, peak: r.peak == null ? null : +r.peak, created: tsMs(r.created), expires: tsMs(r.expires) };
    if (x.kind === 'trail') x.peak = Math.max(x.peak || 0, priceOf(x.sym) || 0);
    (condOrders.get(x.sym) || condOrders.set(x.sym, []).get(x.sym)).push(x);
  }
  for (const r of await rows('select * from lim order by id')) {
    const x = { id: r.id, wid: r.wid, sym: r.sym, side: r.side, kind: r.kind || 'limit', price: +r.price, lpx: r.lpx == null ? null : +r.lpx, amt: +r.amt, units: r.units || 'auto', sl: r.sl == null ? null : +r.sl, tp: r.tp == null ? null : +r.tp, expires: tsMs(r.expires), ts: tsMs(r.ts), tif: r.tif || 'gtc', sess: r.sess || 'regular', rested: !!r.rested, trig: !!r.trig, why: r.why || null };
    (limOrders.get(x.sym) || limOrders.set(x.sym, []).get(x.sym)).push(x);
  }
}
const orderJson = o => ({ ...o, stop: o.kind === 'trail' ? o.peak * (1 - o.trail / 100) : o.price });
async function addCondOrder(wid, sym, kind, { price, pct = 100, trail = null }) {
  const w = await walletRow(wid); if (w.broker) fail('Use your broker connection for this wallet');
  const p = priceOf(sym); if (!p) fail('No live price yet for ' + sym);
  if (!(await row('select 1 from pos where wid=$1 and sym=$2', [wid, sym]))) fail(`Buy ${sym} first, then add a stop loss or take profit`);
  pct = clamp(num(pct, 100), 1, 100);
  if (kind === 'trail') { trail = num(trail); if (!(trail >= 0.1 && trail <= 50)) fail('The trailing distance must be between 0.1% and 50%'); price = p * (1 - trail / 100); }
  else if (kind === 'stop') { if (!(price > 0 && price < p)) fail(`A stop loss has to be below the current price (${pxStr(p)})`); }
  else if (kind === 'take') { if (!(price > p)) fail(`A take profit has to be above the current price (${pxStr(p)})`); }
  else fail('Unknown order type');
  if (isReal(w) && !isCrypto(sym)) price = kind === 'trail' ? price : roundTick(sym, price);
  const same = kind === 'take' ? ['take'] : ['stop', 'trail'];
  if (pct >= 100) await Q('delete from ord where wid=$1 and sym=$2 and kind = any($3::text[])', [wid, sym, same]);
  const gtc = profOf(w).gtcDays && isReal(w) ? new Date(Date.now() + profOf(w).gtcDays * 864e5) : null;
  const r = await row('insert into ord(wid,sym,kind,price,pct,trail,peak,expires) values($1,$2,$3,$4,$5,$6,$7,$8) returning id', [wid, sym, kind, price, pct, kind === 'trail' ? trail : null, kind === 'trail' ? p : null, gtc]);
  await loadOrders(); return r.id;
}
const roundTick = (sym, v) => { const t = tickOf(sym, v); return Math.round(v / t) * t; };
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
function checkTick(sym, v, label) {
  if (isCrypto(sym)) return;
  const t = v >= 1 ? 0.01 : 0.0001;
  if (Math.abs(Math.round(v / t) * t - v) > t / 1000) fail(`${label} for stocks ${v >= 1 ? 'over $1 must be in whole cents' : 'under $1 can use up to 4 decimals'}`);
}
async function addLimit(wid, sym, o) {
  const w = await walletRow(wid); if (w.broker) fail('Use your broker connection for this wallet');
  const p = priceOf(sym); if (!p) fail('No live price yet for ' + sym);
  const real = isReal(w), prof = profOf(w), crypto = isCrypto(sym);
  const side = o.side === 'sell' ? 'sell' : 'buy', kind = ['limit', 'stop', 'stoplimit', 'moo'].includes(o.kind) ? o.kind : 'limit';
  let price = num(o.price), lpx = num(o.lpx) || null, amt = num(o.amount ?? o.amt);
  let units = o.units || (side === 'buy' ? 'usd' : 'pct');
  if (!['usd', 'qty', 'pct'].includes(units)) units = side === 'buy' ? 'usd' : 'pct';
  if (side === 'buy' && units === 'pct') units = 'usd';
  if (!(amt > 0)) fail('Enter how much to ' + side);
  if (kind !== 'moo' && !(price > 0)) fail('Enter the ' + (kind === 'limit' ? 'limit' : 'stop') + ' price');
  if (kind === 'stoplimit' && !(lpx > 0)) fail('Enter the limit price for the stop limit order');
  if (side === 'sell' && !(await row('select 1 from pos where wid=$1 and sym=$2', [wid, sym]))) fail(`You don't own any ${sym} in this wallet`);
  let sess = crypto ? 'regular' : ['regular', 'extended', '24h'].includes(o.sess) ? o.sess : 'regular', tif = o.tif === 'day' ? 'day' : 'gtc';
  if (!real) {
    if (kind === 'limit' && side === 'buy' && price >= p) fail(`A buy limit has to be below the current price (${pxStr(p)}). To buy higher, use a stop order.`);
    if (kind === 'limit' && side === 'sell' && price <= p) fail(`A sell limit has to be above the current price (${pxStr(p)}). To sell lower, use a stop loss.`);
    sess = 'regular';
  } else {
    if (kind !== 'moo') checkTick(sym, price, kind === 'limit' ? 'Limit prices' : 'Stop prices');
    if (lpx) checkTick(sym, lpx, 'Limit prices');
    if (sess !== 'regular' && kind !== 'limit') fail('Only limit orders can trade outside regular hours. Stop orders wait for the regular session.');
    if (sess === '24h' && !prof.overnight) fail('The 24 Hour Market is a Robinhood feature. Pick extended hours instead.');
    if (kind === 'moo') tif = 'day';
  }
  if (kind === 'stop' || kind === 'stoplimit') {
    if (side === 'buy' && price <= p) fail(`A buy stop has to be above the current price (${pxStr(p)})`);
    if (side === 'sell' && price >= p) fail(`A sell stop has to be below the current price (${pxStr(p)})`);
  }
  // Real brokers hold your cash (buys) and shares (sells) for open orders.
  if (real) {
    const pos = await positions(wid), acct = await accountInfo(wid, { w, pos });
    if (side === 'buy') {
      const unitPx = kind === 'stoplimit' ? lpx : kind === 'limit' ? price : p * 1.05;
      const need = units === 'qty' ? amt * unitPx : amt, bp = crypto || !marginable(sym, unitPx) ? acct.bpCrypto : acct.bp;
      if (need > bp + 0.005) fail(`Not enough buying power to hold for this order. It needs ${money(need)}${kind === 'moo' || kind === 'stop' ? ' (with a 5% cushion for price moves)' : ''} and you have ${money(bp)}.`);
    } else {
      const ps = pos.find(x => x.sym === sym), want = units === 'qty' ? amt : units === 'usd' ? amt / (kind === 'limit' ? price : p) : ps.qty * Math.min(100, amt) / 100;
      const free = ps.qty - (acct.holdsShares[sym] || 0);
      if (want > free + 1e-9) fail(`Only ${+Math.max(0, free).toFixed(6)} ${crypto ? sym : 'shares'} are free to sell. The rest are held for other open sell orders.`);
    }
  }
  let expires = null;
  if (kind === 'moo') expires = null;
  else if (tif === 'day') expires = crypto ? Date.now() + 86400e3 : sessionEnd(prof, sess !== 'regular');
  else if (real && prof.gtcDays) expires = Date.now() + prof.gtcDays * 864e5;
  if (o.expires && o.expires !== 'gtc' && o.expires !== 'day' && Number.isFinite(tsMs(o.expires))) expires = tsMs(o.expires);
  if (o.expires === 'day' && !real) expires = crypto ? Date.now() + 86400e3 : sessionClose();
  const q = quoteOf(sym, crypto ? 'regular' : sessionAt(prof));
  const marketable = kind === 'limit' && q && (side === 'buy' ? q.ask <= price : q.bid >= price);
  const r = await row('insert into lim(wid,sym,side,kind,price,lpx,amt,units,sl,tp,expires,tif,sess,rested,why) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) returning id',
    [wid, sym, side, kind, kind === 'moo' ? p : price, lpx, units === 'pct' ? Math.min(100, amt) : amt, units, num(o.sl) || null, num(o.tp) || null, expires ? new Date(expires) : null, tif, sess, !marketable, o.why || null]);
  await loadOrders(); await refreshUserSyms();
  // A marketable order fills right away when its session is open.
  let fill = null;
  const l = (limOrders.get(sym) || []).find(x => x.id === r.id);
  if (l && limHit(l, p, 'ws')) fill = await fireLim(l, p, { quiet: true });
  return { id: r.id, fill };
}

// Can automatic orders fire right now?
function canFire(sym, src) { // practice rules
  if (isCrypto(sym)) return src !== 'cache';
  if (src === 'cache') return false;
  const st = market().state;
  if (st === 'open') return true;
  return !S.rthOrders && src === 'ws';
}
function condAllowed(o, src) {
  const w = walletMeta.get(o.wid); if (!w || w.archived || w.broker) return false;
  if (!isReal(w)) return canFire(o.sym, src);
  if (src === 'cache') return false;
  return isCrypto(o.sym) || sessionAt(profOf(w)) === 'regular';
}
function limAllowed(l, src) {
  const w = walletMeta.get(l.wid); if (!w || w.archived || w.broker) return false;
  if (!isReal(w)) return canFire(l.sym, src) && (l.kind !== 'moo' || isCrypto(l.sym) || market().state === 'open');
  if (src === 'cache') return false;
  if (isCrypto(l.sym)) return true;
  const s = sessionAt(profOf(w));
  if (s === 'regular') return true;
  if (l.kind !== 'limit') return false;
  if (s === 'pre' || s === 'post') return l.sess === 'extended' || l.sess === '24h';
  if (s === 'overnight') return l.sess === '24h';
  return false;
}
engineHook = (sym, p, src) => {
  for (const o of condOrders.get(sym) || []) if (o.kind === 'trail' && p > (o.peak || 0) && condAllowed(o, src === 'cache' ? 'cache' : 'ws')) { o.peak = p; o.dirty = true; }
  checkCond(sym, p, src); checkLims(sym, p, src);
  if (src !== 'cache') checkAlerts(sym, p);
};
function condHit(o, p) {
  if (o.kind === 'trail') return p <= o.peak * (1 - o.trail / 100);
  if (o.kind === 'stop') return p <= o.price;
  if (o.kind === 'take') { const w = walletMeta.get(o.wid); if (!isReal(w)) return p >= o.price; const q = quoteOf(o.sym); return q ? q.bid >= o.price : p >= o.price; }
  return false;
}
async function checkCond(sym, p, src = 'ws') {
  const list = condOrders.get(sym); if (!list || !list.length) return;
  const hit = list.find(o => !firing.has(o.wid + ':' + sym) && condAllowed(o, src) && condHit(o, p));
  if (!hit) return;
  const key = hit.wid + ':' + sym; firing.add(key);
  try {
    const label = hit.kind === 'take' ? 'Take profit' : hit.kind === 'trail' ? 'Trailing stop' : 'Stop loss';
    const pos = await row('select qty from pos where wid=$1 and sym=$2', [hit.wid, sym]);
    if (!pos) { await Q('delete from ord where id=$1', [hit.id]); await loadOrders(); return; }
    const w = walletMeta.get(hit.wid), real = isReal(w);
    const o = { pct: hit.pct, why: label.toLowerCase(), otype: hit.kind === 'take' ? 'limit' : hit.kind === 'trail' ? 'trail' : 'stop', auto: true, liq: hit.kind === 'take' ? 'maker' : 'taker' };
    if (real && hit.kind === 'take') { const q = quoteOf(sym); if (q) o.px = Math.max(hit.price, q.bid); }
    const f = await execTrade(hit.wid, sym, 'sell', o);
    if (!f.closed) await Q('delete from ord where id=$1', [hit.id]);
    await loadOrders();
    await notify('order', `${label} hit · ${sym}`, `Sold ${hit.pct >= 100 ? 'all' : hit.pct + '% of'} your ${sym} (${+f.qty.toFixed(6)}) at ${pxStr(f.price)}${hit.kind !== 'take' && real ? ` (triggered at ${pxStr(p)})` : ''}. ${f.pnl >= 0 ? 'Profit' : 'Loss'}: ${money(f.pnl)} (${pct(f.pnlPct || 0)}).${f.fee ? ` Fees ${money(f.fee)}.` : ''}${walletCount > 1 ? ' Wallet: ' + w.emoji + ' ' + w.name : ''}`, { sym, wid: hit.wid, data: { kind: 'fill', fill: f, label } });
  } catch (e) {
    warn('order', sym, e.message);
    await Q('delete from ord where id=$1', [hit.id]).catch(() => {}); await loadOrders();
    await notify('order', `Couldn't sell ${sym}`, `Your ${hit.kind === 'take' ? 'take profit' : 'stop loss'} triggered but the sale failed: ${e.message}`, { sym, wid: hit.wid });
  } finally { firing.delete(key); }
}
function limHit(l, p, src) {
  if (firing.has('L' + l.id) || (l.expires && l.expires <= Date.now()) || !limAllowed(l, src)) return false;
  const w = walletMeta.get(l.wid);
  if (l.kind === 'moo') return true;
  if (!isReal(w)) return l.kind === 'stop' ? (l.side === 'buy' ? p >= l.price : p <= l.price) : (l.side === 'buy' ? p <= l.price : p >= l.price);
  const q = quoteOf(l.sym, isCrypto(l.sym) ? 'regular' : sessionAt(profOf(w))); if (!q) return false;
  if (l.kind === 'stop') return l.side === 'buy' ? p >= l.price : p <= l.price;
  if (l.kind === 'stoplimit' && !l.trig) return l.side === 'buy' ? p >= l.price : p <= l.price;
  const lim = l.kind === 'stoplimit' ? l.lpx : l.price;
  return l.side === 'buy' ? q.ask <= lim : q.bid >= lim;
}
async function checkLims(sym, p, src = 'ws') {
  const list = limOrders.get(sym); if (!list || !list.length) return;
  const hit = list.find(l => limHit(l, p, src));
  if (hit) await fireLim(hit, p);
}
async function fireLim(hit, p, { quiet = false } = {}) {
  const sym = hit.sym, w = walletMeta.get(hit.wid), real = isReal(w);
  // A stop limit that just triggered becomes a resting limit order.
  if (real && hit.kind === 'stoplimit' && !hit.trig) {
    hit.trig = true; await Q('update lim set trig=true where id=$1', [hit.id]).catch(() => {});
    if (!quiet) await notify('order', `Stop limit triggered · ${sym}`, `${sym} reached ${pxStr(hit.price)}. Your ${hit.side} order is now a limit order at ${pxStr(hit.lpx)}.`, { sym, wid: hit.wid, push: false });
    if (!limHit(hit, p, 'ws')) return null;
  }
  firing.add('L' + hit.id);
  const label = hit.kind === 'moo' ? 'Queued order' : hit.kind === 'stop' ? 'Stop order' : hit.kind === 'stoplimit' ? 'Stop limit order' : 'Limit order';
  try {
    const del = await row('delete from lim where id=$1 returning id', [hit.id]); await loadOrders();
    if (!del) return null;
    const o = { why: hit.why || label.toLowerCase(), limId: hit.id, otype: hit.kind === 'moo' ? 'market' : hit.kind, tif: hit.tif, auto: hit.kind !== 'moo' || hit.why === 'dividend reinvestment', drip: hit.why === 'dividend reinvestment' };
    if (hit.side === 'buy') { if (hit.units === 'qty') o.qty = hit.amt; else o.usd = hit.amt; }
    else { if (hit.units === 'qty') o.qty = hit.amt; else if (hit.units === 'usd') o.usd = hit.amt; else o.pct = hit.amt; }
    if (real && (hit.kind === 'limit' || hit.kind === 'stoplimit')) {
      const q = quoteOf(sym, isCrypto(sym) ? 'regular' : sessionAt(profOf(w))), lim = hit.kind === 'stoplimit' ? hit.lpx : hit.price;
      o.px = hit.side === 'buy' ? Math.min(lim, q.ask) : Math.max(lim, q.bid);
      o.liq = hit.rested ? 'maker' : 'taker';
    }
    const f = await execTrade(hit.wid, sym, hit.side, o);
    if (hit.side === 'buy' && (hit.sl || hit.tp)) f.bracket = await attachBracket(hit.wid, sym, f.price, { sl: hit.sl, tp: hit.tp });
    if (!quiet) await notify('order', `${label} filled · ${sym}`, `${hit.side === 'buy' ? 'Bought' : 'Sold'} ${+f.qty.toFixed(6)} ${sym} at ${pxStr(f.price)} (${money(f.qty * f.price)}).${f.pnl != null ? ` ${f.pnl >= 0 ? 'Profit' : 'Loss'}: ${money(f.pnl)}.` : ''}${f.fee ? ` Fees ${money(f.fee)}.` : ''}`, { sym, wid: hit.wid, data: { kind: 'fill', fill: f, label } });
    return f;
  } catch (e) {
    if (quiet) throw e;
    await notify('order', `${label} not filled · ${sym}`, `It triggered at ${pxStr(p)} but couldn't go through: ${e.message}`, { sym, wid: hit.wid });
    return null;
  } finally { firing.delete('L' + hit.id); }
}
async function expireOrders() {
  const gone = await rows('delete from lim where expires is not null and expires < now() returning *');
  const goneOrd = await rows('delete from ord where expires is not null and expires < now() returning *');
  if (!gone.length && !goneOrd.length) return;
  await loadOrders();
  for (const l of gone) await notify('order', `Order expired · ${l.sym}`, `Your ${{ stop: 'stop', stoplimit: 'stop limit', limit: 'limit', moo: 'queued' }[l.kind] || 'limit'} ${l.side} order${l.kind !== 'moo' ? ' at ' + pxStr(+l.price) : ''} ${l.tif === 'day' ? 'was a day order and ended with the session' : 'reached the 90-day limit for good-till-canceled orders'} without filling.`, { sym: l.sym, wid: l.wid, push: false });
  for (const o of goneOrd) await notify('order', `Order expired · ${o.sym}`, `Your ${o.kind === 'take' ? 'take profit' : o.kind === 'trail' ? 'trailing stop' : 'stop loss'} reached the 90-day limit for good-till-canceled orders, so the broker cancelled it. Set a new one to stay protected.`, { sym: o.sym, wid: o.wid });
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
// Broker connection (Alpaca): live bid/ask quotes, and a wallet that mirrors a real paper or live account.
// Turned on by adding ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway. Live trading also needs ALPACA_ENV=live,
// so real money can never be switched on from inside the app.
// =====================================================================
const crypto_ = crypto;
const ALP = {
  key: (ENV.ALPACA_KEY_ID || ENV.APCA_API_KEY_ID || '').trim(),
  secret: (ENV.ALPACA_SECRET_KEY || ENV.APCA_API_SECRET_KEY || '').trim(),
  env: String(ENV.ALPACA_ENV || 'paper').trim().toLowerCase() === 'live' ? 'live' : 'paper',
  feed: String(ENV.ALPACA_FEED || 'iex').trim().toLowerCase() === 'sip' ? 'sip' : 'iex',
  maxOrder: +ENV.ALPACA_MAX_ORDER || 0,
};
const BROKER = ALP.key && ALP.secret ? {
  env: ALP.env, feed: ALP.feed, base: ENV.ALPACA_BASE || (ALP.env === 'live' ? 'https://api.alpaca.markets' : 'https://paper-api.alpaca.markets'),
  data: ENV.ALPACA_DATA || 'https://data.alpaca.markets', stream: ENV.ALPACA_STREAM || 'wss://stream.data.alpaca.markets',
  ok: false, err: null, account: null, orders: [], lastSync: 0, wid: null, dataOk: false, dataErr: null, tuOk: false,
} : null;
async function apca(pathq, { method = 'GET', body, data = false, timeout = 10000 } = {}) {
  if (!BROKER) fail('No broker is connected. Add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway.', 503);
  let r;
  try {
    r = await fetch((data ? BROKER.data : BROKER.base) + pathq, { method, headers: { 'APCA-API-KEY-ID': ALP.key, 'APCA-API-SECRET-KEY': ALP.secret, 'Content-Type': 'application/json', Accept: 'application/json' }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(timeout) });
  } catch (e) { BROKER.err = 'Could not reach Alpaca'; fail('Could not reach Alpaca. Try again in a moment.', 502); }
  const txt = await r.text(); let j = null; try { j = txt ? JSON.parse(txt) : null; } catch (e) {}
  if (r.status === 401) { BROKER.err = 'Alpaca refused the API keys'; fail('Alpaca refused the API keys. Check ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway.', 502); }
  if (!r.ok) fail('Alpaca says: ' + (j?.message || txt || 'error ' + r.status), r.status >= 500 ? 502 : r.status === 404 ? 404 : 400);
  return j;
}
const alpSym = sym => isCrypto(sym) ? sym + '/USD' : sym;
const fromAlp = (s, cls) => { s = String(s || ''); if (s.includes('/')) return s.split('/')[0]; if (cls === 'crypto' || (/USDT?$/.test(s) && s.length > 5 && CRYPTO_DEFAULT.includes(s.replace(/USDT?$/, '')))) return s.replace(/USDT?$/, ''); return s; };

// ---------- market data: real bid/ask for stocks ----------
let adws = null, adSubs = new Set(), adBackoff = 3000;
function adConnect() {
  if (!BROKER) return;
  let ws;
  try { ws = adws = new WebSocket(`${BROKER.stream}/v2/${ALP.feed}`); } catch (e) { return setTimeout(adConnect, adBackoff); }
  ws.on('open', () => { try { ws.send(JSON.stringify({ action: 'auth', key: ALP.key, secret: ALP.secret })); } catch (e) {} });
  ws.on('message', m => {
    let list; try { list = JSON.parse(m.toString()); } catch (e) { return; }
    for (const d of [].concat(list)) {
      if (d.T === 'success' && d.msg === 'authenticated') { adBackoff = 3000; BROKER.dataOk = true; BROKER.dataErr = null; adSubs = new Set(); adSync(); }
      else if (d.T === 'error') { BROKER.dataErr = d.msg || 'data error'; if (d.code === 402 || d.code === 406) BROKER.dataOk = false; }
      else if (d.T === 'q') { const q = prices.get(d.S); if (q && d.bp > 0 && d.ap > 0) { q.bid = +d.bp; q.ask = +d.ap; q.bqT = Date.now(); } }
      else if (d.T === 't') { const q = prices.get(d.S); if (!FINNHUB_KEY || !(q?.wsT > Date.now() - 60e3)) tick(d.S, +d.p); }
    }
  });
  ws.on('close', () => { BROKER.dataOk = false; if (adws === ws) setTimeout(adConnect, adBackoff = Math.min(adBackoff * 2, 120000)); });
  ws.on('error', e => { BROKER.dataErr = e.message; try { ws.terminate(); } catch (_) {} });
}
function adSync() {
  if (!adws || adws.readyState !== 1 || !BROKER.dataOk) return;
  const want = new Set(stockPriority().slice(0, 30));
  const drop = [...adSubs].filter(s => !want.has(s)), add = [...want].filter(s => !adSubs.has(s));
  if (drop.length) { adws.send(JSON.stringify({ action: 'unsubscribe', quotes: drop, trades: drop })); drop.forEach(s => adSubs.delete(s)); }
  if (add.length) { adws.send(JSON.stringify({ action: 'subscribe', quotes: add, trades: add })); add.forEach(s => adSubs.add(s)); }
}

// ---------- the broker wallet ----------
async function brokerWallet() { return row("select * from wallets where broker='alpaca' and not archived order by id limit 1"); }
const KIND_OF = { market: 'market', limit: 'limit', stop: 'stop', stop_limit: 'stoplimit', trailing_stop: 'trail' };
function mapOrder(o, wid) {
  const sym = fromAlp(o.symbol, o.asset_class), kind = KIND_OF[o.type] || o.type;
  return {
    id: 'a:' + o.id, wid, sym, side: o.side, kind: kind === 'market' ? 'moo' : kind, price: +(o.limit_price || o.stop_price || o.hwm || 0) || null, lpx: o.type === 'stop_limit' ? +o.limit_price : null,
    stop: o.type === 'trailing_stop' ? +o.stop_price || null : null, trail: o.trail_percent ? +o.trail_percent : null, peak: o.hwm ? +o.hwm : null,
    amt: o.qty ? +o.qty : +o.notional, units: o.qty ? 'qty' : 'usd', filled: +o.filled_qty || 0, tif: o.time_in_force, sess: o.extended_hours ? 'extended' : 'regular',
    expires: null, ts: tsMs(o.submitted_at || o.created_at), status: o.status, broker: true, cls: o.order_class || 'simple',
    legs: (o.legs || []).map(l => ({ id: 'a:' + l.id, kind: KIND_OF[l.type] || l.type, price: +(l.limit_price || l.stop_price || 0) })),
  };
}
let syncing = null;
async function brokerSync() {
  if (!BROKER) return;
  if (syncing) return syncing;
  syncing = (async () => {
    const w = await brokerWallet(); BROKER.wid = w ? w.id : null;
    if (!w) { try { BROKER.account = await apca('/v2/account'); BROKER.ok = true; BROKER.err = null; } catch (e) { BROKER.ok = false; BROKER.err = e.message; } return; }
    try {
      const [acct, posL, ords] = await Promise.all([apca('/v2/account'), apca('/v2/positions'), apca('/v2/orders?status=open&limit=200&nested=true')]);
      BROKER.account = acct; BROKER.ok = true; BROKER.err = null; BROKER.lastSync = Date.now();
      let after = (await kvGet('alpaca_after')) || new Date(tsMs(w.created)).toISOString(), acts = [], token = null;
      for (let i = 0; i < 10; i++) {
        const page = await apca(`/v2/account/activities?after=${encodeURIComponent(after)}&direction=asc&page_size=100${token ? '&page_token=' + encodeURIComponent(token) : ''}`);
        if (!Array.isArray(page) || !page.length) break; acts = acts.concat(page); if (page.length < 100) break; token = page[page.length - 1].id;
      }
      const before = await positions(w.id);
      let newSyms = false;
      await tx(async c => {
        for (const a of acts) {
          const t = a.transaction_time || (a.date ? a.date + 'T20:00:00Z' : new Date().toISOString());
          if (t > after) after = t;
          const type = a.activity_type, ext = 'alp:' + a.id;
          if (type === 'FILL') {
            const sym = fromAlp(a.symbol, a.asset_class || (String(a.symbol).includes('/') ? 'crypto' : null));
            if (!assets.has(sym)) { addAsset(sym, String(a.symbol).includes('/') || CRYPTO_DEFAULT.includes(sym) ? 'crypto' : 'stock'); newSyms = true; }
            const pb = before.find(p => p.sym === sym), qty = +a.qty, px = +a.price;
            const pnl = a.side !== 'buy' && pb ? round((px - pb.avg) * qty, 6) : null;
            await c.query("insert into trd(wid,sym,side,qty,price,why,pnl,fee,ext_id,ts,otype) values($1,$2,$3,$4,$5,'broker',$6,0,$7,$8,$9) on conflict (ext_id) do nothing", [w.id, sym, a.side === 'buy' ? 'buy' : 'sell', qty, px, pnl, ext, t, a.order_type || null]);
          } else if (/^DIV/.test(type)) {
            const sym = fromAlp(a.symbol), per = +a.per_share_amount || 0, q = +a.qty || 0, net = +a.net_amount || 0;
            await c.query("insert into trd(wid,sym,side,qty,price,why,ext_id,ts) values($1,$2,'div',$3,$4,'dividend',$5,$6) on conflict (ext_id) do nothing", [w.id, sym, per && q ? q : 1, per && q ? net / q : net, ext, t]);
          } else if (type === 'CSD' || type === 'CSW' || type === 'JNLC') {
            const net = +a.net_amount || 0;
            const r = await c.query("insert into trd(wid,sym,side,qty,price,why,ext_id,ts) values($1,'','cash',1,$2,$3,$4,$5) on conflict (ext_id) do nothing returning id", [w.id, net, net >= 0 ? 'deposit' : 'withdrawal', ext, t]);
            if (r.rows.length) await c.query('update wallets set start_bal = greatest(start_bal + $1, 1) where id=$2', [net, w.id]);
          } else if (type === 'INT' || type === 'FEE' || type === 'CFEE') {
            const net = +a.net_amount || 0;
            await c.query("insert into trd(wid,sym,side,qty,price,why,fee,ext_id,ts) values($1,'',$2,1,$3,$4,$5,$6,$7) on conflict (ext_id) do nothing", [w.id, type === 'INT' ? 'int' : 'fee', type === 'INT' ? net : Math.abs(net), type === 'INT' ? 'interest' : (a.description || 'broker fee'), type === 'INT' ? 0 : Math.abs(net), ext, t]);
          }
        }
        if (acts.length) await c.query("insert into kv values('alpaca_after',$1) on conflict(k) do update set v=excluded.v", [after]);
        await c.query('delete from pos where wid=$1', [w.id]);
        for (const p of posL || []) {
          const sym = fromAlp(p.symbol, p.asset_class);
          if (!assets.has(sym)) { addAsset(sym, p.asset_class === 'crypto' ? 'crypto' : 'stock'); newSyms = true; }
          const since = before.find(x => x.sym === sym)?.since;
          await c.query('insert into pos(wid,sym,qty,avg,since) values($1,$2,$3,$4,$5) on conflict (wid,sym) do update set qty=excluded.qty, avg=excluded.avg', [w.id, sym, +p.qty, +p.avg_entry_price, since ? new Date(since) : new Date()]);
          if (+p.current_price > 0 && !fresh(sym, 60e3)) tick(sym, +p.current_price, { src: 'rest', prev: +p.lastday_price || undefined });
        }
        await c.query('update wallets set cash=$1 where id=$2', [+acct.cash, w.id]);
      });
      BROKER.orders = (ords || []).map(o => mapOrder(o, w.id));
      if (newSyms || acts.length) { refreshUserSymsSoon(); broadcast({ t: 'evt', e: 'fill', wid: w.id }); }
    } catch (e) { BROKER.ok = false; BROKER.err = e.message; warn('broker sync', e.message); }
  })();
  try { await syncing; } finally { syncing = null; }
}
const brokerSyncSoon = debounce(() => brokerSync().catch(() => {}), 700);

// ---------- placing and changing orders ----------
function liveGuard(b, notional) {
  if (BROKER.env !== 'live') return;
  if (b.live !== true) fail('This wallet uses real money. Confirm the order in the app to send it.');
  if (ALP.maxOrder && notional > ALP.maxOrder) fail(`Real-money orders are capped at ${money(ALP.maxOrder)} by ALPACA_MAX_ORDER in Railway.`);
}
async function brokerTrade(w, b) {
  const sym = cleanSym(b.sym); if (!sym) fail('Pick something to trade');
  if (!assets.has(sym)) await trackAsset(sym, b.kind);
  const crypto = isCrypto(sym), side = b.side === 'sell' ? 'sell' : 'buy', otype = b.otype || 'market', px = priceOf(sym) || 0;
  const mode = b.mode || 'usd', amount = num(b.amount); if (!(amount > 0)) fail('Enter an amount');
  const body = { symbol: alpSym(sym), side, type: { market: 'market', limit: 'limit', stop: 'stop', stoplimit: 'stop_limit', trail: 'trailing_stop' }[otype] || 'market', time_in_force: crypto ? 'gtc' : (b.tif === 'gtc' ? 'gtc' : 'day'), client_order_id: 'mango-' + crypto_.randomUUID() };
  const pos = side === 'sell' ? (await positions(w.id)).find(p => p.sym === sym) : null;
  if (side === 'sell' && !pos) fail(`You don't own any ${sym} at Alpaca`);
  if (mode === 'usd') { if (otype === 'market' || otype === 'limit') body.notional = amount.toFixed(2); else body.qty = String(+(amount / px).toFixed(6)); }
  else if (mode === 'pct') body.qty = String(+(pos.qty * Math.min(100, amount) / 100).toFixed(9));
  else body.qty = String(amount);
  if (otype === 'limit' || otype === 'stoplimit') body.limit_price = String(num(otype === 'stoplimit' ? b.lpx : b.price));
  if (otype === 'stop' || otype === 'stoplimit') body.stop_price = String(num(b.price));
  if (otype === 'trail') body.trail_percent = String(num(b.trail));
  if (b.sess === 'extended' && otype === 'limit' && !crypto) { body.extended_hours = true; body.time_in_force = 'day'; }
  if (side === 'buy' && (num(b.sl) > 0 || num(b.tp) > 0) && body.qty && Number.isInteger(+body.qty) && otype !== 'trail') {
    body.order_class = 'bracket'; const ref = num(b.price) || px;
    if (num(b.tp) > 0) body.take_profit = { limit_price: String(+(ref * (1 + num(b.tp) / 100)).toFixed(ref >= 1 ? 2 : 4)) };
    if (num(b.sl) > 0) body.stop_loss = { stop_price: String(+(ref * (1 - num(b.sl) / 100)).toFixed(ref >= 1 ? 2 : 4)) };
    if (!body.take_profit || !body.stop_loss) body.order_class = 'oto';
  }
  liveGuard(b, mode === 'usd' ? amount : (+body.qty || 0) * (num(b.price) || px));
  const o = await apca('/v2/orders', { method: 'POST', body });
  log(`broker order ${BROKER.env}: ${side} ${sym} ${body.type} → ${o.status}`);
  brokerSyncSoon();
  return { broker: true, env: BROKER.env, status: o.status, order: mapOrder(o, w.id) };
}
async function brokerProtect(w, b) {
  const sym = cleanSym(b.sym), pos = (await positions(w.id)).find(p => p.sym === sym); if (!pos) fail(`You don't own any ${sym} at Alpaca`);
  const qty = String(+(pos.qty * clamp(num(b.pct, 100), 1, 100) / 100).toFixed(9)), kind = b.kind;
  const body = { symbol: alpSym(sym), side: 'sell', qty, time_in_force: isCrypto(sym) ? 'gtc' : 'gtc', client_order_id: 'mango-' + crypto_.randomUUID() };
  if (kind === 'trail') Object.assign(body, { type: 'trailing_stop', trail_percent: String(num(b.trail)) });
  else if (kind === 'take') Object.assign(body, { type: 'limit', limit_price: String(+num(b.price).toFixed(2)) });
  else Object.assign(body, { type: 'stop', stop_price: String(+num(b.price).toFixed(2)) });
  liveGuard(b, +qty * (priceOf(sym) || 0));
  const o = await apca('/v2/orders', { method: 'POST', body }); brokerSyncSoon();
  return { id: 'a:' + o.id, status: o.status };
}
async function brokerCancel(id) { await apca('/v2/orders/' + encodeURIComponent(String(id).replace(/^a:/, '')), { method: 'DELETE' }); brokerSyncSoon(); }
async function brokerReplace(id, b) {
  const body = {};
  const cur = BROKER.orders.find(o => o.id === id); if (!cur) fail('That order is gone', 404);
  if (cur.kind === 'trail' && num(b.trail) > 0) body.trail = String(num(b.trail));
  else if (cur.kind === 'limit' && num(b.price) > 0) body.limit_price = String(num(b.price));
  else if ((cur.kind === 'stop' || cur.kind === 'stoplimit') && num(b.price) > 0) body.stop_price = String(num(b.price));
  const o = await apca('/v2/orders/' + encodeURIComponent(id.replace(/^a:/, '')), { method: 'PATCH', body }); brokerSyncSoon();
  return { id: 'a:' + o.id };
}

// ---------- order updates as they happen ----------
let tuws = null, tuBackoff = 3000;
function tuConnect() {
  if (!BROKER || (tuws && tuws.readyState <= 1)) return;
  let ws;
  try { ws = tuws = new WebSocket(BROKER.base.replace(/^http/, 'ws') + '/stream'); } catch (e) { return setTimeout(tuConnect, tuBackoff); }
  ws.on('open', () => { try { ws.send(JSON.stringify({ action: 'auth', key: ALP.key, secret: ALP.secret })); } catch (e) {} });
  ws.on('message', m => {
    let d; try { d = JSON.parse(m.toString()); } catch (e) { return; }
    if (d.stream === 'authorization' && d.data?.status === 'authorized') { tuBackoff = 3000; BROKER.tuOk = true; ws.send(JSON.stringify({ action: 'listen', data: { streams: ['trade_updates'] } })); }
    else if (d.stream === 'trade_updates') onTradeUpdate(d.data).catch(e => warn('trade update', e.message));
  });
  ws.on('close', () => { BROKER.tuOk = false; if (tuws === ws) setTimeout(tuConnect, tuBackoff = Math.min(tuBackoff * 2, 120000)); });
  ws.on('error', () => { try { ws.terminate(); } catch (_) {} });
}
async function onTradeUpdate(u) {
  const o = u?.order; if (!o) return;
  const sym = fromAlp(o.symbol, o.asset_class), money_ = BROKER.env === 'live' ? 'real money' : 'paper';
  if (u.event === 'fill' || u.event === 'partial_fill') {
    await brokerSync();
    const f = { sym, side: o.side, qty: +u.qty || +o.filled_qty, price: +u.price || +o.filled_avg_price, pnl: null };
    await notify('order', `${u.event === 'fill' ? 'Filled' : 'Partly filled'} · ${sym}`, `${o.side === 'buy' ? 'Bought' : 'Sold'} ${+f.qty.toFixed(6)} ${sym} at ${pxStr(f.price)} on Alpaca (${money_}).`, { sym, wid: BROKER.wid, data: { kind: 'fill', fill: f, label: 'Broker' } });
  } else if (u.event === 'rejected' || u.event === 'expired') {
    brokerSyncSoon();
    await notify('order', `Order ${u.event} · ${sym}`, `Alpaca ${u.event} your ${o.side} order for ${sym}.${o.reject_reason ? ' ' + o.reject_reason : ''}`, { sym, wid: BROKER.wid });
  } else brokerSyncSoon();
}
async function brokerLink(confirm) {
  if (!BROKER) fail('Add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway first.');
  if (await brokerWallet()) fail('Your Alpaca account is already connected.');
  if (BROKER.env === 'live' && confirm !== 'REAL MONEY') fail('Type REAL MONEY to connect a live account.');
  const a = await apca('/v2/account');
  if (a.trading_blocked || a.account_blocked) fail('Alpaca says this account is blocked from trading right now.');
  const name = BROKER.env === 'live' ? 'Alpaca Live' : 'Alpaca Paper';
  const r = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,broker) values($1,$2,'sky',$3,$4,(select coalesce(max(sort),0)+1 from wallets),'alpaca',$5,'alpaca') returning *`,
    [name, BROKER.env === 'live' ? '🔴' : '🦙', +a.cash, Math.max(1, +a.equity), +a.multiplier > 1 ? 'margin' : 'cash']);
  await kvSet('alpaca_after', new Date().toISOString());
  await loadWalletMeta(); await brokerSync(); await snapshot(r.id); await refreshUserSyms(); tuConnect();
  return walletJson(await walletRow(r.id));
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
const PUSH_SETTING = { alert: 'pushAlerts', order: 'pushOrders', summary: 'notifySummary', move: 'notifyMoves', achv: 'notifyAchv', game: 'notifyGames', bot: 'pushBots' };
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
  const interest = sum(trades.filter(t => t.side === 'int' && t.price > 0).map(t => t.price));
  const fees = sum(trades.map(t => t.fee)), marginInterest = -sum(trades.filter(t => t.side === 'int' && t.price < 0).map(t => t.price));
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
    realized, unrealized, dividends, interest, fees, deposits, marginInterest,
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
// Money movement and margin: bank transfers, instant deposits, margin interest, margin calls
// =====================================================================
// Bank deposits arrive in a few business days. Instant and margin accounts can use part of a
// pending deposit right away (Robinhood Instant Deposits); cash accounts wait for the money.
async function deposit(wid, amount, how = 'bank') {
  const w = await walletRow(wid);
  if (w.broker) fail('Move money in and out with your broker. This wallet mirrors that account.');
  if (!(amount > 0) || amount > 1e9) fail('Enter a valid amount');
  if (!isReal(w) || how === 'practice') {
    await tx(async c => moveCash(c, wid, amount, 'deposit'));
    return { done: true, instant: amount, pending: 0 };
  }
  const prof = profOf(w);
  const limit = w.acct === 'cash' ? 0 : prof.instant[w.gold ? 'gold' : 'std'];
  const used = +(await row("select coalesce(sum(instant),0) s from xfer where wid=$1 and kind='deposit' and status='pending'", [wid])).s;
  const instant = Math.max(0, Math.min(amount, limit - used));
  const avail = businessDaysLater(prof.achDays);
  const x = await tx(async c => {
    const r = (await c.query("insert into xfer(wid,kind,amount,instant,status,avail,method) values($1,'deposit',$2,$3,'pending',$4,'ach') returning id", [wid, amount, instant, new Date(avail)])).rows[0];
    if (instant > 0) await moveCash(c, wid, instant, 'instant deposit');
    return r;
  });
  return { done: false, id: x.id, instant, pending: amount - instant, avail };
}
async function withdraw(wid, amount, how = 'bank') {
  const w = await walletRow(wid);
  if (w.broker) fail('Move money in and out with your broker. This wallet mirrors that account.');
  if (!(amount > 0)) fail('Enter a valid amount');
  if (!isReal(w) || how === 'practice') { await tx(async c => moveCash(c, wid, -amount, 'withdrawal')); return { done: true }; }
  const acct = await accountInfo(wid, { w });
  if (amount > acct.withdrawable + 0.005) fail(`You can withdraw up to ${money(acct.withdrawable)}.${acct.unsettled > 0 ? ` ${money(acct.unsettled)} from recent stock sales hasn't settled yet.` : ''}${acct.debit > 0 ? ' Pay off your margin balance first.' : ''}${acct.holdsCash > 0 ? ` ${money(acct.holdsCash)} is held for open orders.` : ''}`);
  const prof = profOf(w), avail = businessDaysLater(prof.wdDays);
  await tx(async c => {
    await moveCash(c, wid, -amount, 'withdrawal');
    await c.query("insert into xfer(wid,kind,amount,status,avail,method) values($1,'withdrawal',$2,'pending',$3,'ach')", [wid, amount, new Date(avail)]);
  });
  return { done: false, avail };
}
async function xferJob() {
  for (const x of await rows("select * from xfer where status='pending' and avail <= now() order by id")) {
    await tx(async c => {
      const cur = (await c.query("select status from xfer where id=$1 for update", [x.id])).rows[0]; if (cur?.status !== 'pending') return;
      if (x.kind === 'deposit') { const rest = +x.amount - +x.instant; if (rest > 0.0001) await moveCash(c, x.wid, rest, 'deposit'); }
      await c.query("update xfer set status='done', done=now() where id=$1", [x.id]);
    }).catch(e => warn('transfer', e.message));
    if (x.kind === 'deposit') await notify('cash', 'Deposit complete', `${money(+x.amount)} from your bank is now fully available.`, { wid: x.wid, url: '/#/account' });
    else await notify('cash', 'Withdrawal sent', `${money(+x.amount)} is on its way to your bank.`, { wid: x.wid, push: false, url: '/#/account' });
  }
}
async function cancelXfer(id) {
  const x = await row('select * from xfer where id=$1', [id]); if (!x) fail('That transfer is gone', 404);
  if (x.status !== 'pending') fail('That transfer already went through');
  if (x.kind !== 'deposit') fail('Withdrawals can\'t be cancelled once they are sent');
  await tx(async c => {
    if (+x.instant > 0) {
      const w = (await c.query('select cash from wallets where id=$1 for update', [x.wid])).rows[0];
      if (+w.cash < +x.instant - 0.005) fail(`You've already used the ${money(+x.instant)} instant part of this deposit. Sell something first.`);
      await moveCash(c, x.wid, -+x.instant, 'deposit cancelled');
    }
    await c.query("update xfer set status='cancelled', done=now() where id=$1", [x.id]);
  });
}

// ---------- margin: interest, Gold, maintenance calls ----------
const posted = new Map();
async function marginDaily() {
  const p = etParts(), today = etDate(), mins = p.h * 60 + p.mi;
  if (mins < 16 * 60 + 15 || (await kvGet('job_margin')) === today) return;
  await kvSet('job_margin', today);
  // Interest accrues each night on what you borrowed. Weekends and holidays count, so Fridays carry three days.
  const [ny, nm, nd] = nextTradingDay(p.y, p.m, p.d);
  const days = isTradingDay(p.y, p.m, p.d) ? Math.round((Date.UTC(ny, nm - 1, nd) - Date.UTC(p.y, p.m - 1, p.d)) / 864e5) : 0;
  for (const w of await rows("select * from wallets where not archived and broker is null and profile <> 'practice'")) {
    const prof = profOf(w), debit = Math.max(0, -+w.cash);
    if (days && debit > 0 && w.acct === 'margin') {
      const charged = Math.max(0, debit - (w.gold ? prof.goldFree : 0));
      const amt = charged * marginRate(prof, debit) / 100 / 360 * days;
      if (amt > 0) await Q('update wallets set margin_accrued = margin_accrued + $1 where id=$2', [amt, w.id]);
    }
  }
  // Interest and the Gold subscription are charged once a month.
  const ym = today.slice(0, 7);
  if ((await kvGet('job_month')) !== ym) {
    const first = (await kvGet('job_month')) == null; await kvSet('job_month', ym);
    if (!first) for (const w of await rows("select * from wallets where not archived and broker is null and profile <> 'practice'")) {
      const acc = round(+w.margin_accrued, 2), prof = profOf(w);
      await tx(async c => {
        if (acc >= 0.01) {
          await c.query('update wallets set cash = cash - $1, margin_accrued = 0 where id=$2', [acc, w.id]);
          await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,'','int',1,$2,'margin interest')", [w.id, -acc]);
        }
        if (w.gold && prof.goldFee > 0) {
          await c.query('update wallets set cash = cash - $1 where id=$2', [prof.goldFee, w.id]);
          await c.query("insert into trd(wid,sym,side,qty,price,why,fee) values($1,'','fee',1,$2,'Robinhood Gold',$2)", [w.id, prof.goldFee]);
        }
      });
      if (acc >= 0.01) await notify('cash', 'Margin interest charged', `${money(acc)} of interest on your margin balance was taken from ${w.emoji} ${w.name}.`, { wid: w.id, push: false });
    }
  }
}
// Every minute: is any margin account below its maintenance requirement?
async function marginWatch() {
  for (const w of await rows("select * from wallets where not archived and broker is null and profile <> 'practice' and (acct='margin' or call_amt > 0)")) {
    const pos = await positions(w.id); if (pos.some(p => !priceOf(p.sym))) continue;
    const a = await accountInfo(w.id, { w, pos });
    if (a.debit <= 0) { if (+w.call_amt > 0) { await Q('update wallets set call_amt=0, call_since=null where id=$1', [w.id]); await notify('order', 'Margin call met', `${w.emoji} ${w.name} is back above its maintenance requirement.`, { wid: w.id }); } continue; }
    const deficit = a.maint - a.equity;
    if (deficit <= 0) { if (+w.call_amt > 0) { await Q('update wallets set call_amt=0, call_since=null where id=$1', [w.id]); await notify('order', 'Margin call met', `${w.emoji} ${w.name} is back above its maintenance requirement.`, { wid: w.id }); } continue; }
    if (!(+w.call_amt > 0)) {
      await Q('update wallets set call_amt=$1, call_since=now() where id=$2', [round(deficit, 2), w.id]);
      await notify('order', `⚠️ Margin call · ${w.name}`, `Your equity (${money(a.equity)}) is below the maintenance requirement (${money(a.maint)}). Deposit ${money(deficit)} or sell positions. If it isn't fixed in 2 trading days, or the account keeps falling, positions will be sold for you.`, { wid: w.id, url: '/#/account' });
      continue;
    }
    await Q('update wallets set call_amt=$1 where id=$2', [round(deficit, 2), w.id]);
    const age = Date.now() - tsMs(w.call_since), severe = a.equity < a.maint * 0.6 || a.equity <= 0;
    if ((age > 2 * 86400e3 || severe) && sessionAt(profOf(w)) === 'regular') await liquidate(w, a, pos, deficit);
  }
}
// Sell the biggest marginable positions until the requirement is met again, with a 10% cushion.
async function liquidate(w, a, pos, deficit) {
  const prof = profOf(w);
  const list = pos.map(p => ({ ...p, px: priceOf(p.sym), v: p.qty * priceOf(p.sym) })).sort((x, y) => y.v - x.v);
  let need = deficit * 1.1; const sold = [];
  for (const p of list) {
    if (need <= 0) break;
    // Selling $1 of a position pays down $1 of the loan and lowers the requirement by that position's rate.
    const m = Math.max(0.25, maintRate(prof, p.sym, p.px) / 100);
    const sellV = Math.min(p.v, need / m);
    try { const f = await execTrade(w.id, p.sym, 'sell', { qty: Math.min(p.qty, sellV / p.px), why: 'margin call', auto: true, force: true }); sold.push(`${+f.qty.toFixed(4)} ${p.sym}`); need -= f.qty * f.price * m; }
    catch (e) { warn('liquidation', p.sym, e.message); }
  }
  if (sold.length) await notify('order', `Positions sold for a margin call · ${w.name}`, `To bring the account back above its maintenance requirement, these were sold at market: ${sold.join(', ')}.`, { wid: w.id, url: '/#/account' });
}

// =====================================================================
// Tax lots, realized gains, wash sales, stock splits, tax summary and monthly statements
// =====================================================================
// Long term means held more than one year: sold after the first anniversary of the purchase date.
function isLongTerm(acq, sold) {
  const a = etParts(tsMs(acq)), s = etParts(tsMs(sold));
  const ann = [a.y + 1, a.m, a.d], sd = [s.y, s.m, s.d];
  for (let i = 0; i < 3; i++) { if (sd[i] > ann[i]) return true; if (sd[i] < ann[i]) return false; }
  return false;
}
async function addLot(c, wid, sym, qty, basis, trdId, ts = new Date()) {
  const r = (await c.query('insert into lots(wid,sym,qty,basis,ts,trd_id) values($1,$2,$3,$4,$5,$6) returning id', [wid, sym, qty, basis, ts, trdId || null])).rows[0];
  if (isCrypto(sym)) return r.id;
  // Wash sale: a loss on this stock in the last 30 days is disallowed and added to the new shares' cost.
  const losses = (await c.query("select * from realized where wid=$1 and sym=$2 and not crypto and proceeds < basis and sold > $3::timestamptz - interval '30 days' and wash_qty < qty order by sold", [wid, sym, ts])).rows;
  let free = qty;
  for (const L of losses) {
    if (free <= 1e-12) break;
    const m = Math.min(free, +L.qty - +L.wash_qty); if (m <= 1e-12) continue;
    const dis = (+L.basis - +L.proceeds) / +L.qty * m;
    await c.query('update realized set wash=wash+$1, wash_qty=wash_qty+$2 where id=$3', [dis, m, L.id]);
    await c.query('update lots set basis = basis + $1, wash_used = wash_used + $2 where id=$3', [dis / qty, m, r.id]);
    free -= m;
  }
  return r.id;
}
async function consumeLots(c, w, sym, qty, proceedsNet, trdId, sold = new Date()) {
  const order = { fifo: 'ts, id', lifo: 'ts desc, id desc', hifo: 'basis desc, ts' }[w.lot_method] || 'ts, id';
  const lots = (await c.query(`select * from lots where wid=$1 and sym=$2 and qty > 0 order by ${order} for update`, [w.id, sym])).rows;
  let left = qty; const made = [];
  const crypto = isCrypto(sym);
  for (const lot of lots) {
    if (left <= 1e-12) break;
    const take = Math.min(+lot.qty, left), proceeds = proceedsNet * take / qty, basis = +lot.basis * take;
    const term = isLongTerm(lot.ts, sold) ? 'long' : 'short';
    const r = (await c.query('insert into realized(wid,trd_id,sym,qty,proceeds,basis,acquired,sold,term,crypto) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) returning *', [w.id, trdId, sym, take, proceeds, basis, lot.ts, sold, term, crypto])).rows[0];
    made.push(r);
    const rest = round(+lot.qty - take, 10);
    if (rest <= 1e-10) await c.query('delete from lots where id=$1', [lot.id]);
    else await c.query('update lots set qty=$1, wash_used=least(wash_used, $1) where id=$2', [rest, lot.id]);
    left -= take;
  }
  if (left > 1e-9) { // shares with no recorded lot (very old data): treat the cost as the sale price
    const proceeds = proceedsNet * left / qty;
    made.push((await c.query("insert into realized(wid,trd_id,sym,qty,proceeds,basis,acquired,sold,term,crypto) values($1,$2,$3,$4,$5,$5,$6,$6,'short',$7) returning *", [w.id, trdId, sym, left, proceeds, sold, crypto])).rows[0]);
  }
  // Wash sale the other way: replacement shares bought in the 30 days before this losing sale.
  let wash = 0;
  if (!crypto) for (const r of made) {
    const loss = +r.basis - +r.proceeds; if (loss <= 0) continue;
    const cands = (await c.query("select * from lots where wid=$1 and sym=$2 and qty > wash_used and ts >= $3::timestamptz - interval '30 days' and ts <= $3::timestamptz order by ts", [w.id, sym, sold])).rows;
    let need = +r.qty;
    for (const lot of cands) {
      if (need <= 1e-12) break;
      const m = Math.min(need, +lot.qty - +lot.wash_used), dis = loss / +r.qty * m;
      await c.query('update lots set basis = basis + $1, wash_used = wash_used + $2 where id=$3', [dis / +lot.qty, m, lot.id]);
      await c.query('update realized set wash=wash+$1, wash_qty=wash_qty+$2 where id=$3', [dis, m, r.id]);
      wash += dis; need -= m;
    }
  }
  const sum = t => made.filter(r => r.term === t).reduce((s, r) => s + +r.proceeds - +r.basis, 0);
  return { short: round(sum('short'), 2), long: round(sum('long'), 2), wash: round(wash, 2), lots: made.length };
}
// Rebuild lots from the trade history (used once, when upgrading).
async function backfillLots(c) {
  const ws = (await c.query('select * from wallets')).rows;
  for (const w of ws) {
    const trades = (await c.query("select * from trd where wid=$1 and side in ('buy','sell') order by id", [w.id])).rows;
    const lots = {};
    for (const t of trades) {
      const L = lots[t.sym] ||= [];
      if (t.side === 'buy') L.push({ qty: +t.qty, basis: +t.price + (+t.fee || 0) / +t.qty, ts: t.ts, trd: t.id });
      else {
        let left = +t.qty; const net = +t.qty * +t.price - (+t.fee || 0);
        while (left > 1e-12 && L.length) {
          const lot = L[0], take = Math.min(lot.qty, left);
          await c.query('insert into realized(wid,trd_id,sym,qty,proceeds,basis,acquired,sold,term,crypto) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)', [w.id, t.id, t.sym, take, net * take / +t.qty, lot.basis * take, lot.ts, t.ts, isLongTerm(lot.ts, t.ts) ? 'long' : 'short', CRYPTO_DEFAULT.includes(t.sym)]);
          lot.qty -= take; left -= take; if (lot.qty <= 1e-12) L.shift();
        }
      }
    }
    for (const p of (await c.query('select * from pos where wid=$1', [w.id])).rows) {
      let L = (lots[p.sym] || []).filter(l => l.qty > 1e-12);
      const have = L.reduce((t, l) => t + l.qty, 0);
      if (Math.abs(have - +p.qty) > 1e-6) L = [{ qty: +p.qty, basis: +p.avg, ts: p.since || new Date(), trd: null }];
      for (const l of L) await c.query('insert into lots(wid,sym,qty,basis,ts,trd_id) values($1,$2,$3,$4,$5,$6)', [w.id, p.sym, l.qty, l.basis, l.ts, l.trd]);
    }
  }
}

// ---------- stock splits ----------
async function applySplit(wid, sym, num_, den, exTs) {
  const r = num_ / den; if (!(r > 0) || r === 1) return false;
  return tx(async c => {
    const got = (await c.query('insert into splits(wid,sym,ex) values($1,$2,$3) on conflict do nothing returning sym', [wid, sym, String(exTs)])).rows[0];
    if (!got) return false;
    const p = (await c.query('select * from pos where wid=$1 and sym=$2 for update', [wid, sym])).rows[0]; if (!p) return false;
    await c.query('update pos set qty=qty*$1, avg=avg/$1 where wid=$2 and sym=$3', [r, wid, sym]);
    await c.query('update lots set qty=qty*$1, basis=basis/$1, wash_used=wash_used*$1 where wid=$2 and sym=$3', [r, wid, sym]);
    await c.query('update ord set price=price/$1, peak=peak/$1 where wid=$2 and sym=$3', [r, wid, sym]);
    await c.query("update lim set price=price/$1, lpx=lpx/$1, amt=case when units='qty' then amt*$1 else amt end where wid=$2 and sym=$3", [r, wid, sym]);
    await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,$2,'split',$3,$4,$5)", [wid, sym, +p.qty * r, 0, `${num_}-for-${den} split`]);
    return { before: +p.qty, after: +p.qty * r };
  });
}
async function splitJob() {
  for (const p of await rows("select p.* from pos p join wallets w on w.id=p.wid where not w.archived and w.broker is null")) {
    if (isCrypto(p.sym)) continue;
    let ev; try { ev = Object.values((await cached('splits:' + p.sym, 6 * 3600e3, () => yahooChart(p.sym, 'range=3mo&interval=1d&events=split'))).events?.splits || {}); } catch (e) { continue; }
    for (const s of ev) {
      const ex = s.date * 1000; if (ex < tsMs(p.since) || ex > Date.now() || !(s.numerator > 0 && s.denominator > 0)) continue;
      const done = await applySplit(p.wid, p.sym, +s.numerator, +s.denominator, s.date).catch(e => { warn('split', e.message); return false; });
      if (done) {
        await Q('update alert set price=price/$1 where sym=$2 and not fired', [s.numerator / s.denominator, p.sym]); await loadAlerts(); await loadOrders();
        await notify('div', `${p.sym} split ${s.numerator}-for-${s.denominator}`, `You now have ${+done.after.toFixed(6)} shares instead of ${+done.before.toFixed(6)}. Your cost, orders and alerts were adjusted, so the value didn't change.`, { sym: p.sym, wid: p.wid });
      }
    }
  }
}

// ---------- tax summary (like a 1099-B) ----------
async function taxYear(wid, y) {
  const from = new Date(etToUtc(y, 1, 1)), to = new Date(etToUtc(y + 1, 1, 1));
  const rs = (await rows('select * from realized where wid=$1 and sold >= $2 and sold < $3 order by sold, id', [wid, from, to])).map(r => ({
    id: r.id, sym: r.sym, qty: +r.qty, proceeds: +r.proceeds, basis: +r.basis, wash: +r.wash, gain: +r.proceeds - +r.basis + +r.wash, acquired: tsMs(r.acquired), sold: tsMs(r.sold), term: r.term, crypto: r.crypto,
  }));
  const group = f => { const l = rs.filter(f); return { count: l.length, proceeds: l.reduce((t, r) => t + r.proceeds, 0), basis: l.reduce((t, r) => t + r.basis, 0), wash: l.reduce((t, r) => t + r.wash, 0), gain: l.reduce((t, r) => t + r.gain, 0) }; };
  const inc = await row(`select coalesce(sum(case when side='div' then qty*price end),0) div, coalesce(sum(case when side='int' and price>0 then price end),0) intr,
      coalesce(-sum(case when side='int' and price<0 then price end),0) mint, coalesce(sum(fee),0) fees
    from trd where wid=$1 and ts >= $2 and ts < $3`, [wid, from, to]);
  const years = (await rows("select distinct extract(year from sold at time zone 'America/New_York')::int y from realized where wid=$1 union select distinct extract(year from ts at time zone 'America/New_York')::int from trd where wid=$1 and side='div' order by 1 desc", [wid])).map(r => r.y);
  if (!years.includes(y)) years.unshift(y);
  return { y, years: [...new Set(years)].sort((a, b) => b - a), short: group(r => r.term === 'short' && !r.crypto), long: group(r => r.term === 'long' && !r.crypto), cryptoShort: group(r => r.crypto && r.term === 'short'), cryptoLong: group(r => r.crypto && r.term === 'long'), all: group(() => true), dividends: +inc.div, interest: +inc.intr, marginInterest: +inc.mint, fees: +inc.fees, rows: rs };
}

// ---------- monthly statements ----------
async function statementMonths(wid) {
  return (await rows("select distinct to_char(ts at time zone 'America/New_York','YYYY-MM') m from trd where wid=$1 union select to_char(now() at time zone 'America/New_York','YYYY-MM') order by 1 desc", [wid])).map(r => r.m);
}
async function statement(wid, ym) {
  if (!/^\d{4}-\d{2}$/.test(ym)) fail('Pick a month');
  const [y, m] = ym.split('-').map(Number), [ny, nm] = m === 12 ? [y + 1, 1] : [y, m + 1];
  const from = new Date(etToUtc(y, m, 1)), to = new Date(etToUtc(ny, nm, 1)), w = await walletRow(wid), live = Date.now() < to.getTime();
  const open = await row('select eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [wid, from]);
  const close = live ? null : await row('select eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [wid, to]);
  const trades = (await rows('select * from trd where wid=$1 and ts >= $2 and ts < $3 order by id', [wid, from, to])).map(tradeJson);
  const sum = f => trades.filter(f).reduce((t, x) => t + (x.side === 'cash' || x.side === 'int' || x.side === 'fee' ? x.price : x.qty * x.price), 0);
  const fees = {}; for (const t of trades) { if (t.fees) for (const [k, v] of Object.entries(t.fees)) fees[k] = (fees[k] || 0) + v; else if (t.fee && t.side !== 'fee') fees.custom = (fees.custom || 0) + t.fee; }
  const pos = live ? (await positions(wid)).map(p => ({ ...p, price: priceOf(p.sym) || p.avg, value: p.qty * (priceOf(p.sym) || p.avg) })) : null;
  const closing = live ? +w.cash + pos.reduce((t, p) => t + p.value, 0) : (close ? +close.eq : null);
  const opening = open ? +open.eq : (tsMs(w.season_start) >= from.getTime() ? 0 : null);
  const flows = sum(t => t.side === 'cash');
  return {
    month: ym, wallet: walletJson(w), live, opening, closing, change: opening != null && closing != null ? closing - opening - flows : null,
    deposits: sum(t => t.side === 'cash' && t.price > 0), withdrawals: -sum(t => t.side === 'cash' && t.price < 0),
    bought: sum(t => t.side === 'buy'), sold: sum(t => t.side === 'sell'), buys: trades.filter(t => t.side === 'buy').length, sells: trades.filter(t => t.side === 'sell').length,
    realized: trades.filter(t => t.pnl != null).reduce((s, t) => s + t.pnl, 0), dividends: sum(t => t.side === 'div'),
    interest: sum(t => t.side === 'int' && t.price > 0), marginInterest: -sum(t => t.side === 'int' && t.price < 0), otherFees: sum(t => t.side === 'fee'),
    fees, feeTotal: trades.reduce((s, t) => s + (t.side === 'fee' ? 0 : t.fee || 0), 0), trades, positions: pos, cash: live ? +w.cash : null,
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
// Buzz: what news and social media are saying about a stock or coin.
// Free sources only: Finnhub news, Google News RSS, StockTwits, and Reddit when REDDIT_CLIENT_ID/SECRET are set.
// Scoring is done here with a finance word list and event patterns, so it costs nothing to run.
// =====================================================================
const REDDIT = { id: (ENV.REDDIT_CLIENT_ID || '').trim(), secret: (ENV.REDDIT_SECRET || ENV.REDDIT_CLIENT_SECRET || '').trim(), token: null, exp: 0 };
const buzzStatus = { google: { ok: null, t: 0 }, stocktwits: { ok: null, t: 0 }, reddit: { ok: REDDIT.id ? null : false, t: 0 }, finnhub: { ok: null, t: 0 } };
async function getText(url, { timeout = 9000, headers = {} } = {}) {
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Mango Paper Trading)', ...headers }, signal: AbortSignal.timeout(timeout) });
  if (!r.ok) throw new HttpError(502, 'Source error ' + r.status);
  return r.text();
}
// ---------- scoring words (weights from -3 to +3), in the spirit of the Loughran-McDonald finance lexicon ----------
const LEX = {};
const addWords = (w, list) => list.split(/\s+/).filter(Boolean).forEach(x => { LEX[x] = w; });
addWords(3, 'soar soars soared skyrocket skyrockets skyrocketed surge surges surged breakout record-high blowout smashes crushes');
addWords(2, 'beat beats beating rally rallies rallied jump jumps jumped upgrade upgrades upgraded outperform raises raised boost boosts strong stronger strongest bullish buy approval approved approves partnership wins win profit profitable growth gains gain rebound rebounds expands expansion breakthrough tops exceeds exceeded accelerates momentum optimistic upbeat milestone');
addWords(1, 'up higher rise rises rising positive launch launches launching unveil unveils unveiled release releases released announce announces new demand recover recovers steady solid opportunity undervalued upside dividend buyback');
addWords(-1, 'down lower falls fall falling drop drops slips slip weak weaker concern concerns risk risks pressure volatile uncertainty uncertain delay delays delayed caution cautious slowdown slows headwind headwinds overvalued');
addWords(-2, 'miss misses missed downgrade downgrades downgraded underperform cut cuts cutting bearish sell selloff sell-off plunge plunges plunged slump slumps tumble tumbles tumbled loss losses layoffs layoff probe recall recalls warning warns lowers lowered disappoint disappoints disappointing short shorts dilution offering decline declines declined crash crashes');
addWords(-3, 'fraud bankruptcy bankrupt chapter delist delisted delisting investigation indicted subpoena lawsuit sued hack hacked exploit exploited default defaults collapse collapses insolvency halted halt scandal');
const NEGATORS = new Set(['not', 'no', 'never', "isn't", "wasn't", "didn't", "doesn't", 'without', 'fails', 'failed']);
function scoreText(text) {
  const words = String(text || '').toLowerCase().replace(/[^a-z0-9\-' ]+/g, ' ').split(/\s+/).filter(Boolean);
  let s = 0, n = 0;
  for (let i = 0; i < words.length; i++) {
    const w = LEX[words[i]]; if (!w) continue;
    const neg = NEGATORS.has(words[i - 1]) || NEGATORS.has(words[i - 2]);
    s += neg ? -w * 0.7 : w; n++;
  }
  return n ? Math.max(-1, Math.min(1, s / (n * 2.2))) : 0;
}
// ---------- events that matter for trading ----------
const EVENTS = [
  { k: 'earnings', label: 'Earnings', re: /\b(earnings|quarterly results|q[1-4] results|reports? (?:q[1-4]|results)|eps)\b/i, impact: 0 },
  { k: 'launch', label: 'Launch or release', re: /\b(launch(?:es|ed|ing)?|unveil(?:s|ed|ing)?|releas(?:e|es|ed|ing)|debut(?:s|ed)?|keynote|event on|coming (?:soon|out)|rolls? out|ships?)\b/i, impact: 1 },
  { k: 'upgrade', label: 'Analyst upgrade', re: /\b(upgrade[sd]?|raises? (?:price )?target|outperform|overweight|initiat\w+ .*buy)\b/i, impact: 1 },
  { k: 'downgrade', label: 'Analyst downgrade', re: /\b(downgrade[sd]?|cuts? (?:price )?target|underperform|underweight)\b/i, impact: -1 },
  { k: 'guidance_up', label: 'Raised guidance', re: /\b(raise[sd]? (?:its |full-year |annual )?(?:guidance|outlook|forecast))\b/i, impact: 1 },
  { k: 'guidance_down', label: 'Cut guidance', re: /\b(cut[s]?|lower(?:s|ed)?|slash(?:es|ed)?) (?:its |full-year |annual )?(?:guidance|outlook|forecast)\b/i, impact: -2 },
  { k: 'legal', label: 'Lawsuit or investigation', re: /\b(lawsuit|sued|sues|class action|investigation|probe|subpoena|antitrust|sec charges|doj|fined?|penalty|settlement)\b/i, impact: -2 },
  { k: 'severe', label: 'Severe problem', re: /\b(fraud|bankrupt\w*|chapter 11|delist\w*|insolven\w*|hack(?:ed)?|exploit(?:ed)?|default(?:s|ed)?|trading halt|halted)\b/i, impact: -3 },
  { k: 'regulatory', label: 'Regulatory decision', re: /\b(fda|approval|approved|rejects?|rejected|clearance|etf approval|sec approves?|ban(?:s|ned)?)\b/i, impact: 0 },
  { k: 'deal', label: 'Deal or merger', re: /\b(acquir\w+|acquisition|merger|merge[sd]?|buyout|takeover|stake in|partnership|partners with)\b/i, impact: 1 },
  { k: 'recall', label: 'Recall or outage', re: /\b(recall(?:s|ed)?|outage|breach|defect)\b/i, impact: -2 },
  { k: 'layoffs', label: 'Layoffs', re: /\b(layoffs?|job cuts|cuts? \d[\d,]* jobs)\b/i, impact: -1 },
  { k: 'capital', label: 'Buyback or dividend', re: /\b(buyback|repurchase|dividend (?:hike|increase|raise))\b/i, impact: 1 },
  { k: 'offering', label: 'Share sale', re: /\b(stock offering|share offering|secondary offering|dilut\w+)\b/i, impact: -1 },
  { k: 'leadership', label: 'Leadership change', re: /\b(ceo (?:steps down|resigns|to step down|departs)|names new ceo|cfo resigns)\b/i, impact: -1 },
];
const tagEvents = text => EVENTS.filter(e => e.re.test(text)).map(e => e.k);
const EVENT_INFO = Object.fromEntries(EVENTS.map(e => [e.k, e]));
// ---------- sources ----------
function decodeXml(s) { return String(s || '').replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;|&apos;/g, "'").replace(/&#(\d+);/g, (_, d) => String.fromCharCode(+d)).replace(/<[^>]+>/g, '').trim(); }
async function googleNews(sym) {
  const a = assets.get(sym), crypto = isCrypto(sym), name = (a?.name || sym).replace(/[^\w .&-]/g, '');
  const q = crypto ? `${name} crypto when:7d` : `"${name}" OR "${sym} stock" when:7d`;
  return cached('gn:' + sym, 20 * 60e3, async () => {
    const xml = await getText(`https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-US&gl=US&ceid=US:en`);
    buzzStatus.google = { ok: true, t: Date.now() };
    const items = [];
    for (const m of xml.matchAll(/<item>([\s\S]*?)<\/item>/g)) {
      const x = m[1], get = t => decodeXml((new RegExp(`<${t}[^>]*>([\\s\\S]*?)<\\/${t}>`).exec(x) || [])[1]);
      const title = get('title'), source = get('source'), t = Date.parse(get('pubDate'));
      if (!title || !Number.isFinite(t)) continue;
      items.push({ src: 'news', via: source || 'Google News', t, title: title.replace(new RegExp(`\\s+-\\s+${source.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`), ''), url: get('link') });
      if (items.length >= 40) break;
    }
    return items;
  }).catch(e => { buzzStatus.google = { ok: false, t: Date.now(), err: e.message }; return []; });
}
async function finnhubNews(sym) {
  if (!FINNHUB_KEY) return [];
  try {
    const list = isCrypto(sym) ? await assetNews(sym) : await assetNews(sym);
    buzzStatus.finnhub = { ok: true, t: Date.now() };
    return list.map(n => ({ src: 'news', via: n.source, t: n.t, title: n.title, summary: n.summary, url: n.url }));
  } catch (e) { buzzStatus.finnhub = { ok: false, t: Date.now(), err: e.message }; return []; }
}
async function stocktwits(sym) {
  return cached('st:' + sym, 15 * 60e3, async () => {
    const d = await getJ(`https://api.stocktwits.com/api/2/streams/symbol/${encodeURIComponent(isCrypto(sym) ? sym + '.X' : sym)}.json`, { timeout: 8000 });
    buzzStatus.stocktwits = { ok: true, t: Date.now() };
    return (d.messages || []).slice(0, 30).map(m => ({ src: 'social', via: 'StockTwits', t: Date.parse(m.created_at), title: String(m.body || '').slice(0, 280), url: `https://stocktwits.com/message/${m.id}`, tagged: m.entities?.sentiment?.basic || null, likes: m.likes?.total || 0 }));
  }).catch(e => { buzzStatus.stocktwits = { ok: false, t: Date.now(), err: e.message }; return []; });
}
async function redditToken() {
  if (REDDIT.token && Date.now() < REDDIT.exp - 60e3) return REDDIT.token;
  const r = await fetch('https://www.reddit.com/api/v1/access_token', { method: 'POST', headers: { Authorization: 'Basic ' + Buffer.from(REDDIT.id + ':' + REDDIT.secret).toString('base64'), 'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': 'web:mango-paper-trading:2.2 (personal use)' }, body: 'grant_type=client_credentials', signal: AbortSignal.timeout(8000) });
  if (!r.ok) throw new Error('Reddit login ' + r.status);
  const j = await r.json(); REDDIT.token = j.access_token; REDDIT.exp = Date.now() + (j.expires_in || 3600) * 1000;
  return REDDIT.token;
}
async function reddit(sym) {
  if (!REDDIT.id || !REDDIT.secret) return [];
  const crypto = isCrypto(sym), name = assets.get(sym)?.name || sym;
  const subs = crypto ? 'CryptoCurrency+CryptoMarkets+Bitcoin+ethtrader' : 'wallstreetbets+stocks+investing+StockMarket+options';
  const q = crypto ? `${sym} OR "${name}"` : `$${sym} OR "${name}"`;
  return cached('rd:' + sym, 20 * 60e3, async () => {
    const tok = await redditToken();
    const r = await fetch(`https://oauth.reddit.com/r/${subs}/search?q=${encodeURIComponent(q)}&restrict_sr=1&sort=new&t=week&limit=40`, { headers: { Authorization: 'Bearer ' + tok, 'User-Agent': 'web:mango-paper-trading:2.2 (personal use)' }, signal: AbortSignal.timeout(9000) });
    if (!r.ok) throw new Error('Reddit ' + r.status);
    const j = await r.json(); buzzStatus.reddit = { ok: true, t: Date.now() };
    return (j.data?.children || []).map(c => c.data).map(p => ({ src: 'social', via: 'r/' + p.subreddit, t: p.created_utc * 1000, title: String(p.title || '').slice(0, 280), summary: String(p.selftext || '').slice(0, 300), url: 'https://www.reddit.com' + p.permalink, likes: p.score || 0 }));
  }).catch(e => { buzzStatus.reddit = { ok: false, t: Date.now(), err: e.message }; return []; });
}
// ---------- the summary ----------
async function buzzFor(sym) {
  return cached('buzz:' + sym, 10 * 60e3, async () => {
    const [fh, gn, st, rd, ins] = await Promise.all([finnhubNews(sym), googleNews(sym), stocktwits(sym), reddit(sym), isCrypto(sym) ? null : assetInsights(sym).catch(() => null)]);
    const now = Date.now(), seen = new Set(), items = [];
    for (const it of [...fh, ...gn, ...st, ...rd]) {
      const key = it.title.toLowerCase().replace(/\W+/g, ' ').slice(0, 70);
      if (!it.title || seen.has(key) || !(it.t > now - 8 * 86400e3)) continue; seen.add(key);
      const text = it.title + ' ' + (it.summary || '');
      let score = scoreText(text);
      if (it.tagged === 'Bullish') score = Math.max(score, 0) * 0.5 + 0.5; else if (it.tagged === 'Bearish') score = Math.min(score, 0) * 0.5 - 0.5;
      items.push({ ...it, score: round(score, 3), tags: it.src === 'news' ? tagEvents(text) : [] });
    }
    items.sort((a, b) => b.t - a.t);
    const news = items.filter(i => i.src === 'news'), social = items.filter(i => i.src === 'social');
    // Recent items count more: a 3-day half-life.
    const wavg = list => { let s = 0, w = 0; for (const i of list) { const k = Math.pow(0.5, (now - i.t) / (3 * 86400e3)) * (1 + Math.log10(1 + (i.likes || 0))); s += i.score * k; w += k; } return w ? s / w : null; };
    const newsScore = wavg(news), socialScore = wavg(social);
    const parts = [[newsScore, news.length], [socialScore, social.length]].filter(([v]) => v != null);
    const sentiment = parts.length ? parts.reduce((t, [v, n]) => t + v * Math.min(n, 30), 0) / parts.reduce((t, [, n]) => t + Math.min(n, 30), 0) : 0;
    const day = items.filter(i => i.t > now - 86400e3).length, week = items.filter(i => i.t > now - 7 * 86400e3).length;
    const baseline = Math.max(1, (week - day) / 6);
    const tagged = social.filter(s => s.tagged), bull = tagged.filter(s => s.tagged === 'Bullish').length;
    // Events from the last few days, strongest first.
    const evMap = new Map();
    for (const it of news.filter(i => i.t > now - 5 * 86400e3)) for (const k of it.tags) { const e = evMap.get(k) || { k, label: EVENT_INFO[k].label, impact: EVENT_INFO[k].impact, count: 0, latest: it }; e.count++; if (it.t > e.latest.t) e.latest = it; evMap.set(k, e); }
    const events = [...evMap.values()].sort((a, b) => Math.abs(b.impact) - Math.abs(a.impact) || b.count - a.count).map(e => ({ k: e.k, label: e.label, impact: e.impact, count: e.count, title: e.latest.title, t: e.latest.t, url: e.latest.url }));
    const upcoming = [];
    if (ins?.next?.date) upcoming.push({ k: 'earnings', label: 'Earnings report', date: ins.next.date, hour: ins.next.hour, eps: ins.next.eps });
    const daysTo = d => { const p = etParts(); let [y, m, dd] = [p.y, p.m, p.d], n = 0; const target = d; while (`${y}-${String(m).padStart(2, '0')}-${String(dd).padStart(2, '0')}` < target && n < 60) { [y, m, dd] = nextTradingDay(y, m, dd); n++; } return n; };
    for (const u of upcoming) u.tradingDays = daysTo(u.date);
    const flags = [];
    if (upcoming.some(u => u.k === 'earnings' && u.tradingDays <= 3)) flags.push('earnings_soon');
    if (events.some(e => e.k === 'severe')) flags.push('severe_news');
    if (events.some(e => e.impact <= -2)) flags.push('negative_event');
    if (sentiment <= -0.3 && items.length >= 5) flags.push('negative_mood');
    if (sentiment >= 0.3 && items.length >= 5) flags.push('positive_mood');
    if (day >= 6 && day / baseline >= 3) flags.push('attention_spike');
    return {
      sym, t: now, sentiment: round(sentiment, 3), newsScore: newsScore == null ? null : round(newsScore, 3), socialScore: socialScore == null ? null : round(socialScore, 3),
      count: items.length, news: news.length, social: social.length, day, attention: round(day / baseline, 2), bullPct: tagged.length >= 5 ? round(bull / tagged.length * 100, 1) : null,
      events, upcoming, flags, items: items.slice(0, 40), sources: { finnhub: fh.length, google: gn.length, stocktwits: st.length, reddit: rd.length },
    };
  });
}
const buzzSummary = b => b && ({ sym: b.sym, t: b.t, sentiment: b.sentiment, count: b.count, day: b.day, attention: b.attention, bullPct: b.bullPct, flags: b.flags, events: b.events.slice(0, 3), upcoming: b.upcoming });
function buzzLine(b) {
  if (!b) return 'No news or social data';
  const mood = b.count < 3 ? 'very little chatter' : b.sentiment >= 0.3 ? 'upbeat' : b.sentiment >= 0.1 ? 'leaning positive' : b.sentiment <= -0.3 ? 'gloomy' : b.sentiment <= -0.1 ? 'leaning negative' : 'mixed';
  const bits = [`${b.count} items this week, mood ${mood} (${b.sentiment >= 0 ? '+' : ''}${b.sentiment.toFixed(2)})`];
  if (b.bullPct != null) bits.push(`${Math.round(b.bullPct)}% of tagged posts bullish`);
  if (b.flags.includes('attention_spike')) bits.push(`${b.attention}x the usual attention`);
  const up = b.upcoming.find(u => u.k === 'earnings'); if (up) bits.push(`earnings ${up.tradingDays <= 0 ? 'today' : `in ${up.tradingDays} trading day${up.tradingDays === 1 ? '' : 's'}`} (${up.date})`);
  if (b.events.length) bits.push('news: ' + b.events.slice(0, 2).map(e => e.label.toLowerCase()).join(', '));
  return bits.join('; ');
}

// =====================================================================
// Trading strategies (daily bars) and the history they run on
// Every rule only looks at bars up to the day it decides on, so backtests can't peek at the future.
// =====================================================================
async function dailyBars(sym) {
  return cached('d1:' + sym, 6 * 3600e3, async () => {
    const r = await yahooChart(sym, 'range=10y&interval=1d&events=div,split');
    const z = r.indicators?.quote?.[0] || {}, adj = r.indicators?.adjclose?.[0]?.adjclose, ts = r.timestamp || [];
    const crypto = isCrypto(sym), out = { sym, d: [], o: [], h: [], l: [], c: [], v: [] };
    for (let i = 0; i < ts.length; i++) {
      const c = z.close?.[i]; if (c == null || !(c > 0)) continue;
      const f = adj?.[i] > 0 ? adj[i] / c : 1; // dividend adjustment so returns include dividends
      const d = crypto ? new Date(ts[i] * 1000).toISOString().slice(0, 10) : etDate(ts[i] * 1000);
      if (out.d.length && out.d[out.d.length - 1] === d) { out.c[out.c.length - 1] = c * f; continue; }
      out.d.push(d); out.o.push((z.open?.[i] ?? c) * f); out.h.push((z.high?.[i] ?? c) * f); out.l.push((z.low?.[i] ?? c) * f); out.c.push(c * f); out.v.push(z.volume?.[i] || 0);
    }
    if (out.d.length < 30) fail(`Not enough price history for ${sym}`);
    return out;
  });
}
// ---------- indicators (value at index i using bars 0..i) ----------
const IND = {
  sma(a, n, i) { if (i < n - 1) return null; let s = 0; for (let k = i - n + 1; k <= i; k++) s += a[k]; return s / n; },
  ema(a, n, i, memo) {
    const key = 'ema' + n; let m = memo?.[key];
    if (!m) { m = new Array(a.length).fill(null); const k = 2 / (n + 1); let e = null; for (let j = 0; j < a.length; j++) { if (j === n - 1) { let s = 0; for (let q = 0; q < n; q++) s += a[q]; e = s / n; } else if (j >= n) e = a[j] * k + e * (1 - k); m[j] = j >= n - 1 ? e : null; } if (memo) memo[key] = m; }
    return m[i];
  },
  rsi(a, n, i, memo) {
    const key = 'rsi' + n; let m = memo?.[key];
    if (!m) { m = new Array(a.length).fill(null); let g = 0, l = 0; for (let j = 1; j < a.length; j++) { const ch = a[j] - a[j - 1], up = Math.max(0, ch), dn = Math.max(0, -ch); if (j <= n) { g += up; l += dn; if (j === n) { g /= n; l /= n; m[j] = l === 0 ? 100 : 100 - 100 / (1 + g / l); } } else { g = (g * (n - 1) + up) / n; l = (l * (n - 1) + dn) / n; m[j] = l === 0 ? 100 : 100 - 100 / (1 + g / l); } } if (memo) memo[key] = m; }
    return m[i];
  },
  atr(b, n, i, memo) {
    const key = 'atr' + n; let m = memo?.[key];
    if (!m) { m = new Array(b.c.length).fill(null); let a = null, s = 0; for (let j = 1; j < b.c.length; j++) { const tr = Math.max(b.h[j] - b.l[j], Math.abs(b.h[j] - b.c[j - 1]), Math.abs(b.l[j] - b.c[j - 1])); if (j <= n) { s += tr; if (j === n) a = s / n; } else a = (a * (n - 1) + tr) / n; m[j] = a; } if (memo) memo[key] = m; }
    return m[i];
  },
  highest(a, n, i, skipToday = true) { const e = skipToday ? i - 1 : i, s = e - n + 1; if (s < 0) return null; let v = -Infinity; for (let k = s; k <= e; k++) v = Math.max(v, a[k]); return v; },
  lowest(a, n, i, skipToday = true) { const e = skipToday ? i - 1 : i, s = e - n + 1; if (s < 0) return null; let v = Infinity; for (let k = s; k <= e; k++) v = Math.min(v, a[k]); return v; },
  ret(a, n, i, skip = 0) { const j = i - skip, k = i - n; if (k < 0 || j < 0) return null; return a[j] / a[k] - 1; },
  vol(a, n, i) { if (i < n) return null; let s = 0, s2 = 0; for (let k = i - n + 1; k <= i; k++) { const r = Math.log(a[k] / a[k - 1]); s += r; s2 += r * r; } const m = s / n; return Math.sqrt(Math.max(0, s2 / n - m * m)); },
};
const pf = (v, d = 2) => (v >= 0 ? '+' : '') + (v * 100).toFixed(d) + '%';
const px2 = v => '$' + (+v).toLocaleString('en-US', { maximumFractionDigits: v < 1 ? 6 : 2, minimumFractionDigits: v < 1 ? 2 : 2 });
// Weights for a set of picks: equal, or inverse volatility (calmer assets get more).
function sizeWeights(picks, ctx, maxW) {
  const p = ctx.params, out = {};
  if (!picks.length) return out;
  if (p.sizing === 'invvol') {
    const iv = picks.map(s => { const v = IND.vol(ctx.data[s].c, 20, ctx.idx[s]); return v > 0 ? 1 / v : 0; }), tot = iv.reduce((a, b) => a + b, 0) || 1;
    picks.forEach((s, k) => { out[s] = Math.min(maxW, iv[k] / tot); });
  } else picks.forEach(s => { out[s] = Math.min(maxW, 1 / Math.max(picks.length, p.maxPositions || picks.length)); });
  return out;
}
// ---------- the strategies ----------
const STRATEGIES = {
  trend: {
    name: 'Trend follower', short: 'Trend', icon: 'trail', rebalance: 'none', markets: 'both',
    desc: 'Owns what is in an uptrend: price above its slow average and the fast average above the slow one. Each position has a trailing stop a few ATRs below its high. Rides big moves, gives back some at the turn.',
    params: [{ k: 'fast', label: 'Fast average (days)', def: 50, min: 5, max: 100 }, { k: 'slow', label: 'Slow average (days)', def: 200, min: 50, max: 300 }, { k: 'atrMult', label: 'Trailing stop (ATRs)', def: 3, min: 1, max: 8, step: 0.5 }, { k: 'maxPositions', label: 'Most positions', def: 5, min: 1, max: 20 }, { k: 'sizing', label: 'Sizing', def: 'invvol', choices: [['invvol', 'By volatility'], ['equal', 'Equal']] }],
    grid: { fast: [20, 50], slow: [100, 200], atrMult: [2.5, 3.5] },
    need: p => p.slow + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [];
      for (const s of ctx.syms) {
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.slow) continue;
        const c = b.c[i], sl = IND.sma(b.c, p.slow, i), ef = IND.ema(b.c, p.fast, i, b.memo), es = IND.ema(b.c, p.slow, i, b.memo), atr = IND.atr(b, 14, i, b.memo), mom = IND.ret(b.c, 126, i) ?? 0;
        const up = c > sl && ef > es, held = ctx.held[s];
        out.notes[s] = `Close ${px2(c)} vs ${p.slow}-day average ${px2(sl)}; ${p.fast}-day trend ${ef > es ? 'above' : 'below'} ${p.slow}-day. ${up ? 'In an uptrend' : 'Not trending up'}.`;
        if (up) cand.push({ s, mom, atr, c, held });
        else if (held) out.notes[s] += ' Exit: the trend broke.';
      }
      cand.sort((a, b) => (b.held ? 1 : 0) - (a.held ? 1 : 0) || b.mom - a.mom);
      const picks = cand.slice(0, p.maxPositions);
      Object.assign(out.weights, sizeWeights(picks.map(x => x.s), ctx, ctx.maxW));
      for (const x of picks) { const hi = Math.max(x.c, ctx.held[x.s]?.high || 0); out.stops[x.s] = { trail: true, price: hi - p.atrMult * x.atr, atr: x.atr, mult: p.atrMult }; }
      return out;
    },
  },
  momentum: {
    name: 'Momentum rotation', short: 'Momentum', icon: 'zap', rebalance: 'monthly', markets: 'both',
    desc: 'Once a month, ranks everything by its return over the past 6 months (skipping the latest month) and holds the top few that are still rising. Goes to cash when the benchmark is below its 200-day average. A well-studied effect across stocks, ETFs and crypto.',
    params: [{ k: 'lookback', label: 'Look back (days)', def: 126, min: 21, max: 252 }, { k: 'skip', label: 'Skip latest (days)', def: 21, min: 0, max: 42 }, { k: 'top', label: 'Hold the top', def: 3, min: 1, max: 10 }, { k: 'filterLen', label: 'Market filter (days)', def: 200, min: 0, max: 300 }, { k: 'sizing', label: 'Sizing', def: 'equal', choices: [['equal', 'Equal'], ['invvol', 'By volatility']] }],
    grid: { lookback: [63, 126, 252], top: [2, 3, 5] },
    need: p => Math.max(p.lookback, p.filterLen || 0) + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} };
      const bench = ctx.bench && ctx.data[ctx.bench], bi = ctx.idx[ctx.bench];
      let riskOn = true;
      if (p.filterLen > 0 && bench && bi != null && bi >= p.filterLen) { const bs = IND.sma(bench.c, p.filterLen, bi); riskOn = bench.c[bi] > bs; ctx.regime = `${ctx.bench} ${riskOn ? 'above' : 'below'} its ${p.filterLen}-day average (${px2(bench.c[bi])} vs ${px2(bs)})`; }
      const ranked = [];
      for (const s of ctx.syms) { const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.lookback) continue; const r = IND.ret(b.c, p.lookback, i, p.skip); if (r == null) continue; ranked.push({ s, r }); }
      ranked.sort((a, b) => b.r - a.r);
      ranked.forEach((x, k) => { out.notes[x.s] = `${p.lookback}-day return ${pf(x.r, 1)} (rank ${k + 1} of ${ranked.length})${!riskOn ? '. Market filter says stay in cash' : x.r <= 0 ? '. Not rising, so no' : k < p.top ? '. In the top ' + p.top : ''}.`; });
      if (!riskOn) return out;
      const picks = ranked.filter(x => x.r > 0).slice(0, p.top).map(x => x.s);
      Object.assign(out.weights, sizeWeights(picks, { ...ctx, params: { ...p, maxPositions: p.top } }, ctx.maxW));
      return out;
    },
  },
  rsi2: {
    name: 'Dip buyer (RSI-2)', short: 'Dip buyer', icon: 'arrowDn', rebalance: 'none', markets: 'stocks',
    desc: 'Buys sharp short pullbacks in assets that are in a long-term uptrend: the 2-day RSI under 10 while price is above its 200-day average. Sells on the first bounce above the 5-day average, or after 10 days. Many small wins, works best on index ETFs and big stocks.',
    params: [{ k: 'entry', label: 'Buy when RSI-2 below', def: 10, min: 1, max: 30 }, { k: 'exitLen', label: 'Sell above average (days)', def: 5, min: 2, max: 20 }, { k: 'trendLen', label: 'Trend filter (days)', def: 200, min: 0, max: 300 }, { k: 'maxDays', label: 'Most days to hold', def: 10, min: 2, max: 30 }, { k: 'maxPositions', label: 'Most positions', def: 4, min: 1, max: 10 }, { k: 'stopAtr', label: 'Disaster stop (ATRs, 0 = off)', def: 0, min: 0, max: 8, step: 0.5 }],
    grid: { entry: [5, 10, 15], exitLen: [3, 5, 10] },
    need: p => Math.max(p.trendLen || 0, 20) + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], slot = Math.min(ctx.maxW, 1 / p.maxPositions);
      let held = 0;
      for (const s of ctx.syms) {
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < Math.max(p.trendLen, 20)) continue;
        const c = b.c[i], r = IND.rsi(b.c, 2, i, b.memo), ex = IND.sma(b.c, p.exitLen, i), tr = p.trendLen ? IND.sma(b.c, p.trendLen, i) : 0, h = ctx.held[s];
        if (h) {
          const days = h.days || 0, exit = c > ex || days >= p.maxDays;
          out.notes[s] = `Holding ${days} day${days === 1 ? '' : 's'}. Close ${px2(c)} vs ${p.exitLen}-day average ${px2(ex)}.${exit ? (c > ex ? ' Bounced: sell.' : ' Held too long: sell.') : ' Waiting for the bounce.'}`;
          if (!exit) { out.weights[s] = h.weight || slot; held++; if (p.stopAtr > 0) out.stops[s] = { trail: false, price: h.entry - p.stopAtr * IND.atr(b, 14, i, b.memo) }; }
        } else {
          const ok = r != null && r < p.entry && (!p.trendLen || c > tr);
          out.notes[s] = `RSI-2 ${r == null ? '–' : r.toFixed(1)}${p.trendLen ? `, ${c > tr ? 'above' : 'below'} its ${p.trendLen}-day average` : ''}.${ok ? ' A dip in an uptrend: buy.' : ''}`;
          if (ok) cand.push({ s, r });
        }
      }
      cand.sort((a, b) => a.r - b.r);
      for (const x of cand.slice(0, Math.max(0, p.maxPositions - held))) { out.weights[x.s] = slot; if (p.stopAtr > 0) out.stops[x.s] = { trail: false, price: ctx.data[x.s].c[ctx.idx[x.s]] - p.stopAtr * IND.atr(ctx.data[x.s], 14, ctx.idx[x.s], ctx.data[x.s].memo) }; }
      return out;
    },
  },
  breakout: {
    name: 'Breakout (Turtle)', short: 'Breakout', icon: 'bolt', rebalance: 'none', markets: 'both',
    desc: 'Buys when price closes above its highest high of the last 55 days and sells when it closes below its lowest low of the last 20. Each trade risks about 1% of the wallet, using a stop 2 ATRs below the entry. The classic Turtle rules, strong on crypto and trending stocks.',
    params: [{ k: 'entryLen', label: 'Breakout over (days)', def: 55, min: 10, max: 120 }, { k: 'exitLen', label: 'Exit under (days)', def: 20, min: 5, max: 60 }, { k: 'atrMult', label: 'Stop (ATRs)', def: 2, min: 1, max: 6, step: 0.5 }, { k: 'riskPct', label: 'Risk per trade (%)', def: 1, min: 0.25, max: 3, step: 0.25 }, { k: 'maxPositions', label: 'Most positions', def: 6, min: 1, max: 20 }],
    grid: { entryLen: [20, 55], exitLen: [10, 20], atrMult: [2, 3] },
    need: p => Math.max(p.entryLen, p.exitLen) + 20,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [];
      let held = 0;
      for (const s of ctx.syms) {
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.entryLen + 15) continue;
        const c = b.c[i], hi = IND.highest(b.h, p.entryLen, i), lo = IND.lowest(b.l, p.exitLen, i), atr = IND.atr(b, 20, i, b.memo), h = ctx.held[s];
        const w = Math.min(ctx.maxW, (p.riskPct / 100) * c / (p.atrMult * atr));
        if (h) {
          const exit = c < lo;
          out.notes[s] = `Holding. Close ${px2(c)}, ${p.exitLen}-day low ${px2(lo)}.${exit ? ' Broke the low: sell.' : ''}`;
          if (!exit) { out.weights[s] = h.weight || w; out.stops[s] = { trail: false, price: h.stopPx || (h.entry - p.atrMult * atr) }; held++; }
        } else {
          const ok = c > hi;
          out.notes[s] = `Close ${px2(c)} vs ${p.entryLen}-day high ${px2(hi)}.${ok ? ' New breakout: buy.' : ''}`;
          if (ok) cand.push({ s, w, c, atr, strength: c / hi - 1 });
        }
      }
      cand.sort((a, b) => b.strength - a.strength);
      for (const x of cand.slice(0, Math.max(0, p.maxPositions - held))) { out.weights[x.s] = x.w; out.stops[x.s] = { trail: false, price: x.c - p.atrMult * x.atr }; }
      return out;
    },
  },
  hold: {
    name: 'Buy and hold', short: 'Hold', icon: 'layers', rebalance: 'monthly', markets: 'both',
    desc: 'Owns everything in the list in equal parts and rebalances once a month. No timing at all. Use it as the yardstick: a strategy that cannot beat this after fees is not worth running.',
    params: [], grid: {}, need: () => 2,
    decide(ctx) { const out = { weights: {}, stops: {}, notes: {} }, live = ctx.syms.filter(s => ctx.idx[s] != null); for (const s of live) { out.weights[s] = Math.min(ctx.maxW, 1 / live.length); out.notes[s] = 'Held in equal parts.'; } return out; },
  },
};
const UNIVERSES = {
  megacap: { name: 'Big US stocks', syms: ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'JPM', 'V', 'LLY', 'COST', 'NFLX', 'WMT', 'XOM', 'AMD'] },
  etfs: { name: 'Index and sector ETFs', syms: ['SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF', 'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLU', 'XLB', 'XLC', 'XLRE'] },
  allweather: { name: 'Stocks, bonds and gold', syms: ['SPY', 'QQQ', 'IWM', 'EFA', 'EEM', 'TLT', 'IEF', 'GLD', 'DBC', 'VNQ'] },
  crypto: { name: 'Top crypto', syms: ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'LTC', 'DOT'] },
  mixed: { name: 'Stocks and crypto', syms: ['SPY', 'QQQ', 'GLD', 'TLT', 'BTC', 'ETH', 'SOL'] },
};
function cleanParams(key, p = {}) {
  const st = STRATEGIES[key]; if (!st) fail('Unknown strategy');
  const out = {};
  for (const d of st.params) {
    if (d.choices) out[d.k] = d.choices.some(c => c[0] === p[d.k]) ? p[d.k] : d.def;
    else out[d.k] = clamp(num(p[d.k], d.def), d.min, d.max);
  }
  return out;
}
const strategiesJson = () => Object.fromEntries(Object.entries(STRATEGIES).map(([k, s]) => [k, { name: s.name, short: s.short, icon: s.icon, desc: s.desc, rebalance: s.rebalance, markets: s.markets, params: s.params }]));

// =====================================================================
// Backtester: replays a strategy day by day with the same spreads and fees as live trading
// Decisions use each day's close and fill at that close (bots trade 15 minutes before the bell),
// paying half the spread, a little slippage and the broker's fees. Stops are checked against each day's low.
// =====================================================================
async function loadUniverse(syms) {
  const data = {}, missing = [];
  await Promise.all(syms.map(async s => { try { const b = await dailyBars(s); data[s] = b; } catch (e) { missing.push(s); } }));
  return { data, missing };
}
const benchFor = syms => syms.some(s => !isCrypto(s)) ? 'SPY' : 'BTC';
function runBacktest({ key, params, syms, data, start, end, capital = 10000, profile = 'robinhood', route = 'mm', maxPosPct = 100, slipBps = 2, bench }) {
  const st = STRATEGIES[key], p = params, prof = PROFILES[profile] || PROFILES.robinhood, w = { crypto_route: route };
  const series = {}; for (const s of [...new Set([...syms, bench].filter(Boolean))]) if (data[s]) series[s] = { ...data[s], memo: {}, at: new Map(data[s].d.map((d, i) => [d, i])) };
  const live = syms.filter(s => series[s]);
  const allDates = [...new Set(live.flatMap(s => series[s].d))].sort().filter(d => (!start || d >= start) && (!end || d <= end));
  const need = st.need(p), ptr = {}; for (const s of Object.keys(series)) ptr[s] = -1;
  let cash = capital, peak = capital, maxDD = 0, fees = 0, spreadCost = 0, vol30 = [], exposureDays = 0;
  const pos = {}, curve = [], trades = [], closed = [], daily = [];
  const halfSpread = (s, px) => px * modelSpreadBps(s, 'regular') / 2e4 + px * slipBps / 1e4;
  const fee = (s, side, qty, notional, d) => {
    if (isCrypto(s)) { const v = vol30.filter(x => x.d > addIso(d, -30)).reduce((t, x) => t + x.v, 0); return cryptoFees(prof, w, notional, 'taker', v).total; }
    return stockFees(prof, side, qty, notional, d).total;
  };
  const trade = (s, side, qty, px, d, why) => {
    if (!(qty > 0)) return;
    const fillPx = side === 'buy' ? px + halfSpread(s, px) : Math.max(px * 0.5, px - halfSpread(s, px));
    const notional = qty * fillPx, f = fee(s, side, qty, notional, d);
    spreadCost += Math.abs(fillPx - px) * qty; fees += f;
    if (isCrypto(s)) vol30.push({ d, v: notional });
    if (side === 'buy') {
      cash -= notional + f;
      const h = pos[s] || (pos[s] = { qty: 0, cost: 0, entry: fillPx, entryDate: d, high: px, days: 0 });
      h.qty += qty; h.cost += notional + f;
    } else {
      const h = pos[s]; const part = qty / h.qty, basis = h.cost * part;
      cash += notional - f; const pnl = notional - f - basis;
      closed.push({ sym: s, entryDate: h.entryDate, exitDate: d, entry: h.cost / h.qty, exit: fillPx, qty, pnl, ret: pnl / basis, why, days: daysBetween(h.entryDate, d) });
      h.qty -= qty; h.cost -= basis; if (h.qty <= 1e-9) delete pos[s];
    }
    trades.push({ d, sym: s, side, qty, px: fillPx, fee: f, why });
  };
  const value = () => cash + Object.entries(pos).reduce((t, [s, h]) => t + h.qty * series[s].c[ptr[s]], 0);
  let lastMonth = null, started = false;
  for (const d of allDates) {
    for (const s of Object.keys(series)) { const j = series[s].at.get(d); if (j != null) ptr[s] = j; }
    const ready = live.filter(s => ptr[s] >= need);
    if (!started) { if (!ready.length) continue; started = true; }
    // 1) stops during the day: a gap below the stop fills at the open. No buying back the same day.
    const stoppedToday = new Set();
    for (const [s, h] of Object.entries(pos)) {
      const i = series[s].at.get(d); if (i == null || h.stop == null) continue;
      const b = series[s];
      if (b.l[i] <= h.stop) { trade(s, 'sell', h.qty, Math.min(b.o[i], h.stop), d, 'stop'); stoppedToday.add(s); }
    }
    // 2) decide at the close
    const idx = {}; for (const s of Object.keys(series)) idx[s] = ptr[s] >= 0 ? ptr[s] : null;
    const tradable = live.filter(s => series[s].at.get(d) != null && ptr[s] >= need);
    const held = {}; for (const [s, h] of Object.entries(pos)) { const i = ptr[s]; h.high = Math.max(h.high, series[s].c[i]); held[s] = { qty: h.qty, entry: h.cost / h.qty, entryDate: h.entryDate, high: h.high, days: h.days, weight: h.weight, stopPx: h.stop }; }
    const month = d.slice(0, 7), rebalanceDay = st.rebalance === 'monthly' ? month !== lastMonth : st.rebalance === 'weekly' ? new Date(d + 'T12:00:00Z').getUTCDay() === 1 : false;
    const eq0 = value();
    const acts = st.rebalance === 'none' || rebalanceDay;
    if (tradable.length && acts) {
      const ctx = { syms: tradable, data: series, idx, params: p, held, bench, maxW: maxPosPct / 100 };
      const dec = st.decide(ctx);
      // exits first
      for (const s of Object.keys(pos)) if (tradable.includes(s) && !(dec.weights[s] > 0)) trade(s, 'sell', pos[s].qty, series[s].c[ptr[s]], d, 'signal');
      for (const [s, wgt] of Object.entries(dec.weights)) {
        if (!(wgt > 0) || !tradable.includes(s) || stoppedToday.has(s)) continue;
        const px = series[s].c[ptr[s]], want = wgt * eq0, have = (pos[s]?.qty || 0) * px;
        if (!pos[s] || rebalanceDay) {
          const diff = want - have;
          if (Math.abs(diff) > Math.max(eq0 * 0.01, 5)) {
            if (diff > 0) { const room = cash - 0.5; const usd = Math.min(diff, room); if (usd > 1) trade(s, 'buy', usd / (px + halfSpread(s, px)) * 0.999, px, d, pos[s] ? 'rebalance' : 'signal'); }
            else trade(s, 'sell', Math.min(pos[s].qty, -diff / px), px, d, 'rebalance');
          }
        }
        if (pos[s]) { pos[s].weight = wgt; const sp = dec.stops[s]; pos[s].stop = sp ? (sp.trail ? Math.max(pos[s].stop || 0, sp.price) : sp.price) : null; }
      }
    }
    if (st.rebalance === 'monthly') lastMonth = month;
    for (const h of Object.values(pos)) h.days++;
    const eq = value(); peak = Math.max(peak, eq); maxDD = Math.min(maxDD, eq / peak - 1);
    if (Object.keys(pos).length) exposureDays++;
    curve.push([d, eq]);
  }
  // benchmark: buy and hold over the same days
  let bcurve = null;
  if (bench && series[bench] && curve.length) {
    const b = series[bench]; let units = null; bcurve = [];
    for (const [d] of curve) { const i = b.at.get(d) ?? lastIndexOnOrBefore(b.d, d); if (i < 0) { bcurve.push([d, capital]); continue; } if (units == null) units = capital / b.c[i]; bcurve.push([d, units * b.c[i]]); }
  }
  return { curve, bcurve, trades, closed, fees, spreadCost, maxDD, exposure: curve.length ? exposureDays / curve.length : 0, open: Object.entries(pos).map(([s, h]) => ({ sym: s, qty: h.qty, entry: h.cost / h.qty, entryDate: h.entryDate, value: h.qty * series[s].c[ptr[s]] })) };
}
function lastIndexOnOrBefore(arr, d) { let lo = 0, hi = arr.length - 1, r = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (arr[m] <= d) { r = m; lo = m + 1; } else hi = m - 1; } return r; }
const addIso = (d, n) => new Date(Date.parse(d + 'T12:00:00Z') + n * 864e5).toISOString().slice(0, 10);
const daysBetween = (a, b) => Math.round((Date.parse(b + 'T12:00:00Z') - Date.parse(a + 'T12:00:00Z')) / 864e5);
function curveStats(curve, periodsPerYear) {
  if (!curve || curve.length < 2) return null;
  const v0 = curve[0][1], v1 = curve[curve.length - 1][1], years = Math.max(1 / 365, daysBetween(curve[0][0], curve[curve.length - 1][0]) / 365.25);
  const rets = []; for (let i = 1; i < curve.length; i++) rets.push(curve[i][1] / curve[i - 1][1] - 1);
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length, sd = Math.sqrt(rets.reduce((a, r) => a + (r - mean) ** 2, 0) / Math.max(1, rets.length - 1));
  const down = Math.sqrt(rets.reduce((a, r) => a + Math.min(0, r) ** 2, 0) / Math.max(1, rets.length));
  let peak = -Infinity, dd = 0, ddStart = null, longest = 0, cur = 0; for (const [, v] of curve) { if (v >= peak) { peak = v; cur = 0; } else cur++; longest = Math.max(longest, cur); dd = Math.min(dd, v / peak - 1); }
  return { start: v0, end: v1, ret: v1 / v0 - 1, cagr: Math.pow(v1 / v0, 1 / years) - 1, vol: sd * Math.sqrt(periodsPerYear), sharpe: sd > 0 ? mean / sd * Math.sqrt(periodsPerYear) : null, sortino: down > 0 ? mean / down * Math.sqrt(periodsPerYear) : null, maxDD: dd, longestDD: longest, years };
}
function monthlyReturns(curve) {
  const out = {}; let prev = curve[0]?.[1];
  const last = {}; for (const [d, v] of curve) last[d.slice(0, 7)] = v;
  for (const m of Object.keys(last).sort()) { out[m] = prev ? last[m] / prev - 1 : 0; prev = last[m]; }
  return out;
}
function summarize(r, periodsPerYear, capital) {
  const s = curveStats(r.curve, periodsPerYear), b = curveStats(r.bcurve, periodsPerYear);
  const wins = r.closed.filter(t => t.pnl > 0), losses = r.closed.filter(t => t.pnl <= 0), sum = a => a.reduce((t, x) => t + x, 0);
  return {
    stats: s, bench: b, trades: r.trades.length, round: r.closed.length, win: r.closed.length ? wins.length / r.closed.length : null,
    avgWin: wins.length ? sum(wins.map(t => t.ret)) / wins.length : null, avgLoss: losses.length ? sum(losses.map(t => t.ret)) / losses.length : null,
    pf: losses.length && sum(losses.map(t => t.pnl)) < 0 ? sum(wins.map(t => t.pnl)) / -sum(losses.map(t => t.pnl)) : null,
    avgDays: r.closed.length ? sum(r.closed.map(t => t.days)) / r.closed.length : null, fees: r.fees, spreadCost: r.spreadCost, exposure: r.exposure,
    turnover: capital ? sum(r.trades.map(t => t.qty * t.px)) / capital / Math.max(1, s?.years || 1) : null,
  };
}
const ppy = syms => syms.every(isCrypto) ? 365 : 252;
const downsamplePts = (pts, n = 260) => { if (pts.length <= n) return pts; const out = [], step = (pts.length - 1) / (n - 1); for (let i = 0; i < n; i++) out.push(pts[Math.round(i * step)]); return out; };
async function backtest(o) {
  const key = o.strategy, st = STRATEGIES[key]; if (!st) fail('Pick a strategy');
  const syms = [...new Set((o.syms || []).map(cleanSym).filter(Boolean))].slice(0, 40); if (!syms.length) fail('Pick at least one symbol');
  for (const s of syms) if (!assets.has(s)) addAsset(s, CRYPTO_DEFAULT.includes(s) || o.kinds?.[s] === 'crypto' ? 'crypto' : 'stock');
  const params = cleanParams(key, o.params), years = clamp(num(o.years, 3), 1, 8), capital = clamp(num(o.capital, 10000), 100, 1e8);
  const bench = benchFor(syms);
  const { data, missing } = await loadUniverse([...new Set([...syms, bench])]);
  const usable = syms.filter(s => data[s]); if (!usable.length) fail('No price history for those symbols right now');
  const lastDates = usable.map(s => data[s].d[data[s].d.length - 1]).sort(); const end = lastDates[lastDates.length - 1];
  const start = addIso(end, -Math.round(years * 365.25));
  const firstDates = usable.map(s => data[s].d[0]).sort();
  const args = { key, params, syms: usable, data, start, end, capital, profile: o.profile || 'robinhood', route: o.route || 'mm', maxPosPct: clamp(num(o.maxPosPct, 100), 5, 100), bench };
  const r = runBacktest(args), P = ppy(usable), sum = summarize(r, P, capital);
  const out = {
    strategy: key, name: st.name, params, syms: usable, missing, bench, start: r.curve[0]?.[0] || start, end, capital, years,
    shortHistory: usable.filter(s => data[s].d[0] > start).map(s => ({ sym: s, from: data[s].d[0] })),
    ...sum, curve: downsamplePts(r.curve), bcurve: r.bcurve ? downsamplePts(r.bcurve) : null, monthly: monthlyReturns(r.curve),
    recent: r.closed.slice(-40).reverse(), open: r.open, firstData: firstDates[0], benchSym: bench,
  };
  // Out-of-sample check: tune on the first 70% of the period, then test those settings on the last 30% they never saw.
  if (o.robust !== false && r.curve.length > 200 && Object.keys(st.grid).length) {
    const days = r.curve.map(x => x[0]), split = days[Math.floor(days.length * 0.7)];
    const combos = gridCombos(st.grid).slice(0, 18);
    let best = null;
    for (const g of combos) {
      const pp = { ...params, ...g }; if (key === 'trend' && pp.fast >= pp.slow) continue;
      const rr = runBacktest({ ...args, params: pp, end: split }), ss = curveStats(rr.curve, P);
      if (ss && (!best || (ss.sharpe ?? -9) > (best.s.sharpe ?? -9))) best = { g, s: ss };
    }
    if (best) {
      const oosBest = curveStats(runBacktest({ ...args, params: { ...params, ...best.g }, start: split }).curve, P);
      const oosYours = curveStats(runBacktest({ ...args, start: split }).curve, P);
      const oosBench = r.bcurve ? curveStats(r.bcurve.filter(x => x[0] >= split), P) : null;
      const isYours = curveStats(r.curve.filter(x => x[0] < split), P);
      const pass = oosYours && oosBench ? (oosYours.sharpe ?? 0) > 0.3 && (oosYours.ret > oosBench.ret || (oosYours.sharpe ?? 0) > (oosBench.sharpe ?? 0)) : false;
      out.robust = { split, tuned: best.g, inSample: { best: best.s, yours: isYours }, outSample: { best: oosBest, yours: oosYours, bench: oosBench }, pass,
        verdict: !oosYours ? 'Not enough data to check.' : pass ? 'Held up on data it was not tuned on: it beat or matched the benchmark on risk-adjusted terms in the last 30% of the period.' : (oosYours.ret > 0 ? 'Made money on unseen data but did not beat simply holding the benchmark. Treat it as a diversifier, not an edge.' : 'Lost money on the data it was not tuned on. Do not trust the full-period result.') };
    }
  }
  return out;
}
function gridCombos(grid) {
  const keys = Object.keys(grid); let out = [{}];
  for (const k of keys) { const next = []; for (const o of out) for (const v of grid[k]) next.push({ ...o, [k]: v }); out = next; }
  return out;
}

// =====================================================================
// Bots: strategies that trade a wallet by themselves, inside hard limits you set
// Each bot decides once a day (15 minutes before the stock market closes; crypto every day),
// checks the news and social mood before buying, keeps stop orders on its positions, and writes down why.
// =====================================================================
const MANDATE_DEF = {
  budget: 0, maxPosPct: 25, dailyLossPct: 5, maxDDPct: 20, maxTradesDay: 12, minTrade: 10, runAt: '15:45', stops: true, notify: 'trades',
  newsFilter: true, earningsDays: 2, minSentiment: -0.35, blockSevere: true, sellOnSevere: false, tilt: false, flattenOnKill: true,
};
function cleanMandate(m = {}, base = MANDATE_DEF) {
  const o = { ...base };
  const nums = { budget: [0, 1e9], maxPosPct: [5, 100], dailyLossPct: [0, 50], maxDDPct: [0, 90], maxTradesDay: [1, 100], minTrade: [1, 10000], earningsDays: [0, 10], minSentiment: [-1, 0] };
  for (const [k, [lo, hi]] of Object.entries(nums)) if (m[k] !== undefined) o[k] = clamp(num(m[k], base[k]), lo, hi);
  for (const k of ['stops', 'newsFilter', 'blockSevere', 'sellOnSevere', 'tilt', 'flattenOnKill']) if (m[k] !== undefined) o[k] = !!m[k];
  if (['trades', 'all', 'off'].includes(m.notify)) o.notify = m.notify;
  if (/^\d\d:\d\d$/.test(m.runAt || '') && m.runAt >= '09:45' && m.runAt <= '15:55') o.runAt = m.runAt;
  return o;
}
const botJson = b => ({ id: b.id, name: b.name, wid: b.wid, strategy: b.strategy, params: jsonOr(b.params, {}), syms: jsonOr(b.syms, []), mandate: { ...MANDATE_DEF, ...jsonOr(b.mandate, {}) }, status: b.status, created: tsMs(b.created), lastRun: tsMs(b.last_run), lastDay: b.last_day, peak: b.peak == null ? null : +b.peak, summary: b.summary, killed: b.killed_reason || null });
async function botLog(botId, kind, title, body = '', data = null, sym = null) {
  await Q('insert into bot_log(bot_id,kind,title,body,data,sym) values($1,$2,$3,$4,$5,$6)', [botId, kind, String(title).slice(0, 200), String(body || '').slice(0, 4000), data ? JSON.stringify(data) : null, sym]).catch(e => warn('bot log', e.message));
}
// Today's bars with the live price as today's close.
function withLive(b, sym) {
  const q = prices.get(sym), today = isCrypto(sym) ? new Date().toISOString().slice(0, 10) : etDate();
  const out = { sym, d: b.d.slice(), o: b.o.slice(), h: b.h.slice(), l: b.l.slice(), c: b.c.slice(), v: b.v.slice(), memo: {} };
  if (!q?.p) return { bars: out, live: false };
  const n = out.d.length - 1;
  if (out.d[n] === today) { out.c[n] = q.p; out.h[n] = Math.max(out.h[n], q.p); out.l[n] = Math.min(out.l[n], q.p); }
  else if (out.d[n] < today) { out.d.push(today); out.o.push(q.open || q.p); out.h.push(Math.max(q.hi || q.p, q.p)); out.l.push(Math.min(q.lo || q.p, q.p)); out.c.push(q.p); out.v.push(0); }
  return { bars: out, live: true };
}
const running = new Set();
async function runBot(id, { manual = false, dry = false } = {}) {
  if (running.has(id)) fail('This bot is already deciding. Give it a moment.');
  running.add(id);
  try { return await runBotInner(id, { manual, dry }); } finally { running.delete(id); if (!dry) broadcast({ t: 'evt', e: 'bot', id }); }
}
async function runBotInner(id, { manual, dry }) {
  const b = await row('select * from bots where id=$1', [id]); if (!b) fail('That bot is gone', 404);
  const bot = botJson(b), st = STRATEGIES[bot.strategy], m = bot.mandate, state = jsonOr(b.state, {});
  const w = await walletRow(bot.wid);
  if (w.archived) fail('The bot\'s wallet is archived');
  const real = isReal(w), prof = profOf(w), sess = sessionAt(prof);
  const pos = await positions(bot.wid), acct = await accountInfo(bot.wid, { w, pos });
  const equity = acct.equity, today = etDate(), mine = new Set(bot.syms);
  const capital = m.budget > 0 ? Math.min(m.budget, equity) : equity;
  const log = { rows: [], orders: [], skips: [], risk: [] };
  // ---- risk limits first ----
  let dayStart = +b.day_start || equity, peak = Math.max(+b.peak || equity, equity);
  if (b.day_date !== today) dayStart = equity;
  const dd = peak ? equity / peak - 1 : 0, dayRet = dayStart ? equity / dayStart - 1 : 0;
  let noBuys = false;
  if (m.maxDDPct > 0 && dd <= -m.maxDDPct / 100) {
    const msg = `The wallet is ${pct(dd * 100)} below its peak of ${money(peak)}, past the ${m.maxDDPct}% limit. The bot stopped itself${m.flattenOnKill ? ' and sold its positions' : ''}.`;
    if (!dry) {
      await Q("update bots set status='killed', killed_reason=$1, peak=$2 where id=$3", [msg, peak, id]);
      if (m.flattenOnKill) await flattenBot(bot, 'drawdown limit');
      await botLog(id, 'risk', 'Drawdown limit hit: bot stopped', msg);
      await notify('bot', `🛑 ${bot.name} stopped`, msg, { wid: bot.wid, url: '/#/bot/' + id });
    }
    return { killed: true, message: msg };
  }
  if (m.dailyLossPct > 0 && dayRet <= -m.dailyLossPct / 100) { noBuys = true; log.risk.push(`Down ${pct(dayRet * 100)} today, past the ${m.dailyLossPct}% daily limit: no new buys today.`); }
  const tradesToday = +(await row("select count(*) n from bot_log where bot_id=$1 and kind='trade' and ts > $2", [id, new Date(etToUtc(...today.split('-').map(Number), 0, 0))])).n;
  let tradeBudget = Math.max(0, m.maxTradesDay - tradesToday);
  // ---- data ----
  const syms = bot.syms.filter(s => isCrypto(s) || sess === 'regular' || manual);
  const bench = benchFor(bot.syms);
  const { data, missing } = await loadUniverse([...new Set([...bot.syms, bench])]);
  const series = {}, idx = {};
  for (const s of Object.keys(data)) { const r = withLive(data[s], s); series[s] = r.bars; idx[s] = r.bars.d.length - 1; }
  const need = st.need(bot.params);
  const tradable = syms.filter(s => series[s] && idx[s] >= need && priceOf(s));
  const stoppedToday = new Set((await rows("select distinct sym from trd where wid=$1 and side='sell' and otype in ('stop','trail') and ts > $2", [bot.wid, new Date(etToUtc(...today.split('-').map(Number), 0, 0))])).map(x => x.sym));
  const held = {};
  for (const p of pos) if (mine.has(p.sym)) {
    const s0 = state[p.sym] || {}; const c = priceOf(p.sym) || p.avg;
    const entryDate = s0.entryDate || etDate(p.since), ser = series[p.sym];
    const days = ser ? ser.d.filter(d => d > entryDate).length : (s0.days || 0);
    held[p.sym] = { qty: p.qty, entry: p.avg, entryDate, high: Math.max(s0.high || 0, c), days, weight: s0.weight, stopPx: s0.stopPx };
  }
  const month = today.slice(0, 7), rebalanceDue = st.rebalance === 'none' || !state._lastRebalance || (st.rebalance === 'monthly' ? state._lastRebalance.slice(0, 7) !== month : true) || manual;
  const ctx = { syms: tradable, data: series, idx, params: bot.params, held, bench, maxW: m.maxPosPct / 100 };
  const dec = rebalanceDue ? st.decide(ctx) : { weights: Object.fromEntries(Object.entries(held).map(([s, h]) => [s, h.weight || (h.qty * (priceOf(s) || h.entry)) / capital])), stops: {}, notes: Object.fromEntries(Object.keys(held).map(s => [s, 'Holding until the next monthly rebalance.'])) };
  if (!rebalanceDue) log.risk.push(`${st.name} rebalances monthly; next on the first trading day of next month. Holding as is.`);
  // ---- news and social check before buying ----
  const buzz = {};
  const wantBuy = Object.entries(dec.weights).filter(([s, wt]) => wt > 0 && tradable.includes(s));
  if (m.newsFilter && S.f_buzz !== false) {
    const list = [...new Set([...wantBuy.map(([s]) => s), ...Object.keys(held), ...tradable])].slice(0, 24);
    for (let i = 0; i < list.length; i += 4) await Promise.all(list.slice(i, i + 4).map(s => Promise.race([buzzFor(s), sleep(15000).then(() => null)]).then(x => { if (x) buzz[s] = x; }).catch(() => {})));
  }
  const decisions = {};
  for (const s of new Set([...tradable, ...Object.keys(held)])) {
    const want = dec.weights[s] > 0 ? dec.weights[s] : 0, px = priceOf(s), have = (held[s]?.qty || 0) * (px || 0), bz = buzz[s];
    const d = { sym: s, note: dec.notes[s] || (tradable.includes(s) ? '' : isCrypto(s) ? 'No fresh price yet.' : 'Stock market is closed; stocks are only traded in regular hours.'), want, have, buzz: bz ? buzzLine(bz) : null, flags: bz?.flags || [], action: 'hold', why: [] };
    let wt = want;
    if (bz && m.newsFilter && wt * capital > have + 1) {
      const up = bz.upcoming.find(u => u.k === 'earnings');
      if (m.earningsDays > 0 && up && up.tradingDays <= m.earningsDays) { wt = have / capital; d.why.push(`Skipped buying: earnings ${up.tradingDays <= 0 ? 'today' : `in ${up.tradingDays} trading day${up.tradingDays === 1 ? '' : 's'}`}. Prices can jump either way on the report.`); }
      else if (m.blockSevere && bz.flags.includes('severe_news')) { wt = have / capital; d.why.push(`Skipped buying: serious news (${bz.events.filter(e => e.k === 'severe').map(e => '"' + e.title.slice(0, 90) + '"').join(', ')}).`); }
      else if (bz.count >= 5 && bz.sentiment <= m.minSentiment) { wt = have / capital; d.why.push(`Skipped buying: the mood is negative (${bz.sentiment.toFixed(2)} across ${bz.count} items).`); }
      else if (m.tilt && bz.count >= 5) { const k = clamp(0.75 + bz.sentiment * 0.5, 0.5, 1); if (k < 0.99) { wt = Math.max(have / capital, wt * k); d.why.push(`Sized down to ${Math.round(k * 100)}% because of the news mood.`); } }
    }
    if (bz && held[s] && m.sellOnSevere && bz.flags.includes('severe_news')) { wt = 0; d.why.push('Selling: serious negative news and "sell on bad news" is on.'); }
    if (stoppedToday.has(s) && !held[s] && wt > 0) { wt = 0; d.why.push('Not buying back today: its stop loss sold it earlier today.'); }
    if (noBuys && wt * capital > have) { wt = have / capital; d.why.push('No new buys today: the daily loss limit was hit.'); }
    d.target = wt; decisions[s] = d;
  }
  // ---- orders: sells first to free cash, then buys ----
  const placeOrder = async (s, side, o, label) => {
    if (dry) { log.orders.push({ sym: s, side, ...o, dry: true, label }); return { dry: true }; }
    if (tradeBudget <= 0) { log.skips.push(`${s}: daily trade limit (${m.maxTradesDay}) reached.`); return null; }
    try {
      let r;
      if (!isCrypto(s) && sessionAt(prof) !== 'regular' && real) { const q = await addLimit(bot.wid, s, { side, kind: 'moo', amount: o.usd ?? o.qty ?? o.pct, units: o.usd ? 'usd' : o.qty ? 'qty' : 'pct', why: 'bot: ' + bot.name }); r = { queued: true, id: q.id }; }
      else r = await execTrade(bot.wid, s, side, { ...o, why: 'bot: ' + bot.name.slice(0, 40), auto: true, bot: true, note: label });
      tradeBudget--;
      const text = r.queued ? `${side === 'buy' ? 'Buy' : 'Sell'} queued for the open` : `${side === 'buy' ? 'Bought' : 'Sold'} ${+r.qty.toFixed(6)} ${s} at ${pxStr(r.price)}${r.pnl != null ? ` (${r.pnl >= 0 ? '+' : ''}${money(r.pnl)})` : ''}`;
      log.orders.push({ sym: s, side, text, label, fill: r.queued ? null : { qty: r.qty, price: r.price, fee: r.fee, pnl: r.pnl } });
      await botLog(id, 'trade', text, label, { side, fill: r.queued ? null : { qty: r.qty, price: r.price, fee: r.fee, pnl: r.pnl, id: r.id } }, s);
      return r;
    } catch (e) { log.skips.push(`${s}: ${e.message}`); await botLog(id, 'error', `Couldn't ${side} ${s}`, e.message, null, s); return null; }
  };
  for (const d of Object.values(decisions)) {
    const px = priceOf(d.sym); if (!px || !held[d.sym]) continue;
    if (!(isCrypto(d.sym) || tradable.includes(d.sym))) continue;
    const targetV = d.target * capital, diff = targetV - d.have;
    if (d.target <= 0) { d.action = 'sell'; await placeOrder(d.sym, 'sell', { pct: 100 }, `${st.short}: ${d.why[0] || d.note}`); delete state[d.sym]; }
    else if (rebalanceDue && st.rebalance !== 'none' && -diff > Math.max(m.minTrade, capital * 0.01)) { d.action = 'trim'; await placeOrder(d.sym, 'sell', { qty: Math.min(held[d.sym].qty, -diff / px) }, `${st.short}: rebalance down to ${pct0n(d.target)} of the wallet`); }
  }
  for (const d of Object.values(decisions).sort((a, b) => b.target - a.target)) {
    if (!(d.target > 0) || !tradable.includes(d.sym)) continue;
    const px = priceOf(d.sym), targetV = d.target * capital, diff = targetV - d.have;
    const isNew = !held[d.sym];
    if (!isNew && !(rebalanceDue && st.rebalance !== 'none')) continue;
    if (diff <= Math.max(m.minTrade, capital * 0.01)) continue;
    const a2 = dry ? acct : await accountInfo(bot.wid);
    const bp = isCrypto(d.sym) || !marginable(d.sym, px) ? a2.bpCrypto : a2.bp;
    const usd = Math.floor(Math.min(diff, bp * 0.995) * 100) / 100;
    if (usd < m.minTrade) { d.why.push(`Not enough buying power for this (${money(bp)} left).`); continue; }
    d.action = isNew ? 'buy' : 'add';
    await placeOrder(d.sym, 'buy', { usd }, `${st.short}: ${d.note}${d.why.length ? ' ' + d.why.join(' ') : ''}`.slice(0, 480));
  }
  // ---- protective stops ----
  if (!dry && m.stops) {
    const pnow = await positions(bot.wid);
    for (const p of pnow) {
      if (!mine.has(p.sym)) continue;
      const sp = dec.stops[p.sym] || (state[p.sym]?.stopPx ? { price: state[p.sym].stopPx, trail: false } : null); if (!sp || !(sp.price > 0)) continue;
      const prevStop = state[p.sym]?.stopPx, stopPx = sp.trail && prevStop ? Math.max(prevStop, sp.price) : sp.price, cur = priceOf(p.sym);
      if (!(stopPx < cur * 0.995)) continue;
      const existing = (condOrders.get(p.sym) || []).find(o => o.wid === bot.wid && (o.kind === 'stop' || o.kind === 'trail'));
      if (existing && Math.abs(existing.price / stopPx - 1) < 0.005) { state[p.sym] = { ...(state[p.sym] || {}), stopPx }; continue; }
      try { await addCondOrder(bot.wid, p.sym, 'stop', { price: stopPx, pct: 100 }); state[p.sym] = { ...(state[p.sym] || {}), stopPx }; } catch (e) { log.skips.push(`${p.sym} stop: ${e.message}`); }
    }
  }
  // ---- remember what it holds and why ----
  if (!dry) {
    const pnow = await positions(bot.wid);
    for (const p of pnow) if (mine.has(p.sym)) {
      const s0 = state[p.sym] || {}, d = decisions[p.sym];
      state[p.sym] = { ...s0, entryDate: s0.entryDate || today, high: Math.max(s0.high || 0, priceOf(p.sym) || 0), days: held[p.sym] ? held[p.sym].days : 0, lastDay: today, weight: d?.target || s0.weight };
    }
    for (const s of Object.keys(state)) if (!s.startsWith('_') && !pnow.some(p => p.sym === s)) delete state[s];
    if (rebalanceDue && st.rebalance !== 'none') state._lastRebalance = today;
    const bought = log.orders.filter(o => o.side === 'buy').map(o => o.sym), sold = log.orders.filter(o => o.side === 'sell').map(o => o.sym);
    const summary = log.orders.length ? [bought.length ? 'Bought ' + bought.join(', ') : '', sold.length ? 'sold ' + sold.join(', ') : ''].filter(Boolean).join('; ') : 'No changes';
    await Q('update bots set state=$1, last_run=now(), last_day=$2, peak=$3, day_start=$4, day_date=$5, summary=$6 where id=$7', [JSON.stringify(state), today, peak, dayStart, today, summary, id]);
    const rows = Object.values(decisions).map(d => ({ sym: d.sym, action: d.action, want: round(d.want, 4), target: round(d.target, 4), have: round(d.have, 2), note: d.note, why: d.why, buzz: d.buzz, flags: d.flags }));
    await botLog(id, 'decision', `${manual ? 'Manual run' : 'Daily decision'}: ${summary}`, [...log.risk, ...log.skips].join('\n'), { rows, equity, capital, dd, dayRet, regime: ctx.regime || null, missing });
    if (m.notify === 'all' || (m.notify === 'trades' && log.orders.length)) await notify('bot', `🤖 ${bot.name}: ${summary}`, log.orders.map(o => `${o.text}. ${o.label || ''}`.trim()).join('\n').slice(0, 600) || [...log.risk, ...log.skips].join(' ') || 'Nothing to do today.', { wid: bot.wid, url: '/#/bot/' + id });
  }
  return { dry, equity, capital, dd, dayRet, regime: ctx.regime || null, rows: Object.values(decisions), orders: log.orders, risk: log.risk, skips: log.skips, missing, session: sess };
}
const pct0n = v => (v * 100).toFixed(0) + '%';
async function flattenBot(bot, why) {
  const out = [];
  for (const p of await positions(bot.wid)) {
    if (!bot.syms.includes(p.sym)) continue;
    try {
      if (!isCrypto(p.sym) && sessionAt(profOf(walletMeta.get(bot.wid))) !== 'regular') { await addLimit(bot.wid, p.sym, { side: 'sell', kind: 'moo', amount: 100, units: 'pct', why: 'bot: ' + why }); out.push(`${p.sym} (queued for the open)`); }
      else { const f = await execTrade(bot.wid, p.sym, 'sell', { pct: 100, why: 'bot: ' + why, auto: true, force: true }); out.push(`${+f.qty.toFixed(6)} ${p.sym}`); }
    } catch (e) { out.push(`${p.sym} failed: ${e.message}`); }
  }
  await Q("delete from ord where wid=$1 and sym = any($2::text[])", [bot.wid, bot.syms]).catch(() => {}); await loadOrders();
  await botLog(bot.id, 'trade', 'Sold everything', `${why}: ${out.join(', ') || 'nothing to sell'}`);
  return out;
}
// Every minute: run bots whose time has come.
async function botScheduler() {
  const p = etParts(), hm = String(p.h).padStart(2, '0') + ':' + String(p.mi).padStart(2, '0'), today = etDate();
  for (const b of await rows("select * from bots where status='running'")) {
    const m = { ...MANDATE_DEF, ...jsonOr(b.mandate, {}) };
    const syms = jsonOr(b.syms, []), trading = isTradingDay(p.y, p.m, p.d), early = trading && earlyClose(p.y, p.m, p.d);
    const runAt = early && m.runAt > '12:45' ? '12:45' : m.runAt; // half days close at 1pm
    if (b.last_day === today || hm < runAt || running.has(b.id)) continue;
    if (!syms.some(isCrypto) && market().state !== 'open') continue; // stock-only bots trade in regular hours; a missed day is skipped
    runBot(b.id).catch(async e => { warn('bot', b.id, e.message); await botLog(b.id, 'error', 'The daily decision failed', e.message); await Q('update bots set last_day=$1 where id=$2', [today, b.id]).catch(() => {}); });
  }
}

// Stop losses and queued orders fill on their own; put them in the bot's journal too.
fillCbs.push(async f => {
  const list = (await rows("select * from bots where wid=$1", [f.wid])).map(botJson).filter(b => b.syms.includes(f.sym)); if (!list.length) return;
  const t = await row('select why, otype from trd where id=$1', [f.id]); if (!t) return;
  const bot = list[0], viaBot = String(t.why || '').startsWith('bot:');
  if (viaBot && t.otype !== 'market') return;
  const label = t.otype === 'stop' ? 'Stop loss' : t.otype === 'trail' ? 'Trailing stop' : viaBot ? 'Queued order' : (t.why || 'manual');
  if (viaBot && running.has(bot.id)) return; // logged by the run itself
  const text = `${label}: ${f.side === 'buy' ? 'bought' : 'sold'} ${+f.qty.toFixed(6)} ${f.sym} at ${pxStr(f.price)}${f.pnl != null ? ` (${f.pnl >= 0 ? '+' : ''}${money(f.pnl)})` : ''}`;
  await botLog(bot.id, 'trade', text, viaBot ? 'Filled when the market opened.' : t.otype === 'stop' || t.otype === 'trail' ? 'The protective stop was hit.' : 'Traded outside the bot.', { side: f.side, fill: { qty: f.qty, price: f.price, fee: f.fee, pnl: f.pnl, id: f.id }, outside: !viaBot }, f.sym);
  if ((t.otype === 'stop' || t.otype === 'trail') && bot.mandate.notify !== 'off') await notify('bot', `🤖 ${bot.name}: ${label.toLowerCase()} hit on ${f.sym}`, text, { wid: f.wid, sym: f.sym, url: '/#/bot/' + bot.id });
});

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
const jsonOr = (s, d = null) => { if (!s) return d; try { return JSON.parse(s); } catch (e) { return d; } };
const tradeJson = t => ({ id: t.id, ts: tsMs(t.ts), sym: t.sym, side: t.side, qty: +t.qty, price: +t.price, why: t.why, pnl: t.pnl == null ? null : +t.pnl, fee: +t.fee || 0, note: t.note, tags: t.tags, wid: t.wid,
  fees: jsonOr(t.fees), bid: t.bid == null ? null : +t.bid, ask: t.ask == null ? null : +t.ask, otype: t.otype || null, tif: t.tif || null, sess: t.sess || null, settle: tsMs(t.settle), liq: t.liq || null, via: t.via || null, broker: !!t.ext_id });
function rulesJson(w) {
  const prof = profOf(w), sess = sessionAt(prof);
  return { profile: prof.key, name: prof.name, session: sess, sessionLabel: SESSION_LABEL[sess], pre: prof.pre, post: prof.post, overnight: prof.overnight, gtcDays: prof.gtcDays, minUsd: prof.minUsd,
    instant: w.acct === 'cash' ? 0 : prof.instant[w.gold ? 'gold' : 'std'], achDays: prof.achDays, settleNext: settleTime(prof), real: isReal(w) };
}
const brokerJson = () => BROKER ? { on: true, env: BROKER.env, feed: BROKER.feed, ok: BROKER.ok, err: BROKER.err, dataOk: BROKER.dataOk, wid: BROKER.wid, lastSync: BROKER.lastSync } : { on: false };
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
  const w = wallets.find(x => x.id === wid), wr = walletMeta.get(wid) || await walletRow(wid);
  const ord = [...condOrders.values()].flat().filter(o => o.wid === wid).map(orderJson);
  const lims = wr.broker ? (BROKER?.wid === wid ? BROKER.orders : []) : [...limOrders.values()].flat().filter(l => l.wid === wid);
  const account = await accountInfo(wid, { pos }).catch(() => null);
  const xfers = (await rows("select * from xfer where wid=$1 and (status='pending' or done > now() - interval '3 days') order by id desc limit 10", [wid])).map(xferJson);
  const syms = new Set([...STOCK_DEFAULT, ...INDEXES, ...CRYPTO_DEFAULT, ...assets.keys()]);
  const sp = {}, mine = [...pos.map(p => p.sym), ...watch.map(x => x.sym), ...userSyms.extra];
  for (const s of new Set([...mine, ...syms])) { const a = sparkFor(s); if (a) sp[s] = a; else wantSpark(s); }
  for (const s of new Set(mine)) ensureMeta(s).catch(() => {});
  const eq = await equitySeries(wid, '1D').catch(() => null);
  const out = {
    v: BUILD, now: Date.now(), market: market(), user: APP_USER,
    wallet: w, wallets, pos, ord, lims, account, xfers, rules: rulesJson(wr), broker: brokerJson(), trd: trd.map(tradeJson), watch: watch.map(x => x.sym), alerts: alerts.map(alertJson),
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
GET('/api/quote/:sym', auth, async req => {
  const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol');
  const w = walletMeta.get(await activeWalletId(req)), prof = profOf(w), sess = isCrypto(sym) ? 'regular' : sessionAt(prof);
  focus.set(sym, Date.now());
  return { quote: quoteOf(sym, sess), session: sess, label: SESSION_LABEL[sess], real: isReal(w) };
});
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
const OTYPES = ['market', 'limit', 'stop', 'stoplimit', 'trail'];
function orderInput(b) {
  const side = b.side === 'sell' ? 'sell' : 'buy', otype = OTYPES.includes(b.otype) ? b.otype : 'market';
  const mode = b.mode || (b.usd ? 'usd' : b.pct ? 'pct' : 'qty'), amount = num(b.amount ?? b.usd ?? b.pct ?? b.qty);
  const o = { note: b.note, why: 'manual', otype, tif: otype === 'market' ? 'day' : (b.tif === 'day' ? 'day' : 'gtc') };
  if (mode === 'usd') o.usd = amount; else if (mode === 'pct') o.pct = amount; else o.qty = amount;
  return { side, otype, mode, amount, o, units: mode === 'qty' ? 'qty' : mode === 'pct' ? 'pct' : 'usd' };
}
// Live estimate for the trade screen: price, quantity, every fee, buying power after, settlement.
POST('/api/preview', auth, async req => {
  const wid = await activeWalletId(req), b = req.body, sym = cleanSym(b.sym); if (!sym) fail('Pick something');
  const w = await walletRow(wid), pos = await positions(wid), acct = await accountInfo(wid, { w, pos });
  const { side, otype, mode, amount, o } = orderInput(b);
  const crypto = isCrypto(sym), prof = profOf(w), sess = crypto ? 'regular' : sessionAt(prof), real = isReal(w);
  const out = { sym, side, otype, session: sess, sessionLabel: SESSION_LABEL[sess], real, profile: prof.key, acct: w.acct, broker: !!w.broker, bp: crypto || !marginable(sym, priceOf(sym) || 0) ? acct.bpCrypto : acct.bp, account: acct, warn: [] };
  out.quote = quoteOf(sym, sess);
  const p = pos.find(x => x.sym === sym);
  out.held = acct.holdsShares[sym] || 0; out.owned = p ? p.qty : 0;
  if (!(amount > 0)) return out;
  // Orders that wait: price them at their limit or stop, not at the market.
  const plan = { ...o, auto: false };
  if (otype === 'limit') { plan.px = num(b.price) || null; plan.liq = out.quote && (side === 'buy' ? out.quote.ask <= num(b.price) : out.quote.bid >= num(b.price)) ? 'taker' : 'maker'; }
  if (otype === 'stop') plan.px = null;
  if (otype === 'stoplimit') { plan.px = num(b.lpx) || null; plan.liq = 'maker'; }
  if (otype === 'limit' && plan.px && out.quote) plan.px = side === 'buy' ? Math.min(plan.px, out.quote.ask) : Math.max(plan.px, out.quote.bid);
  if (otype === 'stop' && num(b.price) > 0 && out.quote) { const q = out.quote.ask - out.quote.bid; plan.px = side === 'buy' ? num(b.price) + q / 2 : Math.max(0.0001, num(b.price) - q / 2); }
  if (!real || w.broker) { if (!plan.px && !w.broker) plan.px = (priceOf(sym) || 0) * (side === 'buy' ? 1 + S.slippage / 100 : 1 - S.slippage / 100); }
  try {
    if (!real) {
      if (side === 'sell' && !p) throw new Error(`You don't own any ${sym} in this wallet`);
      const px = plan.px, qty = side === 'sell' && o.pct ? p.qty * Math.min(100, o.pct) / 100 : o.qty || (o.usd || 0) / px;
      const notional = qty * px, fee = notional > 0 ? round(S.feeFlat + notional * S.feePct / 100, 6) : 0;
      Object.assign(out, { px, qty, notional, fees: { lines: fee ? [{ k: 'custom', label: 'Fee', amt: fee, note: 'From your settings' }] : [], total: fee }, total: side === 'buy' ? notional + fee : notional - fee, cashAfter: side === 'buy' ? acct.cash - notional - fee : acct.cash + notional - fee, pnl: side === 'sell' && p ? (px - p.avg) * qty - fee : null });
      if (side === 'buy' && notional + fee > acct.cash + 0.005) out.err = `That's more than your ${money(acct.cash)} of cash`;
      if (side === 'sell' && p && qty > p.qty + 1e-9) out.err = `You only have ${+p.qty.toFixed(6)} ${crypto ? sym : 'shares'}`;
    } else {
      const pl = await planOrder(pool, w, pos, w.broker ? { ...acct, holdsShares: {} } : acct, sym, side, plan);
      Object.assign(out, { px: pl.px, qty: pl.qty, notional: pl.notional, fees: pl.fees, total: side === 'buy' ? pl.notional + pl.fees.total : pl.notional - pl.fees.total, cashAfter: pl.cashAfter, pnl: pl.pnl, settle: pl.settle, via: pl.via, deep: pl.deep, borrow: pl.borrow, liq: pl.liq, whole: !crypto && sess === 'overnight' });
      if (side === 'sell' && p && !crypto) {
        const lotsL = await rows(`select * from lots where wid=$1 and sym=$2 and qty>0 order by ${{ lifo: 'ts desc, id desc', hifo: 'basis desc, ts' }[w.lot_method] || 'ts, id'}`, [wid, sym]);
        let left = pl.qty, st = 0, lt = 0, washRisk = false; const net = pl.notional - pl.fees.total;
        for (const l of lotsL) { if (left <= 1e-12) break; const t = Math.min(+l.qty, left); const g = net * t / pl.qty - +l.basis * t; if (isLongTerm(l.ts, new Date())) lt += g; else st += g; left -= t; }
        if (st + lt < 0) washRisk = !!(await row("select 1 from lots where wid=$1 and sym=$2 and ts > now() - interval '30 days' limit 1", [wid, sym]));
        out.tax = { short: st, long: lt, washRisk };
      }
    }
  } catch (e) { out.err = e.message; }
  if (real && !crypto && otype === 'market' && sess !== 'regular') { out.queued = true; out.opensAt = market().opensAt; out.warn.push(`Market orders wait for the regular session. This will be placed at the open (${new Date(market().opensAt).toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: '2-digit' })} ET) at whatever the price is then.`); }
  if (real && !crypto && (otype === 'stop' || otype === 'stoplimit' || otype === 'trail') && sess !== 'regular') out.warn.push('Stop orders only trigger during regular hours (9:30 AM to 4:00 PM ET).');
  if (real && !crypto && out.quote && !out.quote.live) out.warn.push('Bid and ask are estimated from the last trade. Connect Alpaca in Settings for live stock quotes.');
  if (out.deep) out.warn.push('This order is bigger than the visible order book, so the price is an estimate.');
  if (w.broker && BROKER?.env === 'live') out.warn.unshift('This is a real-money order on your live Alpaca account.');
  return out;
});
POST('/api/trade', auth, async req => {
  const wid = await activeWalletId(req), b = req.body, w = await walletRow(wid);
  const sym = cleanSym(b.sym); if (!sym) fail('Pick something to trade');
  if (!assets.has(sym)) await trackAsset(sym, b.kind);
  if (w.broker) return brokerTrade(w, b);
  const { side, otype, mode, amount, o, units } = orderInput(b);
  if (!(amount > 0)) fail('Enter an amount');
  const real = isReal(w);
  if (otype === 'trail') {
    if (side !== 'sell') fail('Trailing stops sell shares you already own');
    const p = (await positions(wid)).find(x => x.sym === sym); if (!p) fail(`You don't own any ${sym}`);
    const pctv = mode === 'pct' ? amount : mode === 'qty' ? amount / p.qty * 100 : amount / (p.qty * priceOf(sym)) * 100;
    const id = await addCondOrder(wid, sym, 'trail', { trail: num(b.trail), pct: Math.min(100, pctv) }); await refreshUserSyms();
    return { order: { id, kind: 'trail' } };
  }
  if (otype !== 'market') {
    const r = await addLimit(wid, sym, { side, kind: otype, price: b.price, lpx: b.lpx, amount, units, sl: b.sl, tp: b.tp, tif: b.tif, sess: b.sess });
    return r.fill ? { fill: r.fill, bracket: r.fill.bracket || [], order: { id: r.id } } : { order: { id: r.id, kind: otype } };
  }
  if (!isCrypto(sym)) {
    if (real) {
      if (sessionAt(profOf(w)) !== 'regular') { const r = await addLimit(wid, sym, { side, kind: 'moo', amount, units, sl: b.sl, tp: b.tp }); return { queued: true, id: r.id, opensAt: market().opensAt }; }
    } else if (market().state !== 'open') {
      if (S.closedTrading === 'block') fail(`The stock market is closed. It opens ${new Date(market().opensAt).toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: '2-digit' })} New York time.`);
      if (S.closedTrading === 'queue') { const r = await addLimit(wid, sym, { side, kind: 'moo', amount, units, sl: b.sl, tp: b.tp }); return { queued: true, id: r.id }; }
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
  const wid = await activeWalletId(req), b = req.body, sym = cleanSym(b.sym), w = await walletRow(wid);
  if (w.broker) return brokerProtect(w, b);
  const id = await addCondOrder(wid, sym, b.kind, { price: num(b.price), pct: b.pct, trail: b.trail });
  await refreshUserSyms(); return { id };
});
PATCH('/api/order/:id', auth, async req => {
  if (String(req.params.id).startsWith('a:')) return brokerReplace(req.params.id, req.body);
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
DEL('/api/order/:id', auth, async req => { if (String(req.params.id).startsWith('a:')) return brokerCancel(req.params.id); await Q('delete from ord where id=$1', [+req.params.id]); await loadOrders(); await refreshUserSyms(); });
POST('/api/limit', auth, async req => {
  const wid = await activeWalletId(req), sym = cleanSym(req.body.sym); if (!sym) fail('Pick something');
  if (!assets.has(sym)) await trackAsset(sym, req.body.kind_asset);
  const w = await walletRow(wid);
  if (w.broker) return brokerTrade(w, { ...req.body, otype: req.body.kind, mode: req.body.units === 'qty' ? 'qty' : req.body.units === 'pct' ? 'pct' : 'usd', tif: req.body.expires === 'day' ? 'day' : 'gtc' });
  return addLimit(wid, sym, req.body);
});
DEL('/api/limit/:id', auth, async req => { if (String(req.params.id).startsWith('a:')) return brokerCancel(req.params.id); await Q('delete from lim where id=$1', [+req.params.id]); await loadOrders(); await refreshUserSyms(); });
GET('/api/orders', auth, async req => {
  const wid = req.query.all ? null : await activeWalletId(req);
  const ord = [...condOrders.values()].flat().filter(o => !wid || o.wid === wid).map(orderJson);
  const lims = [...limOrders.values()].flat().filter(l => !wid || l.wid === wid).concat(BROKER && (!wid || BROKER.wid === wid) ? BROKER.orders : []);
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
GET('/api/trade/:id', auth, async req => {
  const t = await row('select * from trd where id=$1', [+req.params.id]); if (!t) fail('That trade is gone', 404);
  const lots = (await rows('select * from realized where trd_id=$1 order by id', [t.id])).map(r => ({ qty: +r.qty, proceeds: +r.proceeds, basis: +r.basis, wash: +r.wash, gain: +r.proceeds - +r.basis + +r.wash, acquired: tsMs(r.acquired), term: r.term }));
  return { trade: tradeJson(t), lots };
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
const COLORS = ['mango', 'leaf', 'sky', 'grape', 'rose', 'slate'], PROFILE_KEYS = ['robinhood', 'alpaca', 'practice'], ACCTS = ['cash', 'instant', 'margin'], LOTS = ['fifo', 'lifo', 'hifo'];
GET('/api/wallets', auth, () => walletsSummary());
POST('/api/wallets', auth, async req => {
  const b = req.body, name = String(b.name || '').trim().slice(0, 30) || 'New wallet';
  const start = clamp(num(b.start, 10000), 100, 1e9);
  if (+(await row('select count(*) n from wallets')).n >= 25) fail('You can have up to 25 wallets');
  const r = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,gold,drip,lot_method,crypto_route,margin_max) values($1,$2,$3,$4,$4,(select coalesce(max(sort),0)+1 from wallets),$5,$6,$7,$8,$9,$10,$11) returning *`,
    [name, String(b.emoji || '💼').slice(0, 8), COLORS.includes(b.color) ? b.color : 'sky', start, PROFILE_KEYS.includes(b.profile) ? b.profile : 'robinhood', ACCTS.includes(b.acct) ? b.acct : 'instant', !!b.gold, !!b.drip, LOTS.includes(b.lotMethod) ? b.lotMethod : 'fifo', b.cryptoRoute === 'exchange' ? 'exchange' : 'mm', num(b.marginMax) > 0 ? num(b.marginMax) : null]);
  await loadWalletMeta(); await snapshot(r.id); checkAchievementsSoon();
  return walletJson(r);
});
PATCH('/api/wallets/:id', auth, async req => {
  const w = await walletRow(+req.params.id), b = req.body;
  if (b.archived === true) { const n = +(await row('select count(*) n from wallets where not archived and id<>$1', [w.id])).n; if (!n) fail("You can't archive your only wallet"); }
  await Q('update wallets set name=$1, emoji=$2, color=$3, archived=$4, sort=$5 where id=$6', [
    b.name !== undefined ? (String(b.name).trim().slice(0, 30) || w.name) : w.name, b.emoji !== undefined ? String(b.emoji).slice(0, 8) : w.emoji,
    COLORS.includes(b.color) ? b.color : w.color, b.archived !== undefined ? !!b.archived : w.archived, b.sort !== undefined ? num(b.sort) : w.sort, w.id]);
  if (b.archived === true && S.defaultWallet === w.id) await saveSettings({ defaultWallet: (await row('select id from wallets where not archived order by sort, id limit 1')).id });
  // Trading rules for the wallet
  if (!w.broker) {
    const acct = ACCTS.includes(b.acct) ? b.acct : w.acct;
    if (acct !== 'margin' && w.acct === 'margin' && +w.cash < -0.005) fail(`Pay off the ${money(-+w.cash)} margin balance before switching off margin`);
    await Q('update wallets set profile=$1, acct=$2, gold=$3, drip=$4, lot_method=$5, crypto_route=$6, margin_max=$7 where id=$8', [
      PROFILE_KEYS.includes(b.profile) ? b.profile : w.profile, acct, b.gold !== undefined ? !!b.gold : w.gold, b.drip !== undefined ? !!b.drip : w.drip,
      LOTS.includes(b.lotMethod) ? b.lotMethod : w.lot_method, ['mm', 'exchange'].includes(b.cryptoRoute) ? b.cryptoRoute : w.crypto_route,
      b.marginMax === undefined ? w.margin_max : (num(b.marginMax) > 0 ? num(b.marginMax) : null), w.id]);
  } else if (b.drip !== undefined) await Q('update wallets set drip=$1 where id=$2', [!!b.drip, w.id]);
  await loadWalletMeta(); await loadOrders();
  await refreshUserSyms(); return walletJson(await walletRow(w.id));
});
async function resetWallet(wid, start) {
  const w = await walletRow(wid);
  if (w.broker) fail("A broker wallet mirrors your real account, so it can't be reset here.");
  const pos = await positions(wid), v = +w.cash + marketValue(pos);
  const tr = await row("select count(*) n, count(*) filter (where side='sell') closed, count(*) filter (where side='sell' and pnl>0) wins from trd where wid=$1 and side in ('buy','sell')", [wid]);
  await tx(async c => {
    if (+tr.n > 0) await c.query('insert into seasons(wid,started,start_bal,final_bal,trades,wins,closed,name) values($1,$2,$3,$4,$5,$6,$7,$8)', [wid, w.season_start, w.start_bal, v, +tr.n, +tr.wins, +tr.closed, `${w.name} · ${new Date(tsMs(w.season_start)).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`]);
    for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid', 'lots', 'realized', 'settle', 'xfer', 'splits']) await c.query(`delete from ${t} where wid=$1`, [wid]);
    await c.query('update wallets set cash=$1, start_bal=$1, season_start=now(), margin_accrued=0, call_amt=0, call_since=null where id=$2', [start, wid]);
  });
  lastSnap.delete(wid); await loadOrders(); await snapshot(wid); await refreshUserSyms();
}
POST('/api/wallets/:id/reset', auth, async req => { const w = await walletRow(+req.params.id); await resetWallet(w.id, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
POST('/api/reset', auth, async req => { const wid = await activeWalletId(req); const w = await walletRow(wid); await resetWallet(wid, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
DEL('/api/wallets/:id', auth, async req => {
  const w = await walletRow(+req.params.id);
  if (+(await row('select count(*) n from wallets where id<>$1', [w.id])).n === 0) fail("You can't delete your only wallet");
  await tx(async c => { for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid', 'seasons', 'lots', 'realized', 'settle', 'xfer', 'splits']) await c.query(`delete from ${t} where wid=$1`, [w.id]); await c.query('delete from wallets where id=$1', [w.id]); });
  if (w.broker) await Q("delete from kv where k='alpaca_after'");
  if (S.defaultWallet === w.id) await saveSettings({ defaultWallet: (await row('select id from wallets order by archived, sort, id limit 1')).id });
  await loadOrders(); await refreshUserSyms();
});
// Money in and out. Deposits don't count as profit: the starting balance moves with them.
async function moveCash(c, wid, delta, why) {
  const w = (await c.query('select * from wallets where id=$1 for update', [wid])).rows[0]; if (!w) fail('That wallet does not exist', 404);
  const next = +w.cash + delta; if (delta < 0 && next < -0.005) fail(`${w.name} only has ${money(+w.cash)} in cash`);
  await c.query('update wallets set cash=$1, start_bal=greatest(start_bal+$2, 1) where id=$3', [delta < 0 ? Math.max(0, next) : next, delta, wid]);
  await c.query("insert into trd(wid,sym,side,qty,price,why) values($1,'','cash',1,$2,$3)", [wid, delta, why]);
}
POST('/api/cash', auth, async req => {
  const wid = await activeWalletId(req), a = num(req.body.amount), act = req.body.action, how = req.body.how === 'practice' ? 'practice' : 'bank';
  if (!(a >= 0) || a > 1e9) fail('Enter a valid amount');
  let out = { ok: true };
  if (act === 'add') out = await deposit(wid, a, how);
  else if (act === 'withdraw') out = await withdraw(wid, a, how);
  else {
    const w = await walletRow(wid); if (w.broker) fail("A broker wallet's cash comes from your real account");
    await tx(async c => { const cur = (await c.query('select cash from wallets where id=$1', [wid])).rows[0]; const delta = a - +cur.cash; if (Math.abs(delta) >= 0.005) await moveCash(c, wid, delta, 'balance set'); });
  }
  await snapshot(wid); return out;
});
DEL('/api/xfer/:id', auth, async req => { await cancelXfer(+req.params.id); });
const xferJson = x => ({ id: x.id, kind: x.kind, amount: +x.amount, instant: +x.instant, status: x.status, created: tsMs(x.created), avail: tsMs(x.avail), done: tsMs(x.done), method: x.method });
GET('/api/account', auth, async req => {
  const wid = await activeWalletId(req), w = await walletRow(wid), a = await accountInfo(wid, { w });
  const [xf, st] = await Promise.all([rows('select * from xfer where wid=$1 order by id desc limit 30', [wid]), rows('select s.*, t.sym from settle s left join trd t on t.id=s.trd_id where s.wid=$1 and s.settles > now() order by s.settles', [wid])]);
  return { wallet: walletJson(w), account: a, rules: rulesJson(w), xfers: xf.map(xferJson), settling: st.map(x => ({ amount: +x.amount, settles: tsMs(x.settles), sym: x.sym })), broker: w.broker ? { ...brokerJson(), account: BROKER?.account ? { status: BROKER.account.status, number: String(BROKER.account.account_number || '').replace(/.(?=.{4})/g, '•'), multiplier: +BROKER.account.multiplier, pdt: !!BROKER.account.pattern_day_trader, daytrades: +BROKER.account.daytrade_count || 0, blocked: !!(BROKER.account.trading_blocked || BROKER.account.account_blocked), crypto: BROKER.account.crypto_status || null } : null } : null };
});
POST('/api/wallets/transfer', auth, async req => {
  const from = +req.body.from, to = +req.body.to, a = num(req.body.amount);
  if (from === to) fail('Pick two different wallets'); if (!(a > 0)) fail('Enter an amount');
  const fw = await walletRow(from), tw = await walletRow(to);
  if (fw.broker || tw.broker) fail("Broker wallets mirror a real account. Move that money through the broker.");
  if (isReal(fw)) { const acc = await accountInfo(from, { w: fw }); if (a > acc.withdrawable + 0.005) fail(`${fw.name} can move up to ${money(acc.withdrawable)} right now (settled cash not held for orders).`); }
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
  queue: fhQueue.length, tracked: assets.size, broker: brokerJson(), clients: wss.clients.size, db: { schema: +(await kvGet('schema', '0')), snaps: +(await row('select count(*) n from snap')).n },
}));

// ---------- backup ----------
const BACKUP_TABLES = ['wallets', 'pos', 'ord', 'lim', 'trd', 'snap', 'extra', 'watch', 'alert', 'divpaid', 'seasons', 'preds', 'achv', 'meta', 'lots', 'realized', 'settle', 'xfer', 'splits'];
GET('/api/export', auth, async (req, res) => {
  const wid = await activeWalletId(req);
  if (req.query.fmt === 'csv') {
    const list = await rows('select * from trd where wid=$1 order by id', [wid]);
    const esc = v => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    const lines = ['Date,Type,Symbol,Quantity,Price,Total,Fee,SEC fee,FINRA TAF,CAT fee,Crypto fee,Order type,Session,Bid,Ask,Settles,Profit/Loss,Reason,Note,Tags'];
    for (const t of list) { const f = jsonOr(t.fees, {}); lines.push([new Date(tsMs(t.ts)).toISOString(), t.side, t.sym, +t.qty, +t.price, t.side === 'cash' || t.side === 'int' || t.side === 'fee' ? +t.price : +t.qty * +t.price, +t.fee || 0, f.sec ?? '', f.taf ?? '', f.cat ?? '', f.crypto ?? '', t.otype || '', t.sess || '', t.bid ?? '', t.ask ?? '', t.settle ? new Date(tsMs(t.settle)).toISOString().slice(0, 10) : '', t.pnl == null ? '' : +t.pnl, t.why, t.note, t.tags].map(esc).join(',')); }
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
    for (const t of ['wallets', 'ord', 'lim', 'trd', 'alert', 'seasons', 'preds', 'notifs', 'lots', 'realized', 'settle', 'xfer']) await c.query(`select setval(pg_get_serial_sequence('${t}','id'), greatest((select coalesce(max(id),1) from ${t}),1))`);
  });
  if (b.settings) await saveSettings(b.settings);
  lastSnap.clear(); await loadAssets(); await loadOrders(); await loadAlerts(); await refreshUserSyms();
  return { ok: true };
});

// =====================================================================
// Taxes, statements, rules and the broker
// =====================================================================
GET('/api/taxes', auth, async req => taxYear(await activeWalletId(req), clamp(num(req.query.y, +etDate().slice(0, 4)), 2000, 2100)));
GET('/api/taxes/csv', auth, async (req, res) => {
  const wid = await activeWalletId(req), y = clamp(num(req.query.y, +etDate().slice(0, 4)), 2000, 2100), t = await taxYear(wid, y);
  const d = ms => new Date(ms).toLocaleDateString('en-US', { timeZone: 'America/New_York', month: '2-digit', day: '2-digit', year: 'numeric' });
  const lines = ['Description,Date acquired,Date sold,Proceeds,Cost basis,Wash sale loss disallowed,Gain or loss,Term,Type'];
  for (const r of t.rows) lines.push([`${+r.qty.toFixed(8)} ${r.sym}`, d(r.acquired), d(r.sold), r.proceeds.toFixed(2), r.basis.toFixed(2), r.wash ? r.wash.toFixed(2) : '', r.gain.toFixed(2), r.term === 'long' ? 'Long term' : 'Short term', r.crypto ? 'Digital asset' : 'Security'].join(','));
  return send(req, res, 200, lines.join('\n'), 'text/csv; charset=utf-8', { 'Content-Disposition': `attachment; filename="mango-gains-${y}.csv"` });
});
GET('/api/statements', auth, async req => statementMonths(await activeWalletId(req)));
GET('/api/statements/:m', auth, async req => statement(await activeWalletId(req), req.params.m));
GET('/api/rules', auth, async req => {
  const w = await walletRow(await activeWalletId(req)), today = etDate();
  return { rules: rulesJson(w), sec: REG.sec, taf: REG.taf, cat: REG.cat, secNow: rateAt(REG.sec, today)[1], tafNow: rateAt(REG.taf, today).slice(1), profiles: Object.fromEntries(Object.entries(PROFILES).map(([k, p]) => [k, { name: p.name, pre: p.pre, post: p.post, overnight: p.overnight, gtcDays: p.gtcDays, instant: p.instant, achDays: p.achDays, marginRates: p.marginRates.map(r => [r[0] === Infinity ? null : r[0], r[1]]), cryptoTiers: p.cryptoTiers.map(r => [r[0] === Infinity ? null : r[0], r[1], r[2]]), goldFee: p.goldFee, goldFree: p.goldFree, maintBase: p.maintBase }])), vol30: await cryptoVolume30(w.id) };
});
GET('/api/broker', auth, async () => { if (BROKER && Date.now() - BROKER.lastSync > 20e3) await brokerSync().catch(() => {}); return { ...brokerJson(), live: BROKER?.env === 'live', account: BROKER?.account ? { status: BROKER.account.status, equity: +BROKER.account.equity, cash: +BROKER.account.cash, bp: +BROKER.account.buying_power, multiplier: +BROKER.account.multiplier } : null, maxOrder: ALP.maxOrder || null }; });
POST('/api/broker/link', auth, async req => brokerLink(req.body.confirm));
POST('/api/broker/sync', auth, async () => { await brokerSync(); return brokerJson(); });

// =====================================================================
// Bots, strategy lab and the news/social scanner
// =====================================================================
const botsOn = () => { if (S.f_bots === false) fail('Bots are switched off in Settings → Features'); };
GET('/api/strategies', auth, () => ({ strategies: strategiesJson(), universes: UNIVERSES, mandate: MANDATE_DEF, buzz: { on: S.f_buzz !== false, sources: buzzStatus, reddit: !!REDDIT.id } }));
const btCache = new Map();
POST('/api/backtest', auth, async req => {
  botsOn();
  const b = req.body || {}, key = JSON.stringify([b.strategy, b.params, b.syms, b.years, b.capital, b.profile, b.maxPosPct, b.robust]);
  const hit = btCache.get(key); if (hit && Date.now() - hit.t < 30 * 60e3) return hit.v;
  const v = await backtest(b);
  btCache.set(key, { t: Date.now(), v }); if (btCache.size > 40) btCache.delete(btCache.keys().next().value);
  return v;
});
async function cleanBotSyms(list, kinds = {}) {
  const syms = [...new Set((list || []).map(cleanSym).filter(Boolean))].slice(0, 30);
  if (!syms.length) fail('Pick at least one thing for the bot to trade');
  const bad = [];
  await Promise.all(syms.map(s => {
    const kind = kinds[s] === 'crypto' || (kinds[s] !== 'stock' && CRYPTO_DEFAULT.includes(s)) ? 'crypto' : kinds[s] === 'stock' ? 'stock' : (assets.has(s) ? kindOf(s) : 'stock');
    return trackAsset(s, kind).catch(() => bad.push(s));
  }));
  if (bad.length) fail(`Couldn't find a price for ${bad.join(', ')}. Check the symbol${bad.length > 1 ? 's' : ''}.`);
  return syms;
}
async function botCard(b) {
  const bot = botJson(b), w = await row('select * from wallets where id=$1', [b.wid]);
  if (!w) return { ...bot, wallet: null };
  const pos = await positions(b.wid), value = +w.cash + marketValue(pos);
  const mine = pos.filter(p => bot.syms.includes(p.sym));
  return { ...bot, wallet: { id: w.id, name: w.name, emoji: w.emoji, color: w.color, profile: w.profile, archived: w.archived }, value, start: +w.start_bal, ret: (value / +w.start_bal - 1) * 100, day: dayChange(pos),
    holding: mine.map(p => p.sym), next: nextRunOf(bot), invested: mine.reduce((t, p) => t + p.qty * (priceOf(p.sym) || p.avg), 0), cash: +w.cash, dd: bot.peak ? value / bot.peak - 1 : 0 };
}
GET('/api/bots', auth, async () => {
  const list = await rows('select * from bots order by id');
  return { bots: await Promise.all(list.map(botCard)), on: S.f_bots !== false };
});
GET('/api/bots/:id', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  const card = await botCard(b), state = jsonOr(b.state, {});
  const pos = (await positions(b.wid)).map(p => {
    const c = priceOf(p.sym) || p.avg, s0 = state[p.sym] || {}, stop = (condOrders.get(p.sym) || []).find(o => o.wid === b.wid && (o.kind === 'stop' || o.kind === 'trail'));
    return { ...p, price: c, value: p.qty * c, pl: (c - p.avg) * p.qty, plPct: (c / p.avg - 1) * 100, mine: card.syms.includes(p.sym), entryDate: s0.entryDate || null, stop: stop ? orderJson(stop).stop : null, weight: s0.weight || null };
  });
  const log = (await rows('select * from bot_log where bot_id=$1 order by id desc limit 60', [b.id])).map(logJson);
  const last = await row("select * from bot_log where bot_id=$1 and kind='decision' order by id desc limit 1", [b.id]);
  const queued = [...limOrders.values()].flat().filter(l => l.wid === b.wid).map(l => ({ id: l.id, sym: l.sym, side: l.side, kind: l.kind, amt: l.amt, units: l.units }));
  return { ...card, positions: pos, log, last: last ? logJson(last) : null, queued, strategyInfo: strategiesJson()[card.strategy] || null };
});
const logJson = r => ({ id: r.id, ts: tsMs(r.ts), kind: r.kind, sym: r.sym, title: r.title, body: r.body, data: jsonOr(r.data) });
function nextRunOf(bot) {
  if (bot.status !== 'running') return null;
  const crypto = bot.syms.some(isCrypto), [hh, mm] = bot.mandate.runAt.split(':').map(Number);
  for (let k = 0; k < 10; k++) {
    const t = Date.now() + k * 864e5, p = etParts(t), day = etDate(t);
    if (k === 0 && bot.lastDay === day) continue;
    if (!crypto && !isTradingDay(p.y, p.m, p.d)) continue;
    const at = etToUtc(p.y, p.m, p.d, hh, mm);
    return k === 0 && at < Date.now() ? Date.now() + 60e3 : at;
  }
  return null;
}
GET('/api/bots/:id/log', auth, async req => {
  const before = +req.query.before || 2e9, kind = req.query.kind || null;
  return (await rows(`select * from bot_log where bot_id=$1 and id < $2 ${kind ? 'and kind=$3' : ''} order by id desc limit 80`, kind ? [+req.params.id, before, kind] : [+req.params.id, before])).map(logJson);
});
POST('/api/bots', auth, async req => {
  botsOn();
  const b = req.body || {}, st = STRATEGIES[b.strategy]; if (!st) fail('Pick a strategy');
  if (+(await row('select count(*) n from bots')).n >= 20) fail('You can have up to 20 bots');
  const name = String(b.name || st.name).trim().slice(0, 30) || st.name;
  const syms = await cleanBotSyms(b.syms, b.kinds || {}), params = cleanParams(b.strategy, b.params), mandate = cleanMandate(b.mandate || {});
  let wid = +b.wallet || 0;
  if (!wid) {
    if (+(await row('select count(*) n from wallets')).n >= 25) fail('You have 25 wallets already. Pick an existing wallet for this bot or archive one.');
    const start = clamp(num(b.start, 10000), 100, 1e9);
    const w = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,lot_method,crypto_route) values($1,'🤖',$2,$3,$3,(select coalesce(max(sort),0)+1 from wallets),$4,$5,'fifo','mm') returning *`,
      [name.slice(0, 30), COLORS.includes(b.color) ? b.color : 'grape', start, PROFILE_KEYS.includes(b.profile) ? b.profile : 'robinhood', ACCTS.includes(b.acct) ? b.acct : 'instant']);
    wid = w.id; await loadWalletMeta(); await snapshot(wid);
  } else {
    const w = await walletRow(wid); if (w.archived) fail('That wallet is archived');
    if (w.broker) fail('Bots trade practice and simulated wallets for now. Your broker wallet is left alone.');
    const others = (await rows("select * from bots where wid=$1 and status<>'killed'", [wid])).map(botJson);
    const overlap = others.flatMap(o => o.syms.filter(s => syms.includes(s)));
    if (overlap.length) fail(`Another bot in this wallet already trades ${[...new Set(overlap)].join(', ')}. Give each bot its own symbols or its own wallet.`);
  }
  const acct = await accountInfo(wid);
  const r = await row('insert into bots(name,wid,strategy,params,syms,mandate,status,peak,day_start,day_date,state) values($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$10) returning *',
    [name, wid, b.strategy, JSON.stringify(params), JSON.stringify(syms), JSON.stringify(mandate), b.paused ? 'paused' : 'running', acct.equity, etDate(), '{}']);
  await botLog(r.id, 'info', 'Bot created', `${st.name} on ${syms.join(', ')}. Decides daily at ${mandate.runAt} ET${syms.some(isCrypto) ? ' (crypto every day)' : ' on trading days'}.`, { params, mandate });
  await refreshUserSyms();
  // The first decision reads news for every symbol, which can take a while; it runs in the background
  // unless the caller asks to wait for it.
  let first = null;
  if (b.runNow && b.wait) first = await runBot(r.id, { manual: true }).catch(e => ({ error: e.message }));
  else if (b.runNow) runBot(r.id, { manual: true }).catch(async e => { await botLog(r.id, 'error', 'The first decision failed', e.message); broadcast({ t: 'evt', e: 'bot', id: r.id }); });
  return { bot: await botCard(await row('select * from bots where id=$1', [r.id])), first, deciding: !!(b.runNow && !b.wait) };
});
PATCH('/api/bots/:id', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  const bot = botJson(b), x = req.body || {}, sets = [], vals = [];
  const set = (k, v) => { vals.push(v); sets.push(`${k}=$${vals.length}`); };
  if (x.name !== undefined) set('name', String(x.name).trim().slice(0, 30) || bot.name);
  if (x.params) set('params', JSON.stringify(cleanParams(bot.strategy, { ...bot.params, ...x.params })));
  if (x.mandate) set('mandate', JSON.stringify(cleanMandate(x.mandate, bot.mandate)));
  if (x.syms) set('syms', JSON.stringify(await cleanBotSyms(x.syms, x.kinds || {})));
  const changes = [];
  if (x.status && ['running', 'paused'].includes(x.status) && x.status !== bot.status) {
    set('status', x.status); changes.push(x.status === 'running' ? 'Resumed' : 'Paused');
    if (x.status === 'running' && bot.status === 'killed') { const a = await accountInfo(bot.wid); set('peak', a.equity); set('killed_reason', null); changes[changes.length - 1] = 'Restarted after the drawdown stop (peak reset to today)'; }
  }
  if (!sets.length) return botCard(b);
  vals.push(b.id); await Q(`update bots set ${sets.join(',')} where id=$${vals.length}`, vals);
  const what = [x.name !== undefined && 'name', x.params && 'settings', x.mandate && 'limits', x.syms && 'symbols'].filter(Boolean);
  await botLog(b.id, 'info', changes[0] || 'Settings changed', what.length ? 'Changed ' + what.join(', ') : '', x.params || x.mandate ? { params: x.params, mandate: x.mandate } : null);
  await refreshUserSyms();
  return botCard(await row('select * from bots where id=$1', [b.id]));
});
DEL('/api/bots/:id', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) return { ok: true };
  if (req.query.flatten === '1') await flattenBot(botJson(b), 'bot deleted');
  await Q('delete from bot_log where bot_id=$1', [b.id]); await Q('delete from bots where id=$1', [b.id]);
  await refreshUserSyms();
  return { ok: true };
});
POST('/api/bots/:id/run', auth, async req => { botsOn(); return runBot(+req.params.id, { manual: true, dry: !!req.body?.dry }); });
POST('/api/bots/:id/flatten', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  const sold = await flattenBot(botJson(b), 'you asked it to sell everything');
  if (req.body?.pause !== false) await Q("update bots set status='paused' where id=$1 and status='running'", [b.id]);
  return { sold };
});
// ---- news and social scanner ----
GET('/api/buzz/:sym', auth, async req => {
  if (S.f_buzz === false) fail('The news scanner is switched off in Settings → Features');
  const sym = cleanSym(req.params.sym); if (!sym) fail('Bad symbol');
  return buzzFor(sym);
});
GET('/api/buzz', auth, async req => {
  if (S.f_buzz === false) return { items: [], off: true };
  let syms = String(req.query.syms || '').split(',').map(cleanSym).filter(Boolean);
  if (!syms.length) {
    const held = (await rows('select distinct p.sym from pos p join wallets w on w.id=p.wid where not w.archived')).map(x => x.sym);
    const watch = (await rows('select sym from watch')).map(x => x.sym);
    const bots = (await rows("select syms from bots where status='running'")).flatMap(x => jsonOr(x.syms, []));
    syms = [...new Set([...held, ...watch, ...bots])];
  }
  syms = syms.slice(0, 16);
  const out = [];
  for (let i = 0; i < syms.length; i += 4) await Promise.all(syms.slice(i, i + 4).map(s => Promise.race([buzzFor(s), sleep(20000).then(() => null)]).then(b => { if (b) out.push({ ...buzzSummary(b), line: buzzLine(b), top: b.items.slice(0, 3) }); }).catch(() => {})));
  const rank = x => (x.flags.includes('severe_news') ? 0 : x.flags.includes('earnings_soon') ? 1 : x.flags.includes('negative_event') ? 2 : x.flags.includes('attention_spike') ? 3 : 5) - Math.abs(x.sentiment);
  return { items: out.sort((a, b) => rank(a) - rank(b)), sources: buzzStatus, reddit: !!REDDIT.id };
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
  for (const p of await rows('select p.*, w.drip, w.name wname from pos p join wallets w on w.id=p.wid where not w.archived and w.broker is null')) {
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
      let drip = '';
      if (p.drip && amt >= 0.01) { // dividend reinvestment buys more of the same stock
        try {
          if (market().state === 'open') { const f = await execTrade(p.wid, p.sym, 'buy', { usd: amt, why: 'dividend reinvestment', drip: true, auto: true }); drip = ` It bought ${+f.qty.toFixed(6)} more shares.`; }
          else { await addLimit(p.wid, p.sym, { side: 'buy', kind: 'moo', amount: amt, units: 'usd', why: 'dividend reinvestment' }); drip = ' It will buy more shares when the market opens.'; }
        } catch (e) { drip = ` Reinvesting didn't work: ${e.message}`; }
      }
      await notify('div', `💵 Dividend from ${p.sym}`, `You received ${money(amt)} (${pxStr(v.amount)} a share on ${+(+p.qty).toFixed(4)} shares).${drip}`, { sym: p.sym, wid: p.wid });
      checkAchievementsSoon();
    }
  }
}
async function daily() {
  const p = etParts(), today = etDate(), mk = market(), mins = p.h * 60 + p.mi;
  // interest on cash, once a day
  if (S.cashAPY > 0 && mins >= 5 && (await kvGet('job_interest')) !== today) {
    await kvSet('job_interest', today);
    for (const w of await rows('select * from wallets where not archived and cash > 0 and broker is null')) {
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
  const fast = !!ENV.MANGO_FAST_JOBS; // for tests: run the slow jobs sooner
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
  every(6 * 3600e3, divJob, 'dividends'); setTimeout(() => divJob().catch(() => {}), fast ? 20e3 : 90e3);
  every(6 * 3600e3, splitJob, 'splits'); setTimeout(() => splitJob().catch(() => {}), fast ? 25e3 : 120e3);
  every(fast ? 5e3 : 60e3, xferJob, 'transfers');
  every(60e3, marginDaily, 'margin interest');
  every(fast ? 5e3 : 60e3, marginWatch, 'margin');
  every(fast ? 3000 : 5000, pollExtended, 'extended hours');
  every(fast ? 5e3 : 60e3, () => { if (S.f_bots !== false) return botScheduler(); }, 'bots');
  every(3600e3, () => Q("delete from bot_log where ts < now() - interval '2 years'"), 'bot log cleanup');
  every(3600e3, () => Q("delete from settle where settles < now() - interval '7 days'"), 'settlement cleanup');
  if (BROKER) { every(15e3, () => brokerSync(), 'broker'); every(30e3, () => { adSync(); if (BROKER.wid) tuConnect(); }, 'broker streams'); setTimeout(() => brokerSync().then(() => { if (BROKER.wid) tuConnect(); }).catch(() => {}), 3000); }
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
  server.listen(PORT, () => log(`🥭 Mango is running on port ${PORT} · build ${BUILD} · ${FINNHUB_KEY ? 'stocks on' : 'stocks off (no FINNHUB_KEY)'}${BROKER ? ` · Alpaca ${BROKER.env}` : ''}`));
  cbConnect(); fhConnect(); if (BROKER) adConnect(); startJobs();
})().catch(e => { console.error('Mango could not start:', e); process.exit(1); });
