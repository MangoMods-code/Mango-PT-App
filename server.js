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
  // fixed paths win over ones with :params, whatever order the files register them in (/api/bots/board before /api/bots/:id)
  if (keys.length) routes.push({ method, re, keys, handlers }); else routes.unshift({ method, re, keys, handlers });
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
    req.body = ['POST', 'PATCH', 'PUT', 'DELETE'].includes(method) ? await readBody(req, req.path === '/api/backup/restore' ? 60 * 1024 * 1024 : undefined) : {};
    let out;
    for (const h of r.handlers) { out = await h(req, res); if (res.headersSent) return; }
    if (out === undefined) out = { ok: true };
    send(req, res, 200, out);
  } catch (e) {
    const status = e.status || 500;
    if (status >= 500) warn(req.method, req.path, e.stack || e.message);
    send(req, res, status, { error: status >= 500 ? 'Something went wrong on the server. Try again in a moment.' : e.message, ...(e.step ? { step: true } : {}) });
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
  // 4: daily price history kept in the database, so bots keep working when a data source is down
  async c => { await c.query('create table if not exists bars(sym text primary key, data text not null, t timestamptz not null default now(), src text)'); },
  // 5: Face ID / passkeys, and automatic backups
  async c => {
    await c.query(`
      create table if not exists passkeys(id text primary key, pub text not null, alg int not null, counter bigint not null default 0, name text, rp text, created timestamptz not null default now(), last_used timestamptz);
      create table if not exists backups(id serial primary key, ts timestamptz not null default now(), kind text not null default 'auto', size int not null, data text not null, note text);
    `);
  },
  // 6: the Council's votes, checked a week later
  async c => {
    await c.query(`
      create table if not exists council(id serial primary key, ts timestamptz not null default now(), bot_id int not null, sym text not null, action text not null, price numeric, votes text not null,
        yes int not null default 0, no int not null default 0, chair text, settle_at timestamptz not null, settled boolean not null default false, result numeric, right_ text);
      create index if not exists council_b on council(bot_id, id desc);
      create table if not exists opt_pos(id serial primary key, sym text not null, kind text not null, strike numeric not null, exp text not null, qty int not null, prem numeric not null, iv numeric, opened timestamptz not null default now(), closed timestamptz, close_px numeric, how text);
      alter table alert add column if not exists ind text;
    `);
  },
  // 7: price improvement on each fill
  async c => { await c.query('alter table trd add column if not exists imp numeric'); },
  // 8: cash-account violations, IRA and Roth IRA wallets
  async c => {
    await c.query(`
      alter table wallets add column if not exists restricted_until timestamptz;
      alter table wallets add column if not exists ira_opened timestamptz;
      alter table wallets add column if not exists ira_basis numeric not null default 0;
      create table if not exists cashfund(id serial primary key, wid int not null, sym text not null, qty numeric not null, amount numeric not null, settles timestamptz not null, trd_id int, ts timestamptz not null default now());
      create index if not exists cashfund_w on cashfund(wid, sym);
      create table if not exists cashviol(id serial primary key, wid int not null, kind text not null, sym text, note text, trd_id int, ts timestamptz not null default now());
      create table if not exists contrib(id serial primary key, wid int not null, year int not null, amount numeric not null, kind text not null, note text, penalty numeric, tax numeric, ts timestamptz not null default now());
    `);
  },
  // 9: options in wallets
  async c => {
    await c.query(`
      alter table wallets add column if not exists opt_level int not null default 0;
      create table if not exists optpos(id serial primary key, wid int not null, sym text not null, kind text not null, strike numeric not null, exp text not null, qty int not null, avg numeric not null,
        coll text, spread int, why text, bot_id int, opened timestamptz not null default now(), closed timestamptz, close_px numeric, how text, pnl numeric);
      create index if not exists optpos_w on optpos(wid) where closed is null;
      create table if not exists opttrd(id serial primary key, wid int not null, ts timestamptz not null default now(), pos_id int, sym text not null, kind text not null, strike numeric not null, exp text not null,
        action text not null, qty int not null, price numeric not null, fee numeric not null default 0, pnl numeric, why text, note text, bid numeric, ask numeric);
      create index if not exists opttrd_w on opttrd(wid, id desc);
    `);
  },
  // 10: the daily call
  async c => { await c.query('create table if not exists dcall(d text primary key, pick text not null, made timestamptz not null default now(), close numeric, prev numeric, result text, house text)'); },
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
  closedTrading: 'fill', rthOrders: true,
  cashRules: true, f_mwcb: true, f_luld: true, fracRules: true, priceImprove: true, sanity: true, san_far: true, san_big: true, san_open: true, san_earn: true, san_thin: true, autoSL: 0, autoTP: 0, defaultBuy: 0,
  confirmStyle: 'hold', confirmBuy: false, confirmSell: true, cashAPY: 0, startBalance: 10000,
  // notifications
  pushAlerts: true, pushOrders: true, notifySummary: true, notifyMoves: true, moveThreshold: 5,
  decayAlerts: true, notifyAchv: true, notifyGames: true, pushBots: true, botWeekly: true, pushReal: true, realDailyLossPct: 3, realHalt: false, standbyActive: false, standbyActiveAt: 0, councilWeighted: true, vetoAll: true, promoGate: true, promoMinDays: 60, promoMinTrades: 10, promoBeatSpy: true, promoMaxDD: 15, panicAt: 0, offsiteOn: true, offsiteKeep: 14, briefOn: true, briefAt: '08:45', letterOn: true, aiPostmortem: true, aiChat: true, aiExplain: true, moodSwitch: true, corrGuard: true, corrMax: 0.9, sharedRisk: true, maxSymPct: 30, maxSectorPct: 50, maxCryptoPct: 50, autoBench: false, autoTune: true, aiVeto: true, aiReview: true, aiSizeUp: true, nightOn: true, nightRuns: 2, aiOn: true, aiSize: true, aiDebate: true, aiHawk: true, aiGuard: true, aiChart: true, aiDailyMax: 120, faceReal: true, autoBackup: true, quietStart: '', quietEnd: '',
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
  promoMinDays: [0, 365], promoMinTrades: [0, 200], promoMaxDD: [1, 90], offsiteKeep: [3, 60], maxSymPct: [5, 100], maxSectorPct: [10, 100], maxCryptoPct: [0, 100], corrMax: [0.5, 1], nightRuns: [1, 3], aiDailyMax: [0, 500],
  realDailyLossPct: [0, 50],
  feeFlat: [0, 1000], feePct: [0, 10], slippage: [0, 5], maxPos: [0, 100], reservePct: [0, 100], autoSL: [0, 90], autoTP: [0, 1000],
  defaultBuy: [0, 1e9], cashAPY: [0, 100], startBalance: [100, 1e9], moveThreshold: [1, 50], defaultWallet: [1, 1e9],
};
const CHOICES = {
  closedTrading: ['fill', 'queue', 'block'], confirmStyle: ['hold', 'tap', 'none'], defaultRange: ['1D', '1W', '1M', '3M', '1Y', 'ALL', '5Y'],
  defaultChart: ['Candles', 'Line'], theme: ['mango', 'midnight', 'light', 'auto'], accent: ['mango', 'leaf', 'sky', 'grape', 'rose'],
  plColors: ['classic', 'colorblind', 'reversed'], defaultTab: ['portfolio', 'market', 'bots', 'activity', 'games'],
  briefAt: ['07:30', '08:45', '09:15'], posMetric: ['day', 'total', 'value', 'pl'], posSort: ['value', 'day', 'total', 'name'],
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
  for (const l of OPTBOOK.values()) for (const o of l) userSyms.orders.add(undOf(o.sym));
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
// Market-wide circuit breakers and single-stock price bands (Limit Up-Limit Down)
// As the exchanges run them (investor.gov, "Measures to address market volatility"):
// - S&P 500 down 7% (Level 1) or 13% (Level 2) from yesterday's close before 3:25 pm → all stocks halt 15 minutes;
//   at or after 3:25 pm those levels don't halt. Each level halts at most once a day. 20% (Level 3) → closed for the day.
// - Each stock trades inside a band around its average price of the last 5 minutes: 5% (S&P 500 / Russell 1000
//   stocks and big ETFs over $3), 10% (other stocks over $3), 20% ($0.75-$3), the lesser of $0.15 or 75% (under $0.75),
//   doubled in the last 25 minutes for the first group and for anything at $3 or less. Outside the band for 15 seconds
//   → that stock pauses for 5 minutes. Crypto has none of this.
// The S&P 500 is followed through SPY, which tracks it.
// =====================================================================
const MWCB_LEVELS = [{ level: 3, drop: 0.20 }, { level: 2, drop: 0.13 }, { level: 1, drop: 0.07 }];
const MSTATE = { mwcb: null, done: { date: null, levels: [] }, luld: new Map(), win: new Map() };
const mwcbOn = () => S.f_mwcb !== false, luldOn = () => S.f_luld !== false;
function etMinutes(t = Date.now()) { const p = etParts(t); return p.h * 60 + p.mi; }
// is a market-wide halt on right now? → { level, until, at, drop } or null
function mwcbNow() {
  const h = MSTATE.mwcb; if (!h) return null;
  if (h.until && Date.now() >= h.until) { MSTATE.mwcb = null; saveMstate(); broadcast({ t: 'evt', e: 'mstate', ev: mstateJson() }); notify('market', 'Trading resumes', `The market-wide halt (Level ${h.level}) is over. Queued stock orders fill now.`, {}).catch(() => {}); return null; }
  if (h.date && h.date !== etDate()) { MSTATE.mwcb = null; return null; }
  return h;
}
// why a stock can't trade this moment, or null
function haltOf(sym) {
  if (!sym || isCrypto(sym)) return null;
  const m = mwcbOn() ? mwcbNow() : null;
  if (m) return { kind: 'mwcb', level: m.level, until: m.until, why: m.level === 3 ? 'The S&P 500 fell more than 20% today: a Level 3 circuit breaker closed the stock market for the rest of the day.' : `The S&P 500 fell more than ${m.level === 1 ? 7 : 13}% today: a Level ${m.level} circuit breaker halts all stock trading until ${timeEt(m.until)}.` };
  const p = luldOn() ? MSTATE.luld.get(sym) : null;
  if (p?.until && Date.now() < p.until) return { kind: 'luld', until: p.until, why: `${sym} moved outside its price band for 15 seconds, so trading in it pauses until ${timeEt(p.until)} (Limit Up-Limit Down).` };
  return null;
}
const timeEt = t => new Date(t).toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' });
// the band a stock may trade in: Tier 1 = big companies and big ETFs
function luldTier(sym) { if (FUNDS.has(sym)) return 1; const a = assets.get(sym); if (a?.kind === 'etf' || (a?.cap && a.cap >= 5e9)) return 1; if (STOCK_DEFAULT.includes(sym)) return 1; return 2; }
function luldBand(sym, ref, t = Date.now()) {
  const tier = luldTier(sym); let pct;
  if (ref > 3) pct = tier === 1 ? 0.05 : 0.10;
  else if (ref >= 0.75) pct = 0.20;
  else pct = Math.min(0.75, 0.15 / ref);
  const mk = market(t), close = mk.closesAt ? etMinutes(mk.closesAt) : 960, late = etMinutes(t) >= close - 25;
  if (late && (tier === 1 || ref <= 3)) pct *= 2;
  return { tier, pct, lo: ref * (1 - pct), hi: ref * (1 + pct) };
}
function luldOf(sym) {
  const w = MSTATE.win.get(sym); if (!w || w.length < 2 || Date.now() - w[0][0] < 60e3) return null;
  const ref = w.reduce((s, x) => s + x[1], 0) / w.length, b = luldBand(sym, ref), st = MSTATE.luld.get(sym);
  return { ref, ...b, paused: st?.until && Date.now() < st.until ? st.until : null };
}
// every price tick comes through here
function mstateTick(sym, p, src) {
  if (src === 'cache' || !(p > 0) || isCrypto(sym)) return;
  const mk = market(); if (mk.state !== 'open') { if (MSTATE.win.size) MSTATE.win.clear(); return; }
  const now = Date.now();
  if (sym === 'SPY' && mwcbOn()) checkMwcb(p, now);
  if (!luldOn()) return;
  const st = MSTATE.luld.get(sym);
  if (st?.until) { if (now < st.until) return; MSTATE.luld.delete(sym); MSTATE.win.set(sym, [[now, p]]); broadcast({ t: 'evt', e: 'mstate', ev: mstateJson() }); return; }
  let w = MSTATE.win.get(sym); if (!w) { w = []; MSTATE.win.set(sym, w); }
  w.push([now, p]); while (w.length && now - w[0][0] > 5 * 60e3) w.shift(); if (w.length > 600) w.splice(0, w.length - 600);
  if (now - w[0][0] < 60e3) return; // the band needs a minute of prices first
  const ref = w.reduce((s, x) => s + x[1], 0) / w.length, b = luldBand(sym, ref, now);
  if (p < b.lo || p > b.hi) {
    const s = MSTATE.luld.get(sym) || {}; if (!s.out) { s.out = now; MSTATE.luld.set(sym, s); return; }
    if (now - s.out >= 15e3) {
      s.until = now + 5 * 60e3; s.out = null; s.dir = p > b.hi ? 'up' : 'down'; s.at = now; MSTATE.luld.set(sym, s);
      log(`LULD pause: ${sym} ${s.dir} at ${p} (band ${b.lo.toFixed(2)}-${b.hi.toFixed(2)})`);
      broadcast({ t: 'evt', e: 'mstate', ev: mstateJson() });
      if (userSyms.pos.has(sym) || userSyms.orders.has(sym)) notify('market', `${sym} paused`, `${sym} moved more than ${Math.round(b.pct * 100)}% ${s.dir} in under 5 minutes, so it pauses for 5 minutes (Limit Up-Limit Down). Orders wait until ${timeEt(s.until)}.`, { sym }).catch(() => {});
    }
  } else { const s = MSTATE.luld.get(sym); if (s?.out) { s.out = null; } }
}
function checkMwcb(p, now) {
  const q = prices.get('SPY'), prev = q?.prev; if (!(prev > 0)) return;
  const today = etDate(now); if (MSTATE.done.date !== today) MSTATE.done = { date: today, levels: [] };
  const drop = 1 - p / prev;
  for (const L of MWCB_LEVELS) {
    if (drop < L.drop || MSTATE.done.levels.includes(L.level)) continue;
    // a lower level can't fire once a higher one has
    for (const x of MWCB_LEVELS) if (x.level <= L.level && !MSTATE.done.levels.includes(x.level)) MSTATE.done.levels.push(x.level);
    const late = ENV.MARKET_OVERRIDE ? false : etMinutes(now) >= 15 * 60 + 25;
    let until = null;
    if (L.level === 3) { const c = market(now).closesAt; until = c && c > now ? c : now + 6 * 3600e3; }
    else if (!late) until = now + 15 * 60e3;
    if (until) {
      MSTATE.mwcb = { level: L.level, until, at: now, drop, date: today };
      log(`Circuit breaker Level ${L.level}: S&P 500 (SPY) down ${(drop * 100).toFixed(1)}%`);
      notify('market', `🚨 Level ${L.level} circuit breaker`, L.level === 3 ? `The S&P 500 is down ${(drop * 100).toFixed(1)}% today. Stock trading is closed for the rest of the day. Crypto still trades.` : `The S&P 500 is down ${(drop * 100).toFixed(1)}% today. All stock trading halts for 15 minutes, until ${timeEt(until)}. Stock orders you place now wait for the reopen.`, {}).catch(() => {});
    } else notify('market', `S&P 500 down ${(drop * 100).toFixed(1)}%`, `That's past the Level ${L.level} circuit breaker, but after 3:25 pm the market doesn't halt for Level 1 or 2.`, { push: false }).catch(() => {});
    saveMstate(); broadcast({ t: 'evt', e: 'mstate', ev: mstateJson() });
    break;
  }
}
const saveMstate = () => kvSet('mstate', JSON.stringify({ mwcb: MSTATE.mwcb, done: MSTATE.done })).catch(() => {});
async function loadMstate() { try { const v = JSON.parse(await kvGet('mstate', '{}') || '{}'); if (v.mwcb && v.mwcb.date === etDate()) MSTATE.mwcb = v.mwcb; if (v.done?.date === etDate()) MSTATE.done = v.done; } catch (e) {} }
function mstateJson() {
  const m = mwcbOn() ? mwcbNow() : null, paused = {};
  for (const [s, v] of MSTATE.luld) if (v.until && Date.now() < v.until) paused[s] = { until: v.until, dir: v.dir };
  const q = prices.get('SPY'), spyDrop = q?.prev && q.p ? 1 - q.p / q.prev : null;
  return { mwcb: m ? { level: m.level, until: m.until, at: m.at, drop: m.drop } : null, done: MSTATE.done.date === etDate() ? MSTATE.done.levels : [], paused, spyDrop, on: { mwcb: mwcbOn(), luld: luldOn() } };
}
// the exchange calendar: holidays and 1 pm early closes, this year and next
function calendarJson(years) {
  const out = [];
  for (const y of years) {
    for (let m = 1; m <= 12; m++) { const days = new Date(Date.UTC(y, m, 0)).getUTCDate(); for (let d = 1; d <= days; d++) {
      const iso = `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`, hn = holidayName(y, m, d), w = dow(y, m, d);
      if (hn && w !== 0 && w !== 6) out.push({ date: iso, kind: 'closed', name: hn });
      else if (earlyClose(y, m, d)) out.push({ date: iso, kind: 'early', name: m === 11 ? 'Day after Thanksgiving' : m === 12 ? 'Christmas Eve' : 'Day before Independence Day', close: '1:00 pm' });
    } }
  }
  return out;
}
// the note for the home screen: tomorrow (the next weekday) is a holiday or an early close
function calendarNote(t = Date.now()) {
  const p = etParts(t); let [y, m, d] = addDays(p.y, p.m, p.d, 1);
  while (dow(y, m, d) === 0 || dow(y, m, d) === 6) [y, m, d] = addDays(y, m, d, 1);
  const hn = holidayName(y, m, d), iso = `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`, wd = WD[dow(y, m, d)];
  if (hn) return { date: iso, kind: 'closed', text: `The stock market is closed ${wd === WD[(p.wd + 1) % 7] ? 'tomorrow' : wd} for ${hn}. Crypto trades as usual.` };
  if (earlyClose(y, m, d)) return { date: iso, kind: 'early', text: `The stock market closes early at 1:00 pm ${wd === WD[(p.wd + 1) % 7] ? 'tomorrow' : 'on ' + wd}.` };
  return null;
}
// ---------- order sanity checks: the "are you sure?" a careful broker asks before a manual order ----------
const SANITY = [
  ['far', 'Limit price far from the market'], ['big', 'Order over 25% of the wallet'], ['open', 'Market order in the first 5 minutes'],
  ['earn', 'Buying right before earnings'], ['thin', 'Market order in a thinly traded stock'],
];
async function sanityChecks(w, { sym, side, otype, price, lpx, notional, equity }) {
  if (S.sanity === false) return [];
  const out = [], crypto = isCrypto(sym), p = priceOf(sym), on = k => S['san_' + k] !== false;
  const lim = otype === 'stoplimit' ? num(lpx) : otype === 'limit' ? num(price) : 0;
  if (on('far') && lim > 0 && p > 0 && Math.abs(lim / p - 1) > 0.10) out.push({ k: 'far', text: `Your limit price ${pxStr(lim)} is ${Math.round(Math.abs(lim / p - 1) * 100)}% ${lim > p ? 'above' : 'below'} the market price ${pxStr(p)}. Typo?` });
  if (on('big') && side === 'buy' && equity > 0 && notional > equity * 0.25) out.push({ k: 'big', text: `This puts ${Math.round(notional / equity * 100)}% of the wallet into one order. Most traders keep a single trade under 10 to 25%.` });
  if (!crypto && otype === 'market') {
    const mk = market();
    if (on('open') && mk.state === 'open' && etMinutes() < 9 * 60 + 35 && !ENV.MARKET_OVERRIDE) out.push({ k: 'open', text: 'The first minutes after the open have the widest spreads and the wildest prices. A limit order protects your price.' });
    const a = assets.get(sym), dollarVol = a?.adv && p ? a.adv * p : null;
    if (on('thin') && dollarVol != null && dollarVol < 1e6) out.push({ k: 'thin', text: `${sym} trades only about ${money(dollarVol)} a day, so a market order can fill far from the price you see. A limit order is safer.` });
  }
  if (on('earn') && side === 'buy' && !crypto && FINNHUB_KEY) {
    try { const e = await Promise.race([earningsFor(sym), sleep(1500).then(() => null)]); const d = e?.next?.date; if (d) { const days = Math.round((Date.parse(d + 'T12:00:00Z') - Date.parse(etDate() + 'T12:00:00Z')) / 864e5); if (days >= 0 && days <= 1) out.push({ k: 'earn', text: `${sym} reports earnings ${days === 0 ? 'today' : 'tomorrow'}${e.next.hour === 'amc' ? ' after the close' : e.next.hour === 'bmo' ? ' before the open' : ''}. The price can jump 5 to 10% either way.` }); } } catch (x) {}
  }
  return out;
}
// ---------- fractional-share rules (most brokers): fractions trade as market orders or day limit orders, in regular hours ----------
function fracProblem(w, { sym, kind, units, amt, tif, sess }) {
  if (S.fracRules === false || isCrypto(sym) || !isReal(w)) return null;
  const frac = units === 'usd' || (units === 'qty' && !Number.isInteger(+amt)) || units === 'pct';
  if (!frac) return null;
  if (units === 'pct') return null; // a percentage of whole shares is checked when it fills
  if (kind === 'stop' || kind === 'stoplimit') return 'Stop orders need whole shares. Fractional shares (or orders in dollars) can only be market or limit orders.';
  if (kind === 'limit' && tif === 'gtc') return 'Fractional limit orders (or orders in dollars) last for the day only. Pick "Today", or enter whole shares for good-till-canceled.';
  if (kind === 'limit' && sess && sess !== 'regular') return 'Fractional shares (or orders in dollars) only trade in regular hours. Enter whole shares to trade extended hours.';
  return null;
}

// =====================================================================
// Real-market rules: broker profiles, sessions, quotes, fees, settlement, margin
// Sources (checked Oct 2026): SEC fee rate advisory 2026-2, FINRA SR-FINRA-2026-021 (TAF pause),
// Robinhood "Trading fees", crypto fee schedule, margin rates; Alpaca crypto fees and margin docs.
// =====================================================================
const PROFILES = {
  robinhood: {
    key: 'robinhood', pi: 0.35, name: 'Robinhood rules', pre: 7 * 60, post: 20 * 60, overnight: true, gtcDays: 90, minUsd: 1,
    instant: { std: 1000, gold: 5000 }, achDays: 3, wdDays: 2, goldFee: 5, goldFree: 1000, maintBase: 25,
    marginRates: [[50e3, 5.25], [100e3, 5.05], [1e6, 4.75], [10e6, 4.5], [50e6, 4.45], [Infinity, 4.2]],
    // exchange routing tiers by 30-day crypto volume: [up to, taker %, maker %]
    cryptoTiers: [[10e3, 0.95, 0.5], [50e3, 0.75, 0.35], [250e3, 0.25, 0.125], [500e3, 0.15, 0.075], [1e6, 0.125, 0.06], [5e6, 0.10, 0.04], [10e6, 0.04, 0.02], [25e6, 0.03, 0.01], [Infinity, 0.03, 0]],
  },
  alpaca: {
    key: 'alpaca', pi: 0.2, name: 'Alpaca rules', pre: 4 * 60, post: 20 * 60, overnight: false, gtcDays: 90, minUsd: 1,
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
  call: +w.call_amt > 0 ? { amount: +w.call_amt, since: tsMs(w.call_since) } : null, optLevel: +w.opt_level || 0,
});
async function positions(wid, c = null) {
  const out = ((await (c || pool).query('select * from pos where wid=$1 order by sym', [wid])).rows).map(p => ({ sym: p.sym, qty: +p.qty, avg: +p.avg, since: tsMs(p.since) }));
  if (OPTBOOK.has(wid)) Object.defineProperty(out, 'optVal', { value: optMark(wid) }); // options count toward the wallet (short ones against it)
  return out;
}
const marketValue = pos => pos.reduce((t, x) => t + x.qty * (priceOf(x.sym) || x.avg), 0) + (pos.optVal || 0);
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
  // options: shares behind covered calls, cash behind puts and spreads, the requirement on naked ones
  const oh = optHolds(wid); out.cash += oh.cash; out.optCash = oh.cash; out.optShares = oh.shares;
  for (const [s, q] of Object.entries(oh.shares)) out.shares[s] = (out.shares[s] || 0) + q;
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
  const equity = cash + lmv + (pos.optVal || 0), debit = Math.max(0, -cash), avail = cash - holds.cash;
  let bp, bpCrypto, withdrawable, marginOn = false;
  if (w.broker && BROKER?.account) { // the broker's own numbers
    const a = BROKER.account;
    return { profile: 'alpaca', acct: w.acct, broker: true, cash, settled: cash, unsettled: 0, nextSettle: null, pendingDeposits: 0, holdsCash: 0, holdsShares: {}, lmv, lmvMarginable: lmvM, equity: +a.equity || equity, debit, maint: +a.maintenance_margin || maint, excess: (+a.equity || equity) - (+a.maintenance_margin || maint),
      bp: +a.buying_power || 0, bpCrypto: +a.non_marginable_buying_power || Math.max(0, cash), withdrawable: +a.cash_withdrawable || 0, marginOn: +a.multiplier > 1, marginRate: 0, accrued: 0, call: 0, daytrades: +a.daytrade_count || 0, status: a.status };
  }
  // a cash account may spend money from sales that haven't settled (selling what it buys too soon is a violation),
  // unless it's restricted to settled cash for 90 days, or the rule is switched off
  const restricted = real && w.acct === 'cash' && tsMs(w.restricted_until) > Date.now() ? tsMs(w.restricted_until) : null;
  if (!real) { bp = bpCrypto = Math.max(0, cash); withdrawable = Math.max(0, cash); }
  else if (w.acct === 'cash') { const settledOnly = restricted || S.cashRules === false; bp = bpCrypto = Math.max(0, cash - (settledOnly ? unsettled : 0) - holds.cash); withdrawable = Math.max(0, cash - unsettled - holds.cash); }
  else if (IRA_ACCTS.includes(w.acct)) { bp = bpCrypto = Math.max(0, avail); withdrawable = Math.max(0, cash - unsettled - holds.cash); } // IRAs: no borrowing; unsettled sale money can be reused (limited margin)
  else if (w.acct === 'margin' && equity >= 2000 && !isMinor()) {
    marginOn = true;
    const regT = lmvM + 2 * avail, maintCap = (equity - maint) / (prof.maintBase / 100);
    const cap = w.margin_max == null ? Infinity : avail + +w.margin_max;
    bp = Math.max(0, Math.min(regT, maintCap, cap)); bpCrypto = Math.max(0, avail);
    withdrawable = debit > 0 ? 0 : Math.max(0, Math.min(cash - unsettled - holds.cash, equity - maint));
  } else { bp = bpCrypto = Math.max(0, avail); withdrawable = Math.max(0, cash - unsettled - holds.cash); }
  return {
    profile: prof.key, acct: w.acct, restricted, cash, settled: cash - unsettled, unsettled, nextSettle, pendingDeposits: pending, holdsCash: holds.cash, holdsShares: holds.shares, optHold: holds.optCash || 0, optShares: holds.optShares || {}, optVal: pos.optVal || 0,
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
  // price improvement: market makers usually fill retail market orders a little inside the bid and ask (in fractions of a cent)
  let improve = 0;
  if (!crypto && !o.px && !o.noImp && S.priceImprove !== false && quote.ask > quote.bid) {
    improve = Math.round((quote.ask - quote.bid) / 2 * Math.random() * (prof.pi ?? 0.3) * 1e4) / 1e4;
    if (improve > 0) px = side === 'buy' ? px - improve : px + improve;
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
    const oc = acct.optShares?.[sym] || 0;
    if (qty > avail + 1e-9 && oc > 0) fail(`${oc} of your ${sym} shares cover calls you sold${held > oc ? `, and ${+(held - oc).toFixed(6)} are held for open sell orders` : ''}. You can sell up to ${+Math.max(0, avail).toFixed(6)}, or buy the calls back first.`);
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
    if (S.maxPos > 0 && !o.bot && !o.optx && ((p ? p.qty : 0) + qty) * px > equity * S.maxPos / 100 + 0.01) fail(`One position can be at most ${S.maxPos}% of this wallet (${money(equity * S.maxPos / 100)}). Change this in Settings → Trading.`);
    if (S.reservePct > 0 && !o.bot && !o.optx && cashAfter < equity * S.reservePct / 100 - 0.01) fail(`That would leave less than your ${S.reservePct}% cash reserve (${money(equity * S.reservePct / 100)}).`);
  }
  const pnl = side === 'sell' ? (px - p.avg) * qty - fees.total : null;
  return {
    sym, side, qty, px, notional, fees, quote, sess, via, deep, pnl, cashAfter, liq: o.liq || 'taker', improve, imp: improve * qty,
    settle: !crypto && side === 'sell' ? settleTime(prof) : null, borrow: side === 'buy' && cashAfter < 0 ? -cashAfter : 0,
  };
}

const firing = new Set();
const fillCbs = []; // functions to run after every trade
// The heart of the app: buys and sells, with every rule applied.
async function execTrade(wid, sym, side, o = {}) {
  sym = cleanSym(sym); if (!sym) fail('Pick something to trade');
  if (side !== 'buy' && side !== 'sell') fail('Choose buy or sell');
  const halt = haltOf(sym); if (halt && !o.force) fail(halt.why + ' Stock orders wait until trading resumes.');
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
    // a cash wallet selling shares bought with money that hasn't settled yet: a good faith violation (bots skip it)
    const gfv = side === 'sell' ? await gfvRisk(c, w, sym, pl.qty) : null;
    if (gfv && o.bot) fail(`Skipped: selling ${sym} now would be a good faith violation (it was bought with money that settles ${new Date(gfv.settles).toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long' })}).`);
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
    const t = (await c.query(`insert into trd(wid,sym,side,qty,price,why,pnl,fee,note,fees,bid,ask,otype,tif,sess,settle,liq,via,imp)
      values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) returning id, ts`,
      [wid, sym, side, pl.qty, pl.px, o.why || 'manual', pl.pnl == null ? null : round(pl.pnl, 6), pl.fees.total, o.note ? String(o.note).slice(0, 500) : null,
        JSON.stringify(feeMap), pl.quote.bid, pl.quote.ask, o.otype || 'market', o.tif || null, pl.sess, pl.settle ? new Date(pl.settle) : null, pl.liq, pl.via, pl.imp ? round(pl.imp, 6) : null])).rows[0];
    let tax = null;
    if (side === 'buy') await addLot(c, wid, sym, pl.qty, (pl.notional + pl.fees.total) / pl.qty, t.id);
    else tax = await consumeLots(c, o.lotPick ? { ...w, lotPick: o.lotPick } : w, sym, pl.qty, pl.notional - pl.fees.total, t.id);
    if (side === 'buy') await noteUnsettledBuy(c, w, acct, sym, pl.qty, pl.notional + pl.fees.total, t.id);
    let gfvCount = null; if (gfv) gfvCount = await recordGfv(c, w, sym, gfv, t.id);
    if (pl.settle) await c.query('insert into settle(wid,amount,settles,trd_id) values($1,$2,$3,$4)', [wid, round(pl.notional - pl.fees.total, 6), new Date(pl.settle), t.id]);
    return {
      id: t.id, ts: tsMs(t.ts), wid, sym, side, qty: pl.qty, price: pl.px, fee: pl.fees.total, fees: pl.fees.lines, pnl: pl.pnl, pnlPct: pl.pnl != null && p ? pl.pnl / (p.avg * pl.qty) * 100 : null,
      cash: newCash, closed, bid: pl.quote.bid, ask: pl.quote.ask, quoteSrc: pl.quote.src, sess: pl.sess, settle: pl.settle, via: pl.via, borrow: pl.borrow, tax, profile: prof.key,
      stale: prices.get(sym)?.src === 'cache', improve: pl.improve || 0, imp: pl.imp || 0, gfv: gfvCount,
    };
  });
  for (const cb of fillCbs) try { await cb(fill); } catch (e) { warn('after trade', e.message); }
  if (fill.gfv) notify('order', `Good faith violation · ${sym}`, `You sold ${sym} before the money you bought it with settled. That's ${fill.gfv} in the last 12 months.${fill.gfv >= 3 ? ' This wallet can only use settled cash for the next 90 days.' : ' At 3, the wallet is restricted to settled cash for 90 days.'}`, { wid, sym, url: '/#/account' }).catch(() => {});
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
  await loadOptBook();
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
    const fp = o.why ? null : fracProblem(w, { sym, kind, units, amt, tif, sess }); if (fp) fail(fp);
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
  if (haltOf(o.sym)) return false; // halted or paused: stops wait for the reopen
  if (!isReal(w)) return canFire(o.sym, src);
  if (src === 'cache') return false;
  return isCrypto(o.sym) || sessionAt(profOf(w)) === 'regular';
}
function limAllowed(l, src) {
  const w = walletMeta.get(l.wid); if (!w || w.archived || w.broker) return false;
  if (haltOf(l.sym)) return false;
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
  try { mstateTick(sym, p, src); } catch (e) { warn('market state', e.message); }
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
    if (a.kind !== 'price') continue; // chart-signal alerts are checked by their own job
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
// Account-wide brakes for the Alpaca side: the kill switch, and no new buys after a bad day. Selling is always allowed.
function realDay() { const a = BROKER?.account; if (!a) return null; const eq = +a.equity, last = +a.last_equity; return last > 0 ? eq / last - 1 : null; }
function realGuard() {
  if (S.realHalt) fail('Trading on Alpaca is stopped by the kill switch. Resume it in Alpaca controls. Selling still works.');
  const d = realDay();
  if (S.realDailyLossPct > 0 && d != null && d <= -S.realDailyLossPct / 100) fail(`Your Alpaca account is down ${pct(d * 100)} today, past your ${S.realDailyLossPct}% daily limit. No new buys until tomorrow. Selling still works.`);
}
function liveGuard(b, notional) {
  if (BROKER.env !== 'live') return;
  if (b.live !== true) fail('This wallet uses real money. Confirm the order in the app to send it.');
  if (ALP.maxOrder && notional > ALP.maxOrder) fail(`Real-money orders are capped at ${money(ALP.maxOrder)} by ALPACA_MAX_ORDER in Railway.`);
}
async function brokerTrade(w, b) {
  if (standbyPaused()) fail('This is the standby copy and it\'s switched off, so no Alpaca orders go out from here.');
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
  if (side === 'buy') realGuard();
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
    await notify('real', `${BROKER.env === 'live' ? '🔴 LIVE' : '🔵 Alpaca'} · ${u.event === 'fill' ? 'Filled' : 'Partly filled'} · ${sym}`, `${o.side === 'buy' ? 'Bought' : 'Sold'} ${+f.qty.toFixed(6)} ${sym} at ${pxStr(f.price)} on Alpaca (${money_}).`, { sym, wid: BROKER.wid, data: { kind: 'fill', fill: f, label: 'Broker' } });
  } else if (u.event === 'rejected' || u.event === 'expired') {
    brokerSyncSoon();
    await notify('real', `${BROKER.env === 'live' ? '🔴 LIVE' : '🔵 Alpaca'} · Order ${u.event} · ${sym}`, `Alpaca ${u.event} your ${o.side} order for ${sym}.${o.reject_reason ? ' ' + o.reject_reason : ''}`, { sym, wid: BROKER.wid });
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
const PUSH_SETTING = { alert: 'pushAlerts', order: 'pushOrders', summary: 'notifySummary', move: 'notifyMoves', achv: 'notifyAchv', game: 'notifyGames', bot: 'pushBots', real: 'pushReal', brief: 'briefOn', letter: 'letterOn' };
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
// Tax planning: estimates with your bracket and state, the long-term countdown, loss harvesting,
// the $3,000 loss limit and carryover, dividend types, and a Form 8949 / Schedule D style summary.
// Estimates for learning, not tax advice. 2026 figures: IRS Rev. Proc. 2025-32 (brackets, standard deduction),
// IRS capital gains thresholds, New York FY2026 budget (first five rates cut 0.1 point for 2026).
// =====================================================================
const TAX = {
  2026: {
    std: { single: 16100, joint: 32200, hoh: 24150, sep: 16100 },
    ord: { // [top of bracket, rate]
      single: [[12400, .10], [50400, .12], [105700, .22], [201775, .24], [256225, .32], [640600, .35], [Infinity, .37]],
      joint: [[24800, .10], [100800, .12], [211400, .22], [403550, .24], [512450, .32], [768700, .35], [Infinity, .37]],
      hoh: [[17700, .10], [67450, .12], [105700, .22], [201750, .24], [256200, .32], [640600, .35], [Infinity, .37]],
      sep: [[12400, .10], [50400, .12], [105700, .22], [201775, .24], [256225, .32], [384350, .35], [Infinity, .37]],
    },
    ltcg: { single: [49450, 545500], joint: [98900, 613700], hoh: [66200, 579600], sep: [49450, 306850] }, // top of 0%, top of 15%
    niit: { single: 200000, joint: 250000, hoh: 200000, sep: 125000 },
    lossLimit: { single: 3000, joint: 3000, hoh: 3000, sep: 1500 },
    states: {
      NY: { name: 'New York', std: { single: 8000, joint: 16050, hoh: 11200, sep: 8000 },
        brackets: {
          single: [[8500, .039], [11700, .044], [13900, .0515], [80650, .054], [215400, .059], [1077550, .0685], [5e6, .0965], [25e6, .103], [Infinity, .109]],
          joint: [[17150, .039], [23600, .044], [27900, .0515], [161550, .054], [323200, .059], [2155350, .0685], [5e6, .0965], [25e6, .103], [Infinity, .109]],
          hoh: [[12800, .039], [17650, .044], [20900, .0515], [107650, .054], [269300, .059], [1616450, .0685], [5e6, .0965], [25e6, .103], [Infinity, .109]],
        },
        city: { name: 'New York City', brackets: { single: [[12000, .03078], [25000, .03762], [50000, .03819], [Infinity, .03876]], joint: [[21600, .03078], [45000, .03762], [90000, .03819], [Infinity, .03876]], hoh: [[14400, .03078], [30000, .03762], [60000, .03819], [Infinity, .03876]] } } },
    },
    noTax: ['AK', 'FL', 'NV', 'NH', 'SD', 'TN', 'TX', 'WA', 'WY'],
  },
};
const taxYearRules = y => TAX[y] || TAX[Math.max(...Object.keys(TAX).map(Number))];
const bracketTax = (br, x) => { let t = 0, lo = 0; for (const [hi, r] of br) { if (x <= lo) break; t += (Math.min(x, hi) - lo) * r; lo = hi; } return t; };
// federal + state tax on a year: ordinary income, short-term gains (ordinary), long-term gains and qualified dividends (stacked on top)
function taxOf({ ordinary = 0, short = 0, long = 0, qdiv = 0, odiv = 0 }, prof = PROFILE, y = +etDate().slice(0, 4)) {
  const R = taxYearRules(y), f = prof?.filing && R.ord[prof.filing] ? prof.filing : 'single';
  const ord = Math.max(0, ordinary + short + odiv), pref = Math.max(0, long + qdiv);
  const taxable = Math.max(0, ord + pref - R.std[f]), ordTaxable = Math.max(0, Math.min(taxable, ord - R.std[f])), prefTaxable = taxable - ordTaxable;
  let fed = bracketTax(R.ord[f], ordTaxable);
  const [z, m] = R.ltcg[f]; let at = ordTaxable, left = prefTaxable;
  const at0 = Math.max(0, Math.min(left, z - at)); left -= at0; at += at0;
  const at15 = Math.max(0, Math.min(left, m - at)); left -= at15; fed += at15 * 0.15 + left * 0.20;
  const magi = ord + pref, niit = Math.max(0, Math.min(short + long + qdiv + odiv, magi - R.niit[f])) * 0.038;
  let state = 0, city = 0; const st = prof?.state;
  if (st && R.states[st]) {
    const S0 = R.states[st], sf = S0.brackets[f] ? f : 'single', base = Math.max(0, ord + pref - (S0.std[f] || 0)); // New York taxes gains as ordinary income
    state = bracketTax(S0.brackets[sf], base);
    if (prof.city && S0.city) city = bracketTax(S0.city.brackets[sf] || S0.city.brackets.single, base);
  } else if (st && st !== 'none' && !R.noTax.includes(st) && prof?.stateRate > 0) state = Math.max(0, ord + pref) * prof.stateRate / 100;
  return { fed, niit, state, city, total: fed + niit + state + city };
}
// the extra tax this one change adds on top of your income
function taxOn(extra, prof = PROFILE) {
  const base = { ordinary: Math.max(0, prof?.income || 0) }, b = taxOf(base, prof), a = taxOf({ ...base, ...extra, ordinary: base.ordinary + (extra.ordinary || 0) }, prof);
  const out = {}; for (const k of ['fed', 'niit', 'state', 'city', 'total']) out[k] = a[k] - b[k];
  return out;
}
const taxSetupOk = () => !!PROFILE && PROFILE.income != null;
const taxedWallets = async () => (await rows('select * from wallets where not archived and broker is null')).filter(w => !isIra(w));
// realized gains for a year across every taxable wallet, with the loss carried in from earlier years
async function yearGains(y) {
  const ws = await taxedWallets(), ids = ws.map(w => w.id); if (!ids.length) return { short: 0, long: 0, carryIn: 0, ids };
  const all = await rows("select extract(year from sold at time zone 'America/New_York')::int y, term, sum(proceeds - basis + wash) g from realized where wid = any($1) group by 1, 2 order by 1", [ids]);
  const years = [...new Set(all.map(r => r.y))].filter(v => v <= y).sort();
  let carryS = 0, carryL = 0, res = null; const lim = taxYearRules(y).lossLimit[PROFILE?.filing || 'single'] || 3000;
  for (const yy of [...years, ...(years.includes(y) ? [] : [y])]) {
    const s = all.filter(r => r.y === yy && r.term === 'short').reduce((t, r) => t + +r.g, 0), l = all.filter(r => r.y === yy && r.term === 'long').reduce((t, r) => t + +r.g, 0);
    const carryIn = carryS + carryL;
    let ns = s - carryS, nl = l - carryL; // carried losses keep their kind
    const net = ns + nl, deduct = net < 0 ? Math.min(lim, -net) : 0;
    if (yy === y) res = { short: s, long: l, carryIn, netShort: ns, netLong: nl, net, deduct, carryOut: net < 0 ? -net - deduct : 0, ids };
    // what's left of a net loss carries into next year, short-term first
    if (net < 0) { let rest = -net - deduct; const sh = Math.min(rest, Math.max(0, -ns)); carryS = sh; carryL = rest - sh; } else { carryS = 0; carryL = 0; }
  }
  return res;
}
// dividends: qualified if the shares were held more than 60 days around the ex-date (we use the pay date as the ex-date);
// bond, T-bill and real-estate funds pay ordinary dividends
const ORD_DIV = new Set(['TLT', 'IEF', 'SHY', 'BIL', 'SGOV', 'AGG', 'BND', 'TIP', 'VNQ', 'LQD', 'HYG', 'JNK', 'MUB', 'O', 'XLRE']);
const INTL_FUNDS = new Set(['EFA', 'EEM', 'VEA', 'VWO', 'VXUS', 'IEFA', 'IEMG', 'VT', 'ACWI']);
async function dividendsYear(y, ids) {
  const from = new Date(etToUtc(y, 1, 1)), to = new Date(etToUtc(y + 1, 1, 1));
  const divs = await rows("select * from trd where wid = any($1) and side='div' and ts >= $2 and ts < $3 order by ts", [ids, from, to]);
  const out = [];
  for (const d of divs) {
    const amt = +d.qty * +d.price, ex = tsMs(d.ts);
    // oldest shares of it held at the time (still held, or sold later)
    const lot = await row('select min(ts) t from (select ts from lots where wid=$1 and sym=$2 and ts < $3 union all select acquired ts from realized where wid=$1 and sym=$2 and acquired < $3 and sold > $3) x', [d.wid, d.sym, new Date(ex)]);
    const held = lot?.t ? (Math.min(Date.now(), ex + 60 * 864e5) - tsMs(lot.t)) / 864e5 : 0;
    const kind = ORD_DIV.has(d.sym) ? 'ordinary' : held > 60 ? 'qualified' : 'ordinary';
    out.push({ sym: d.sym, ts: ex, amount: amt, kind, why: ORD_DIV.has(d.sym) ? 'Bond, T-bill and real-estate funds pay ordinary dividends' : held > 60 ? 'Held more than 60 days around the ex-date' : `Held ${Math.max(0, Math.floor(held))} days around the ex-date (needs more than 60)`, intl: INTL_FUNDS.has(d.sym) });
  }
  return out;
}
// funds to hold while waiting out a wash sale: similar, but tracking a different index or a wider group
const SWAP = { SPY: 'SCHX', VOO: 'SCHX', IVV: 'SCHX', QQQ: 'VGT', VTI: 'SCHB', IWM: 'VB', DIA: 'SPY', EFA: 'VEA', EEM: 'VWO', TLT: 'VGLT', IEF: 'VGIT', GLD: 'GLDM', XLK: 'VGT', XLF: 'VFH', XLE: 'VDE', XLV: 'VHT', XLY: 'VCR', XLI: 'VIS', XLU: 'VPU', XLP: 'VDC', XLB: 'VAW', XLC: 'VOX', VNQ: 'SCHH', BTC: 'ETH', ETH: 'BTC' };
const SECTOR_ETF = { Technology: 'XLK', 'Information Technology': 'XLK', Semiconductors: 'SMH', 'Financial Services': 'XLF', Financials: 'XLF', Banking: 'XLF', Healthcare: 'XLV', 'Health Care': 'XLV', Pharmaceuticals: 'XLV', Biotechnology: 'XBI', Energy: 'XLE', 'Consumer Cyclical': 'XLY', Retail: 'XLY', Automobiles: 'XLY', 'Consumer Defensive': 'XLP', Beverages: 'XLP', 'Food Products': 'XLP', Industrials: 'XLI', 'Aerospace & Defense': 'ITA', Utilities: 'XLU', 'Communication Services': 'XLC', Media: 'XLC', 'Real Estate': 'XLRE', 'Basic Materials': 'XLB' };
const swapFor = sym => SWAP[sym] || SECTOR_ETF[assets.get(sym)?.industry] || 'SPY';
const daysToLong = acq => { const a = etParts(tsMs(acq)), ann = Date.UTC(a.y + 1, a.m - 1, a.d + 1), now = Date.parse(etDate() + 'T00:00:00Z'); return Math.ceil((ann - now) / 864e5); };
async function taxPlan(y = +etDate().slice(0, 4)) {
  const ws = await taxedWallets(), ids = ws.map(w => w.id), wname = Object.fromEntries(ws.map(w => [w.id, `${w.emoji} ${w.name}`]));
  const g = await yearGains(y), divs = await dividendsYear(y, ids);
  const qdiv = divs.filter(d => d.kind === 'qualified').reduce((t, d) => t + d.amount, 0), odiv = divs.filter(d => d.kind === 'ordinary').reduce((t, d) => t + d.amount, 0);
  const inc = await row("select coalesce(sum(case when side='int' and price>0 then price end),0) intr from trd where wid = any($1) and ts >= $2 and ts < $3", [ids, new Date(etToUtc(y, 1, 1)), new Date(etToUtc(y + 1, 1, 1))]);
  // the year so far: short-term net is taxed like pay, long-term net at the lower rates; a net loss takes up to $3,000 off your income
  const st = Math.max(0, g.netShort + Math.min(0, g.netLong)), lt = Math.max(0, g.netLong + Math.min(0, g.netShort));
  const est = taxOn({ short: st, long: lt, qdiv, odiv: odiv + +inc.intr, ordinary: -(g.deduct || 0) });
  // every open lot: how long until it's long term, and what that saves; losses you could harvest
  const lots = await rows("select l.*, w.name wn from lots l join wallets w on w.id=l.wid where l.wid = any($1) and l.qty > 0 order by l.ts", [ids]);
  const countdown = [], bySym = {};
  for (const l of lots) {
    const px = priceOf(l.sym); if (!px) continue;
    const qty = +l.qty, gain = (px - +l.basis) * qty, days = daysToLong(l.ts), k = l.wid + ':' + l.sym;
    (bySym[k] ||= { wid: l.wid, wallet: wname[l.wid], sym: l.sym, qty: 0, basis: 0, value: 0, lots: [] });
    bySym[k].qty += qty; bySym[k].basis += +l.basis * qty; bySym[k].value += px * qty; bySym[k].lots.push({ qty, gain, days, short: days > 0 });
    if (days > 0 && gain > 1) {
      const asShort = taxOn({ short: gain }).total, asLong = taxOn({ long: gain }).total;
      countdown.push({ wid: l.wid, wallet: wname[l.wid], sym: l.sym, qty, gain, days, longOn: (() => { const a = etParts(tsMs(l.ts)); return new Date(Date.UTC(a.y + 1, a.m - 1, a.d + 1)).toISOString().slice(0, 10); })(), save: Math.max(0, asShort - asLong), taxNow: asShort });
    }
  }
  countdown.sort((a, b) => a.days - b.days || b.save - a.save);
  const harvest = [];
  for (const p of Object.values(bySym)) {
    const loss = p.value - p.basis; if (loss > -5) continue;
    const shortLoss = p.lots.filter(l => l.short && l.gain < 0).reduce((t, l) => t + l.gain, 0), longLoss = p.lots.filter(l => !l.short && l.gain < 0).reduce((t, l) => t + l.gain, 0);
    const saves = -(taxOn({ short: st + shortLoss, long: lt + longLoss, qdiv, odiv }).total - taxOn({ short: st, long: lt, qdiv, odiv }).total);
    const recent = await row("select max(ts) t, sum(qty) q from lots where wid=$1 and sym=$2 and ts > now() - interval '30 days'", [p.wid, p.sym]);
    const crypto = isCrypto(p.sym), swap = swapFor(p.sym);
    harvest.push({ wid: p.wid, wallet: p.wallet, sym: p.sym, qty: p.qty, loss, value: p.value, saves: Math.max(0, saves), swap: swap === p.sym ? null : swap, crypto,
      wash: !crypto && recent?.t ? { bought: tsMs(recent.t), qty: +recent.q, text: `You bought ${+(+recent.q).toFixed(4)} ${p.sym} in the last 30 days: selling those at a loss now is a wash sale, so the loss moves into the newer shares.` } : null,
      rebuyAfter: crypto ? null : new Date(Date.now() + 31 * 864e5).toISOString().slice(0, 10) });
  }
  harvest.sort((a, b) => b.saves - a.saves || a.loss - b.loss);
  return { y, setup: taxSetupOk(), profile: profileJson(), wallets: ws.length, gains: g, dividends: { qualified: qdiv, ordinary: odiv, list: divs }, interest: +inc.intr, estimate: est, countdown: countdown.slice(0, 40), harvest: harvest.slice(0, 30),
    note: 'Estimates for learning, not tax advice. They assume the income you entered, the standard deduction and no other gains or credits.' };
}
GET('/api/taxes/plan', auth, async req => taxPlan(clamp(num(req.query.y, +etDate().slice(0, 4)), 2000, 2100)));
// one step: sell the loser, buy the stand-in with the money
POST('/api/taxes/harvest', auth, async req => {
  const wid = +req.body.wid, sym = cleanSym(req.body.sym), w = await walletRow(wid); if (isIra(w)) fail('Losses inside an IRA don\'t count for taxes');
  const p = (await positions(wid)).find(x => x.sym === sym); if (!p) fail(`You don't own ${sym} in ${w.name}`);
  const sell = await execTrade(wid, sym, 'sell', { pct: 100, why: 'tax-loss harvest', note: 'Tax-loss harvest' });
  let buy = null; const swap = req.body.swap === false ? null : cleanSym(req.body.swap || swapFor(sym));
  if (swap && swap !== sym) {
    if (!assets.has(swap)) await trackAsset(swap, isCrypto(swap) ? 'crypto' : 'stock').catch(() => {});
    for (let i = 0; i < 12 && !priceOf(swap); i++) await sleep(500); // a new symbol's first price takes a moment
    try { buy = await execTrade(wid, swap, 'buy', { usd: Math.floor(sell.qty * sell.price * 0.995 * 100) / 100, why: 'tax-loss harvest swap', note: `Holding while ${sym}'s wash-sale window passes` }); }
    catch (e) { return { sell, buy: null, buyError: e.message }; }
  }
  return { sell, buy, rebuyAfter: new Date(Date.now() + 31 * 864e5).toISOString().slice(0, 10) };
});
// Form 8949 boxes: A/D = basis reported to the IRS (stocks), C/F = not reported (crypto)
async function form8949(y) {
  const ws = await taxedWallets(), ids = ws.map(w => w.id), from = new Date(etToUtc(y, 1, 1)), to = new Date(etToUtc(y + 1, 1, 1));
  const rs = await rows('select r.*, w.name wn from realized r join wallets w on w.id=r.wid where r.wid = any($1) and r.sold >= $2 and r.sold < $3 order by r.sold, r.id', [ids, from, to]);
  const box = r => r.term === 'short' ? (r.crypto ? 'C' : 'A') : (r.crypto ? 'F' : 'D');
  const lines = rs.map(r => ({ box: box(r), desc: `${+(+r.qty).toFixed(8)} ${r.sym}`, wallet: r.wn, acquired: tsMs(r.acquired), sold: tsMs(r.sold), proceeds: +r.proceeds, basis: +r.basis, adj: +r.wash, code: +r.wash > 0 ? 'W' : '', gain: +r.proceeds - +r.basis + +r.wash }));
  const tot = b => { const l = lines.filter(x => x.box === b); return { count: l.length, proceeds: l.reduce((t, x) => t + x.proceeds, 0), basis: l.reduce((t, x) => t + x.basis, 0), adj: l.reduce((t, x) => t + x.adj, 0), gain: l.reduce((t, x) => t + x.gain, 0) }; };
  const boxes = Object.fromEntries(['A', 'C', 'D', 'F'].map(b => [b, tot(b)])), g = await yearGains(y);
  return { y, lines, boxes, scheduleD: { line7: g?.netShort ?? boxes.A.gain + boxes.C.gain, line15: g?.netLong ?? boxes.D.gain + boxes.F.gain, net: g?.net ?? 0, deduct: g?.deduct ?? 0, carryOut: g?.carryOut ?? 0, carryIn: g?.carryIn ?? 0 } };
}
GET('/api/taxes/8949', auth, async req => form8949(clamp(num(req.query.y, +etDate().slice(0, 4)), 2000, 2100)));
// estimated tax on one sale, for the sell screen
function saleTaxEst(st, lt) { if (!PROFILE) return null; const t = taxOn({ short: st, long: lt }); return t.total; }
// =====================================================================
// Account types and the rules that come with them
// - Your profile: date of birth (asked once), filing status, income. Real ages drive the age rules.
// - Cash accounts: you can buy with money from a sale that hasn't settled, but selling that new position before
//   the money settles is a good faith violation. 3 in 12 months restricts the wallet to settled cash for 90 days.
// - IRA and Roth IRA wallets: a yearly contribution limit shared across all of them, no margin or shorting,
//   no taxes on trades inside, withdrawals with the 10% early penalty, required withdrawals for traditional IRAs.
// Figures for 2026 (IRS Notice 2025-67; IRS RMD FAQs).
// =====================================================================
const RET_RULES = {
  2026: { limit: 7500, catchUp: 1100, roth: { single: [153000, 168000], hoh: [153000, 168000], joint: [242000, 252000], sep: [0, 10000] } },
  2025: { limit: 7000, catchUp: 1000, roth: { single: [150000, 165000], hoh: [150000, 165000], joint: [236000, 246000], sep: [0, 10000] } },
};
const retRules = y => RET_RULES[y] || RET_RULES[Math.max(...Object.keys(RET_RULES).map(Number).filter(k => k <= y))] || RET_RULES[2026];
// IRS Uniform Lifetime Table (2022 onward): the divisor for required minimum distributions
const ULT = { 72: 27.4, 73: 26.5, 74: 25.5, 75: 24.6, 76: 23.7, 77: 22.9, 78: 22.0, 79: 21.1, 80: 20.2, 81: 19.4, 82: 18.5, 83: 17.7, 84: 16.8, 85: 16.0, 86: 15.2, 87: 14.4, 88: 13.7, 89: 12.9, 90: 12.2, 91: 11.5, 92: 10.8, 93: 10.1, 94: 9.5, 95: 8.9, 96: 8.4, 97: 7.8, 98: 7.3, 99: 6.8, 100: 6.4 };
const IRA_ACCTS = ['ira', 'roth'];
const isIra = w => IRA_ACCTS.includes(w?.acct);
// ---------- profile ----------
let PROFILE = null;
async function loadProfile() { try { PROFILE = JSON.parse(await kvGet('profile', 'null') || 'null'); } catch (e) { PROFILE = null; } return PROFILE; }
const profileJson = () => PROFILE ? { dob: PROFILE.dob, filing: PROFILE.filing || 'single', income: PROFILE.income ?? null, earned: PROFILE.earned ?? null, state: PROFILE.state || null, city: !!PROFILE.city, stateRate: PROFILE.stateRate ?? null, age: ageOn(), set: true } : { set: false };
// age in whole years on a date (ET)
function ageOn(iso = etDate()) { if (!PROFILE?.dob) return null; const [y, m, d] = PROFILE.dob.split('-').map(Number), [Y, M, D] = iso.split('-').map(Number); return Y - y - ((M < m || (M === m && D < d)) ? 1 : 0); }
// the date someone turns 59½
function halfAgeDate() { if (!PROFILE?.dob) return null; const [y, m, d] = PROFILE.dob.split('-').map(Number); const t = new Date(Date.UTC(y + 59, m - 1 + 6, d)); return t.toISOString().slice(0, 10); }
const pastHalf = (iso = etDate()) => { const h = halfAgeDate(); return h ? iso >= h : false; };
const rmdAge = () => { if (!PROFILE?.dob) return 73; return +PROFILE.dob.slice(0, 4) >= 1960 ? 75 : 73; }; // SECURE 2.0: 73 now, 75 for people born in 1960 or later
const isMinor = () => { const a = ageOn(); return a != null && a < 18; };
POST('/api/profile', auth, async req => {
  const b = req.body || {}, cur = PROFILE || {};
  const dob = b.dob !== undefined ? String(b.dob) : cur.dob;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dob || '') || Number.isNaN(Date.parse(dob + 'T12:00:00Z'))) fail('Enter your date of birth');
  const yr = +dob.slice(0, 4), now = +etDate().slice(0, 4); if (yr < now - 110 || dob > etDate()) fail('Check your date of birth');
  if (cur.dob && cur.dob !== dob && !b.confirmDob) fail('Changing your date of birth changes your age rules. Confirm to change it.');
  const filing = ['single', 'joint', 'hoh', 'sep'].includes(b.filing) ? b.filing : cur.filing || 'single';
  const money0 = v => v === null || v === '' ? null : clamp(num(v, 0), 0, 1e9);
  const STATES = ['none', 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'OT'];
  const state = b.state !== undefined ? (STATES.includes(b.state) ? b.state : 'none') : cur.state || null;
  PROFILE = { dob, filing, income: b.income !== undefined ? money0(b.income) : cur.income ?? null, earned: b.earned !== undefined ? money0(b.earned) : cur.earned ?? null,
    state, city: b.city !== undefined ? !!b.city : !!cur.city, stateRate: b.stateRate !== undefined ? clamp(num(b.stateRate, 0), 0, 15) : cur.stateRate ?? null, at: cur.at || Date.now() };
  await kvSet('profile', JSON.stringify(PROFILE));
  // under 18: no margin (brokers require an adult for margin); margin wallets act as cash-and-instant
  broadcast({ t: 'evt', e: 'settings' });
  return { profile: profileJson() };
});
GET('/api/profile', auth, () => ({ profile: profileJson() }));

// ---------- cash accounts: good faith violations ----------
const cashRulesOn = () => S.cashRules !== false;
async function restrictedUntil(wid, c = pool) { const r = (await c.query('select restricted_until t from wallets where id=$1', [wid])).rows[0]; const t = tsMs(r?.t); return t && t > Date.now() ? t : null; }
// a buy in a cash wallet paid partly with money from sales that haven't settled: remember it
async function noteUnsettledBuy(c, w, acct, sym, qty, need, trdId) {
  if (w.acct !== 'cash' || !cashRulesOn()) return;
  const settledFree = acct.cash - acct.unsettled - acct.holdsCash;
  const used = Math.min(need, need - Math.max(0, settledFree)); if (!(used > 0.005)) return;
  // the money settles when the newest of the sales it came from settles
  const st = (await c.query('select max(settles) t from settle where wid=$1 and settles > now()', [w.id])).rows[0];
  if (!st?.t) return;
  await c.query("insert into cashfund(wid,sym,qty,amount,settles,trd_id) values($1,$2,$3,$4,$5,$6)", [w.id, sym, qty * used / need, used, st.t, trdId]);
}
// would selling this many shares now be a good faith violation? → { qty, settles } or null
async function gfvRisk(c, w, sym, qty) {
  if (w?.acct !== 'cash' || !cashRulesOn() || isCrypto(sym)) return null;
  const rows0 = (await c.query('select * from cashfund where wid=$1 and sym=$2 and settles > now() and qty > 0 order by id', [w.id, sym])).rows; if (!rows0.length) return null;
  const pos = (await c.query('select qty from pos where wid=$1 and sym=$2', [w.id, sym])).rows[0], held = pos ? +pos.qty : 0;
  // shares bought with unsettled money are sold last (the paid-for ones go first)
  const risky = rows0.reduce((t, r) => t + +r.qty, 0), safe = Math.max(0, held - risky);
  const over = qty - safe; if (over <= 1e-9) return null;
  return { qty: Math.min(over, risky), settles: tsMs(rows0[rows0.length - 1].settles) };
}
async function recordGfv(c, w, sym, risk, trdId) {
  await c.query("insert into cashviol(wid,kind,sym,note,trd_id) values($1,'gfv',$2,$3,$4)", [w.id, sym, `Sold ${+risk.qty.toFixed(6)} ${sym} bought with money that settles ${new Date(risk.settles).toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'short', month: 'short', day: 'numeric' })}`, trdId]);
  // use up the rows behind the shares just sold
  let left = risk.qty; for (const r of (await c.query('select * from cashfund where wid=$1 and sym=$2 and settles > now() and qty > 0 order by id', [w.id, sym])).rows) { if (left <= 1e-12) break; const t = Math.min(+r.qty, left); await c.query('update cashfund set qty=qty-$1 where id=$2', [t, r.id]); left -= t; }
  const n = +(await c.query("select count(*) n from cashviol where wid=$1 and kind='gfv' and ts > now() - interval '12 months'", [w.id])).rows[0].n;
  if (n >= 3) await c.query("update wallets set restricted_until = now() + interval '90 days' where id=$1 and (restricted_until is null or restricted_until < now() + interval '90 days')", [w.id]);
  return n;
}
async function violationsJson(wid) {
  const v = await rows("select * from cashviol where wid=$1 and ts > now() - interval '12 months' order by id desc", [wid]);
  return { list: v.map(x => ({ id: x.id, kind: x.kind, sym: x.sym, note: x.note, ts: tsMs(x.ts), dropsOff: tsMs(x.ts) + 365 * 864e5 })), count: v.filter(x => x.kind === 'gfv').length, restrictedUntil: await restrictedUntil(wid) };
}
GET('/api/violations', auth, async req => violationsJson(await activeWalletId(req)));

// ---------- IRA and Roth IRA ----------
async function contribYear(year, c = pool) { return +(await c.query("select coalesce(sum(amount),0) s from contrib where year=$1 and kind='contribution'", [year])).rows[0].s; }
// how much more can go into IRAs this year
async function contribRoom(w, year = +etDate().slice(0, 4), c = pool) {
  const R = retRules(year), age = PROFILE?.dob ? (year - +PROFILE.dob.slice(0, 4)) : null; // age at year end
  let cap = R.limit + (age != null && age >= 50 ? R.catchUp : 0), why = [`${year} limit ${money(R.limit)}${age != null && age >= 50 ? ` + ${money(R.catchUp)} catch-up at 50+` : ''}`];
  if (PROFILE?.earned != null) { if (PROFILE.earned < cap) why.push(`capped at your earned income of ${money(PROFILE.earned)}`); cap = Math.min(cap, PROFILE.earned); }
  else if (isMinor()) { cap = 0; why.push('a custodial IRA needs earned income: add yours in Settings → Profile'); }
  let rothCap = cap;
  if (w?.acct === 'roth' && PROFILE?.income != null) {
    const [lo, hi] = R.roth[PROFILE.filing || 'single'] || R.roth.single;
    if (PROFILE.income >= hi) { rothCap = 0; why.push(`your income is over the ${money(hi)} Roth limit`); }
    else if (PROFILE.income > lo) { let r = cap * (hi - PROFILE.income) / (hi - lo); r = Math.ceil(r / 10) * 10; if (r < 200) r = 200; rothCap = Math.min(cap, r); why.push(`reduced to ${money(rothCap)} by your income`); }
  }
  const used = await contribYear(year, c), lim = w?.acct === 'roth' ? rothCap : cap;
  return { year, limit: lim, used, left: Math.max(0, lim - used), why };
}
// called before money goes into an IRA wallet
async function checkContribution(c, w, amount) {
  if (!isIra(w)) return;
  const room = await contribRoom(w, undefined, c);
  if (amount > room.left + 0.005) fail(room.left > 0 ? `You can put ${money(room.left)} more into IRAs in ${room.year} (${room.why.join('; ')}). This year you've put in ${money(room.used)}.` : `You've hit your ${room.year} IRA limit (${room.why.join('; ')}). It resets on January 1.`);
}
async function noteContribution(c, w, amount, note = 'deposit') {
  if (!isIra(w)) return;
  await c.query("insert into contrib(wid,year,amount,kind,note) values($1,$2,$3,'contribution',$4)", [w.id, +etDate().slice(0, 4), amount, note]);
  await c.query('update wallets set ira_opened = coalesce(ira_opened, now()), ira_basis = coalesce(ira_basis,0) + $1 where id=$2', [amount, w.id]);
}
// what taking money out of an IRA costs: the 10% early penalty and income tax, as estimates
function iraWithdrawCost(w, amount) {
  const today = etDate(), early = !pastHalf(today), out = { amount, early, penalty: 0, taxable: 0, free: 0, lines: [] };
  if (!PROFILE?.dob) out.lines.push('Add your date of birth in Settings → Profile so the age rules are right.');
  if (w.acct === 'ira') {
    out.taxable = amount; if (early) out.penalty = amount * 0.10;
    out.lines.push(`All of it counts as income on your taxes${early ? ', plus a 10% early-withdrawal penalty because you are under 59½' : ''}.`);
  } else {
    const basis = Math.max(0, +w.ira_basis || 0), fromBasis = Math.min(amount, basis), earn = amount - fromBasis;
    const fiveYears = w.ira_opened && (Date.now() - tsMs(w.ira_opened)) >= 5 * 365.25 * 864e5;
    out.free = fromBasis;
    if (earn > 0.005) { if (!early && fiveYears) out.free += earn; else { out.taxable = earn; out.penalty = early ? earn * 0.10 : 0; } }
    out.lines.push(fromBasis > 0 ? `${money(fromBasis)} is money you put in, so it comes out tax-free and penalty-free.` : 'You have no contributions left to take out first.');
    if (earn > 0.005) out.lines.push(!early && fiveYears ? 'The rest is earnings, tax-free because you are over 59½ and the account is 5+ years old.' : `The other ${money(earn)} is earnings: taxed as income${early ? ', plus the 10% penalty' : ''}${!fiveYears ? ', because the account is under 5 years old' : ''}.`);
  }
  return out;
}
async function noteWithdrawal(c, w, amount) {
  if (!isIra(w)) return null;
  const cost = iraWithdrawCost(w, amount);
  await c.query("insert into contrib(wid,year,amount,kind,note,penalty,tax) values($1,$2,$3,'withdrawal',$4,$5,$6)", [w.id, +etDate().slice(0, 4), amount, cost.lines.join(' '), round(cost.penalty, 2), round(cost.taxable, 2)]);
  if (w.acct === 'roth') await c.query('update wallets set ira_basis = greatest(0, coalesce(ira_basis,0) - $1) where id=$2', [Math.min(amount, +w.ira_basis || 0), w.id]);
  return cost;
}
// required minimum distributions for traditional IRAs
async function rmdFor(w) {
  if (w.acct !== 'ira' || !PROFILE?.dob) return null;
  const by = +PROFILE.dob.slice(0, 4), startAge = rmdAge(), firstYear = by + startAge, year = +etDate().slice(0, 4);
  if (year < firstYear) return { due: false, startYear: firstYear, startAge, text: `Required withdrawals start in ${firstYear}, the year you turn ${startAge}.` };
  const age = year - by, factor = ULT[Math.min(100, Math.max(72, age))];
  const dec31 = await row('select eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [w.id, new Date(etToUtc(year, 1, 1))]);
  const bal = dec31 ? +dec31.eq : +w.cash + marketValue(await positions(w.id));
  const amount = bal / factor, taken = +(await row("select coalesce(sum(amount),0) s from contrib where wid=$1 and year=$2 and kind='withdrawal'", [w.id, year])).s;
  const deadline = year === firstYear ? `${year + 1}-04-01` : `${year}-12-31`;
  return { due: true, year, age, factor, balance: bal, amount, taken, left: Math.max(0, amount - taken), deadline, text: `Take at least ${money(amount)} out by ${deadline} (last year-end balance ${money(bal)} ÷ ${factor}). Missing it costs a 25% excise tax on the shortfall, 10% if fixed within 2 years.` };
}
async function iraJson(w) {
  if (!isIra(w)) return null;
  const year = +etDate().slice(0, 4), room = await contribRoom(w, year), hist = await rows('select * from contrib where wid=$1 order by id desc limit 20', [w.id]);
  return { kind: w.acct, room, basis: +w.ira_basis || 0, opened: tsMs(w.ira_opened), rmd: await rmdFor(w), halfAge: halfAgeDate(), pastHalf: pastHalf(), age: ageOn(),
    history: hist.map(h => ({ ts: tsMs(h.ts), kind: h.kind, amount: +h.amount, note: h.note, penalty: h.penalty == null ? null : +h.penalty, tax: h.tax == null ? null : +h.tax })) };
}
GET('/api/ira', auth, async req => { const w = await walletRow(await activeWalletId(req)); return { ira: await iraJson(w), profile: profileJson() }; });
POST('/api/ira/preview', auth, async req => { const w = await walletRow(await activeWalletId(req)); if (!isIra(w)) fail('This is not an IRA wallet'); return iraWithdrawCost(w, num(req.body.amount)); });

// =====================================================================
// Money movement and margin: bank transfers, instant deposits, margin interest, margin calls
// =====================================================================
// Bank deposits arrive in a few business days. Instant and margin accounts can use part of a
// pending deposit right away (Robinhood Instant Deposits); cash accounts wait for the money.
async function deposit(wid, amount, how = 'bank') {
  const w = await walletRow(wid);
  if (w.broker) fail('Move money in and out with your broker. This wallet mirrors that account.');
  if (!(amount > 0) || amount > 1e9) fail('Enter a valid amount');
  if (isIra(w)) await checkContribution(pool, w, amount);
  if (!isReal(w) || how === 'practice') {
    await tx(async c => { await moveCash(c, wid, amount, 'deposit'); await noteContribution(c, w, amount); });
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
    await noteContribution(c, w, amount);
    return r;
  });
  return { done: false, id: x.id, instant, pending: amount - instant, avail };
}
async function withdraw(wid, amount, how = 'bank') {
  const w = await walletRow(wid);
  if (w.broker) fail('Move money in and out with your broker. This wallet mirrors that account.');
  if (!(amount > 0)) fail('Enter a valid amount');
  if (!isReal(w) || how === 'practice') { let cost = null; await tx(async c => { await moveCash(c, wid, -amount, 'withdrawal'); cost = await noteWithdrawal(c, w, amount); }); return { done: true, ira: cost }; }
  const acct = await accountInfo(wid, { w });
  if (amount > acct.withdrawable + 0.005) fail(`You can withdraw up to ${money(acct.withdrawable)}.${acct.unsettled > 0 ? ` ${money(acct.unsettled)} from recent stock sales hasn't settled yet.` : ''}${acct.debit > 0 ? ' Pay off your margin balance first.' : ''}${acct.holdsCash > 0 ? ` ${money(acct.holdsCash)} is held for open orders.` : ''}`);
  const prof = profOf(w), avail = businessDaysLater(prof.wdDays);
  let cost = null;
  await tx(async c => {
    await moveCash(c, wid, -amount, 'withdrawal');
    await c.query("insert into xfer(wid,kind,amount,status,avail,method) values($1,'withdrawal',$2,'pending',$3,'ach')", [wid, amount, new Date(avail)]);
    cost = await noteWithdrawal(c, w, amount);
  });
  return { done: false, avail, ira: cost };
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
    const w = (await c.query('select * from wallets where id=$1', [x.wid])).rows[0];
    if (isIra(w)) { await c.query("insert into contrib(wid,year,amount,kind,note) values($1,$2,$3,'contribution','deposit cancelled')", [x.wid, +etDate(tsMs(x.created)).slice(0, 4), -+x.amount]); await c.query('update wallets set ira_basis = greatest(0, ira_basis - $1) where id=$2', [+x.amount, x.wid]); }
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
  const order = { fifo: 'ts, id', lifo: 'ts desc, id desc', hifo: 'basis desc, ts' }[w.lotPick || w.lot_method] || 'ts, id'; // lotPick: a tax-aware bot sells its highest-cost shares first
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
  const wq = await walletRow(wid);
  return { sheltered: isIra(wq) ? wq.acct : null, y, years: [...new Set(years)].sort((a, b) => b - a), short: group(r => r.term === 'short' && !r.crypto), long: group(r => r.term === 'long' && !r.crypto), cryptoShort: group(r => r.crypto && r.term === 'short'), cryptoLong: group(r => r.crypto && r.term === 'long'), all: group(() => true), dividends: +inc.div, interest: +inc.intr, marginInterest: +inc.mint, fees: +inc.fees, rows: rs };
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
    improved: trades.reduce((s, t) => s + (t.imp || 0), 0), improvedYear: +(await row('select coalesce(sum(imp),0) s from trd where wid=$1 and ts >= $2 and ts < $3', [wid, new Date(etToUtc(y, 1, 1)), to])).s,
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
  ['w_ask', 'Ask the Council about 3 stocks', c => c.wAsk, 3], ['w_whatif', 'Try the what-if machine twice', c => c.wWhatif, 2],
  ['w_opts', 'Make an options trade', c => c.wOpts, 1], ['w_signal', 'Set a chart-signal alert', c => c.wSignal, 1],
  ['w_review', 'Read your monthly review', c => c.wReview, 1], ['w_bot', 'Start a new bot', c => c.wBot, 1],
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
    wAsk: await chalCount('ask'), wWhatif: await chalCount('whatif'), wReview: await chalCount('review'),
    wOpts: +(await row('select count(*) n from opttrd where ts >= $1', [new Date(ws)]).catch(() => ({ n: 0 }))).n,
    wSignal: +(await row("select count(*) n from alert where kind='ind' and ts >= $1", [new Date(ws)])).n,
    wBot: +(await row('select count(*) n from bots where created >= $1', [new Date(ws)])).n,
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
  // Google News covers headlines too; skip Finnhub when its queue is busy so prices keep flowing.
  if (!cache.get('news:' + sym)?.v && fhQueue.length > 6) return [];
  try {
    const list = await Promise.race([assetNews(sym), sleep(4000).then(() => { throw new Error('Finnhub is busy'); })]);
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
// Upcoming earnings for every company in one request (cached 6 hours), so scanning many stocks
// doesn't use up the Finnhub limit that live prices also need.
async function earningsCal() {
  return cached('earncal', 6 * 3600e3, async () => {
    const d = await fhApi(`calendar/earnings?from=${etDate()}&to=${etDate(Date.now() + 30 * 864e5)}`, 1);
    const m = {};
    for (const x of (d?.earningsCalendar || []).sort((a, b) => a.date.localeCompare(b.date))) if (x.symbol && !m[x.symbol]) m[x.symbol] = { date: x.date, hour: x.hour, eps: x.epsEstimate };
    return m;
  });
}
async function earningsFor(sym) {
  const ins = cache.get('ins:' + sym)?.v; // the stock page already looked it up
  if (ins?.next?.date >= etDate()) return { next: ins.next };
  try { const m = await Promise.race([earningsCal(), sleep(6000).then(() => null)]); return m ? { next: m[sym] || null } : null; } catch (e) { return null; }
}
// ---------- the summary ----------
async function buzzFor(sym) {
  return cached('buzz:' + sym, 10 * 60e3, async () => {
    const [fh, gn, st, rd, ins] = await Promise.all([finnhubNews(sym), googleNews(sym), stocktwits(sym), reddit(sym), isCrypto(sym) ? null : earningsFor(sym)]);
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
// Where daily prices came from lately, shown on the bot health card.
const dataHealth = { yahoo: { ok: null, t: 0, err: null, fails: 0 }, coinbase: { ok: null, t: 0, err: null }, saved: { used: 0, t: 0, syms: [] } };
function parseYahooDaily(r, sym) {
  const z = r.indicators?.quote?.[0] || {}, adj = r.indicators?.adjclose?.[0]?.adjclose, ts = r.timestamp || [];
  const crypto = isCrypto(sym), out = { sym, d: [], o: [], h: [], l: [], c: [], v: [] };
  for (let i = 0; i < ts.length; i++) {
    const c = z.close?.[i]; if (c == null || !(c > 0)) continue;
    const f = adj?.[i] > 0 ? adj[i] / c : 1; // dividend adjustment so returns include dividends
    const d = crypto ? new Date(ts[i] * 1000).toISOString().slice(0, 10) : etDate(ts[i] * 1000);
    if (out.d.length && out.d[out.d.length - 1] === d) { out.c[out.c.length - 1] = c * f; continue; }
    out.d.push(d); out.o.push((z.open?.[i] ?? c) * f); out.h.push((z.high?.[i] ?? c) * f); out.l.push((z.low?.[i] ?? c) * f); out.c.push(c * f); out.v.push(z.volume?.[i] || 0);
  }
  return out;
}
// Crypto backup: Coinbase daily candles, 300 at a time, about 5 years back.
async function coinbaseDaily(sym) {
  const out = { sym, d: [], o: [], h: [], l: [], c: [], v: [] }, all = new Map();
  let end = Date.now();
  for (let k = 0; k < 7; k++) {
    const start = end - 299 * 864e5;
    const d = await getJ(`${CBX}/products/${sym}-USD/candles?granularity=86400&start=${new Date(start).toISOString()}&end=${new Date(end).toISOString()}`);
    if (!Array.isArray(d) || !d.length) break;
    for (const x of d) all.set(x[0], x);
    end = start - 864e5;
  }
  for (const x of [...all.values()].sort((a, b) => a[0] - b[0])) { out.d.push(new Date(x[0] * 1000).toISOString().slice(0, 10)); out.l.push(x[1]); out.h.push(x[2]); out.o.push(x[3]); out.c.push(x[4]); out.v.push(x[5]); }
  return out;
}
async function dailyBars(sym) {
  return cached('d1:' + sym, 6 * 3600e3, async () => {
    let out = null, src = 'yahoo', err = null;
    try { out = parseYahooDaily(await yahooChart(sym, 'range=10y&interval=1d&events=div,split'), sym); dataHealth.yahoo = { ok: true, t: Date.now(), err: null, fails: 0 }; }
    catch (e) { err = e; dataHealth.yahoo = { ...dataHealth.yahoo, ok: false, t: Date.now(), err: e.message, fails: (dataHealth.yahoo.fails || 0) + 1 }; }
    if ((!out || out.d.length < 30) && isCrypto(sym)) {
      try { out = await coinbaseDaily(sym); src = 'coinbase'; dataHealth.coinbase = { ok: true, t: Date.now(), err: null }; } catch (e) { dataHealth.coinbase = { ok: false, t: Date.now(), err: e.message }; }
    }
    if (out && out.d.length >= 30) {
      Q('insert into bars(sym,data,t,src) values($1,$2,now(),$3) on conflict(sym) do update set data=excluded.data, t=now(), src=excluded.src', [sym, JSON.stringify(out), src]).catch(() => {});
      return out;
    }
    // Last resort: the copy saved the last time it worked (good for up to 10 days; today's live price is added on top).
    const saved = await row("select data, t from bars where sym=$1 and t > now() - interval '10 days'", [sym]).catch(() => null);
    if (saved) { dataHealth.saved = { used: dataHealth.saved.used + 1, t: Date.now(), syms: [...new Set([...dataHealth.saved.syms, sym])].slice(-20) }; return JSON.parse(saved.data); }
    fail(`Not enough price history for ${sym}${err ? ' (' + err.message + ')' : ''}`);
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
// money the picks don't use waits in a T-bill fund, when the bot has one in its list
function parkIdle(out, ctx, p, why) {
  const safe = p.safe && p.safe !== 'cash' && ctx.syms.includes(p.safe) ? p.safe : null; if (!safe) return;
  const used = Object.entries(out.weights).filter(([k]) => k !== safe).reduce((t, [, x]) => t + x, 0);
  if (used < 0.98) { out.weights[safe] = Math.min(1, 1 - used); out.notes[safe] = why || `Money not in a trade waits in ${safe} (T-bills) instead of cash.`; }
}
// how much each pick gets: spread over the picks there are (at least half the slots), capped per position
const spreadW = (n, slots, maxW) => Math.min(maxW, 1 / Math.max(n, Math.ceil(slots / 2), 1));
// fit to the wallet: never more positions than the money can fill with a useful size ($25, or twice the smallest trade)
const MIN_SLOT = 25;
const fitSlots = (ctx, n) => { const cap = ctx.capital || 0, min = Math.max(MIN_SLOT, num(ctx.params?.minPos, 0), (ctx.minTrade || 0) * 2); return cap > 0 ? Math.max(1, Math.min(n, Math.floor(cap / min))) : n; };
// broad index, bond and gold funds are already spread out inside, so strategies built on them may hold more than the per-position limit
const FUNDS = new Set(['SPY', 'VOO', 'IVV', 'VTI', 'QQQ', 'IWM', 'DIA', 'EFA', 'VEA', 'EEM', 'VWO', 'VT', 'ACWI', 'AGG', 'BND', 'IEF', 'TLT', 'SHY', 'TIP', 'GLD', 'IAU', 'SGOV', 'BIL', 'DBC', 'VNQ']);
// tiny wallets: the per-position limit never forces a position smaller than a useful size
const walletMaxW = (maxW, capital, minTrade = 0) => capital > 0 ? Math.max(maxW, Math.min(1, Math.max(MIN_SLOT, minTrade * 2) / capital)) : maxW;
const fundCap = (ctx, s) => ctx.params?.fullFund !== 'off' && FUNDS.has(s) ? 1 : ctx.maxW;
const FUND_P = { k: 'fullFund', label: 'Index funds can go past the per-position limit', def: 'on', choices: [['on', 'Yes (they are spread out inside)'], ['off', 'No']] };
// strategies that park idle money need the T-bill fund in their list (not for crypto-only lists: it only trades in stock hours)
const withPark = (syms, p) => p?.safe && p.safe !== 'cash' && syms.length && syms.some(x => !isCrypto(x)) && !syms.includes(p.safe) ? [...syms, p.safe] : syms;
const SAFE_P = { k: 'safe', label: 'Idle money', def: 'SGOV', choices: [['SGOV', 'SGOV (T-bills)'], ['BIL', 'BIL (T-bills)'], ['cash', 'Cash']] };
const FLOW_P = { k: 'flow', label: 'New money', def: 'flow', choices: [['flow', 'Top up what it holds'], ['rebal', 'Only on new trades']] };
const STRATEGIES = {
  trend: {
    name: 'Trend follower', short: 'Trend', icon: 'trail', rebalance: 'none', markets: 'both',
    overlays: { mood: false, exits: false, tighten: false, council: 'advise' },
    desc: 'Owns what is in a real uptrend: above its slow average, the fast average above the slow one, and up over the last 6 months. The strongest trends (return per unit of swing) come first. Each position rides with a trailing stop a few ATRs under its high and is sold when the trend breaks. Money not in a trend waits in T-bills, and new money tops up the holdings.',
    params: [{ k: 'fast', label: 'Fast average (days)', def: 50, min: 5, max: 100 }, { k: 'slow', label: 'Slow average (days)', def: 200, min: 50, max: 300 }, { k: 'atrMult', label: 'Trailing stop (ATRs)', def: 3.5, min: 1, max: 8, step: 0.5 }, { k: 'maxPositions', label: 'Most positions', def: 5, min: 1, max: 20 }, { k: 'sizing', label: 'Sizing', def: 'invvol', choices: [['invvol', 'By volatility'], ['equal', 'Equal']] },
      { k: 'momLen', label: 'Must be up over (days, 0 = off)', def: 126, min: 0, max: 252 }, SAFE_P, FLOW_P],
    grid: { fast: [20, 50], slow: [100, 200], atrMult: [3, 4] },
    need: p => p.slow + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], momLen = num(p.momLen, 0);
      for (const s of ctx.syms) {
        if (s === p.safe) continue;
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.slow) continue;
        const c = b.c[i], sl = IND.sma(b.c, p.slow, i), ef = IND.ema(b.c, p.fast, i, b.memo), es = IND.ema(b.c, p.slow, i, b.memo), atr = IND.atr(b, 14, i, b.memo), mom = IND.ret(b.c, momLen || 126, i) ?? 0, vol = IND.vol(b.c, 63, i) || 0.02;
        const rising = !momLen || mom > 0, up = c > sl && ef > es && rising, held = ctx.held[s];
        out.notes[s] = `Close ${px2(c)} vs ${p.slow}-day average ${px2(sl)}; ${p.fast}-day trend ${ef > es ? 'above' : 'below'} ${p.slow}-day${momLen ? `; ${momLen}-day return ${pf(mom, 1)}` : ''}. ${up ? 'In an uptrend' : 'Not trending up'}.`;
        if (up) cand.push({ s, q: mom / vol, atr, c, held });
        else if (held) out.notes[s] += ' Exit: the trend broke.';
      }
      cand.sort((a, b) => (b.held ? 1 : 0) - (a.held ? 1 : 0) || b.q - a.q);
      const slots = fitSlots(ctx, p.maxPositions), picks = cand.slice(0, slots);
      // with only a couple of trends it doesn't go all in: the picks share at most picks/(half the slots) of the money
      const frac = picks.length / Math.max(picks.length, Math.ceil(slots / 2), 1);
      if (p.sizing === 'invvol') { const w = sizeWeights(picks.map(x => x.s), ctx, 1); for (const k in w) out.weights[k] = Math.min(ctx.maxW, w[k] * frac); }
      else for (const x of picks) out.weights[x.s] = spreadW(picks.length, slots, ctx.maxW);
      for (const x of picks) { const hi = Math.max(x.c, ctx.held[x.s]?.high || 0); out.stops[x.s] = { trail: true, price: hi - p.atrMult * x.atr, atr: x.atr, mult: p.atrMult }; }
      parkIdle(out, ctx, p, `Money not in an uptrend waits in ${p.safe} (T-bills).`);
      return out;
    },
  },
  momentum: {
    name: 'Momentum rotation', short: 'Momentum', icon: 'zap', rebalance: 'monthly', markets: 'both',
    desc: 'Once a month, ranks everything by its return over the past 6 months (skipping the latest month) and holds the top few that are still rising. When the market is below its 200-day average it parks the money in cash or T-bills. Built for small accounts too: it holds fewer positions when there is less money, keeps a winner until it really falls behind, and puts new deposits to work without selling.',
    params: [{ k: 'lookback', label: 'Look back (days)', def: 126, min: 21, max: 252 }, { k: 'skip', label: 'Skip latest (days)', def: 21, min: 0, max: 42 }, { k: 'top', label: 'Hold the top', def: 3, min: 1, max: 10 }, { k: 'filterLen', label: 'Market filter (days)', def: 200, min: 0, max: 300 }, { k: 'sizing', label: 'Sizing', def: 'equal', choices: [['equal', 'Equal'], ['invvol', 'By volatility']] },
      { k: 'minPos', label: 'Smallest position ($)', def: 25, min: 1, max: 5000 }, { k: 'buffer', label: 'Keep a holding until it drops below rank (top +)', def: 1, min: 0, max: 10 },
      { k: 'flow', label: 'New money', def: 'flow', choices: [['flow', 'Top up, sell less'], ['rebal', 'Full monthly rebalance']] }, { k: 'safe', label: 'When the market is weak', def: 'SGOV', choices: [['cash', 'Hold cash'], ['SGOV', 'SGOV (T-bills)'], ['BIL', 'BIL (T-bills)']] }],
    grid: { lookback: [63, 126, 252], top: [2, 3, 5] },
    need: p => Math.max(p.lookback, p.filterLen || 0) + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {}, noTrim: p.flow === 'flow' };
      const bench = ctx.bench && ctx.data[ctx.bench], bi = ctx.idx[ctx.bench];
      const safe = p.safe && p.safe !== 'cash' && ctx.syms.includes(p.safe) ? p.safe : null; // the T-bill fund is only used if it's in the bot's list
      let riskOn = true;
      if (p.filterLen > 0 && bench && bi != null && bi >= p.filterLen) { const bs = IND.sma(bench.c, p.filterLen, bi); riskOn = bench.c[bi] > bs; ctx.regime = `${ctx.bench} ${riskOn ? 'above' : 'below'} its ${p.filterLen}-day average (${px2(bench.c[bi])} vs ${px2(bs)})`; }
      // how many positions the money can carry: each one at least the smallest position size
      // older bots and Autopilot tryouts don't have the small-account settings: they behave exactly as before
      const minPos = num(p.minPos, 1), buffer = num(p.buffer, 0);
      const slots = Math.max(1, Math.min(p.top, ctx.capital > 0 ? Math.floor(ctx.capital / Math.max(1, minPos)) : p.top));
      const ranked = [];
      for (const s of ctx.syms) { if (s === safe) continue; const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.lookback) continue; const r = IND.ret(b.c, p.lookback, i, p.skip); if (r == null) continue; ranked.push({ s, r }); }
      ranked.sort((a, b) => b.r - a.r);
      const keepTo = slots + buffer;
      // holdings stay while they're still rising and inside the wider band; open slots go to the best of the rest
      const keep = ranked.filter((x, k) => ctx.held[x.s] && x.r > 0 && k < keepTo).slice(0, slots).map(x => x.s);
      const picks = [...keep, ...ranked.filter(x => x.r > 0 && !keep.includes(x.s)).map(x => x.s)].slice(0, slots);
      ranked.forEach((x, k) => { const inP = picks.includes(x.s), kept = inP && k >= slots; out.notes[x.s] = `${p.lookback}-day return ${pf(x.r, 1)} (rank ${k + 1} of ${ranked.length})${!riskOn ? `. Market filter says stay ${safe ? 'in ' + safe : 'in cash'}` : x.r <= 0 ? '. Not rising, so no' : kept ? `. Kept: still inside the top ${keepTo}, so no need to sell` : inP ? `. In the top ${slots}` : ctx.held[x.s] && k >= keepTo ? `. Fell below rank ${keepTo}: sell` : ''}.`; });
      if (slots < p.top) ctx.regime = `${ctx.regime ? ctx.regime + '. ' : ''}With ${'$' + Math.round(ctx.capital)} it holds ${slots} position${slots === 1 ? '' : 's'} instead of ${p.top}, so each is at least $${minPos}`;
      if (!riskOn) { if (safe) { out.weights[safe] = 1; out.notes[safe] = `Market is weak: parking the money in ${safe} (T-bills) until it recovers.`; } return out; }
      Object.assign(out.weights, sizeWeights(picks, { ...ctx, params: { ...p, maxPositions: slots } }, ctx.maxW));
      if (safe) { const used = Object.values(out.weights).reduce((t, x) => t + x, 0); if (used < 0.98) { out.weights[safe] = Math.min(1, 1 - used); out.notes[safe] = `Money the picks don't use waits in ${safe} (T-bills) instead of cash.`; } }
      return out;
    },
  },
  rsi2: {
    name: 'Dip buyer (RSI-2)', short: 'Dip buyer', icon: 'arrowDn', rebalance: 'none', markets: 'stocks',
    overlays: { mood: false, corr: false, exits: false, tighten: false, council: 'advise', news: 'severe', review: false },
    desc: 'Buys sharp short pullbacks in assets that are in a long-term uptrend: the 2-day RSI deeply oversold (on one day, or added up over two) while price is above its 200-day average. Sells on the first bounce above the 5-day average or when RSI-2 gets hot, or after 10 days. Many small wins; works best on a spread of index and sector ETFs. Waiting money sits in T-bills.',
    params: [{ k: 'entry', label: 'Buy when RSI-2 below', def: 10, min: 1, max: 30 }, { k: 'mode', label: 'Oversold means', def: 'cum', choices: [['cum', '2 days added up (more trades)'], ['one', 'One day']] }, { k: 'exitLen', label: 'Sell above average (days)', def: 5, min: 2, max: 20 }, { k: 'exitRsi', label: 'Or sell when RSI-2 above', def: 70, min: 50, max: 99 }, { k: 'trendLen', label: 'Trend filter (days)', def: 200, min: 0, max: 300 }, { k: 'maxDays', label: 'Most days to hold', def: 10, min: 2, max: 30 }, { k: 'maxPositions', label: 'Most positions', def: 4, min: 1, max: 10 }, { k: 'stopAtr', label: 'Disaster stop (ATRs, 0 = off)', def: 0, min: 0, max: 8, step: 0.5 }, SAFE_P],
    grid: { entry: [5, 10, 15], exitLen: [3, 5, 10] },
    need: p => Math.max(p.trendLen || 0, 20) + 5,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], slots = fitSlots(ctx, p.maxPositions), slot = Math.min(ctx.maxW, 1 / slots), cum = p.mode === 'cum', exitRsi = num(p.exitRsi, 101);
      let held = 0;
      for (const s of ctx.syms) {
        if (s === p.safe) continue;
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < Math.max(p.trendLen, 20)) continue;
        const c = b.c[i], r = IND.rsi(b.c, 2, i, b.memo), r1 = IND.rsi(b.c, 2, i - 1, b.memo), ex = IND.sma(b.c, p.exitLen, i), tr = p.trendLen ? IND.sma(b.c, p.trendLen, i) : 0, h = ctx.held[s];
        if (h) {
          const days = h.days || 0, hot = r != null && r > exitRsi, exit = c > ex || hot || days >= p.maxDays;
          out.notes[s] = `Holding ${days} day${days === 1 ? '' : 's'}. Close ${px2(c)} vs ${p.exitLen}-day average ${px2(ex)}, RSI-2 ${r == null ? '–' : r.toFixed(0)}.${exit ? (c > ex ? ' Bounced: sell.' : hot ? ' RSI-2 is hot: sell.' : ' Held too long: sell.') : ' Waiting for the bounce.'}`;
          if (!exit) { out.weights[s] = h.weight || slot; held++; if (p.stopAtr > 0) out.stops[s] = { trail: false, price: h.entry - p.stopAtr * IND.atr(b, 14, i, b.memo) }; }
        } else {
          const lvl = cum ? (r != null && r1 != null ? r + r1 : null) : r, lim = cum ? p.entry * 3.5 : p.entry, uptrend = !p.trendLen || c > tr;
          const ok = lvl != null && lvl < lim && uptrend;
          out.notes[s] = `RSI-2 ${r == null ? '–' : r.toFixed(1)}${cum ? ` (2-day sum ${lvl == null ? '–' : lvl.toFixed(0)} vs ${lim})` : ''}${p.trendLen ? `, ${uptrend ? 'above' : 'below'} its ${p.trendLen}-day average` : ''}.${ok ? ' A dip in an uptrend: buy.' : ''}`;
          if (ok) cand.push({ s, r: lvl });
        }
      }
      cand.sort((a, b) => a.r - b.r);
      for (const x of cand.slice(0, Math.max(0, slots - held))) { out.weights[x.s] = slot; if (p.stopAtr > 0) out.stops[x.s] = { trail: false, price: ctx.data[x.s].c[ctx.idx[x.s]] - p.stopAtr * IND.atr(ctx.data[x.s], 14, ctx.idx[x.s], ctx.data[x.s].memo) }; }
      parkIdle(out, ctx, p, `Waiting for a dip: the money sits in ${p.safe} (T-bills) and is sold first when one comes.`);
      return out;
    },
  },
  breakout: {
    name: 'Breakout (Turtle)', short: 'Breakout', icon: 'bolt', rebalance: 'none', markets: 'both',
    overlays: { mood: false, exits: false, tighten: false, council: 'advise' },
    desc: 'Buys when price closes above its highest high of the last 55 days, but only in a long-term uptrend and on stronger-than-usual volume, which cuts out most false breakouts. Each trade risks about 1% of the wallet. It sells when price closes under its 20-day low or falls a few ATRs from its best close. Money between trades waits in T-bills.',
    params: [{ k: 'entryLen', label: 'Breakout over (days)', def: 55, min: 10, max: 120 }, { k: 'exitLen', label: 'Exit under (days)', def: 20, min: 5, max: 60 }, { k: 'atrMult', label: 'Stop (ATRs)', def: 2, min: 1, max: 6, step: 0.5 }, { k: 'riskPct', label: 'Risk per trade (%)', def: 1.5, min: 0.25, max: 5, step: 0.25 }, { k: 'maxPositions', label: 'Most positions', def: 4, min: 1, max: 20 },
      { k: 'trendLen', label: 'Only in an uptrend over (days, 0 = off)', def: 200, min: 0, max: 300 }, { k: 'volX', label: 'Breakout volume at least (× normal, 0 = off)', def: 1.2, min: 0, max: 4, step: 0.1 }, { k: 'trailAtr', label: 'Trailing stop from best close (ATRs, 0 = off)', def: 4, min: 0, max: 10, step: 0.5 }, SAFE_P],
    grid: { entryLen: [20, 55], exitLen: [10, 20], atrMult: [2, 3] },
    need: p => Math.max(p.entryLen, p.exitLen, num(p.trendLen, 0)) + 20,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], trendLen = num(p.trendLen, 0), volX = num(p.volX, 0), trailAtr = num(p.trailAtr, 0);
      let held = 0;
      for (const s of ctx.syms) {
        if (s === p.safe) continue;
        const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < p.entryLen + 15) continue;
        const c = b.c[i], hi = IND.highest(b.h, p.entryLen, i), lo = IND.lowest(b.l, p.exitLen, i), atr = IND.atr(b, 20, i, b.memo), h = ctx.held[s];
        const w = Math.min(ctx.maxW, (p.riskPct / 100) * c / (p.atrMult * atr));
        if (h) {
          const best = Math.max(h.high || c, c), trail = trailAtr > 0 ? best - trailAtr * atr : 0, exit = c < lo || (trail && c < trail);
          out.notes[s] = `Holding. Close ${px2(c)}, ${p.exitLen}-day low ${px2(lo)}${trail ? `, trailing stop ${px2(trail)}` : ''}.${exit ? (c < lo ? ' Broke the low: sell.' : ' Fell too far from its best: sell.') : ''}`;
          if (!exit) { out.weights[s] = h.weight || w; out.stops[s] = { trail: false, price: Math.max(h.stopPx || (h.entry - p.atrMult * atr), trail || 0) }; held++; }
        } else {
          const tr = trendLen && i >= trendLen ? IND.sma(b.c, trendLen, i) : null, inTrend = !trendLen || (tr != null && c > tr);
          let vOk = true, vr = null;
          if (volX > 0 && b.v && b.v[i] > 0) { let sum = 0, n = 0; for (let k = i - 50; k < i; k++) if (k >= 0 && b.v[k] > 0) { sum += b.v[k]; n++; } if (n > 20) { vr = b.v[i] / (sum / n); vOk = vr >= volX; } }
          const brk = hi != null && c > hi, ok = brk && inTrend && vOk;
          out.notes[s] = `Close ${px2(c)} vs ${p.entryLen}-day high ${px2(hi)}.${brk ? (!inTrend ? ` Broke out, but it is under its ${trendLen}-day average: skip.` : !vOk ? ` Broke out on weak volume (${vr.toFixed(1)}× normal): skip.` : ' New breakout: buy.') : ''}`;
          if (ok) cand.push({ s, w, c, atr, strength: c / hi - 1 });
        }
      }
      cand.sort((a, b) => b.strength - a.strength);
      const slots = fitSlots(ctx, p.maxPositions);
      for (const x of cand.slice(0, Math.max(0, slots - held))) { out.weights[x.s] = Math.max(x.w, Math.min(ctx.maxW, 1 / slots / 2)); out.stops[x.s] = { trail: false, price: x.c - p.atrMult * x.atr }; }
      parkIdle(out, ctx, p, `Money between breakouts waits in ${p.safe} (T-bills).`);
      return out;
    },
  },
  hold: {
    name: 'Buy and hold', short: 'Hold', icon: 'layers', rebalance: 'monthly', markets: 'both',
    overlays: { council: 'off', mood: false, corr: false, exits: false, tighten: false, news: 'none', review: false, stops: false },
    desc: 'Owns everything in the list and just holds it: no timing, no stops, no vetoes. New money goes to whatever is furthest below its share. Once a month it only rebalances if something has drifted more than 5 points off its target, so it trades very little. Equal shares, or calmer assets get more.',
    params: [{ k: 'weighting', label: 'Shares', def: 'equal', choices: [['equal', 'Equal'], ['invvol', 'Calmer assets get more']] }, { k: 'band', label: 'Rebalance when off by (points, 0 = always)', def: 5, min: 0, max: 50 }, FLOW_P],
    grid: {}, need: () => 64,
    decide(ctx) {
      const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, all = ctx.syms.filter(s => ctx.idx[s] != null);
      if (!all.length) return out;
      // a small wallet holds the first few in the list (what it already owns first) and adds the rest as it grows
      const n = fitSlots(ctx, all.length), live = [...all.filter(s => ctx.held[s]), ...all.filter(s => !ctx.held[s])].slice(0, n);
      if (n < all.length) out.regime = `A ${'$' + Math.round(ctx.capital)} wallet holds ${n} of the ${all.length} in its list, so each is a useful size. The rest join as the wallet grows.`;
      let w = {};
      if (p.weighting === 'invvol') { const iv = live.map(s => { const v = IND.vol(ctx.data[s].c, 63, ctx.idx[s]); return v > 0 ? 1 / v : 0; }), tot = iv.reduce((a, b) => a + b, 0) || 1; live.forEach((s, k) => { w[s] = iv[k] / tot; }); }
      else live.forEach(s => { w[s] = 1 / live.length; });
      // cap and spread what the cap cut off over the rest
      for (let r = 0; r < 4; r++) { let extra = 0, free = 0; for (const s of live) { if (w[s] > ctx.maxW) { extra += w[s] - ctx.maxW; w[s] = ctx.maxW; } else if (w[s] < ctx.maxW) free += w[s]; } if (extra < 1e-6 || !free) break; for (const s of live) if (w[s] < ctx.maxW) w[s] += extra * w[s] / free; }
      const cap = ctx.capital || 0, cur = s => { const h = ctx.held[s]; return h && cap > 0 ? h.qty * ctx.data[s].c[ctx.idx[s]] / cap : 0; };
      const band = num(p.band, 0) / 100, tot = live.reduce((t, s) => t + cur(s), 0);
      let drift = 0;
      for (const s of live) { const now = cur(s); drift = Math.max(drift, Math.abs(now - w[s])); out.weights[s] = w[s]; out.notes[s] = `Target ${(w[s] * 100).toFixed(0)}%, now ${(now * 100).toFixed(0)}%.`; }
      // only trim winners when something drifted past the band; otherwise just top up
      if (band > 0 && tot > 0.5 && drift < band) { out.noTrim = true; out.regime = (out.regime ? out.regime + ' ' : '') + `Within ${(band * 100).toFixed(0)} points of target: no rebalance.`; }
      return out;
    },
  },
};
const SAFE = new Set(['TLT', 'IEF', 'SHY', 'BIL', 'SGOV', 'AGG', 'BND', 'TIP']);
STRATEGIES.dual = {
  name: 'Dual momentum', short: 'Dual momentum', icon: 'swap', rebalance: 'monthly', markets: 'both',
  overlays: { council: 'advise', mood: false, corr: false, exits: false, tighten: false, news: 'none', review: false, stops: false },
  desc: 'Once a month, holds the risky fund with the strongest momentum (an average of its 1, 3, 6 and 12-month returns), but only if it beats T-bills. Otherwise it moves to bonds, or to T-bills if bonds are falling too. Index funds can hold all the money, since they are spread out inside. Gary Antonacci\'s rule: it sat out most of 2008 and 2022.',
  params: [{ k: 'lookback', label: 'Look back (days, when not blended)', def: 252, min: 63, max: 300 }, { k: 'score', label: 'Momentum means', def: 'blend', choices: [['blend', 'Average of 1, 3, 6 and 12 months'], ['one', 'The look-back only']] }, { k: 'top', label: 'Hold the top', def: 1, min: 1, max: 3 }, SAFE_P, FUND_P],
  grid: { lookback: [126, 252], top: [1, 2] },
  need: p => Math.max(p.lookback, p.score === 'blend' ? 252 : 0) + 5,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, risky = [], bonds = [], blend = p.score === 'blend';
    const safe = p.safe && p.safe !== 'cash' && ctx.syms.includes(p.safe) ? p.safe : null;
    const score = (b, i) => blend ? [21, 63, 126, 252].map(n => IND.ret(b.c, n, i)).reduce((t, x) => x == null ? null : t == null ? null : t + x / 4, 0) : IND.ret(b.c, p.lookback, i, 0);
    let bill = 0;
    for (const s of ctx.syms) {
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < (blend ? 252 : p.lookback)) continue; const r = score(b, i); if (r == null) continue;
      if (s === 'SGOV' || s === 'BIL') { bill = Math.max(bill, r); out.notes[s] = `T-bills: ${blend ? 'momentum' : `${p.lookback}-day return`} ${pf(r, 1)}. The bar the others must beat.`; continue; }
      (SAFE.has(s) ? bonds : risky).push({ s, r });
    }
    risky.sort((a, b) => b.r - a.r); bonds.sort((a, b) => b.r - a.r);
    const bond = bonds[0], bar = Math.max(bill, 0), what = blend ? 'Momentum (1-12 months)' : `${p.lookback}-day return`;
    const picks = risky.filter(x => x.r > bar).slice(0, fitSlots(ctx, p.top));
    for (const x of risky) out.notes[x.s] = `${what} ${pf(x.r, 1)} vs T-bills ${pf(bill, 1)}.${picks.includes(x) ? ' Strongest and beating T-bills: hold.' : ''}`;
    for (const x of bonds) out.notes[x.s] = `Bonds: ${what.toLowerCase()} ${pf(x.r, 1)}.`;
    if (picks.length) picks.forEach(x => { out.weights[x.s] = Math.min(fundCap(ctx, x.s), 1 / picks.length); });
    else if (bond && bond.r > bar) { out.weights[bond.s] = fundCap(ctx, bond.s); out.notes[bond.s] += ' Nothing risky beats T-bills: hold bonds.'; }
    parkIdle(out, ctx, p, `Money not in a fund waits in ${p.safe} (T-bills).`);
    out.regime = picks.length ? `Risk on: ${picks.map(x => x.s).join(', ')}` : bond && bond.r > bar ? `Defensive: holding ${bond.s}` : `Defensive: everything is falling, ${safe ? `waiting in ${safe}` : 'holding cash'}`;
    ctx.regime = out.regime;
    return out;
  },
};
STRATEGIES.lowvol = {
  name: 'Low volatility', short: 'Low volatility', icon: 'shield', rebalance: 'monthly', markets: 'stocks',
  overlays: { mood: false, corr: false, exits: false, tighten: false, council: 'advise', review: false },
  desc: 'Once a month, holds the calmest stocks in the list (the smallest daily swings over the past 3 months) that are above their 200-day average and up over the past 6 months. Calmer ones get a bigger share, a holding stays until it falls well down the ranking, and new money tops up without selling. Empty slots wait in T-bills. Calm stocks have historically earned about as much as wild ones with much smaller drops.',
  params: [{ k: 'volLen', label: 'Measure swings over (days)', def: 63, min: 20, max: 126 }, { k: 'trendLen', label: 'Trend filter (days)', def: 200, min: 0, max: 300 }, { k: 'top', label: 'Hold the calmest', def: 5, min: 1, max: 10 }, { k: 'momLen', label: 'Must be up over (days, 0 = off)', def: 126, min: 0, max: 252 }, { k: 'sizing', label: 'Sizing', def: 'invvol', choices: [['invvol', 'Calmer gets more'], ['equal', 'Equal']] }, { k: 'buffer', label: 'Keep a holding until it drops below rank (top +)', def: 2, min: 0, max: 10 }, SAFE_P, FLOW_P],
  grid: { volLen: [42, 63], top: [3, 5] },
  need: p => Math.max(p.volLen, p.trendLen || 0, num(p.momLen, 0)) + 5,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {}, noTrim: p.flow === 'flow' }, ranked = [], momLen = num(p.momLen, 0), buf = num(p.buffer, 0);
    for (const s of ctx.syms) {
      if (s === p.safe) continue;
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < Math.max(p.volLen, p.trendLen, momLen)) continue;
      const v = IND.vol(b.c, p.volLen, i), up = (!p.trendLen || b.c[i] > IND.sma(b.c, p.trendLen, i)) && (!momLen || (IND.ret(b.c, momLen, i) ?? 0) > 0); if (v == null) continue; ranked.push({ s, v, up });
    }
    ranked.sort((a, b) => a.v - b.v);
    const ok = ranked.filter(x => x.up), top = fitSlots(ctx, p.top);
    // holdings keep their place while they're inside the top + buffer; new names fill what's left
    const keep = ok.filter((x, k) => ctx.held[x.s] && k < top + buf).slice(0, top), picks = [...keep, ...ok.filter(x => !keep.includes(x))].slice(0, top);
    ranked.forEach((x, k) => { out.notes[x.s] = `Daily swings ${(x.v * 100).toFixed(2)}% (calmest ${k + 1} of ${ranked.length})${x.up ? '' : ', not in an uptrend'}.${picks.includes(x) ? (keep.includes(x) && !ok.slice(0, top).includes(x) ? ' Still close to the top: keep.' : ' Calm and rising: hold.') : ''}`; });
    const share = picks.length / Math.max(picks.length, top);
    if (p.sizing === 'invvol') { const iv = picks.map(x => 1 / x.v), t = iv.reduce((a, b) => a + b, 0) || 1; picks.forEach((x, k) => { out.weights[x.s] = Math.min(ctx.maxW, iv[k] / t * share); }); }
    else picks.forEach(x => { out.weights[x.s] = Math.min(ctx.maxW, 1 / Math.max(picks.length, top)); });
    parkIdle(out, ctx, p, `Slots without a calm, rising stock wait in ${p.safe} (T-bills).`);
    return out;
  },
};
STRATEGIES.drift = {
  name: 'News jump drift', short: 'Jump drift', icon: 'bolt', rebalance: 'none', markets: 'stocks',
  overlays: { mood: false, corr: false, exits: false, tighten: false, council: 'advise', news: 'severe' },
  desc: 'Buys a stock within a few days after it jumps on heavy volume and closes near its high (usually good earnings or big news), as long as it is holding most of the jump. Holds it for about a month. Big surprises tend to keep drifting the same way as the market slowly catches up. Works best on a long list of stocks, since jumps are rare. Waiting money sits in T-bills.',
  params: [{ k: 'jump', label: 'Jump of at least (ATRs)', def: 1.5, min: 1, max: 6, step: 0.5 }, { k: 'volMult', label: 'Volume of at least (x normal)', def: 1.5, min: 1, max: 5, step: 0.25 }, { k: 'within', label: 'Buy within (days after the jump)', def: 3, min: 1, max: 10 }, { k: 'holdDays', label: 'Hold for (days)', def: 20, min: 3, max: 60 }, { k: 'stopAtr', label: 'Stop (ATRs)', def: 2.5, min: 1, max: 6, step: 0.5 }, { k: 'maxPositions', label: 'Most positions', def: 5, min: 1, max: 15 }, SAFE_P],
  grid: { jump: [1.5, 2.5], holdDays: [10, 20, 40] },
  need: () => 60,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], slots = fitSlots(ctx, p.maxPositions), slot = Math.min(ctx.maxW, 1 / slots), within = num(p.within, 1);
    let held = 0;
    for (const s of ctx.syms) {
      if (s === p.safe) continue;
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < 55) continue;
      const c = b.c[i], h = ctx.held[s];
      if (h) {
        const days = h.days || 0, atr = IND.atr(b, 20, i, b.memo);
        out.notes[s] = `Bought after a jump; day ${days} of ${p.holdDays}.${days >= p.holdDays ? ' Time is up: sell.' : ''}`;
        if (days < p.holdDays) { out.weights[s] = h.weight || slot; out.stops[s] = { trail: false, price: h.stopPx || h.entry - p.stopAtr * (atr || 0) }; held++; }
        continue;
      }
      // the jump: a finished day in the last few (today's volume isn't complete yet while the market is open)
      let ev = null;
      for (let k = i - 1; k >= Math.max(25, i - within); k--) {
        const atr = IND.atr(b, 20, k - 1, b.memo); if (!atr) continue;
        const move = (b.c[k] - b.c[k - 1]) / atr, range = b.h[k] - b.l[k], nearHigh = range > 0 ? (b.c[k] - b.l[k]) / range : 1;
        let avgV = 0, n = 0; for (let q = k - 20; q < k; q++) if (b.v?.[q] > 0) { avgV += b.v[q]; n++; } avgV = n ? avgV / n : 0;
        const volX = avgV && b.v?.[k] ? b.v[k] / avgV : null, volOk = volX == null || volX >= p.volMult;
        if (move >= p.jump && volOk && nearHigh >= 0.6 && b.c[k] > IND.sma(b.c, 50, k)) { ev = { k, move, volX, atr, base: b.c[k - 1], top: b.c[k] }; break; }
      }
      if (!ev) { out.notes[s] = `No jump on heavy volume in the last ${within} day${within === 1 ? '' : 's'}.`; continue; }
      const kept = (c - ev.base) / (ev.top - ev.base), ago = i - ev.k, ok = kept >= 0.5;
      out.notes[s] = `Jumped ${ev.move.toFixed(1)} ATRs ${ago === 1 ? 'yesterday' : `${ago} days ago`}${ev.volX ? ` on ${ev.volX.toFixed(1)}x volume` : ''}, holding ${Math.round(kept * 100)}% of it.${ok ? ' Buy the drift.' : ' Gave back too much: skip.'}`;
      if (ok) cand.push({ s, move: ev.move * kept, atr: ev.atr, c });
    }
    cand.sort((a, b) => b.move - a.move);
    for (const x of cand.slice(0, Math.max(0, slots - held))) { out.weights[x.s] = slot; out.stops[x.s] = { trail: false, price: x.c - p.stopAtr * x.atr }; }
    parkIdle(out, ctx, p, `Waiting for a jump: the money sits in ${p.safe} (T-bills).`);
    return out;
  },
};
// Turn of the month: money from paychecks and pension funds flows into stocks around month end.
const monthPos = (d, dates, i) => { // trading days left in this month after i, and trading days into it
  const m = d.slice(0, 7); let left = 0, into = 0;
  for (let k = i + 1; k < dates.length && dates[k].slice(0, 7) === m; k++) left++;
  for (let k = i; k >= 0 && dates[k].slice(0, 7) === m; k--) into++;
  return { left, into };
};
STRATEGIES.tom = {
  name: 'Turn of the month', short: 'Turn of month', icon: 'calendar', rebalance: 'none', markets: 'stocks',
  overlays: { council: 'off', mood: false, corr: false, exits: false, tighten: false, news: 'none', review: false, stops: false },
  desc: 'Holds index funds only from the last trading day of each month through the first few days of the next, and waits in T-bills the rest of the time, so it trades about twice a month and earns interest in between. Paychecks, 401(k) contributions and fund rebalancing push money into stocks then; for decades a big part of the market\'s gains came on these few days. Index funds can hold all the money.',
  params: [{ k: 'before', label: 'Buy this many days before month end', def: 1, min: 1, max: 4 }, { k: 'after', label: 'Sell after this many days into the month', def: 3, min: 1, max: 6 }, { k: 'trendLen', label: 'Only above this average (days, 0 = off)', def: 0, min: 0, max: 300 }, SAFE_P, FUND_P],
  grid: { before: [1, 2], after: [2, 3, 4] },
  need: p => Math.max(25, p.trendLen || 0) + 5,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, picks = [];
    let next = null;
    for (const s of ctx.syms) {
      if (s === p.safe) continue;
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null) continue;
      const d = b.d[i]; let { left, into } = monthPos(d, b.d, i);
      // The last bar is today: the calendar, not the data, says how many trading days are left.
      if (i === b.d.length - 1) { let [y, m, dd] = d.split('-').map(Number), n = 0; left = 0; while (n++ < 10) { [y, m, dd] = nextTradingDay(y, m, dd); if (`${y}-${String(m).padStart(2, '0')}` !== d.slice(0, 7)) break; left++; } if (left >= p.before) next = left - p.before + 1; }
      const trendOk = !p.trendLen || b.c[i] > IND.sma(b.c, p.trendLen, i);
      const inWindow = left < p.before || into < p.after; // sold at the close of day 'after'
      out.notes[s] = left < p.before ? `${left === 0 ? 'Last trading day' : `${left + 1} trading days left`} of the month: the window is open.` : into <= p.after ? `Trading day ${into} of the month: still in the window.` : `Trading day ${into} of the month: outside the window.`;
      if (inWindow && !trendOk) out.notes[s] += ` Below its ${p.trendLen}-day average, so it sits this one out.`;
      if (inWindow && trendOk) picks.push(s);
    }
    const n = Math.min(picks.length, fitSlots(ctx, picks.length));
    picks.slice(0, n).forEach(s => { out.weights[s] = Math.min(fundCap(ctx, s), 1 / n); });
    parkIdle(out, ctx, p, `Outside the window the money earns T-bill interest in ${p.safe}.`);
    ctx.regime = out.regime = picks.length ? 'The turn-of-the-month window is open: invested.' : next ? `Waiting for the window: it opens in ${next} trading day${next === 1 ? '' : 's'}.` : 'Outside the window.';
    return out;
  },
};
// Snap-back: very stretched below its Bollinger band in an uptrend, sell at the middle.
STRATEGIES.snapback = {
  name: 'Snap-back', short: 'Snap-back', icon: 'refresh', rebalance: 'none', markets: 'both',
  overlays: { mood: false, corr: false, exits: false, tighten: false, council: 'advise', news: 'severe', review: false },
  desc: 'Buys when something in a long uptrend closes far below its 20-day Bollinger band and near the low of its day, and sells when it gets back to its average, or after 10 days. Panics in strong assets usually snap back fast. A disaster stop limits the ones that keep falling. Works best on a spread of index and sector ETFs; waiting money sits in T-bills.',
  params: [{ k: 'bands', label: 'Below the band by (std devs)', def: 1.75, min: 1, max: 3.5, step: 0.25 }, { k: 'len', label: 'Average over (days)', def: 20, min: 10, max: 50 }, { k: 'trendLen', label: 'Trend filter (days)', def: 200, min: 0, max: 300 }, { k: 'maxDays', label: 'Most days to hold', def: 10, min: 3, max: 30 }, { k: 'stopAtr', label: 'Disaster stop (ATRs)', def: 3, min: 1, max: 8, step: 0.5 }, { k: 'maxPositions', label: 'Most positions', def: 4, min: 1, max: 10 }, { k: 'exitZ', label: 'Sell at (std devs from average)', def: 0, min: -1, max: 2, step: 0.25 }, SAFE_P],
  grid: { bands: [1.5, 2], len: [10, 20] },
  need: p => Math.max(p.trendLen || 0, p.len) + 5,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], slots = fitSlots(ctx, p.maxPositions), slot = Math.min(ctx.maxW, 1 / slots), exitZ = num(p.exitZ, 0);
    let held = 0;
    for (const s of ctx.syms) {
      if (s === p.safe) continue;
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < Math.max(p.trendLen, p.len)) continue;
      const c = b.c[i], m = IND.sma(b.c, p.len, i); let v = 0; for (let k = i - p.len + 1; k <= i; k++) v += (b.c[k] - m) ** 2;
      const sd = Math.sqrt(v / p.len), z = sd > 0 ? (c - m) / sd : 0, h = ctx.held[s], trend = !p.trendLen || c > IND.sma(b.c, p.trendLen, i);
      if (h) {
        const back = z >= exitZ, exit = back || (h.days || 0) >= p.maxDays;
        out.notes[s] = `${z.toFixed(1)} std devs from its ${p.len}-day average, day ${h.days || 0}.${exit ? (back ? ' Snapped back: sell.' : ' Held too long: sell.') : ' Waiting for the snap-back.'}`;
        if (!exit) { out.weights[s] = h.weight || slot; out.stops[s] = { trail: false, price: h.stopPx || h.entry - p.stopAtr * (IND.atr(b, 14, i, b.memo) || 0) }; held++; }
      } else {
        const range = b.h[i] - b.l[i], low = range > 0 ? (c - b.l[i]) / range <= 0.5 : true, ok = z <= -p.bands && trend && low;
        out.notes[s] = `${z.toFixed(1)} std devs from its ${p.len}-day average${p.trendLen ? `, ${trend ? 'above' : 'below'} its ${p.trendLen}-day trend` : ''}.${ok ? ' Stretched in an uptrend: buy.' : z <= -p.bands && trend && !low ? ' Stretched, but it already bounced off the low today: skip.' : ''}`;
        if (ok) cand.push({ s, z, c, atr: IND.atr(b, 14, i, b.memo) || 0 });
      }
    }
    cand.sort((a, b) => a.z - b.z);
    for (const x of cand.slice(0, Math.max(0, slots - held))) { out.weights[x.s] = slot; out.stops[x.s] = { trail: false, price: x.c - p.stopAtr * x.atr }; }
    parkIdle(out, ctx, p, `Waiting for a snap-back setup: the money sits in ${p.safe} (T-bills).`);
    return out;
  },
};
// Autopilot is not a strategy of its own: it blends the others (see 09f-autopilot.js).
STRATEGIES.autopilot = {
  name: 'Autopilot', short: 'Autopilot', icon: 'sparkle', rebalance: 'daily', markets: 'both', auto: true, band: 0.03,
  desc: 'Runs 32 strategy tryouts with pretend money on today\'s real prices and gives this wallet\'s money to the ones that are working. It favors steady winners and the strategies that do well in the current kind of market, skips near-copies of each other, and breeds new variations of the best ones. It re-plans monthly (and weekly when things shift a lot), parks idle money in T-bills and scales down for small accounts.',
  params: [{ k: 'markets', label: 'Trades', def: 'both', choices: [['both', 'Stocks and crypto'], ['stocks', 'Stocks only'], ['crypto', 'Crypto only']] }, { k: 'maxSleeve', label: 'Most in one strategy (%)', def: 35, min: 15, max: 100 }, { k: 'volTarget', label: 'Swing target (% a year, 0 = off)', def: 20, min: 0, max: 80 }, { k: 'safety', label: 'Safety switch', def: 'on', choices: [['on', 'On'], ['off', 'Off']] }, { k: 'safetyCut', label: 'Safety switch cuts (%)', def: 50, min: 10, max: 100 }, { k: 'brake', label: 'Brake after a drop of (%, 0 = off)', def: 10, min: 0, max: 50 },
    { k: 'allocMode', label: 'How it splits the money', def: 'blend', choices: [['blend', 'Score per risk'], ['score', 'By score'], ['equal', 'Evenly']] }, { k: 'regimeFit', label: 'Favor what works in this kind of market', def: 'on', choices: [['on', 'On'], ['off', 'Off']] },
    { k: 'recheck', label: 'Re-plan', def: 'weekly', choices: [['weekly', 'Weekly if it changed a lot'], ['monthly', 'Monthly only']] }, { k: 'evolve', label: 'Breed new variations', def: 'on', choices: [['on', 'On'], ['off', 'Off']] },
    { k: 'park', label: 'Idle money', def: 'SGOV', choices: [['SGOV', 'SGOV (T-bills)'], ['cash', 'Cash']] }, { k: 'minSleeveUsd', label: 'Smallest slice per strategy ($)', def: 50, min: 5, max: 10000 }, { k: 'aiReview', label: 'Analyst reviews each plan', def: 'on', choices: [['on', 'On'], ['off', 'Off']] }],
  grid: {}, need: () => 2, decide() { fail('Autopilot decides through its tryouts'); },
};
// Covered calls: own 100-share lots, sell a call against each one every month for income (the calls are handled in 09i-options.js)
STRATEGIES.covcall = {
  name: 'Covered calls', short: 'Covered calls', icon: 'target', rebalance: 'monthly', markets: 'stocks', optLevel: 1,
  overlays: { council: 'off', mood: false, corr: false, exits: false, tighten: false, news: 'none', review: false, stops: false },
  desc: 'Buys steady stocks in 100-share lots and sells one call a month against each lot for extra income: about 30 delta, 30 to 45 days out. When a call has made 80% of what it can, it buys it back early and sells the next one. It skips calls that would run past earnings or an ex-dividend date. If the stock climbs past the strike, the shares get called away at the strike and it buys a new lot. Needs options level 1 on its wallet. Backtests show the stocks only, not the calls.',
  params: [{ k: 'delta', label: 'How far out (delta × 100, lower is safer)', def: 30, min: 10, max: 50 }, { k: 'dteMin', label: 'Days to expiration, at least', def: 30, min: 7, max: 90 }, { k: 'dteMax', label: 'Days to expiration, at most', def: 45, min: 7, max: 120 },
    { k: 'takePct', label: 'Buy back early at this % of the premium earned (0 = never)', def: 80, min: 0, max: 100 }, { k: 'skipEarn', label: 'Around earnings', def: 'on', choices: [['on', 'Skip'], ['off', 'Sell anyway']] },
    { k: 'skipDiv', label: 'Around ex-dividend dates', def: 'on', choices: [['on', 'Skip'], ['off', 'Sell anyway']] }, { k: 'atExp', label: 'In the money near expiration', def: 'assign', choices: [['assign', 'Let the shares go'], ['roll', 'Roll to a later month']] }],
  grid: {}, need: () => 64,
  decide(ctx) {
    // round lots: as many 100-share lots as the money buys, cheapest first, what it already owns first
    const out = { weights: {}, stops: {}, notes: {} }, cap = ctx.capital || 0, all = ctx.syms.filter(s => ctx.idx[s] != null);
    const px = s => ctx.data[s].c[ctx.idx[s]], lot = s => px(s) * 100 * 1.003;
    const order = [...all.filter(s => ctx.held[s]), ...all.filter(s => !ctx.held[s]).sort((a, b) => px(a) - px(b))];
    let left = cap; const lots = {};
    for (const s of order) { const have = Math.floor((ctx.held[s]?.qty || 0) / 100 + 1e-9); if (have && left >= have * lot(s)) { lots[s] = have; left -= have * lot(s); } }
    for (let more = true; more;) { more = false; for (const s of order) if (left >= lot(s)) { lots[s] = (lots[s] || 0) + 1; left -= lot(s); more = true; } }
    for (const s of all) { const n = lots[s] || 0; out.weights[s] = n ? (n * 100 + 0.2) * px(s) * 1.001 / cap : 0; out.notes[s] = n ? `${n} lot${n > 1 ? 's' : ''} of 100 shares: ${n} call${n > 1 ? 's' : ''} a month.` : `A lot of 100 costs about $${Math.round(lot(s))}.`; }
    const cheapest = Math.min(...all.map(lot));
    if (!Object.keys(lots).length) { out.regime = `Covered calls need 100 shares. The cheapest lot here costs about $${Math.round(cheapest)} and the bot has $${Math.round(cap)}, so it waits in cash. Add cheaper stocks or more money.`; out.weights = {}; }
    else out.regime = `${Object.values(lots).reduce((t, n) => t + n, 0)} lot${Object.values(lots).reduce((t, n) => t + n, 0) > 1 ? 's' : ''} of 100 shares; $${Math.round(left)} left in cash.`;
    out.noTrim = true; // never sells part of a lot (that would uncover a call)
    return out;
  },
};
const UNIVERSES = {
  megacap: { name: 'Big US stocks', syms: ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'JPM', 'V', 'LLY', 'COST', 'NFLX', 'WMT', 'XOM', 'AMD'] },
  etfs: { name: 'Index and sector ETFs', syms: ['SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF', 'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLU', 'XLB', 'XLC', 'XLRE'] },
  allweather: { name: 'Stocks, bonds and gold', syms: ['SPY', 'QQQ', 'IWM', 'EFA', 'EEM', 'TLT', 'IEF', 'GLD', 'DBC', 'VNQ'] },
  crypto: { name: 'Top crypto', syms: ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'LTC', 'DOT'] },
  mixed: { name: 'Stocks and crypto', syms: ['SPY', 'QQQ', 'GLD', 'TLT', 'BTC', 'ETH', 'SOL'] },
  dual: { name: 'Stocks vs bonds', syms: ['SPY', 'QQQ', 'EFA', 'EEM', 'IEF', 'TLT'] },
  defensive: { name: 'Calm, steady stocks', syms: ['KO', 'PG', 'JNJ', 'PEP', 'WMT', 'MCD', 'COST', 'MSFT'] },
  income: { name: 'Steady stocks for covered calls', syms: ['KO', 'PFE', 'VZ', 'T', 'CSCO', 'BAC', 'F', 'INTC'] },
};
function cleanParams(key, p = {}) {
  const st = STRATEGIES[key]; if (!st) fail('Unknown strategy');
  const out = {};
  for (const d of st.params) {
    if (d.choices) out[d.k] = d.choices.some(c => c[0] === p[d.k]) ? p[d.k] : d.def;
    else out[d.k] = clamp(num(p[d.k], d.def), d.min, d.max);
  }
  if (key === 'hybrid') { // a crossbred child: both parents' strategies, settings and symbols
    for (const [k, pk, sk] of [['a', 'pa', 'sa'], ['b', 'pb', 'sb']]) {
      const pst = STRATEGIES[p[k]]; if (!pst || pst.auto || pst.live || p[k] === 'hybrid') fail('A crossbreed needs two parent strategies');
      out[k] = p[k]; out[pk] = cleanParams(p[k], p[pk] || {}); out[sk] = [...new Set((p[sk] || []).map(cleanSym).filter(Boolean))].slice(0, 30);
    }
  }
  return out;
}
const strategiesJson = () => Object.fromEntries(Object.entries(STRATEGIES).map(([k, s]) => [k, { name: s.name, short: s.short, icon: s.icon, desc: s.desc, rebalance: s.rebalance, markets: s.markets, params: s.params, auto: !!s.auto, hidden: !!s.hidden, live: !!s.live, ver: typeof stratVersion === 'function' ? stratVersion(k) : null }]));

// =====================================================================
// Backtester: replays a strategy day by day with the same spreads and fees as live trading
// Decisions use each day's close and fill at that close (bots trade 15 minutes before the bell),
// paying half the spread, a little slippage and the broker's fees. Stops are checked against each day's low.
// =====================================================================
async function loadUniverse(syms) {
  const data = {}, missing = [];
  const list = syms.slice(); // a few at a time, so Yahoo doesn't throttle a big universe
  await Promise.all(Array.from({ length: Math.min(5, list.length) }, async () => { while (list.length) { const s = list.shift(); try { data[s] = await dailyBars(s); } catch (e) { missing.push(s); } } }));
  return { data, missing };
}
const benchFor = syms => syms.some(s => !isCrypto(s)) ? 'SPY' : 'BTC';
function runBacktest({ key, params, syms, data, start, end, capital = 10000, profile = 'robinhood', route = 'mm', maxPosPct = 100, slipBps = 2, bench, addWeekly = 0, flows = null }) {
  const st = STRATEGIES[key], p = params, prof = PROFILES[profile] || PROFILES.robinhood, w = { crypto_route: route };
  const series = {}; for (const s of [...new Set([...syms, bench].filter(Boolean))]) if (data[s]) series[s] = { ...data[s], memo: {}, at: new Map(data[s].d.map((d, i) => [d, i])) };
  const live = syms.filter(s => series[s]);
  const allDates = [...new Set(live.flatMap(s => series[s].d))].sort().filter(d => (!start || d >= start) && (!end || d <= end));
  const need = st.need(p), ptr = {}; for (const s of Object.keys(series)) ptr[s] = -1;
  let cash = capital, peak = capital, maxDD = 0, fees = 0, spreadCost = 0, vol30 = [], exposureDays = 0;
  const pos = {}, curve = [], trades = [], closed = [], daily = [], money = [];
  let units = capital, invested = capital, lastWeek = null; // weekly deposits buy 'units' at the day's value, so returns stay time-weighted
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
    if (addWeekly > 0) { const t = new Date(d + 'T12:00:00Z'), wd = (t.getUTCDay() + 6) % 7; t.setUTCDate(t.getUTCDate() - wd); const wk = t.toISOString().slice(0, 10); if (lastWeek && wk !== lastWeek) { const nav = value() / units; cash += addWeekly; invested += addWeekly; units += addWeekly / nav; } lastWeek = wk; }
    if (flows) { const f = flows[d] || 0; if (f) { const nav = value() / units; cash += f; invested += f; units += f / nav; } } // real deposits and withdrawals on the days they happened
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
    const flow = p.flow === 'flow';
    if (tradable.length && acts) {
      const ctx = { syms: tradable, data: series, idx, params: p, held, bench, maxW: walletMaxW(maxPosPct / 100, eq0), capital: eq0 };
      const dec = st.decide(ctx);
      // exits first
      for (const s of Object.keys(pos)) if (tradable.includes(s) && !(dec.weights[s] > 0)) trade(s, 'sell', pos[s].qty, series[s].c[ptr[s]], d, 'signal');
      // the T-bill parking spot is sold down first (any day) so its money can go to the real trades, and filled last
      const park = p.safe && p.safe !== 'cash' ? p.safe : null;
      if (park && pos[park] && tradable.includes(park)) { const px = series[park].c[ptr[park]], have = pos[park].qty * px, want = (dec.weights[park] || 0) * eq0; if (want > 0 && have - want > Math.max(eq0 * 0.01, 5)) trade(park, 'sell', Math.min(pos[park].qty, (have - want) / px), px, d, 'rebalance'); }
      for (const [s, wgt] of Object.entries(dec.weights).sort((a, b) => (a[0] === park) - (b[0] === park))) {
        if (!(wgt > 0) || !tradable.includes(s) || stoppedToday.has(s)) continue;
        const px = series[s].c[ptr[s]], want = wgt * eq0, have = (pos[s]?.qty || 0) * px;
        if (!pos[s] || rebalanceDay || s === park) {
          const diff = want - have;
          if (Math.abs(diff) > Math.max(eq0 * 0.01, 5)) {
            if (diff > 0) { const room = cash - 0.5; const usd = Math.min(diff, room); if (usd > 1) trade(s, 'buy', usd / (px + halfSpread(s, px)) * 0.999, px, d, pos[s] ? 'rebalance' : 'signal'); }
            else if (!dec.noTrim && s !== park) trade(s, 'sell', Math.min(pos[s].qty, -diff / px), px, d, 'rebalance');
          }
        }
        if (pos[s]) { pos[s].weight = wgt; const sp = dec.stops[s]; pos[s].stop = sp ? (sp.trail ? Math.max(pos[s].stop || 0, sp.price) : sp.price) : null; }
      }
    }
    // topping up: spare cash (new deposits) goes into the current holdings on any day, without selling anything
    if (flow && tradable.length && !(acts) && cash > Math.max(5, eq0 * 0.01)) {
      const cand = Object.entries(pos).filter(([s, h]) => tradable.includes(s) && h.weight > 0).map(([s, h]) => ({ s, gap: h.weight * eq0 - h.qty * series[s].c[ptr[s]] })).sort((x, y) => y.gap - x.gap)[0];
      if (cand) { const s = cand.s, px = series[s].c[ptr[s]], usd = Math.min(cand.gap, cash - 0.5); if (usd > 5) trade(s, 'buy', usd / (px + halfSpread(s, px)) * 0.999, px, d, 'top up'); }
    }
    if (st.rebalance === 'monthly') lastMonth = month;
    for (const h of Object.values(pos)) h.days++;
    const eqm = value(), eq = eqm * capital / units; peak = Math.max(peak, eq); maxDD = Math.min(maxDD, eq / peak - 1);
    if (Object.keys(pos).length) exposureDays++;
    curve.push([d, eq]); money.push([d, eqm]);
  }
  // benchmark: buy and hold over the same days
  let bcurve = null;
  if (bench && series[bench] && curve.length) {
    const b = series[bench]; let units = null; bcurve = [];
    for (const [d] of curve) { const i = b.at.get(d) ?? lastIndexOnOrBefore(b.d, d); if (i < 0) { bcurve.push([d, capital]); continue; } if (units == null) units = capital / b.c[i]; bcurve.push([d, units * b.c[i]]); }
  }
  return { money, invested, curve, bcurve, trades, closed, fees, spreadCost, maxDD, exposure: curve.length ? exposureDays / curve.length : 0, open: Object.entries(pos).map(([s, h]) => ({ sym: s, qty: h.qty, entry: h.cost / h.qty, entryDate: h.entryDate, value: h.qty * series[s].c[ptr[s]], stop: h.stop || null })) };
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
  const key = o.strategy, st = STRATEGIES[key]; if (!st) fail('Pick a strategy'); if (st.auto) fail('Autopilot picks its own strategies, so it is tested by its tryouts instead');
  const params0 = cleanParams(key, o.params), syms = withPark([...new Set((o.syms || []).map(cleanSym).filter(Boolean))].slice(0, 40), params0); if (!syms.length) fail('Pick at least one symbol');
  for (const s of syms) if (!assets.has(s)) addAsset(s, CRYPTO_DEFAULT.includes(s) || o.kinds?.[s] === 'crypto' ? 'crypto' : 'stock');
  const params = params0, years = clamp(num(o.years, 3), 1, 8), capital = clamp(num(o.capital, 10000), 25, 1e8), addWeekly = clamp(num(o.addWeekly, 0), 0, 1e6);
  const bench = benchFor(syms);
  const { data, missing } = await loadUniverse([...new Set([...syms, bench])]);
  const usable = syms.filter(s => data[s]); if (!usable.length) fail('No price history for those symbols right now');
  const lastDates = usable.map(s => data[s].d[data[s].d.length - 1]).sort(); const end = lastDates[lastDates.length - 1];
  const start = addIso(end, -Math.round(years * 365.25));
  const firstDates = usable.map(s => data[s].d[0]).sort();
  const args = { key, params, syms: usable, data, start, end, capital, profile: o.profile || 'robinhood', route: o.route || 'mm', maxPosPct: clamp(num(o.maxPosPct, 100), 5, 100), bench, addWeekly };
  const r = runBacktest(args), P = ppy(usable), sum = summarize(r, P, capital);
  const out = {
    strategy: key, name: st.name, params, syms: usable, missing, bench, start: r.curve[0]?.[0] || start, end, capital, years,
    shortHistory: usable.filter(s => data[s].d[0] > start).map(s => ({ sym: s, from: data[s].d[0] })),
    ...sum, curve: downsamplePts(r.curve), bcurve: r.bcurve ? downsamplePts(r.bcurve) : null, monthly: monthlyReturns(r.curve),
    recent: r.closed.slice(-40).reverse(), open: r.open, firstData: firstDates[0], benchSym: bench,
  };
  if (addWeekly > 0) { // with weekly deposits: what went in, what came out, and the same deposits into the benchmark
    const mEnd = r.money[r.money.length - 1]?.[1] ?? capital; let bEnd = null;
    if (data[bench]) { const b = data[bench]; let u = 0, lw = null; for (const [d] of r.money) { const i = lastIndexOnOrBefore(b.d, d); if (i < 0) continue; const t = new Date(d + 'T12:00:00Z'), wd = (t.getUTCDay() + 6) % 7; t.setUTCDate(t.getUTCDate() - wd); const wk = t.toISOString().slice(0, 10); if (lw == null) u += capital / b.c[i]; else if (wk !== lw) u += addWeekly / b.c[i]; lw = wk; bEnd = u * b.c[i]; } }
    out.dca = { addWeekly, invested: r.invested, end: mEnd, profit: mEnd - r.invested, bench: bEnd, benchProfit: bEnd != null ? bEnd - r.invested : null, money: downsamplePts(r.money) };
  }
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
  newsFilter: true, earningsDays: 2, minSentiment: -0.35, blockSevere: true, sellOnSevere: false, tilt: false, flattenOnKill: true, allowLive: false, liveCap: 0, councilVeto: false,
  takeProfitPct: 15, timeStopDays: 30, trailTighten: true, addWeekly: 0, taxAware: true,
};
function cleanMandate(m = {}, base = MANDATE_DEF) {
  const o = { ...base };
  const nums = { liveCap: [0, 1e7], budget: [0, 1e9], maxPosPct: [5, 100], dailyLossPct: [0, 50], maxDDPct: [0, 90], maxTradesDay: [1, 100], minTrade: [1, 10000], earningsDays: [0, 10], minSentiment: [-1, 0], takeProfitPct: [0, 500], timeStopDays: [0, 365], addWeekly: [0, 100000] };
  for (const [k, [lo, hi]] of Object.entries(nums)) if (m[k] !== undefined) o[k] = clamp(num(m[k], base[k]), lo, hi);
  for (const k of ['stops', 'newsFilter', 'blockSevere', 'sellOnSevere', 'tilt', 'flattenOnKill', 'allowLive', 'councilVeto', 'trailTighten', 'taxAware']) if (m[k] !== undefined) o[k] = !!m[k];
  if (['trades', 'all', 'off'].includes(m.notify)) o.notify = m.notify;
  if (/^\d\d:\d\d$/.test(m.runAt || '') && m.runAt >= '09:45' && m.runAt <= '15:55') o.runAt = m.runAt;
  return o;
}
const botJson = b => ({ id: b.id, name: b.name, wid: b.wid, strategy: b.strategy, params: jsonOr(b.params, {}), syms: jsonOr(b.syms, []), mandate: { ...MANDATE_DEF, ...jsonOr(b.mandate, {}) }, status: b.status, created: tsMs(b.created), lastRun: tsMs(b.last_run), lastDay: b.last_day, peak: b.peak == null ? null : +b.peak, summary: b.summary, killed: b.killed_reason || null, sv: jsonOr(b.state, {})._sv || 1 });
async function botLog(botId, kind, title, body = '', data = null, sym = null) {
  await Q('insert into bot_log(bot_id,kind,title,body,data,sym) values($1,$2,$3,$4,$5,$6)', [botId, kind, String(title).slice(0, 200), String(body || '').slice(0, 4000), data ? JSON.stringify(data) : null, sym]).catch(e => warn('bot log', e.message));
  broadcast({ t: 'evt', e: 'hq', ev: { kind, botId, title: String(title).slice(0, 140), body: String(body || '').slice(0, 120), sym, side: data?.side || null, pnl: data?.fill?.pnl ?? null, qty: data?.fill?.qty ?? null, price: data?.fill?.price ?? null, ts: Date.now() } });
}
// Today's bars with the live price as today's close.
function withLive(b, sym) {
  const q = prices.get(sym), today = isCrypto(sym) ? new Date().toISOString().slice(0, 10) : etDate();
  const out = { sym, d: b.d.slice(), o: b.o.slice(), h: b.h.slice(), l: b.l.slice(), c: b.c.slice(), v: b.v.slice(), memo: {} };
  if (!q?.p) return { bars: out, live: false };
  if (!isCrypto(sym)) { const t = etParts(); if (!isTradingDay(t.y, t.m, t.d) || t.h * 60 + t.mi < 9 * 60 + 30) return { bars: out, live: false }; } // no stock bar before today's open
  const n = out.d.length - 1;
  if (out.d[n] === today) { out.c[n] = q.p; out.h[n] = Math.max(out.h[n], q.p); out.l[n] = Math.min(out.l[n], q.p); }
  else if (out.d[n] < today) { out.d.push(today); out.o.push(q.open || q.p); out.h.push(Math.max(q.hi || q.p, q.p)); out.l.push(Math.min(q.lo || q.p, q.p)); out.c.push(q.p); out.v.push(0); }
  return { bars: out, live: true };
}
const running = new Set();
async function runBot(id, { manual = false, dry = false, night = false } = {}) {
  if (!dry && standbyPaused()) fail('This is the standby copy and it\'s switched off, so bots don\'t trade here. Switch it on in Settings if your main server is down.');
  if (running.has(id)) fail('This bot is already deciding. Give it a moment.');
  running.add(id); if (!dry) broadcast({ t: 'evt', e: 'bot', id, deciding: true });
  try { return await runBotInner(id, { manual, dry, night }); } finally { running.delete(id); if (!dry) broadcast({ t: 'evt', e: 'bot', id }); }
}
// ---------- strategy upgrades: when a strategy gets better, every bot that uses it gets the new version ----------
// Settings a bot doesn't have yet get the new defaults; a version step can also move a setting that was still on
// its old default to the new one (a value you changed yourself is never touched) and add symbols the new version needs.
// strategy versions: when a strategy gets better, bots on it upgrade themselves on their next run (and at boot).
// set: { param: [old default, new default] } only changes params still at the old default; addSyms adds symbols the new version needs.
const DRIFT_SYMS = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'JPM', 'V', 'LLY', 'COST', 'NFLX', 'WMT', 'XOM', 'AMD', 'ORCL', 'CRM', 'ADBE', 'UNH', 'HD', 'MA', 'BAC', 'PG'];
const DIP_ETFS = ['SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF', 'XLV', 'XLE', 'XLY', 'XLI'];
const withSafe = p => p.safe && p.safe !== 'cash' ? [p.safe] : [];
const STRAT_VER = {
  momentum: [{ v: 2, title: 'Built for small accounts', note: 'Momentum rotation 2: holds fewer positions when there is less money, keeps a winner until it falls past a wider rank, puts new money in without selling, and parks money in SGOV (T-bills) when the market is weak.',
    changes: ['Fewer positions when there is less money ($25 smallest position)', 'Keeps a winner until it falls past a wider rank', 'New money tops up without selling', 'SGOV (T-bills) when the market is weak'],
    set: { flow: [undefined, 'flow'], safe: [undefined, 'SGOV'], buffer: [undefined, 1], minPos: [undefined, 25] }, addSyms: withSafe }],
  autopilot: [{ v: 2, title: 'Autopilot 2', note: 'Autopilot 2: 32 tryouts (11 new), favors what works in the current kind of market, splits money by score per unit of risk, re-plans weekly when things shift a lot, breeds variations of its best tryouts, parks idle money in T-bills, scales down for small accounts, and the Analyst reviews each new plan.',
    changes: ['32 tryouts, 11 new', 'Favors what works in today\'s kind of market', 'Money split by score per unit of risk', 'Weekly re-check, breeds variations of its best', 'Idle money in T-bills, sized for small accounts'] }],
  trend: [{ v: 2, title: 'Rides real trends, no cash drag', note: 'Trend follower 2: only buys trends that are also up over 6 months, ranks them by return per unit of swing, uses a looser 3.5 ATR trailing stop, keeps money between trends in SGOV (T-bills) and tops up with new money. The market-mood switch, take-profit, time stop, stop tightening and council veto no longer cut its trades short: those were selling winners early.',
    changes: ['Must also be up over the last 6 months', 'Ranked by return per unit of swing', 'Looser trailing stop: 3.5 ATRs', 'Idle money waits in SGOV (T-bills)', 'No more take-profit, time stop or mood cuts selling winners early; the council advises but can\'t veto'],
    set: { atrMult: [3, 3.5], momLen: [undefined, 126], safe: [undefined, 'SGOV'], flow: [undefined, 'flow'] }, addSyms: withSafe }],
  rsi2: [{ v: 2, title: 'Actually buys the dip', note: 'Dip buyer 2: also buys when RSI-2 is oversold over two days added up (far more trades), sells as soon as RSI-2 gets hot, and waits in SGOV (T-bills) between dips. It now trades a spread of index and sector ETFs. The council, market mood, news mood, correlation guard and daily AI check no longer block it: dips always come with bad news and a bad mood, so those were stopping every buy.',
    changes: ['Two-day oversold signal: many more trades', 'Sells when RSI-2 gets hot, or the bounce', 'Trades 10 index and sector ETFs', 'Waits in SGOV (T-bills) between dips', 'No longer blocked by mood, news mood, correlation or the council (only serious news stops a buy)'],
    set: { mode: [undefined, 'cum'], exitRsi: [undefined, 70], safe: [undefined, 'SGOV'] }, addSyms: (p, syms) => [...(syms.some(isCrypto) ? [] : DIP_ETFS), ...withSafe(p)] }],
  breakout: [{ v: 2, title: 'Fewer false breakouts', note: 'Breakout 2: only buys breakouts in a long-term uptrend and on stronger-than-normal volume, adds a trailing stop from the best close, risks 1.5% per trade with at most 4 positions, and parks money between trades in SGOV (T-bills). Take-profit, time stop, mood cuts and the council veto are off for it.',
    changes: ['Only in an uptrend (200-day)', 'Needs 1.2× normal volume', 'Trailing stop 4 ATRs from the best close', '1.5% risk per trade, up to 4 positions', 'Idle money in SGOV; no take-profit or time stop'],
    set: { trendLen: [undefined, 200], volX: [undefined, 1.2], trailAtr: [undefined, 4], riskPct: [1, 1.5], maxPositions: [6, 4], safe: [undefined, 'SGOV'] }, addSyms: withSafe }],
  hold: [{ v: 2, title: 'Really just holds', note: 'Buy and hold 2: no stops, vetoes, mood cuts or news filters any more, so it stays invested. New money goes to whatever is furthest below its share, and it only rebalances when something drifts more than 5 points off target. Shares can be equal or by calmness.',
    changes: ['No stops, vetoes, mood or news filters', 'New money fills the most underweight holding', 'Rebalances only past a 5-point drift', 'Equal shares or calmer assets get more'],
    set: { weighting: [undefined, 'equal'], band: [undefined, 5], flow: [undefined, 'flow'] } }],
  dual: [{ v: 2, title: 'Fully invested, steadier signal', note: 'Dual momentum 2: momentum is now the average of the 1, 3, 6 and 12-month returns (fewer false switches), the bar to beat is T-bills instead of zero, and when nothing beats them the money waits in SGOV. Index funds can hold all of the money instead of being capped at the per-position limit, which had left most of it in cash. Market mood, news, correlation, stops and the council veto no longer interfere with its monthly switch.',
    changes: ['Was only 25% invested: index funds now get all the money', 'Momentum = average of 1, 3, 6 and 12 months', 'Must beat T-bills, not just zero', 'Waits in SGOV when everything is falling', 'No more stops, mood or news cuts on its monthly switch'],
    set: { score: [undefined, 'blend'], safe: [undefined, 'SGOV'], fullFund: [undefined, 'on'] }, addSyms: withSafe }],
  lowvol: [{ v: 2, title: 'Calm and rising', note: 'Low volatility 2: also needs to be up over 6 months, calmer stocks get a bigger share, a holding stays while it is within 2 places of the top, new money tops up without selling, and empty slots wait in SGOV. Market mood, correlation, take-profit, time stop and the daily AI trim no longer touch it.',
    changes: ['Must also be up over the last 6 months', 'Calmer stocks get a bigger share', 'Keeps a holding within 2 places of the top', 'New money tops up without selling', 'Empty slots wait in SGOV'],
    set: { momLen: [undefined, 126], sizing: [undefined, 'invvol'], buffer: [undefined, 2], safe: [undefined, 'SGOV'], flow: [undefined, 'flow'] }, addSyms: withSafe }],
  drift: [{ v: 2, title: 'Actually catches the jumps', note: 'News jump drift 2: it only looked at today, when the day\'s volume isn\'t complete yet, so it never saw a jump. Now it looks for a jump on heavy volume in the last 3 finished days and buys if the stock is still holding at least half of it. The jump needed is 1.5 ATRs instead of 2, and the list grows to 24 big stocks if it was short. Mood, correlation, news mood, take-profit, time stop and the council veto no longer block it. Waiting money sits in SGOV.',
    changes: ['Was blind to jumps (today\'s volume is incomplete): now checks the last 3 finished days', 'Buys if it\'s still holding half the jump', 'Jump of 1.5 ATRs instead of 2', 'Trades 24 big stocks if the list was short', 'Not blocked by mood, news mood or the council; waits in SGOV'],
    set: { jump: [2, 1.5], within: [undefined, 3], safe: [undefined, 'SGOV'] }, addSyms: (p, syms) => [...(syms.filter(x => !isCrypto(x)).length < 12 ? DRIFT_SYMS : []), ...withSafe(p)] }],
  tom: [{ v: 2, title: 'Earns interest between windows', note: 'Turn of the month 2: between windows the money earns T-bill interest in SGOV instead of sitting in cash, and index funds can hold all of it (it was capped at 25% each). The council, mood, news, correlation guard and stops are off for it: the correlation guard was blocking its second fund. It only trades from the last trading day of the month through day 3, so quiet weeks are normal; the bot page says when the next window opens.',
    changes: ['Waits in SGOV (T-bills) between windows', 'Index funds can hold all the money', 'Correlation guard no longer blocks its second fund', 'No council, mood, news or stops in the way', 'Shows when the next window opens'],
    set: { safe: [undefined, 'SGOV'], fullFund: [undefined, 'on'] }, addSyms: withSafe }],
  snapback: [{ v: 2, title: 'Cleaner panics, more of them', note: 'Snap-back 2: buys at 1.75 standard deviations under the band instead of 2 (more trades), only when it closed in the lower half of its day (still panicking), sells when it is back at its average, and waits in SGOV between setups. It trades 10 index and sector ETFs. Mood, news mood, correlation and the council veto no longer block it.',
    changes: ['1.75 std devs instead of 2: more trades', 'Must close near the low of its day', 'Trades 10 index and sector ETFs', 'Waits in SGOV between setups', 'Not blocked by mood, news mood, correlation or the council'],
    set: { bands: [2, 1.75], exitZ: [undefined, 0], safe: [undefined, 'SGOV'] }, addSyms: (p, syms) => [...(syms.some(isCrypto) ? [] : DIP_ETFS), ...withSafe(p)] }],
  earnings: [{ v: 2, title: 'Fits any wallet', note: 'Earnings drift 2: holds fewer positions when the wallet is small so each one is a useful size, and the market mood, correlation guard, news mood, take-profit, time stop and council veto no longer block it.',
    changes: ['Fewer, bigger positions in a small wallet', 'Not blocked by mood, news mood or the council'] }],
};
// every strategy: fewer positions when the wallet is small, so nothing is bought in dust-sized pieces
const FIT = { v: 3, title: 'Fits any wallet', changes: ['Holds fewer positions when the wallet is small, so each one is at least $25', 'In a tiny wallet the per-position limit stretches to that minimum'] };
for (const k of ['trend', 'rsi2', 'breakout', 'hold']) STRAT_VER[k].push({ ...FIT, note: `${{ trend: 'Trend follower', rsi2: 'Dip buyer', breakout: 'Breakout', hold: 'Buy and hold' }[k]} 3: holds fewer positions when the wallet is small so each one is at least $25, and in a tiny wallet the per-position limit stretches to that minimum.` });
const V1 = { title: 'Original', changes: [] };
const stratVersion = k => { const l = STRAT_VER[k] || []; const last = l[l.length - 1]; return last ? { v: last.v, title: last.title, changes: last.changes || [], history: l.map(x => ({ v: x.v, title: x.title, changes: x.changes || [] })) } : { v: 1, ...V1, history: [] }; };
async function upgradeBot(b) {
  const steps = STRAT_VER[b.strategy]; if (!steps || !STRATEGIES[b.strategy]) return b;
  const state = jsonOr(b.state, {}), have = state._sv || 1, todo = steps.filter(x => x.v > have); if (!todo.length) return b;
  const raw = jsonOr(b.params, {}), params = { ...raw }, syms = jsonOr(b.syms, []), notes = [];
  for (const step of todo) {
    for (const [k, [from, to]] of Object.entries(step.set || {})) if (params[k] === from || params[k] === undefined) params[k] = to;
    for (const s2 of step.addSyms?.(params, syms) || []) if (!syms.includes(s2) && !STRATEGIES[b.strategy].auto) syms.push(s2);
    notes.push(step.note);
  }
  const clean = STRATEGIES[b.strategy].auto ? { ...raw, ...params } : cleanParams(b.strategy, params); state._sv = todo[todo.length - 1].v;
  await Q('update bots set params=$1, syms=$2, state=$3 where id=$4', [JSON.stringify(clean), JSON.stringify(syms), JSON.stringify(state), b.id]);
  await botLog(b.id, 'info', `Upgraded to ${STRATEGIES[b.strategy].name} v${state._sv}`, notes.join('\n') + ' Settings you changed yourself were kept.');
  refreshUserSyms().catch(() => {});
  return { ...b, params: JSON.stringify(clean), syms: JSON.stringify(syms), state: JSON.stringify(state) };
}
async function upgradeAllBots() { for (const b of await rows('select * from bots')) await upgradeBot(b).catch(e => warn('upgrade', b.id, e.message)); }
async function runBotInner(id, { manual, dry, night }) {
  let b = await row('select * from bots where id=$1', [id]); if (!b) fail('That bot is gone', 404);
  if (!dry) b = await upgradeBot(b);
  const bot = botJson(b), st = STRATEGIES[bot.strategy], m = bot.mandate, state = jsonOr(b.state, {});
  const w = await walletRow(bot.wid);
  if (w.archived) fail('The bot\'s wallet is archived');
  if (st.auto) { const want = apSyms(bot.params.markets || 'both'); if (want.join() !== bot.syms.join()) { bot.syms = want; await Q('update bots set syms=$1 where id=$2', [JSON.stringify(want), id]); refreshUserSyms().catch(() => {}); } }
  const brk = !!w.broker, liveMoney = brk && BROKER?.env === 'live';
  if (brk && !BROKER) fail('This bot trades through Alpaca, but the Alpaca keys are not set in Railway.');
  if (brk && S.realHalt && !dry) fail('Alpaca trading is stopped by the kill switch.');
  if (liveMoney && !m.allowLive) fail('This bot is on a real-money wallet but real-money trading is off for it. Turn it on in its limits.');
  // ---- weekly money in: paper wallets get the deposit, Alpaca bots get a bigger budget (you deposit at Alpaca) ----
  if (m.addWeekly > 0 && !dry) {
    const wk = weekOf(etDate());
    if (!state._addWeek) state._addWeek = wk; // the starting money counts as the first week
    else if (state._addWeek !== wk) {
      state._addWeek = wk;
      if (!w.broker) { await deposit(bot.wid, m.addWeekly, 'practice'); await botLog(id, 'info', `Weekly deposit: ${money(m.addWeekly)}`, `Added ${money(m.addWeekly)} to ${bot.name}'s wallet, like a weekly paycheck going in.`); }
      else if (m.budget > 0) { m.budget = round(m.budget + m.addWeekly, 2); await Q('update bots set mandate=$1 where id=$2', [JSON.stringify(m), id]); await botLog(id, 'info', `Budget up ${money(m.addWeekly)} to ${money(m.budget)}`, `Your weekly add. Remember to deposit ${money(m.addWeekly)} at Alpaca too; the bot only uses money that's really there.`); }
      await Q('update bots set state=$1 where id=$2', [JSON.stringify(state), id]);
    }
  }
  const real = isReal(w), prof = profOf(w), sess = sessionAt(prof);
  const pos = await positions(bot.wid), acct = await accountInfo(bot.wid, { w, pos });
  const today = etDate(), mine = new Set(bot.syms);
  const equity = acct.equity;
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
  const syms = bot.syms.filter(s => isCrypto(s) || sess === 'regular' || manual || night);
  if (!isCrypto(bot.syms[0] || '') && mwcbOn() && mwcbNow()) log.risk.push(haltOf('SPY')?.why || 'The stock market is halted by a circuit breaker.');
  const bench = benchFor(bot.syms);
  const moodSyms = [bot.syms.some(s => !isCrypto(s)) ? 'SPY' : null, bot.syms.some(isCrypto) ? 'BTC' : null].filter(Boolean);
  const { data, missing } = await loadUniverse([...new Set([...bot.syms, bench, ...moodSyms])]);
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
  const band = st.band || 0.01;
  // which of the shared safety layers this strategy uses: a dip buyer shouldn't be blocked by bad-mood news, buy and hold shouldn't be stopped out
  const OV = { council: 'veto', mood: true, corr: true, exits: true, tighten: true, news: 'all', review: true, stops: true, ...(st.overlays || {}), ...(bot.params?.overlays || {}) };
  const binding = OV.council === 'veto' || !!m.councilVeto, parkSym = bot.params?.safe && bot.params.safe !== 'cash' ? bot.params.safe : null;
  const ctx = { syms: tradable, data: series, idx, params: bot.params, held, bench, maxW: walletMaxW(m.maxPosPct / 100, capital, m.minTrade), capital, minTrade: m.minTrade, beats: bot.strategy === 'earnings' ? await earningsBeats(tradable).catch(() => null) : null };
  if (st.prep) ctx.extra = await st.prep(bot, ctx).catch(e => { log.skips.push('Filings check: ' + e.message); return null; });
  // the market's mood (S&P 500 for stocks, Bitcoin for crypto) scales new buys; Autopilot and Buy and hold manage their own exposure
  const moodOn = S.moodSwitch !== false && !st.auto && OV.mood, mood = moodOn ? { stock: moodOf(series.SPY, 'The S&P 500'), crypto: moodOf(series.BTC, 'Bitcoin') } : null;
  const dec = st.auto ? await autopilotDecide(bot, state, series, today, { dry, manual, dd, capital }) : rebalanceDue ? st.decide(ctx) : { weights: Object.fromEntries(Object.entries(held).map(([s, h]) => [s, h.weight || (h.qty * (priceOf(s) || h.entry)) / capital])), stops: {}, notes: Object.fromEntries(Object.keys(held).map(s => [s, 'Holding until the next monthly rebalance.'])) };
  if (!st.auto && dec.regime && !ctx.regime) ctx.regime = dec.regime;
  if (ctx.maxW > m.maxPosPct / 100 + 1e-9) log.risk.push(`Small wallet: a position can be up to ${Math.round(ctx.maxW * 100)}% (instead of ${m.maxPosPct}%) so each one is at least $${Math.max(25, m.minTrade * 2)}.`);
  if (st.auto) { ctx.regime = dec.regime; for (const k of Object.keys(dec.weights)) dec.weights[k] = Math.min(dec.weights[k], m.maxPosPct / 100); }
  if (!rebalanceDue) log.risk.push(`${st.name} rebalances monthly; next on the first trading day of next month. Holding as is.`);
  // ---- news and social check before buying ----
  const buzz = {};
  const wantBuy = Object.entries(dec.weights).filter(([s, wt]) => wt > 0 && tradable.includes(s));
  const phase = p => { if (!dry) broadcast({ t: 'evt', e: 'hq', ev: { kind: 'phase', phase: p, botId: id, ts: Date.now() } }); };
  phase('news');
  if (m.newsFilter && S.f_buzz !== false) {
    const list = [...new Set([...wantBuy.map(([s]) => s), ...Object.keys(held), ...tradable])].filter(s => !night || isCrypto(s)).slice(0, 24);
    for (let i = 0; i < list.length; i += 8) await Promise.all(list.slice(i, i + 8).map(s => Promise.race([buzzFor(s), sleep(12000).then(() => null)]).then(x => { if (x) buzz[s] = x; }).catch(() => {})));
  }
  const decisions = {}; let moodCut = 0;
  for (const s of new Set([...tradable, ...Object.keys(held)])) {
    if (night && !isCrypto(s)) continue; // the night shift only handles crypto; stocks wait for the day crew
    const want = dec.weights[s] > 0 ? dec.weights[s] : 0, px = priceOf(s), have = (held[s]?.qty || 0) * (px || 0), bz = buzz[s];
    const d = { sym: s, note: dec.notes[s] || (tradable.includes(s) ? '' : isCrypto(s) ? 'No fresh price yet.' : 'Stock market is closed; stocks are only traded in regular hours.'), want, have, buzz: bz ? buzzLine(bz) : null, flags: bz?.flags || [], action: 'hold', why: [] };
    let wt = want;
    if (bz && m.newsFilter && OV.news !== 'none' && wt * capital > have + 1) {
      const only = OV.news === 'severe';
      const up = bz.upcoming.find(u => u.k === 'earnings');
      if (!only && m.earningsDays > 0 && up && up.tradingDays <= m.earningsDays) { wt = have / capital; d.why.push(`Skipped buying: earnings ${up.tradingDays <= 0 ? 'today' : `in ${up.tradingDays} trading day${up.tradingDays === 1 ? '' : 's'}`}. Prices can jump either way on the report.`); }
      else if (m.blockSevere && bz.flags.includes('severe_news')) { wt = have / capital; d.why.push(`Skipped buying: serious news (${bz.events.filter(e => e.k === 'severe').map(e => '"' + e.title.slice(0, 90) + '"').join(', ')}).`); }
      else if (!only && bz.count >= 5 && bz.sentiment <= m.minSentiment) { wt = have / capital; d.why.push(`Skipped buying: the mood is negative (${bz.sentiment.toFixed(2)} across ${bz.count} items).`); }
      else if (!only && m.tilt && bz.count >= 5) { const k = clamp(0.75 + bz.sentiment * 0.5, 0.5, 1); if (k < 0.99) { wt = Math.max(have / capital, wt * k); d.why.push(`Sized down to ${Math.round(k * 100)}% because of the news mood.`); } }
    }
    if (bz && held[s] && OV.news !== 'none' && m.sellOnSevere && bz.flags.includes('severe_news')) { wt = 0; d.why.push('Selling: serious negative news and "sell on bad news" is on.'); }
    const parking = s === bot.params?.safe && bot.params.safe !== 'cash';
    // weak mood: new positions start smaller and held ones aren't added to, so repeated runs don't nibble
    if (mood && !parking && wt * capital > have + 1) { const md = isCrypto(s) ? mood.crypto : mood.stock; if (md && md.scale < 1) { const w0 = wt; wt = held[s] ? have / capital : (have + (wt * capital - have) * md.scale) / capital; moodCut += w0 - wt; d.why.push(md.line); d.mood = md.k; } }
    // correlation guard: don't open a position that moves almost in lockstep with one it already holds
    if (S.corrGuard !== false && OV.corr && !parking && !held[s] && wt > 0 && series[s]) { for (const hs of Object.keys(held)) { if (hs === s) continue; const r = corr60(series[s], series[hs]); if (r != null && r >= (S.corrMax ?? 0.9)) { wt = 0; d.why.push(`Skipped buying: over the last 60 days it moved almost exactly like ${hs} (correlation ${r.toFixed(2)}), which it already holds.`); break; } } }
    if (stoppedToday.has(s) && !held[s] && wt > 0) { wt = 0; d.why.push('Not buying back today: its stop loss sold it earlier today.'); }
    if (noBuys && wt * capital > have) { wt = have / capital; d.why.push('No new buys today: the daily loss limit was hit.'); }
    const hl = haltOf(s); if (hl) { wt = Math.min(wt, have / capital); if (!d.why.includes(hl.why)) d.why.push(`Waiting: ${hl.why}`); d.halted = true; } // halted stocks hold as they are until trading resumes
    d.target = wt; decisions[s] = d;
  }
  // what the mood switch held back waits in the T-bill fund (if the bot has one) instead of sitting in cash
  { const ps = bot.params?.safe, pd = ps && ps !== 'cash' ? decisions[ps] : null; if (pd && moodCut > 0.01 && !noBuys) { pd.target = Math.min(1, pd.target + moodCut); pd.why.push(`The ${Math.round(moodCut * 100)}% the mood switch held back waits in ${ps} (T-bills).`); } }
  // ---- smarter exits: take some profit, and let go of trades that went nowhere ----
  for (const d of OV.exits ? Object.values(decisions) : []) {
    const h = held[d.sym], px = priceOf(d.sym); if (!h || !px || !(d.target > 0)) continue;
    const gain = px / h.entry - 1, s0 = state[d.sym] || {};
    if (m.takeProfitPct > 0 && gain * 100 >= m.takeProfitPct && !s0.tookProfit && d.target * capital > d.have * 0.6) { d.aiTrim = true; d.target = d.have / 2 / capital; d.takeProfit = true; if (d.mood) { d.why = d.why.filter(w => !/New buys at/.test(w)); delete d.mood; } d.why.push(`Trimming by half: up ${(gain * 100).toFixed(1)}%, past the ${m.takeProfitPct}% take-profit. The rest keeps running with a tighter stop.`); continue; }
    if (m.timeStopDays > 0 && st.rebalance === 'none' && !st.auto && bot.strategy !== 'hold' && (h.days || 0) >= m.timeStopDays && gain < 0.03) { d.target = 0; d.why.push(`Selling: time stop. ${h.days} trading days and only ${(gain * 100).toFixed(1)}%, so the money can work somewhere else.`); }
  }
  // ---- tax-aware selling: an exit that can wait a few days for shares to turn long term waits (not for serious news or a time stop)
  if (m.taxAware && !isIra(w) && !w.broker) for (const d of Object.values(decisions)) {
    const h = held[d.sym], px = priceOf(d.sym); if (!h || !px || d.target > 0 || d.halted || d.why.some(x => /serious news|time stop|Analyst/.test(x))) continue;
    let soon = 0, days = 0, on = null;
    for (const l of await rows('select ts, qty, basis from lots where wid=$1 and sym=$2 and qty > 0', [bot.wid, d.sym])) { const dd = daysToLong(l.ts), g = (px - +l.basis) * +l.qty; if (dd > 0 && dd <= 10 && g > 0) { soon += g; if (dd > days) { days = dd; on = tsMs(l.ts) + 366 * 864e5; } } }
    if (!(soon > 0)) continue;
    const save = taxOn({ short: soon }).total - taxOn({ long: soon }).total;
    if (save >= Math.max(10, d.have * 0.005)) { d.target = d.have / capital; d.taxWait = true; d.why.push(`Waiting ${days} day${days === 1 ? '' : 's'} to sell: then ${money(soon)} of gains turns long term, which saves about ${money(save)} in tax. Its stop stays in place.`); }
  }
  // ---- the council: six kinds of evidence vote on each buy and sell ----
  phase('council');
  const sessions = [], councilRec = await councilRecord();
  for (const d of OV.council === 'off' ? [] : Object.values(decisions).sort((a, b) => b.want - a.want)) {
    const buying = d.want * capital > d.have + Math.max(m.minTrade, 1), selling = d.have > 0 && !(d.target > 0);
    if (!(buying || selling) || sessions.length >= 12 || !series[d.sym] || (d.sym === bot.params?.safe && bot.params.safe !== 'cash')) continue;
    const bz = buzz[d.sym], up = bz?.upcoming?.find(u => u.k === 'earnings') || null;
    const s = night ? await nightSession(d, series, bz, up, { strategy: STRATEGIES[bot.strategy]?.name || bot.strategy }) : councilSession(bot, d, series, bz, up, m, capital);
    // the AI members (Claude) vote too: specialists first, then the Analyst, who debates the others
    const ais = night ? [] : await aiRound(d.sym, series[d.sym], { side: buying ? 'buy' : 'sell', target: d.target }, bz, up, { strategy: STRATEGIES[bot.strategy]?.name || bot.strategy, bench: series.SPY ? IND.ret(series.SPY.c, 63, series.SPY.c.length - 1) : null, have: d.have / capital, positions: Object.keys(held).length, maxPos: m.maxPosPct, stops: !!m.stops }, s.votes).catch(e => { warn('ai round', e.message); return []; });
    if (ais.length) { s.votes.push(...ais); s.yes = s.votes.filter(v => v.vote === 'yes').length; s.no = s.votes.filter(v => v.vote === 'no').length;
      // the Analyst has the last word on size; without its vote, the most cautious specialist decides
      const an = ais.find(v => v.k === 'ai' && v.c === 'ai' && v.vote !== 'pass'), cut = an || ais.filter(v => v.c === 'ai' && v.vote !== 'pass').sort((a, b) => a.size - b.size)[0];
      if (cut && buying && binding && S.aiSize !== false && cut.size < 1 && d.target * capital > d.have + 1) { const add = d.target * capital - d.have, nt = (d.have + add * cut.size) / capital; d.why.push(`The ${nameOf(cut.k)} cut this buy to ${Math.round(cut.size * 100)}%: ${cut.line}`); d.target = nt; } }
    // at night the Night Owl gets the same say over size that the Analyst has in the day
    if (night && buying && binding && S.aiSize !== false && d.target * capital > d.have + 1) { const ow = s.votes.find(v => v.k === 'owl' && v.c === 'ai' && v.vote !== 'pass'); if (ow && ow.size < 1) { const add = d.target * capital - d.have; d.target = (d.have + add * ow.size) / capital; d.why.push(`The Night Owl cut this buy to ${Math.round(ow.size * 100)}%: ${ow.line}`); } }
    Object.assign(s, weighVotes(s.votes, situation(series[d.sym], bz, up), councilRec));
    const lead = s.votes.find(v => (v.k === 'ai' || v.k === 'owl') && v.c === 'ai'), stillBuying = () => buying && d.target * capital > d.have + 1;
    // AI veto: a confident NO from the Analyst (or the Night Owl), backed by at least one other member, stops the buy
    if (stillBuying() && binding && S.aiVeto !== false && lead?.vote === 'no' && lead.conf >= 0.75 && s.votes.some(v => v !== lead && v.vote === 'no' && (v.c === 'ai' || night))) { d.target = d.have / capital; d.why.push(`Skipped buying: the ${nameOf(lead.k)} vetoed it (${Math.round(lead.conf * 100)}% sure): ${lead.line}`); }
    // council veto (on for every bot unless switched off in Settings): a majority NO stops the buy
    if ((m.councilVeto || (binding && S.vetoAll !== false)) && stillBuying() && leansNo(s)) { d.target = d.have / capital; d.why.push(`Skipped buying: the council voted ${tallyText(s)} against${weightedOn() ? ' (weighted)' : ''}, and council veto is on.`); }
    // sizing up: when the AI lead is very sure and the council agrees, the buy can grow, but never past the bot's max position
    if (stillBuying() && S.aiSizeUp !== false && lead?.vote === 'yes' && lead.size > 1 && lead.conf >= 0.7 && !leansNo(s) && (s.wy ?? s.yes) > (s.wn ?? s.no)) {
      const add = d.target * capital - d.have, cap = (m.maxPosPct || 25) / 100 * capital, nt = Math.min(d.have + add * lead.size, cap) / capital;
      if (nt > d.target + 1e-4) { d.why.push(`The ${nameOf(lead.k)} sized this buy up to ${Math.round(nt / d.target * 100)}% (${Math.round(lead.conf * 100)}% sure): ${lead.line}`); d.target = nt; } }
    d.council = s; sessions.push(d);
  }
  // the daily position check: the Analyst reviews what the bot already holds and can trim or sell a position that's turning
  if (!night && !dry && OV.review && S.aiReview !== false && analystReady()) {
    let n = 0;
    for (const d of Object.values(decisions)) {
      if (n >= 8 || !held[d.sym] || sessions.includes(d) || !(d.target > 0) || !series[d.sym] || !priceOf(d.sym)) continue;
      const h = held[d.sym], bz = buzz[d.sym], up = bz?.upcoming?.find(u => u.k === 'earnings') || null; n++;
      const r = await aiReview(d.sym, series[d.sym], bz, up, { strategy: STRATEGIES[bot.strategy]?.name || bot.strategy, entry: h.entry, pnl: priceOf(d.sym) / h.entry - 1, days: h.days, stopPx: h.stopPx, weight: d.have / capital, high: h.high }, id).catch(() => null);
      if (!r || r.c !== 'ai') continue;
      d.review = { action: r.action, conf: r.conf, line: r.line };
      if (r.action === 'sell' && r.conf >= 0.75) { d.target = 0; d.why.push(`Selling: the Analyst's daily check says it's time (${Math.round(r.conf * 100)}% sure): ${r.line}`); }
      else if (r.action === 'trim' && r.conf >= 0.6) { d.aiTrim = true; d.target = d.have / 2 / capital; d.why.push(`Trimming by half: the Analyst's daily check (${Math.round(r.conf * 100)}% sure): ${r.line}`); }
      else continue;
      const s = { votes: [{ k: 'ai', vote: 'no', conf: r.conf, size: 1, line: r.line, c: 'ai', review: r.action }], yes: 0, no: 1 }; Object.assign(s, weighVotes(s.votes, {}, councilRec));
      d.council = s; sessions.push(d);
    }
  }
  phase('trade');
  // ---- orders: sells first to free cash, then buys ----
  const placeOrder = async (s, side, o, label) => {
    if (dry) { log.orders.push({ sym: s, side, ...o, dry: true, label }); return { dry: true }; }
    if (tradeBudget <= 0) { log.skips.push(`${s}: daily trade limit (${m.maxTradesDay}) reached.`); return null; }
    try {
      let r;
      if (brk) {
        // through Alpaca: stocks sent outside market hours wait for the open there
        if (side === 'sell') for (const o of (BROKER.orders || []).filter(o => o.sym === s && o.side === 'sell')) await brokerCancel(o.id).catch(() => {}); // free shares held by its stop
        let amount = o.usd ?? o.qty ?? o.pct, mode = o.usd ? 'usd' : o.qty ? 'qty' : 'pct';
        if (liveMoney && side === 'buy') {
          const inv = (await positions(bot.wid)).filter(p => mine.has(p.sym)).reduce((t, p) => t + p.qty * (priceOf(p.sym) || p.avg), 0);
          const room = Math.max(0, (m.liveCap || 0) - inv); if (amount > room) amount = Math.floor(room * 100) / 100;
          if (amount < m.minTrade) { log.skips.push(`${s}: the real-money cap of ${money(m.liveCap)} is used up.`); return null; }
        }
        const q = await brokerTrade(w, { sym: s, side, mode, amount, live: true });
        r = { broker: true, status: q.status, id: q.order?.id };
        tradeBudget--;
        const text = `${side === 'buy' ? 'Buy' : 'Sell'} ${s}${mode === 'usd' ? ' ' + money(amount) : ''} sent to Alpaca${liveMoney ? ' (real money)' : ''}: ${q.status}`;
        log.orders.push({ sym: s, side, text, label });
        await botLog(id, 'trade', text, label, { side, broker: true, status: q.status, live: liveMoney }, s);
        return r;
      }
      if (!isCrypto(s) && sessionAt(prof) !== 'regular' && real) { const q = await addLimit(bot.wid, s, { side, kind: 'moo', amount: o.usd ?? o.qty ?? o.pct, units: o.usd ? 'usd' : o.qty ? 'qty' : 'pct', why: 'bot: ' + bot.name }); r = { queued: true, id: q.id }; }
      else r = await execTrade(bot.wid, s, side, { ...o, why: 'bot: ' + bot.name.slice(0, 40), auto: true, bot: true, note: label, lotPick: m.taxAware && !isIra(w) ? 'hifo' : undefined });
      tradeBudget--;
      const text = r.queued ? `${side === 'buy' ? 'Buy' : 'Sell'} queued for the open` : `${side === 'buy' ? 'Bought' : 'Sold'} ${+r.qty.toFixed(6)} ${s} at ${pxStr(r.price)}${r.pnl != null ? ` (${r.pnl >= 0 ? '+' : ''}${money(r.pnl)})` : ''}`;
      log.orders.push({ sym: s, side, text, label, fill: r.queued ? null : { qty: r.qty, price: r.price, fee: r.fee, pnl: r.pnl } });
      await botLog(id, 'trade', text, label, { side, fill: r.queued ? null : { qty: r.qty, price: r.price, fee: r.fee, pnl: r.pnl, id: r.id } }, s);
      return r;
    } catch (e) { log.skips.push(`${s}: ${e.message}`); await botLog(id, 'error', `Couldn't ${side} ${s}`, e.message, null, s); return null; }
  };
  // the T-bill spot is sold down outside rebalance days only to pay for a brand-new position (daily strategies: whenever their targets say so)
  const parkNeed = Object.values(decisions).some(x => x.sym !== parkSym && !held[x.sym] && x.target > 0);
  const parkFree = !!parkSym && (st.rebalance === 'none' || Object.values(decisions).some(x => x.sym !== parkSym && !held[x.sym] && x.target > 0));
  for (const d of Object.values(decisions)) {
    const px = priceOf(d.sym); if (!px || !held[d.sym]) continue;
    if (!(isCrypto(d.sym) || tradable.includes(d.sym))) continue;
    const targetV = d.target * capital, diff = targetV - d.have;
    if (d.target <= 0) { d.action = 'sell'; await placeOrder(d.sym, 'sell', { pct: 100 }, `${st.short}: ${d.why.find(w => /^Selling/.test(w)) || d.why[0] || d.note}`); delete state[d.sym]; }
    else if (d.aiTrim) { d.action = 'trim'; await placeOrder(d.sym, 'sell', { qty: held[d.sym].qty / 2 }, `${st.short}: ${d.why.find(w => /^Trimming/.test(w))}`); if (d.takeProfit) state[d.sym] = { ...(state[d.sym] || {}), tookProfit: today }; }
    else if (((rebalanceDue && st.rebalance !== 'none' && !dec.noTrim) || (d.sym === parkSym && parkFree)) && -diff > Math.max(m.minTrade, capital * (d.sym === parkSym && !parkNeed ? 0.03 : band))) { d.action = 'trim'; await placeOrder(d.sym, 'sell', { qty: Math.min(held[d.sym].qty, -diff / px) }, `${st.short}: rebalance down to ${pct0n(d.target)} of the wallet`); }
  }
  const exposure = S.sharedRisk === false ? null : await botExposure(brk).catch(() => null);
  // topping up between rebalances: all the spare cash goes to the single most underweight holding (one small trade, not several)
  const topUp = bot.params?.flow === 'flow' && !rebalanceDue ? Object.values(decisions).filter(d => held[d.sym] && d.sym !== parkSym && d.target > 0 && tradable.includes(d.sym)).sort((a, b) => (b.target * capital - b.have) - (a.target * capital - a.have))[0]?.sym : null;
  for (const d of Object.values(decisions).sort((a, b) => (a.sym === parkSym) - (b.sym === parkSym) || b.target - a.target)) {
    if (!(d.target > 0) || !tradable.includes(d.sym)) continue;
    if (bot.params?.flow === 'flow' && !rebalanceDue && held[d.sym] && d.sym !== topUp && d.sym !== parkSym) continue;
    const px = priceOf(d.sym), targetV = d.target * capital, diff = targetV - d.have;
    const isNew = !held[d.sym];
    if (!isNew && !(rebalanceDue && st.rebalance !== 'none') && bot.params?.flow !== 'flow' && d.sym !== parkSym) continue; // 'top up' bots put spare cash to work any day
    // small daily wobbles in the T-bill spot and in top-ups of daily strategies aren't worth a trade
    const minGap = d.sym === parkSym ? 0.03 : isNew ? 0.01 : st.rebalance === 'none' ? Math.max(band, 0.025) : band;
    if (diff <= Math.max(m.minTrade, capital * minGap)) continue;
    const a2 = dry ? acct : await accountInfo(bot.wid);
    const bp = isCrypto(d.sym) || !marginable(d.sym, px) ? a2.bpCrypto : a2.bp;
    const room = sharedRoom(exposure, d.sym), usd = Math.floor(Math.min(diff, bp * 0.995, room) * 100) / 100;
    if (room < diff) d.why.push(room < m.minTrade ? `Not buying: all bots together already have the most allowed in ${room === 0 && sectorOf(d.sym) !== 'Other' ? (exposure.bySym[d.sym] ? d.sym : sectorOf(d.sym)) : d.sym}.` : `Bought less: all bots together are near the cap for ${d.sym} / ${sectorOf(d.sym)}.`);
    if (usd < m.minTrade) { d.why.push(`Not enough buying power for this (${money(bp)} left).`); continue; }
    d.action = isNew ? 'buy' : 'add';
    await placeOrder(d.sym, 'buy', { usd }, `${st.short}: ${d.note}${d.why.length ? ' ' + d.why.join(' ') : ''}`.slice(0, 480));
  }
  for (const d of sessions) d.council.chair = chairLine(d, d.council);
  if (!dry) for (const d of sessions) await saveCouncil(id, d, d.council, priceOf(d.sym));
  // ---- protective stops ----
  if (!dry && m.stops && OV.stops) {
    const pnow = await positions(bot.wid);
    for (const p of pnow) {
      if (!mine.has(p.sym)) continue;
      let sp = dec.stops[p.sym] || (state[p.sym]?.stopPx ? { price: state[p.sym].stopPx, trail: false } : null);
      if (!sp && m.trailTighten && OV.tighten) { const t = tightStop(p.avg, Math.max(state[p.sym]?.high || 0, priceOf(p.sym) || 0), priceOf(p.sym)); if (t) sp = { price: t, trail: false }; } // a winner with no stop yet gets one
      if (!sp || !(sp.price > 0)) continue;
      const prevStop = state[p.sym]?.stopPx, cur = priceOf(p.sym), tight = m.trailTighten && OV.tighten ? tightStop(p.avg, Math.max(state[p.sym]?.high || 0, cur || 0), cur) : null;
      let stopPx = sp.trail && prevStop ? Math.max(prevStop, sp.price) : sp.price; if (tight) stopPx = Math.max(stopPx, tight, prevStop || 0);
      if (!(stopPx < cur * 0.995)) continue;
      if (brk) {
        const ex = (BROKER.orders || []).find(o => o.sym === p.sym && o.side === 'sell' && (o.kind === 'stop' || o.kind === 'trail'));
        if (ex && Math.abs((ex.price || 0) / stopPx - 1) < 0.005) { state[p.sym] = { ...(state[p.sym] || {}), stopPx }; continue; }
        try { if (ex) await brokerCancel(ex.id); await brokerProtect(w, { sym: p.sym, kind: 'stop', price: stopPx, pct: 100, live: true }); state[p.sym] = { ...(state[p.sym] || {}), stopPx }; } catch (e) { log.skips.push(`${p.sym} stop at Alpaca: ${e.message}`); }
        continue;
      }
      const existing = (condOrders.get(p.sym) || []).find(o => o.wid === bot.wid && (o.kind === 'stop' || o.kind === 'trail'));
      if (existing && Math.abs(existing.price / stopPx - 1) < 0.005) { state[p.sym] = { ...(state[p.sym] || {}), stopPx }; continue; }
      try { await addCondOrder(bot.wid, p.sym, 'stop', { price: stopPx, pct: 100 }); state[p.sym] = { ...(state[p.sym] || {}), stopPx }; } catch (e) { log.skips.push(`${p.sym} stop: ${e.message}`); }
    }
  }
  if (bot.strategy === 'covcall' && !night && !brk) await covcallRun(bot, w, log, dry).catch(e => log.skips.push('Options step: ' + e.message));
  // ---- remember what it holds and why ----
  if (!dry) {
    const pnow = await positions(bot.wid);
    for (const p of pnow) if (mine.has(p.sym)) {
      const s0 = state[p.sym] || {}, d = decisions[p.sym];
      state[p.sym] = { ...s0, entryDate: s0.entryDate || today, high: Math.max(s0.high || 0, priceOf(p.sym) || 0), days: held[p.sym] ? held[p.sym].days : 0, lastDay: today, weight: d?.target || s0.weight };
    }
    for (const s of Object.keys(state)) if (!s.startsWith('_') && !pnow.some(p => p.sym === s)) delete state[s];
    if (rebalanceDue && st.rebalance !== 'none' && !night) state._lastRebalance = today;
    const bought = log.orders.filter(o => o.side === 'buy').map(o => o.sym), sold = log.orders.filter(o => o.side === 'sell').map(o => o.sym);
    const optLines = log.orders.filter(o => o.side === 'opt').map(o => o.short);
    const summary = log.orders.length ? [bought.length ? 'Bought ' + bought.join(', ') : '', sold.length ? 'sold ' + sold.join(', ') : '', ...optLines].filter(Boolean).join('; ').replace(/^./, c => c.toUpperCase()) : 'No changes';
    state._health = { t: Date.now(), missing, noPrice: bot.syms.filter(x => !priceOf(x)), news: m.newsFilter && S.f_buzz !== false ? { checked: Object.keys(buzz).length, wanted: Math.min(24, new Set([...wantBuy.map(([x]) => x), ...Object.keys(held), ...tradable]).size) } : null, problems: log.skips.length, saved: dataHealth.saved.syms.filter(x => bot.syms.includes(x)) };
    // a night run doesn't count as the day's decision, so the day crew still meets at its usual time
    if (night) await Q('update bots set state=$1, last_run=now(), peak=$2, day_start=$3, day_date=$4, summary=$5 where id=$6', [JSON.stringify(state), peak, dayStart, today, summary, id]);
    else await Q('update bots set state=$1, last_run=now(), last_day=$2, peak=$3, day_start=$4, day_date=$5, summary=$6 where id=$7', [JSON.stringify(state), today, peak, dayStart, today, summary, id]);
    const rows = Object.values(decisions).map(d => ({ sym: d.sym, action: d.action, want: round(d.want, 4), target: round(d.target, 4), have: round(d.have, 2), note: d.note, why: d.why, buzz: d.buzz, flags: d.flags, review: d.review || null, council: d.council || null }));
    if (dec.report) { await botLog(id, 'report', dec.report.title, dec.report.body, dec.report.data); await notify('bot', `🧭 ${bot.name}: ${dec.report.title.toLowerCase()}`, dec.report.body.split('\n').filter(l => !/: none/.test(l)).slice(0, 5).join('\n') || 'Holding cash: no strategy is working well enough right now.', { wid: bot.wid, url: '/#/bot/' + id }); }
    await botLog(id, 'decision', `${night ? 'Night shift' : manual ? 'Manual run' : 'Daily decision'}: ${summary}`, [...log.risk, ...log.skips].join('\n'), { rows, equity, capital, dd, dayRet, regime: ctx.regime || null, missing });
    if (m.notify === 'all' || (m.notify === 'trades' && log.orders.length)) await notify(brk ? 'real' : 'bot', `${brk ? (liveMoney ? '🔴 LIVE · ' : '🔵 Alpaca · ') : '🤖 '}${bot.name}: ${summary}`, log.orders.map(o => `${o.text}. ${o.label || ''}`.trim()).join('\n').slice(0, 600) || [...log.risk, ...log.skips].join(' ') || 'Nothing to do today.', { wid: bot.wid, url: '/#/bot/' + id });
  }
  phase('done');
  return { dry, equity, capital, dd, dayRet, regime: ctx.regime || null, rows: Object.values(decisions), orders: log.orders, risk: log.risk, skips: log.skips, missing, session: sess };
}
const pct0n = v => (v * 100).toFixed(0) + '%';
async function flattenBot(bot, why) {
  const out = [];
  for (const p of await positions(bot.wid)) {
    if (!bot.syms.includes(p.sym)) continue;
    try {
      const w = walletMeta.get(bot.wid);
      if (w?.broker) { for (const o of (BROKER?.orders || []).filter(o => o.sym === p.sym && o.side === 'sell')) await brokerCancel(o.id).catch(() => {}); const q = await brokerTrade(w, { sym: p.sym, side: 'sell', mode: 'pct', amount: 100, live: true }); out.push(`${p.sym} (sent to Alpaca: ${q.status})`); continue; }
      if (!isCrypto(p.sym) && sessionAt(profOf(walletMeta.get(bot.wid))) !== 'regular') { await addLimit(bot.wid, p.sym, { side: 'sell', kind: 'moo', amount: 100, units: 'pct', why: 'bot: ' + why }); out.push(`${p.sym} (queued for the open)`); }
      else { const f = await execTrade(bot.wid, p.sym, 'sell', { pct: 100, why: 'bot: ' + why, auto: true, force: true }); out.push(`${+f.qty.toFixed(6)} ${p.sym}`); }
    } catch (e) { out.push(`${p.sym} failed: ${e.message}`); }
  }
  await Q("delete from ord where wid=$1 and sym = any($2::text[])", [bot.wid, bot.syms]).catch(() => {}); await loadOrders();
  await botLog(bot.id, 'trade', 'Sold everything', `${why}: ${out.join(', ') || 'nothing to sell'}`);
  return out;
}
// Every minute: run bots whose time has come, catch up on a missed day, and say so when one is missed.
const weekOf = d => { const t = new Date(d + 'T12:00:00Z'), wd = (t.getUTCDay() + 6) % 7; t.setUTCDate(t.getUTCDate() - wd); return t.toISOString().slice(0, 10); }; // the Monday of that week
const ymd = ([y, m, d]) => `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
function prevTradingDate(y, m, d) { for (let i = 1; i < 15; i++) { const t = addDays(y, m, d, -i); if (isTradingDay(...t)) return ymd(t); } return ymd(addDays(y, m, d, -1)); }
async function botScheduler() {
  if (FINNHUB_KEY && S.f_buzz !== false) earningsCal().catch(() => {}); // keep the earnings calendar warm
  const p = etParts(), hm = String(p.h).padStart(2, '0') + ':' + String(p.mi).padStart(2, '0'), today = etDate();
  const trading = isTradingDay(p.y, p.m, p.d), early = trading && earlyClose(p.y, p.m, p.d), open = market().state === 'open';
  const lastTrading = prevTradingDate(p.y, p.m, p.d), yesterday = ymd(addDays(p.y, p.m, p.d, -1));
  for (const b of await rows("select * from bots where status='running'")) {
    const m = { ...MANDATE_DEF, ...jsonOr(b.mandate, {}) };
    const syms = jsonOr(b.syms, []), anyCrypto = syms.some(isCrypto), onlyCrypto = syms.length && syms.every(isCrypto);
    const runAt = early && m.runAt > '12:45' ? '12:45' : m.runAt; // half days close at 1pm
    if (b.last_day === today || running.has(b.id)) continue;
    const ranOnce = !!b.last_day, missed = ranOnce && b.last_day < (onlyCrypto ? yesterday : lastTrading);
    const due = hm >= runAt || (missed && hm >= '09:45'); // a missed day is made up the next morning
    if (due && (anyCrypto || open)) {
      runBot(b.id).catch(async e => { warn('bot', b.id, e.message); await botLog(b.id, 'error', 'The daily decision failed', e.message); await Q('update bots set last_day=$1 where id=$2', [today, b.id]).catch(() => {}); await notify('bot', `⚠️ ${b.name} couldn't decide today`, e.message, { url: '/#/bot/' + b.id }); });
      if (missed) botLog(b.id, 'info', 'Catching up', `It missed its decision on ${b.last_day < lastTrading ? 'the last trading day' : 'yesterday'} (the server was probably restarting or offline), so it decided this morning instead.`).catch(() => {});
      continue;
    }
    // After the close, a stock bot that never decided today gets a heads-up (once).
    const late = onlyCrypto ? hm >= '23:30' : trading && hm >= (early ? '13:10' : '16:10');
    if (late && (ranOnce || tsMs(b.created) < etToUtc(p.y, p.m, p.d, 9, 30)) && (await kvGet('botmiss:' + b.id)) !== today) {
      await kvSet('botmiss:' + b.id, today);
      const why = (await row("select body from bot_log where bot_id=$1 and kind='error' order by id desc limit 1", [b.id]))?.body;
      await botLog(b.id, 'error', 'Missed today\'s decision', why ? `Last problem: ${why}` : 'The server was probably offline at decision time. It will catch up tomorrow morning.');
      await notify('bot', `⚠️ ${b.name} missed today's decision`, why ? `Last problem: ${why}` : 'It will catch up tomorrow morning at 9:45.', { url: '/#/bot/' + b.id });
    }
  }
  await nightScheduler(p, hm, today).catch(e => warn('night shift', e.message));
  await weeklySummary(p, hm, today).catch(e => warn('bot weekly', e.message));
}
// ---------- the night shift ----------
// While the Council sleeps (9 PM to 7 AM ET), crypto bots get extra check-ins at set times. Only crypto is
// touched, only for strategies that decide every day (monthly ones and Autopilot wait for the day crew),
// and two night members vote instead of ten: the Night Watch (rules) and the Night Owl (one AI call).
const NIGHT_SLOTS = { 1: ['01:00'], 2: ['23:00', '03:00'], 3: ['22:00', '02:00', '06:00'] };
const nightOn = () => S.nightOn !== false;
const nightSlots = () => NIGHT_SLOTS[S.nightRuns] || NIGHT_SLOTS[2];
const inNight = hm => ENV.NIGHT_OVERRIDE ? ENV.NIGHT_OVERRIDE === '1' : hm >= '21:00' || hm < '07:00'; // NIGHT_OVERRIDE is for tests only
const nightEligible = b => { const syms = jsonOr(b.syms, []), st = STRATEGIES[b.strategy]; return syms.some(isCrypto) && st && !st.auto && st.rebalance === 'none'; };
function nightJson() {
  const p = etParts(), hm = String(p.h).padStart(2, '0') + ':' + String(p.mi).padStart(2, '0');
  const slots = nightSlots(), next = slots.slice().sort((a, b) => ((a < '12:00' ? '24' + a : a) > (b < '12:00' ? '24' + b : b) ? 1 : -1)).find(s => (s < '12:00' ? '24' + s : s) > (hm < '12:00' ? '24' + hm : hm)) || slots[0];
  return { on: nightOn(), active: nightOn() && inNight(hm), slots, next };
}
async function nightScheduler(p, hm, today) {
  if (!nightOn() || standbyPaused()) return;
  for (const slot of nightSlots()) {
    // run within the hour after each slot (no catching up on missed nights, that's the day crew's job)
    const [sh, sm] = slot.split(':').map(Number), mins = p.h * 60 + p.mi - (sh * 60 + sm); if (mins < 0 || mins >= 60) continue;
    const key = today + ' ' + slot;
    for (const b of await rows("select * from bots where status='running'")) {
      if (!nightEligible(b) || running.has(b.id)) continue;
      if ((await kvGet('night:' + b.id)) === key) continue;
      await kvSet('night:' + b.id, key);
      runBot(b.id, { night: true }).catch(e => { warn('night', b.id, e.message); botLog(b.id, 'error', 'The night shift check failed', e.message).catch(() => {}); });
    }
  }
}
// Friday after the close: one notification with how every bot did this week.
async function weeklySummary(p, hm, today) {
  if (S.botWeekly === false || dow(p.y, p.m, p.d) !== 5 || hm < '16:20' || (await kvGet('job_botweekly')) === today) return;
  await kvSet('job_botweekly', today);
  const lines = [];
  for (const b of await rows("select * from bots where status<>'killed' order by id")) {
    const w = await row('select * from wallets where id=$1', [b.wid]); if (!w) continue;
    const pos = await positions(b.wid), now = +w.cash + marketValue(pos);
    const then = await row("select eq from snap where wid=$1 and ts <= now() - interval '7 days' order by ts desc limit 1", [b.wid]);
    const base = then ? +then.eq : +w.start_bal, ch = now - base;
    const trades = +(await row("select count(*) n from bot_log where bot_id=$1 and kind='trade' and ts > now() - interval '7 days'", [b.id])).n;
    const decs = await rows("select data from bot_log where bot_id=$1 and kind='decision' and ts > now() - interval '7 days'", [b.id]);
    const skipped = new Set(); for (const d of decs) for (const r of jsonOr(d.data, {})?.rows || []) if ((r.why || []).some(x => /^Skipped buying/.test(x))) skipped.add(r.sym);
    const top = pos.filter(x => jsonOr(b.syms, []).includes(x.sym)).sort((a, c) => c.qty * (priceOf(c.sym) || c.avg) - a.qty * (priceOf(a.sym) || a.avg)).slice(0, 3).map(x => x.sym);
    lines.push(`${b.name}: ${ch >= 0 ? '+' : '-'}${money(Math.abs(ch))} (${pct(base ? ch / base * 100 : 0)}) · ${trades} trade${trades === 1 ? '' : 's'}${top.length ? ' · holds ' + top.join(', ') : ' · in cash'}${skipped.size ? ` · skipped ${[...skipped].slice(0, 3).join(', ')} on news` : ''}`);
  }
  if (lines.length) await notify('bot', '🤖 Your bots this week', lines.join('\n'), { url: '/#/bots' });
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
// Autopilot: runs every strategy as a tryout with pretend money, and gives its own wallet's money
// to the ones that are working. Tryouts are simulated day by day on real daily prices from a fixed
// start, so each one's record is exactly what it would have done; from the Autopilot's first day on,
// that record is live and unseen. Money moves between strategies once a month, and right away when
// one goes cold.
// =====================================================================
const AP_STOCKS = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'JPM', 'LLY', 'COST', 'NFLX'];
const AP_ETFS = ['SPY', 'QQQ', 'IWM', 'EFA', 'EEM', 'TLT', 'IEF', 'GLD', 'DBC', 'VNQ'];
const AP_CRYPTO = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LINK'];
const AP_SECTORS = ['XLK', 'XLF', 'XLE', 'XLV', 'XLY', 'XLU'];
const AP_DEFENSIVE = ['KO', 'PG', 'JNJ', 'PEP', 'WMT', 'MCD', 'COST', 'MSFT'];
const AP_DUAL = ['SPY', 'QQQ', 'EFA', 'EEM', 'IEF', 'TLT'];
// Each idea runs in a couple of versions (fam = the idea), so the Autopilot doesn't bet on one lucky setting.
const AP_TRYOUTS = [
  { k: 'trend-stocks', fam: 'trend-stocks', strategy: 'trend', syms: AP_STOCKS, label: 'Trend on big US stocks', market: 'stocks' },
  { k: 'trend-stocks-fast', fam: 'trend-stocks', strategy: 'trend', syms: AP_STOCKS, label: 'Trend on big US stocks, faster', market: 'stocks', params: { fast: 20, slow: 100, atrMult: 2.5 } },
  { k: 'breakout-stocks', fam: 'breakout-stocks', strategy: 'breakout', syms: AP_STOCKS, label: 'Breakouts in big US stocks', market: 'stocks' },
  { k: 'breakout-stocks-fast', fam: 'breakout-stocks', strategy: 'breakout', syms: AP_STOCKS, label: 'Breakouts in big US stocks, faster', market: 'stocks', params: { entryLen: 20, exitLen: 10 } },
  { k: 'drift-stocks', fam: 'drift', strategy: 'drift', syms: AP_STOCKS, label: 'Riding news jumps in big US stocks', market: 'stocks' },
  { k: 'momentum-mix', fam: 'momentum-mix', strategy: 'momentum', syms: AP_ETFS, label: 'Momentum across stocks, bonds and gold', market: 'stocks' },
  { k: 'momentum-mix-12m', fam: 'momentum-mix', strategy: 'momentum', syms: AP_ETFS, label: 'Momentum across stocks, bonds and gold, 12 months', market: 'stocks', params: { lookback: 252 } },
  { k: 'sectors', fam: 'sectors', strategy: 'momentum', syms: AP_SECTORS, label: 'Rotating between sectors', market: 'stocks', params: { lookback: 126, top: 2 } },
  { k: 'dual', fam: 'dual', strategy: 'dual', syms: AP_DUAL, label: 'Dual momentum: stocks or bonds', market: 'stocks' },
  { k: 'dual-6m', fam: 'dual', strategy: 'dual', syms: AP_DUAL, label: 'Dual momentum, 6 months', market: 'stocks', params: { lookback: 126 } },
  { k: 'lowvol', fam: 'lowvol', strategy: 'lowvol', syms: AP_DEFENSIVE, label: 'Calm, steady stocks', market: 'stocks' },
  { k: 'tom-index', fam: 'tom', strategy: 'tom', syms: ['SPY', 'QQQ', 'IWM'], label: 'Turn of the month in index ETFs', market: 'stocks' },
  { k: 'snap-index', fam: 'snapback', strategy: 'snapback', syms: ['SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF'], label: 'Snap-backs in index and sector ETFs', market: 'stocks' },
  { k: 'dips-index', fam: 'dips', strategy: 'rsi2', syms: ['SPY', 'QQQ', 'IWM', 'DIA'], label: 'Buying dips in index ETFs', market: 'stocks' },
  { k: 'hold-mix', fam: 'hold', strategy: 'hold', syms: ['SPY', 'QQQ', 'TLT', 'GLD'], label: 'Steady mix of stocks, bonds and gold', market: 'stocks' },
  { k: 'trend-crypto', fam: 'trend-crypto', strategy: 'trend', syms: AP_CRYPTO, label: 'Trend on top crypto', market: 'crypto' },
  { k: 'trend-crypto-fast', fam: 'trend-crypto', strategy: 'trend', syms: AP_CRYPTO, label: 'Trend on top crypto, faster', market: 'crypto', params: { fast: 20, slow: 100, atrMult: 2.5 } },
  { k: 'breakout-crypto', fam: 'breakout-crypto', strategy: 'breakout', syms: AP_CRYPTO, label: 'Breakouts in crypto', market: 'crypto' },
  { k: 'breakout-crypto-fast', fam: 'breakout-crypto', strategy: 'breakout', syms: AP_CRYPTO, label: 'Breakouts in crypto, faster', market: 'crypto', params: { entryLen: 20, exitLen: 10 } },
  { k: 'snap-crypto', fam: 'snapback-crypto', strategy: 'snapback', syms: AP_CRYPTO, label: 'Snap-backs in crypto', market: 'crypto', params: { bands: 2.25, stopAtr: 2.5 } },
  { k: 'momentum-all', fam: 'momentum-all', strategy: 'momentum', syms: ['SPY', 'QQQ', 'GLD', 'TLT', 'BTC', 'ETH', 'SOL'], label: 'Momentum across stocks and crypto', market: 'both', params: { filterLen: 0 } },
  // added in Autopilot 2
  { k: 'momentum-tbills', fam: 'momentum-mix', strategy: 'momentum', syms: [...AP_ETFS, 'SGOV'], label: 'Momentum, 2 picks, T-bills when weak', market: 'stocks', params: { top: 2, safe: 'SGOV' } },
  { k: 'sectors-3m', fam: 'sectors', strategy: 'momentum', syms: AP_SECTORS, label: 'Rotating between sectors, 3 months', market: 'stocks', params: { lookback: 63, top: 2 } },
  { k: 'trend-etfs', fam: 'trend-etfs', strategy: 'trend', syms: AP_ETFS, label: 'Trend on index, bond and gold ETFs', market: 'stocks' },
  { k: 'breakout-etfs', fam: 'breakout-etfs', strategy: 'breakout', syms: ['SPY', 'QQQ', 'IWM', ...AP_SECTORS], label: 'Breakouts in index and sector ETFs', market: 'stocks' },
  { k: 'defense-trend', fam: 'defense', strategy: 'trend', syms: ['GLD', 'TLT', 'IEF'], label: 'Trend in gold and bonds', market: 'stocks', params: { fast: 20, slow: 100 } },
  { k: 'dips-stocks', fam: 'dips-stocks', strategy: 'rsi2', syms: AP_STOCKS, label: 'Buying dips in big US stocks', market: 'stocks' },
  { k: 'earnings-stocks', fam: 'earnings', strategy: 'earnings', syms: AP_STOCKS, label: 'Riding strong earnings reactions', market: 'stocks' },
  { k: 'lowvol-etfs', fam: 'lowvol', strategy: 'lowvol', syms: ['XLU', 'XLV', 'XLF', 'GLD', 'TLT', 'IEF', 'SPY'], label: 'Calm ETFs', market: 'stocks' },
  { k: 'momentum-crypto', fam: 'momentum-crypto', strategy: 'momentum', syms: AP_CRYPTO, label: 'Momentum among top crypto', market: 'crypto', params: { lookback: 63, top: 2, filterLen: 0 } },
  { k: 'dips-crypto', fam: 'dips-crypto', strategy: 'rsi2', syms: ['BTC', 'ETH'], label: 'Buying dips in Bitcoin and Ether', market: 'crypto', params: { trendLen: 100 } },
  { k: 'hold-crypto', fam: 'hold-crypto', strategy: 'hold', syms: ['BTC', 'ETH'], label: 'Holding Bitcoin and Ether', market: 'crypto' },
];
const apTryouts = markets => AP_TRYOUTS.filter(t => markets === 'both' || t.market === markets);
const apSyms = markets => [...new Set([...apTryouts(markets).flatMap(t => t.syms), ...(markets === 'crypto' ? [] : ['SGOV'])])]; // SGOV: where idle money waits
const AP_WINDOW = 126, AP_MIN_LIVE = 20, AP_MIN_SLEEVE = 0.08;
// How good a track record is: return per unit of risk over the last ~6 months, docked for deep drops.
function apWindowStats(c, P) {
  const rets = []; for (let i = 1; i < c.length; i++) rets.push(c[i][1] / c[i - 1][1] - 1);
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length, sd = Math.sqrt(rets.reduce((a, r) => a + (r - mean) ** 2, 0) / Math.max(1, rets.length - 1));
  let peak = -Infinity, dd = 0; for (const [, v] of c) { peak = Math.max(peak, v); dd = Math.min(dd, v / peak - 1); }
  const sharpe = sd > 0 ? mean / sd * Math.sqrt(P) : 0;
  return { sharpe, dd, score: sharpe * (1 - Math.min(0.6, Math.abs(dd))) };
}
// How good a track record is: return per unit of risk, docked for deep drops. The last 6 months count most,
// the last year steadies it, and a tryout whose own results are sliding (below their 50-day average) is cooled off.
function apScore(curve, P) {
  const c = curve.slice(-AP_WINDOW - 1); if (c.length < 40) return null;
  const s6 = apWindowStats(c, P), long = curve.slice(-253), s12 = long.length > 180 ? apWindowStats(long, P) : null;
  const q = c.slice(-64), ret3m = q.length > 1 ? q[q.length - 1][1] / q[0][1] - 1 : 0, ret6m = c[c.length - 1][1] / c[0][1] - 1;
  const tail = curve.slice(-50), avg50 = tail.reduce((t, x) => t + x[1], 0) / tail.length, cooling = curve[curve.length - 1][1] < avg50 * 0.995;
  let score = s12 ? 0.65 * s6.score + 0.35 * s12.score : s6.score;
  if (cooling) score *= 0.5;
  return { sharpe: s6.sharpe, sharpe12: s12 ? s12.sharpe : null, dd: s6.dd, ret3m, ret6m, score, cooling, eligible: s6.sharpe > 0.3 && ret3m > -0.03 && (!s12 || s12.sharpe > 0) };
}
// Daily returns of each tryout over the same dates, to see which ones move together.
function apCorr(a, b) {
  const mb = new Map(b.map(x => [x[0], x[1]])), ra = [], rb = [];
  for (let i = 1; i < a.length; i++) { const d0 = a[i - 1][0], d1 = a[i][0]; if (mb.has(d0) && mb.has(d1)) { ra.push(a[i][1] / a[i - 1][1] - 1); rb.push(mb.get(d1) / mb.get(d0) - 1); } }
  if (ra.length < 30) return 0;
  const m = x => x.reduce((t, v) => t + v, 0) / x.length, ma = m(ra), mbb = m(rb);
  let num = 0, da = 0, db = 0; for (let i = 0; i < ra.length; i++) { num += (ra[i] - ma) * (rb[i] - mbb); da += (ra[i] - ma) ** 2; db += (rb[i] - mbb) ** 2; }
  return da > 0 && db > 0 ? num / Math.sqrt(da * db) : 0;
}
// Split the money: in proportion to score, capped per strategy, small slices dropped, the rest left in cash.
function apAllocate(board, maxSleeve, pick) {
  const w = apAllocateRaw(board, maxSleeve, pick), fam = {};
  for (const [k, v] of Object.entries(w)) { const f = board.find(t => t.k === k)?.fam || k; fam[f] = (fam[f] || 0) + v; }
  for (const [k, v] of Object.entries(w)) { const f = board.find(t => t.k === k)?.fam || k; if (fam[f] > maxSleeve) w[k] = round(v * maxSleeve / fam[f], 4); } // one idea, however many versions, gets at most the cap
  return w;
}
const AP_PICK = 6;
function apAllocateRaw(board, maxSleeve, pick = AP_PICK) {
  // The best version of each idea first, so the money spreads across different ideas; second versions only fill empty slots.
  const ok = board.filter(t => t.eligible && t.score > 0).map(t => ({ ...t })).sort((a, b) => b.score - a.score), seen = new Set(), firsts = [], seconds = [];
  for (const t of ok) (seen.has(t.fam) ? seconds : (seen.add(t.fam), firsts)).push(t);
  // Skip a tryout that moves almost in lockstep with one already picked: two copies of the same bet aren't diversification.
  const live = [];
  for (const t of [...firsts, ...seconds]) {
    if (live.length >= pick) break;
    const c = Math.max(0, ...live.map(x => apCorr(x.curve || [], t.curve || [])));
    t.corr = round(c, 2);
    if (c > 0.85) continue;
    live.push(c > 0.6 ? Object.assign(t, { score: t.score * (1 - (c - 0.6)) }) : t);
  }
  let pool = live;
  for (let pass = 0; pass < 4; pass++) {
    const tot = pool.reduce((a, t) => a + t.score, 0); if (!tot) return {};
    const w = {}; let left = 1, open = pool.slice();
    // water-fill so capped strategies hand their extra to the others
    for (let it = 0; it < 6 && open.length; it++) {
      const sum = open.reduce((a, t) => a + t.score, 0), next = [];
      let used = 0;
      for (const t of open) { const share = left * t.score / sum, cap = t.cap; if (share >= cap) { w[t.k] = cap; used += cap; } else next.push(t); }
      if (next.length === open.length) { for (const t of open) w[t.k] = left * t.score / sum; open = []; break; }
      left -= used; open = next;
    }
    const small = pool.filter(t => (w[t.k] || 0) < AP_MIN_SLEEVE);
    if (!small.length) return Object.fromEntries(Object.entries(w).map(([k, v]) => [k, round(v, 4)]));
    pool = pool.filter(t => !small.includes(t));
  }
  return {};
}
// ---------- Autopilot 2 helpers ----------
// The market's mood on each day: above both averages = up, above the 200-day only = choppy, below it = down
function apRegimes(b) {
  const out = new Map(); if (!b) return out;
  for (let i = 200; i < b.c.length; i++) { const c = b.c[i], m200 = IND.sma(b.c, 200, i), m50 = IND.sma(b.c, 50, i); out.set(b.d[i], c > m200 ? (c > m50 ? 'up' : 'choppy') : 'down'); }
  return out;
}
// How a tryout has done on days like today (same market mood): its Sharpe on just those days
function apRegimeFit(curve, regimes, now, P) {
  const r = []; for (let i = 1; i < curve.length; i++) if (regimes.get(curve[i][0]) === now) r.push(curve[i][1] / curve[i - 1][1] - 1);
  if (r.length < 30) return { n: r.length, sharpe: null };
  const m = r.reduce((a, b) => a + b, 0) / r.length, sd = Math.sqrt(r.reduce((a, x) => a + (x - m) ** 2, 0) / (r.length - 1));
  return { n: r.length, sharpe: sd > 0 ? m / sd * Math.sqrt(P) : 0 };
}
const apVol = (curve, P) => { const c = curve.slice(-127), r = []; for (let i = 1; i < c.length; i++) r.push(c[i][1] / c[i - 1][1] - 1); if (r.length < 20) return null; const m = r.reduce((a, b) => a + b, 0) / r.length; return Math.sqrt(r.reduce((a, x) => a + (x - m) ** 2, 0) / (r.length - 1)) * Math.sqrt(P); };
const MOOD_WORD = { up: 'rising', choppy: 'choppy', down: 'falling' };
async function autopilotDecide(bot, state, series, today, { dry = false, manual = false, dd = 0, capital = 0 } = {}) {
  const p = { ...cleanParams('autopilot', {}), ...bot.params }, maxSleeve = (p.maxSleeve || 35) / 100;
  if (!state._since) state._since = today;
  if (!state._histStart) state._histStart = addIso(state._since, -460);
  // evolving tryouts: variations of the best ones, born on a date and judged only on what happened after it
  const mutants = (p.evolve !== 'off' ? state._mutants || [] : []).filter(m => AP_TRYOUTS.some(t => t.k === m.base));
  const tryouts = [...apTryouts(p.markets || 'both'), ...mutants.map(m => { const t = AP_TRYOUTS.find(x => x.k === m.base); return { ...t, k: m.k, label: `${t.label} · variant ${m.n}`, params: m.params, born: m.born, mutant: true }; }).filter(t => apTryouts(p.markets || 'both').some(x => x.k === t.k.replace(/^m\d+-/, '') || x.fam === t.fam))];
  const regS = apRegimes(series.SPY), regC = apRegimes(series.BTC), lastOf = m => [...m.values()].pop() || null, moodS = lastOf(regS), moodC = lastOf(regC);
  const board = [], curves = {}, shadows = {};
  for (const t of tryouts) {
    const st = STRATEGIES[t.strategy], syms = t.syms.filter(s => series[s]);
    if (!syms.length || !st) continue;
    const params = cleanParams(t.strategy, t.params || {}), bench = benchFor(syms);
    const r = runBacktest({ key: t.strategy, params, syms, data: series, start: state._histStart, end: null, capital: 10000, maxPosPct: 100, bench });
    if (!r.curve.length) continue;
    const P = ppy(syms), judged = t.mutant ? r.curve.filter(x => x[0] >= t.born) : r.curve;
    let sc = apScore(judged, P) || { sharpe: 0, dd: 0, ret3m: 0, ret6m: 0, score: 0, eligible: false };
    const crypto = t.market === 'crypto', mood = crypto ? moodC : moodS, fit = p.regimeFit !== 'off' && mood ? apRegimeFit(r.curve, crypto ? regC : regS, mood, P) : { sharpe: null, n: 0 };
    let fitK = 1;
    if (fit.sharpe != null) { fitK = clamp(1 + 0.45 * Math.tanh(fit.sharpe - 0.4), 0.55, 1.45); if (fit.sharpe < -0.6 && fit.n >= 40) sc = { ...sc, eligible: false, badMood: true }; }
    const vol = apVol(r.curve, P), liveC = r.curve.filter(x => x[0] >= state._since), liveDays = liveC.length;
    const eq = r.curve[r.curve.length - 1][1], holds = {}; for (const o of r.open) holds[o.sym] = { w: o.value / eq, stop: o.stop || null };
    shadows[t.k] = holds; curves[t.k] = r.curve.slice(-90);
    const mutantDays = t.mutant ? judged.length : null;
    board.push({ k: t.k, fam: t.fam || t.k, label: t.label, strategy: t.strategy, name: st.name, market: t.market, syms, mutant: !!t.mutant, born: t.born || null, ...Object.fromEntries(Object.entries(sc).map(([a, v]) => [a, typeof v === 'number' ? round(v, 4) : v])),
      score: round((sc.score || 0) * fitK, 4), baseScore: round(sc.score || 0, 4), fit: fit.sharpe == null ? null : round(fit.sharpe, 2), fitK: round(fitK, 2), mood, vol: vol == null ? null : round(vol, 3),
      eligible: !!sc.eligible && (!t.mutant || mutantDays >= 40),
      live: liveC.length > 1 ? round(liveC[liveC.length - 1][1] / liveC[0][1] - 1, 4) : 0, liveDays, cap: t.mutant ? Math.min(maxSleeve, 0.15) : liveDays < AP_MIN_LIVE ? Math.min(maxSleeve, 0.25) : maxSleeve,
      cooling: !!sc.cooling, sharpe12: sc.sharpe12 == null ? null : round(sc.sharpe12, 3), holding: Object.keys(holds), cash: round(1 - Object.values(holds).reduce((a, h) => a + h.w, 0), 3), spark: downsamplePts(r.curve.slice(-AP_WINDOW), 40).map(x => round(x[1], 2)) });
  }
  // how the money is split: by score, by score per unit of risk (steadier strategies get more), or evenly among the good ones
  const mode = p.allocMode || 'blend', forAlloc = board.map(t => ({ ...t, curve: curves[t.k], score: mode === 'equal' ? (t.eligible ? 1 : 0) : mode === 'blend' ? t.score / Math.max(0.06, t.vol || 0.2) : t.score }));
  // small accounts: fewer slices, each worth at least the smallest sleeve
  const pick = Math.max(1, Math.min(AP_PICK, capital > 0 ? Math.floor(capital / Math.max(5, p.minSleeveUsd || 50)) : AP_PICK));
  const month = today.slice(0, 7), week = weekOf(today), prev = state._alloc || {};
  let alloc = prev, changed = false, why = '';
  const fresh = () => apAllocate(forAlloc, maxSleeve, pick);
  if (!state._allocMonth || state._allocMonth !== month || manual && !Object.keys(prev).length) { alloc = fresh(); changed = true; why = state._allocMonth ? 'monthly' : 'first'; }
  else if (p.recheck === 'weekly' && state._allocWeek !== week) {
    // a weekly look: only switch if the new plan is really different (more than 15% of the money would move)
    const cand = fresh(), keys = new Set([...Object.keys(cand), ...Object.keys(prev)]), turn = [...keys].reduce((t, k) => t + Math.abs((cand[k] || 0) - (prev[k] || 0)), 0) / 2;
    state._allocWeek = week; if (turn > 0.15) { alloc = cand; changed = true; why = 'weekly'; }
  }
  if (!changed) {
    const cold = Object.keys(prev).filter(k => { const t = board.find(x => x.k === k); return !t || !t.eligible; });
    if (cold.length) { alloc = { ...prev }; for (const k of cold) delete alloc[k]; changed = true; why = 'cold:' + cold.join(','); }
  }
  if (changed) state._allocWeek = week;
  for (const t of board) t.alloc = alloc[t.k] || 0;
  const out = { weights: {}, stops: {}, notes: {} }, from = {}, stopSum = {};
  for (const [k, a] of Object.entries(alloc)) for (const [s, h] of Object.entries(shadows[k] || {})) {
    out.weights[s] = (out.weights[s] || 0) + a * h.w;
    (from[s] = from[s] || []).push(`${board.find(x => x.k === k)?.label || k} (${Math.round(h.w * 100)}% of its money)`);
    if (h.stop) { const z = stopSum[s] || (stopSum[s] = { w: 0, v: 0 }); z.w += a * h.w; z.v += a * h.w * h.stop; }
  }
  for (const [s, z] of Object.entries(stopSum)) if (z.w > 0) out.stops[s] = { trail: false, price: z.v / z.w };
  for (const s of Object.keys(series)) if (!out.notes[s]) out.notes[s] = from[s] ? `Held by ${from[s].join(' and ')}.` : 'None of the strategies it is using hold this right now.';
  const risk = { scale: 1, vol: null, stocksOff: false, cryptoOff: false, mood: moodS, moodCrypto: moodC };
  if (p.safety !== 'off') {
    risk.stocksOff = moodS === 'down'; risk.cryptoOff = moodC === 'down';
    const cut = (p.safetyCut ?? 50) / 100;
    for (const s of Object.keys(out.weights)) {
      const crypto = isCrypto(s), defensive = SAFE.has(s) || s === 'GLD' || s === 'SGOV';
      if ((crypto && risk.cryptoOff) || (!crypto && !defensive && risk.stocksOff)) { out.weights[s] *= 1 - cut; out.notes[s] += ` Cut ${Math.round(cut * 100)}% by the safety switch (${crypto ? 'BTC' : 'SPY'} is below its 200-day average).`; }
    }
  }
  if (p.volTarget > 0 && Object.keys(alloc).length) {
    const dates = [...new Set(Object.keys(alloc).flatMap(k => (curves[k] || []).map(x => x[0])))].sort().slice(-61);
    if (dates.length > 21) {
      const val = k => { const m = new Map(curves[k].map(x => [x[0], x[1]])); let last = null; const first = curves[k].find(x => x[0] >= dates[0])?.[1] || curves[k][0][1]; return dates.map(d => { if (m.has(d)) last = m.get(d); return (last ?? first) / first; }); };
      const vs = Object.fromEntries(Object.keys(alloc).map(k => [k, val(k)])), cashW = 1 - Object.values(alloc).reduce((a, b) => a + b, 0);
      const port = dates.map((_, i) => cashW + Object.entries(alloc).reduce((t, [k, a]) => t + a * vs[k][i], 0));
      const rets = port.slice(1).map((v, i) => v / port[i] - 1);
      const span = Math.max(1, daysBetween(dates[0], dates[dates.length - 1])), P = dates.length / (span / 365);
      const sdOf = a => { const m = a.reduce((x, y) => x + y, 0) / a.length; return Math.sqrt(a.reduce((x, r) => x + (r - m) ** 2, 0) / Math.max(1, a.length - 1)); };
      const vol = Math.max(sdOf(rets.slice(-20)), sdOf(rets)) * Math.sqrt(P);
      risk.vol = round(vol, 4);
      if (vol > 0) risk.scale = round(clamp(p.volTarget / 100 / vol, 0.2, 1), 3);
      if (risk.scale < 0.999) for (const s of Object.keys(out.weights)) out.weights[s] *= risk.scale;
    }
  }
  const brakeAt = -(p.brake ?? 10) / 100;
  if (brakeAt < 0) {
    if (dd <= brakeAt) state._brake = true; else if (dd > brakeAt / 2) state._brake = false;
    risk.brake = !!state._brake;
    if (state._brake) for (const s of Object.keys(out.weights)) out.weights[s] *= 0.5;
  }
  // small accounts: positions too small to matter are skipped (their money goes to T-bills or cash below)
  if (capital > 0) { const tiny = Math.max(5, (p.minSleeveUsd || 50) * 0.2); for (const [s2, w2] of Object.entries(out.weights)) if (w2 * capital < tiny) { delete out.weights[s2]; out.notes[s2] = (out.notes[s2] || '') + ` Skipped: its share would be under $${Math.round(tiny)}, too small to be worth a trade.`; } }
  // money no strategy is using (and whatever the safety switch, swing target and brake held back) waits in T-bills
  const parkOn = p.park === 'SGOV' && series.SGOV && (p.markets || 'both') !== 'crypto';
  const used = Object.values(out.weights).reduce((a, b) => a + b, 0);
  if (parkOn && used < 0.97) { out.weights.SGOV = round((out.weights.SGOV || 0) + (1 - used), 4); out.notes.SGOV = `${Math.round((1 - used) * 100)}% of the money isn't needed by any strategy right now, so it waits in SGOV (T-bills) and earns interest instead of sitting in cash.`; risk.parked = round(1 - used, 3); }
  for (const s of Object.keys(out.weights)) out.weights[s] = round(out.weights[s], 4);
  const cash = 1 - Object.values(out.weights).reduce((a, b) => a + b, 0);
  const riskBits = [moodS ? `the stock market is ${MOOD_WORD[moodS]}${moodC && (p.markets || 'both') !== 'stocks' ? `, crypto is ${MOOD_WORD[moodC]}` : ''}` : '', risk.vol != null ? `the mix has swung ${Math.round(risk.vol * 100)}% a year${risk.scale < 0.999 ? `, so it holds ${Math.round(risk.scale * 100)}% of the plan to stay near the ${p.volTarget}% target` : ', within the target'}` : '', risk.stocksOff ? 'safety switch on for stocks' : '', risk.cryptoOff ? 'safety switch on for crypto' : '', risk.brake ? `brake on: the wallet is ${Math.round(Math.abs(dd) * 100)}% below its high` : '', risk.parked ? `${Math.round(risk.parked * 100)}% parked in T-bills` : '', cash > 0.02 ? `${Math.round(cash * 100)}% in cash` : '', pick < AP_PICK ? `with ${'$' + Math.round(capital)} it uses at most ${pick} strateg${pick === 1 ? 'y' : 'ies'}` : ''].filter(Boolean);
  out.regime = (Object.keys(alloc).length ? `Using ${Object.keys(alloc).length} of ${board.length} strategies: ${board.filter(t => t.alloc > 0).sort((a, b) => b.alloc - a.alloc).map(t => `${t.label} ${Math.round(t.alloc * 100)}%`).join(', ')}.` : `Sitting out: none of the ${board.length} strategies has a good enough record right now.`) + (riskBits.length ? ' ' + riskBits.join('; ').replace(/^./, c => c.toUpperCase()) + '.' : '');
  out.risk = risk;
  if (!dry) {
    // evolve once a month: a variation of each of the 3 best ideas joins; variants that did worse than their parent after 4 months leave
    if (p.evolve !== 'off' && changed && why !== 'cold' && !String(why).startsWith('cold') && why !== 'weekly') {
      let ms = (state._mutants || []).filter(m => { const me = board.find(t => t.k === m.k), par = board.find(t => t.k === m.base); return me && !(daysBetween(m.born, today) > 120 && par && me.baseScore < par.baseScore); });
      const R = srng(1 + (today.replace(/-/g, '') % 2147483000)), tops = board.filter(t => !t.mutant && t.eligible).sort((a, b) => b.score - a.score).slice(0, 3);
      for (const t of tops) { if (ms.length >= 8) break; const base = AP_TRYOUTS.find(x => x.k === t.k), bp = cleanParams(t.strategy, base.params || {}), mp = mutate(t.strategy, bp, R); if (JSON.stringify(mp) === JSON.stringify(bp)) continue; const n = (state._mutN = (state._mutN || 0) + 1); ms.push({ k: `m${n}-${t.k}`, base: t.k, n, born: today, params: mp }); }
      state._mutants = ms.slice(-8);
    }
    state._board = board; state._boardT = Date.now(); state._risk = risk;
    if (changed) {
      state._alloc = alloc; state._allocMonth = month;
      const lines = board.slice().sort((a, b) => b.alloc - a.alloc || b.score - a.score).map(t => `${t.label}: ${t.alloc ? Math.round(t.alloc * 100) + '%' : 'none'}${prev[t.k] != null && Math.round((prev[t.k] || 0) * 100) !== Math.round(t.alloc * 100) ? ` (was ${Math.round((prev[t.k] || 0) * 100)}%)` : ''} · 6 months ${pct(t.ret6m * 100)}, Sharpe ${t.sharpe.toFixed(2)}${t.fit != null ? `, in ${MOOD_WORD[t.mood] || ''} markets ${t.fit.toFixed(2)}` : ''}${t.eligible ? '' : t.badMood ? ' · sits out: does badly in this kind of market' : ' · not working lately'}`);
      const title = why === 'first' ? 'First plan' : why === 'monthly' ? `Plan for ${new Date(today + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'long', year: 'numeric', timeZone: 'UTC' })}` : why === 'weekly' ? 'Weekly check: the plan changed' : 'Pulled money from a strategy that went cold';
      let note = null;
      if (p.aiReview !== 'off' && typeof analystReady === 'function' && analystReady() && why !== 'cold' && !String(why).startsWith('cold')) {
        const facts = `Market: stocks ${MOOD_WORD[moodS] || 'unknown'}, crypto ${MOOD_WORD[moodC] || 'unknown'}. Wallet ${'$' + Math.round(capital)}, ${Math.round(Math.abs(dd) * 100)}% below its high.\nNew plan:\n${lines.slice(0, 14).join('\n')}`;
        const a = await aiText('autopilot', 'You are the Analyst reviewing an automated Autopilot\'s new monthly plan for a paper-trading account. Using only the facts, in 3 short sentences: what the plan is betting on, the biggest risk in it, and one thing to watch this month. Plain and specific. No advice about real money.', facts, { max: 220 }).catch(() => null);
        if (a?.ok) note = a.text;
      }
      out.report = { title, body: (note ? `Analyst: ${note}\n\n` : '') + lines.join('\n'), data: { board, alloc, prev, why, note, mood: moodS, mutants: state._mutants || [] } };
    }
  }
  return out;
}
// =====================================================================
// Smarter bots: portfolio-wide guards and tools that sit on top of every strategy
//  - market mood switch: buys get smaller in a choppy or falling market
//  - correlation guard: no new position that moves almost exactly like one already held
//  - shared risk: caps on how much of all the bots' money can sit in one stock or one sector
//  - smarter exits: stops that tighten as profit grows, a partial take-profit, a time stop
//  - report card and benching, settings re-tuning, the bot tournament, and the earnings-drift strategy
// =====================================================================

// ---------- sectors (common names; anything else counts as "other" and isn't capped) ----------
const SECTOR_OF = (() => {
  const m = {}, add = (sec, list) => list.split(' ').forEach(s => { m[s] = sec; });
  add('Tech', 'AAPL MSFT NVDA AMD INTC AVGO ORCL CRM ADBE CSCO QCOM TXN IBM NOW INTU AMAT MU LRCX KLAC SNPS CDNS PANW CRWD SHOP PLTR SNOW NET DDOG ZS MDB TEAM UBER ABNB SMCI ARM DELL HPQ ANET MRVL XLK SMH SOXX VGT');
  add('Communication', 'GOOGL GOOG META NFLX DIS CMCSA T VZ TMUS SPOT RBLX SNAP PINS EA TTWO XLC');
  add('Consumer', 'AMZN TSLA HD NKE MCD SBUX LOW TGT WMT COST PG KO PEP PM MO CL EL LULU CMG BKNG MAR GM F RIVN XLY XLP');
  add('Finance', 'JPM BAC WFC C GS MS BLK SCHW AXP V MA PYPL SQ COF USB PNC BRK.B COIN HOOD SOFI XLF KRE');
  add('Health', 'UNH JNJ LLY PFE MRK ABBV TMO ABT DHR BMY AMGN GILD CVS MDT ISRG VRTX REGN MRNA ZTS XLV IBB');
  add('Energy', 'XOM CVX COP SLB EOG OXY PSX MPC VLO KMI XLE USO');
  add('Industrial', 'BA CAT DE GE HON UPS FDX LMT RTX NOC UNP CSX MMM GD XLI');
  add('Real estate & utilities', 'PLD AMT EQIX O SPG NEE DUK SO D XLRE XLU');
  add('Materials', 'LIN SHW APD FCX NEM DOW XLB');
  add('Broad market', 'SPY QQQ DIA IWM VTI VOO IVV RSP VT EFA EEM');
  add('Bonds & gold', 'TLT IEF SHY BND AGG LQD HYG GLD SLV IAU');
  return m;
})();
const sectorOf = s => isCrypto(s) ? 'Crypto' : SECTOR_OF[s] || 'Other';
const UNCAPPED = new Set(['Other', 'Broad market', 'Bonds & gold']);

// ---------- market mood ----------
// From the S&P 500 (stocks) or Bitcoin (crypto): calm uptrend = full size, choppy = 70%, downtrend = 40%.
function moodOf(b, label) {
  const i = b?.c?.length - 1; if (!(i >= 210)) return null;
  const c = b.c[i], s50 = IND.sma(b.c, 50, i), s200 = IND.sma(b.c, 200, i), v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : v20, vr = v250 ? v20 / v250 : 1;
  if (c < s200) return { k: 'bear', scale: 0.4, line: `${label} is below its 200-day average: a falling market. New buys at 40% size.` };
  if (c < s50 || vr > 1.35) return { k: 'choppy', scale: 0.7, line: `${label} is ${c < s50 ? 'below its 50-day average' : `swinging ${vr.toFixed(1)}x harder than usual`}: a choppy market. New buys at 70% size.` };
  return { k: 'bull', scale: 1, line: `${label} is in a calm uptrend. Full size.` };
}

// ---------- correlation of daily returns over the last 60 days ----------
function corr60(a, b) {
  if (!a?.c || !b?.c) return null;
  const n = 60, ra = [], rb = [], ma = new Map(a.d.map((d, i) => [d, i]));
  for (let j = b.d.length - 1; j > 0 && ra.length < n; j--) { const i = ma.get(b.d[j]); if (i == null || i < 1) continue; ra.push(a.c[i] / a.c[i - 1] - 1); rb.push(b.c[j] / b.c[j - 1] - 1); }
  if (ra.length < 30) return null;
  const mean = x => x.reduce((t, v) => t + v, 0) / x.length, xa = mean(ra), xb = mean(rb);
  let sab = 0, saa = 0, sbb = 0; for (let k = 0; k < ra.length; k++) { const da = ra[k] - xa, db = rb[k] - xb; sab += da * db; saa += da * da; sbb += db * db; }
  return saa && sbb ? sab / Math.sqrt(saa * sbb) : null;
}

// ---------- shared risk across bots (each side, paper and Alpaca, counted separately) ----------
async function botExposure(broker) {
  const out = { bySym: {}, bySector: {}, total: 0, n: 0 };
  for (const b of await rows("select b.wid, w.broker from bots b join wallets w on w.id=b.wid where b.status <> 'killed'")) {
    if (!!b.broker !== !!broker) continue;
    const w = await row('select cash from wallets where id=$1', [b.wid]); if (!w) continue;
    const pos = await positions(b.wid); out.n++; out.total += +w.cash + marketValue(pos);
    for (const p of pos) { const v = p.qty * (priceOf(p.sym) || p.avg); out.bySym[p.sym] = (out.bySym[p.sym] || 0) + v; const sec = sectorOf(p.sym); out.bySector[sec] = (out.bySector[sec] || 0) + v; }
  }
  return out;
}
// how many dollars a buy of `sym` may add without breaking the shared caps (Infinity when there's no cap)
function sharedRoom(ex, sym) {
  if (sym === 'SGOV' || sym === 'BIL') return Infinity; // T-bill parking isn't a concentration risk
  if (FUNDS.has(sym) && S.sharedRisk !== false && ex?.total > 0 && ex.n >= 2) return Math.max(0, (Math.max(S.maxSymPct ?? 30, 60)) / 100 * ex.total - (ex.bySym[sym] || 0)); // broad index funds are spread out inside: a looser cap
  if (S.sharedRisk === false || !(ex?.total > 0) || ex.n < 2) return Infinity; // one bot alone is governed by its own limits
  const symCap = (S.maxSymPct ?? 30) / 100 * ex.total, sec = sectorOf(sym), secCap = (S.maxSectorPct ?? 50) / 100 * ex.total;
  let room = symCap - (ex.bySym[sym] || 0);
  if (!UNCAPPED.has(sec) && sec !== 'Crypto') room = Math.min(room, secCap - (ex.bySector[sec] || 0));
  if (sec === 'Crypto') room = Math.min(room, (S.maxCryptoPct ?? 50) / 100 * ex.total - (ex.bySector.Crypto || 0));
  return Math.max(0, room);
}

// ---------- smarter exits ----------
// Stops move up as profit grows: to break-even at +6%, 7% under the high at +12%, 5% under it at +20%.
function tightStop(entry, high, cur) {
  const gain = cur / entry - 1; if (!(gain >= 0.06)) return null;
  let s = entry * 1.002; if (gain >= 0.12) s = Math.max(s, high * 0.93); if (gain >= 0.2) s = Math.max(s, high * 0.95);
  return s < cur * 0.995 ? s : null;
}

// ---------- report card and benching ----------
async function botReport(b, spy) {
  const bot = botJson(b), w = await row('select * from wallets where id=$1', [b.wid]); if (!w) return null;
  const since = new Date(bot.created), pos = await positions(b.wid), value = +w.cash + marketValue(pos);
  const sells = (await rows("select pnl from trd where wid=$1 and side='sell' and pnl is not null and ts >= $2", [b.wid, since])).map(x => +x.pnl);
  const wins = sells.filter(x => x > 0), losses = sells.filter(x => x <= 0);
  const back = await row("select eq from snap where wid=$1 and ts <= now() - interval '60 days' order by ts desc limit 1", [b.wid]);
  const days = Math.round((Date.now() - bot.created) / 864e5);
  let vs60 = null;
  if (back && spy) { const i = lastIndexOnOrBefore(spy.d, etDate(Date.now() - 60 * 864e5)), s0 = i >= 0 ? spy.c[i] : null, s1 = priceOf('SPY') || spy.c[spy.c.length - 1]; if (s0) { const pf60 = await flowPerf(b.wid, Date.now() - 60 * 864e5, value); vs60 = pf60.ret - (s1 / s0 - 1); } }
  const benched = days >= 60 && vs60 != null && vs60 < -0.03;
  return { avgWin: wins.length ? wins.reduce((t, x) => t + x, 0) / wins.length : null, avgLoss: losses.length ? losses.reduce((t, x) => t + x, 0) / losses.length : null, payoff: wins.length && losses.length ? (wins.reduce((t, x) => t + x, 0) / wins.length) / Math.abs(losses.reduce((t, x) => t + x, 0) / losses.length) : null, vs60, benched, days };
}
// once a week: flag (and if switched on, pause) bots that have trailed the S&P 500 for 60 days
async function benchCheck() {
  let spy = null; try { spy = await dailyBars('SPY'); } catch (e) { return; }
  for (const b of await rows("select * from bots where status='running'")) {
    if (STRATEGIES[b.strategy]?.auto) continue;
    const r = await botReport(b, spy).catch(() => null); if (!r?.benched) continue;
    const st = jsonOr(b.state, {}); if (st._benchedAt && Date.now() - st._benchedAt < 6 * 864e5) continue;
    st._benchedAt = Date.now(); await Q('update bots set state=$1 where id=$2', [JSON.stringify(st), b.id]);
    const msg = `It has trailed the S&P 500 by ${(Math.abs(r.vs60) * 100).toFixed(1)}% over the last 60 days.${S.autoBench ? ' It has been paused.' : ''}`;
    if (S.autoBench) await Q("update bots set status='paused' where id=$1", [b.id]);
    await botLog(b.id, 'risk', S.autoBench ? 'Benched: paused for trailing the market' : 'Benched: trailing the market', msg);
    await notify('bot', `🪑 ${b.name} is benched`, msg, { url: '/#/bot/' + b.id });
  }
}

// ---------- re-tuning: retest a bot's settings on recent history and suggest better ones ----------
async function tuneBot(id, { quiet = false } = {}) {
  const b = await row('select * from bots where id=$1', [id]); if (!b) fail('That bot is gone', 404);
  const bot = botJson(b), st = STRATEGIES[bot.strategy]; if (!st || st.auto || !Object.keys(st.grid || {}).length) fail('This strategy has no settings to tune.');
  const r = await backtest({ strategy: bot.strategy, syms: bot.syms, params: bot.params, years: 3, maxPosPct: bot.mandate.maxPosPct });
  const R = r.robust, state = jsonOr(b.state, {});
  let tune = { ts: Date.now(), checked: true, better: false, yours: R?.outSample?.yours || null, bench: R?.outSample?.bench || null };
  if (R?.tuned && R.outSample?.best && R.outSample?.yours) {
    const changed = Object.entries(R.tuned).some(([k, v]) => bot.params[k] !== v), gain = (R.outSample.best.sharpe ?? 0) - (R.outSample.yours.sharpe ?? 0);
    if (changed && gain >= 0.2 && (R.outSample.best.ret ?? 0) > (R.outSample.yours.ret ?? 0)) tune = { ...tune, better: true, params: { ...bot.params, ...R.tuned }, best: R.outSample.best, gain, split: R.split };
  }
  state._tune = tune; await Q('update bots set state=$1 where id=$2', [JSON.stringify(state), id]);
  if (tune.better && !quiet) await notify('bot', `🔧 Better settings for ${bot.name}`, `On the last part of 3 years it never saw while tuning, the suggested settings did better (Sharpe +${tune.gain.toFixed(2)}). Review them on the bot page.`, { url: '/#/bot/' + id });
  return tune;
}
// one bot per hour, each bot about once a month
async function tuneJob() {
  if (S.autoTune === false) return;
  const list = (await rows("select * from bots where status='running' order by id")).filter(b => !STRATEGIES[b.strategy]?.auto && Object.keys(STRATEGIES[b.strategy]?.grid || {}).length);
  const due = list.find(b => !(jsonOr(b.state, {})._tune?.ts > Date.now() - 30 * 864e5)); if (!due) return;
  await tuneBot(due.id).catch(e => warn('tune', e.message));
}
GET('/api/bots/:id/tune', auth, async req => jsonOr((await row('select state from bots where id=$1', [+req.params.id]))?.state, {})._tune || null);
POST('/api/bots/:id/tune', auth, async req => tuneBot(+req.params.id, { quiet: true }));
POST('/api/bots/:id/tune/apply', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  const state = jsonOr(b.state, {}), t = state._tune; if (!t?.better) fail('No suggestion to apply.');
  const params = cleanParams(b.strategy, t.params); state._tune = { ...t, better: false, applied: Date.now() };
  await Q('update bots set params=$1, state=$2 where id=$3', [JSON.stringify(params), JSON.stringify(state), b.id]);
  await botLog(b.id, 'info', 'Settings updated from the re-test', Object.entries(params).map(([k, v]) => `${k}: ${v}`).join(', '));
  return { ok: true, params };
});
POST('/api/bots/:id/tune/dismiss', auth, async req => { const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404); const state = jsonOr(b.state, {}); if (state._tune) state._tune.better = false; await Q('update bots set state=$1 where id=$2', [JSON.stringify(state), b.id]); return { ok: true }; });

// ---------- the bot tournament ----------
// Up to 20 variations of one strategy trade pretend money from the day they join (tested on the real prices since then).
// Once a month the bottom of the table is replaced by mutations of the top 3. Adopt a winner as a real bot any time.
const srng = seed => () => { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; };
let TOURN = null;
async function tournaments() { if (!TOURN) TOURN = jsonOr(await kvGet('tournaments').catch(() => null), null) || []; return TOURN; }
const saveTourn = () => kvSet('tournaments', JSON.stringify(TOURN));
function mutate(key, base, R = Math.random) {
  const st = STRATEGIES[key], out = { ...base };
  for (const p of st.params || []) { if (p.choices || p.min == null) continue; if (R() < 0.5) continue; const span = (p.max - p.min) * 0.25, step = p.step || 1; out[p.k] = clamp(Math.round((num(out[p.k], p.def) + (R() * 2 - 1) * span) / step) * step, p.min, p.max); }
  if (key === 'trend' && out.fast >= out.slow) out.fast = Math.max(5, Math.round(out.slow / 3));
  return cleanParams(key, out);
}
async function scoreTournament(t) {
  const { data } = await loadUniverse([...new Set([...t.syms, benchFor(t.syms)])]); const usable = t.syms.filter(s => data[s]); if (!usable.length) return;
  const end = usable.map(s => data[s].d[data[s].d.length - 1]).sort().pop();
  for (const e of t.entrants) {
    // pretend money from the day it joined; before 5 trading days there's nothing to judge
    const start = e.born > addIso(end, -7) ? addIso(end, -7) : e.born;
    try { const r = runBacktest({ key: t.strategy, params: e.params, syms: usable, data, start, end, capital: 10000, maxPosPct: t.maxPosPct || 25 }), c = r.curve; e.ret = c.length ? c[c.length - 1][1] / 10000 - 1 : 0; let pk = -Infinity, dd = 0; for (const [, v] of c) { pk = Math.max(pk, v); dd = Math.min(dd, v / pk - 1); } e.dd = dd; e.trades = r.closed.length; e.days = c.length; } catch (x) { e.ret = e.ret ?? 0; }
  }
  t.entrants.sort((a, b) => (b.ret ?? -9) - (a.ret ?? -9)); t.scored = Date.now();
}
async function tournamentJob() {
  const list = await tournaments(); if (!list.length) return;
  for (const t of list) {
    await scoreTournament(t).catch(e => warn('tournament', e.message));
    // monthly cull: keep the top 3, refill with mutations of them
    if (Date.now() - (t.roundAt || t.created) > 30 * 864e5 && t.entrants.length > 3) {
      const top = t.entrants.slice(0, 3), today = etDate();
      t.entrants = [...top, ...Array.from({ length: t.size - 3 }, (_, i) => ({ id: `${t.id}-${t.round + 1}-${i}`, params: mutate(t.strategy, top[i % 3].params), born: today, ret: 0 }))];
      t.round++; t.roundAt = Date.now();
      await notify('bot', `🏆 Tournament round ${t.round}: ${STRATEGIES[t.strategy].name}`, `The top 3 stay; ${t.size - 3} new variations of them joined.`, { url: '/#/tournament/' + t.id });
    }
  }
  await saveTourn();
}
GET('/api/tournaments', auth, async () => (await tournaments()).map(t => ({ ...t, name: STRATEGIES[t.strategy]?.name || t.strategy })));
POST('/api/tournaments', auth, async req => {
  const b = req.body || {}, key = b.strategy, st = STRATEGIES[key]; if (!st || st.auto) fail('Pick a strategy');
  const syms = await cleanBotSyms(b.syms || [], b.kinds); if (!syms.length) fail('Pick at least one symbol');
  const list = await tournaments(); if (list.length >= 5) fail('You can run up to 5 tournaments at once. End one first.');
  const size = clamp(Math.round(num(b.size, 12)), 6, 20), base = cleanParams(key, b.params || {}), today = etDate(), id = 't' + Date.now().toString(36);
  const R = srng(1 + Date.now() % 2147483000), entrants = [{ id: id + '-0-0', params: base, born: today }, ...Array.from({ length: size - 1 }, (_, i) => ({ id: `${id}-0-${i + 1}`, params: mutate(key, base, R), born: today }))];
  const t = { id, strategy: key, syms, size, created: Date.now(), round: 0, roundAt: Date.now(), maxPosPct: clamp(num(b.maxPosPct, 25), 5, 100), entrants };
  list.push(t); await scoreTournament(t).catch(() => {}); await saveTourn(); return { ...t, name: st.name };
});
POST('/api/tournaments/:id/delete', auth, async req => { const list = await tournaments(); TOURN = list.filter(t => t.id !== req.params.id); await saveTourn(); return { ok: true }; });
POST('/api/tournaments/:id/score', auth, async req => { const t = (await tournaments()).find(x => x.id === req.params.id); if (!t) fail('That tournament is gone', 404); await scoreTournament(t); await saveTourn(); return { ...t, name: STRATEGIES[t.strategy]?.name }; });

// ---------- earnings: confirmed beats, fetched for the live bot (the lab tests the price pattern only) ----------
async function earningsBeats(syms) {
  const out = {}; if (!FINNHUB_KEY) return null; // no key: fall back to the price pattern alone
  const from = etDate(Date.now() - 20 * 864e5), to = etDate();
  for (const s of syms.filter(x => !isCrypto(x)).slice(0, 25)) {
    out[s] = await cached('beat:' + s + ':' + to, 12 * 3600e3, async () => { const d = await fhApi(`calendar/earnings?from=${from}&to=${to}&symbol=${encodeURIComponent(s)}`, 1).catch(() => null); return (d?.earningsCalendar || []).filter(x => x.epsActual != null && x.epsEstimate != null).map(x => ({ date: x.date, surprise: x.epsEstimate ? (x.epsActual - x.epsEstimate) / Math.abs(x.epsEstimate) : 0 })).sort((a, b) => b.date.localeCompare(a.date))[0] || null; }).catch(() => null);
  }
  return out;
}
STRATEGIES.earnings = {
  name: 'Earnings drift', short: 'Earnings', icon: 'chart', rebalance: 'none', markets: 'stocks',
  overlays: { mood: false, corr: false, exits: false, tighten: false, council: 'advise', news: 'severe' },
  desc: 'Buys a stock right after a strong earnings report: the company beat what analysts expected and the price jumped and held up the next day. Holds it for about five weeks. Stocks that beat tend to keep drifting up for weeks as the market slowly catches up (post-earnings drift). Live, it checks the real report dates and beats; in the lab only the price pattern can be tested.',
  params: [{ k: 'beat', label: 'Beat estimates by at least (%)', def: 5, min: 0, max: 50 }, { k: 'gap', label: 'Price jump of at least (%)', def: 2, min: 0, max: 15, step: 0.5 }, { k: 'holdDays', label: 'Hold for (days)', def: 25, min: 5, max: 60 }, { k: 'stopAtr', label: 'Stop (ATRs)', def: 2.5, min: 1, max: 6, step: 0.5 }, { k: 'maxPositions', label: 'Most positions', def: 5, min: 1, max: 15 }],
  grid: { gap: [1.5, 3], holdDays: [15, 25, 40] },
  need: () => 60,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, cand = [], slots = fitSlots(ctx, p.maxPositions), slot = Math.min(ctx.maxW, 1 / slots), beats = ctx.beats || null;
    let held = 0;
    for (const s of ctx.syms) {
      const b = ctx.data[s], i = ctx.idx[s]; if (i == null || i < 55) continue;
      const c = b.c[i], atr = IND.atr(b, 20, i - 1, b.memo), h = ctx.held[s];
      if (h) { const days = h.days || 0; out.notes[s] = `Bought after earnings; day ${days} of ${p.holdDays}.${days >= p.holdDays ? ' Time is up: sell.' : ''}`; if (days < p.holdDays) { out.weights[s] = h.weight || slot; out.stops[s] = { trail: false, price: h.stopPx || h.entry - p.stopAtr * (atr || 0) }; held++; } continue; }
      if (!atr) continue;
      // the reaction: the report day's jump (one of the last 3 days) still holding today
      let best = null; for (let k = Math.max(1, i - 2); k <= i; k++) { const g = b.c[k] / b.c[k - 1] - 1; if (!best || g > best.g) best = { g, k }; }
      const holds = best && c >= b.c[best.k] * 0.985, trend = c > IND.sma(b.c, 50, i);
      let ok = best && best.g * 100 >= p.gap && holds && trend, why = `Biggest move in 3 days ${best ? (best.g * 100).toFixed(1) : 0}%.`;
      if (beats) { const e = beats[s]; const recent = e && e.date >= b.d[Math.max(0, i - 3)]; why += recent ? ` Reported ${e.date}: ${e.surprise >= 0 ? 'beat' : 'missed'} by ${Math.abs(e.surprise * 100).toFixed(0)}%.` : ' No report in the last few days.'; ok = ok && recent && e.surprise * 100 >= p.beat; }
      out.notes[s] = why + (ok ? ' A real beat that the market liked: buy.' : '');
      if (ok) cand.push({ s, g: best.g, atr, c });
    }
    cand.sort((a, b) => b.g - a.g);
    for (const x of cand.slice(0, Math.max(0, slots - held))) { out.weights[x.s] = slot; out.stops[x.s] = { trail: false, price: x.c - p.stopAtr * x.atr }; }
    return out;
  },
};
// A wallet's return that ignores money moved in or out (deposits, withdrawals, weekly adds): each stretch between
// snapshots is judged on what the money already there did, then the stretches are chained (time-weighted return).
async function flowPerf(wid, fromTs, value, startEq = null) {
  const from = new Date(fromTs);
  const snaps = (await rows('select ts, eq from snap where wid=$1 and ts >= $2 order by ts', [wid, from])).map(r => [tsMs(r.ts), +r.eq]);
  const flows = (await rows("select ts, price from trd where wid=$1 and side='cash' and ts >= $2 order by ts", [wid, from])).map(r => [tsMs(r.ts), +r.price]);
  // the money at the start: what was there before the first flow and snapshot after the start
  let e0 = startEq;
  if (!(e0 > 0)) { const before = await row('select eq from snap where wid=$1 and ts <= $2 order by ts desc limit 1', [wid, from]); e0 = before ? +before.eq : snaps.length ? snaps[0][1] - flows.filter(f => f[0] <= snaps[0][0]).reduce((t, f) => t + f[1], 0) : value - flows.reduce((t, f) => t + f[1], 0); }
  const pts = [...snaps, [Date.now(), value]];
  let idx = 1, peak = 1, dd = 0, t0 = +from, prev = e0, fi = 0;
  const curve = [];
  for (const [t, e] of pts) {
    let f = 0; while (fi < flows.length && flows[fi][0] <= t) { if (flows[fi][0] > t0 || t0 === +from) f += flows[fi][1]; fi++; }
    if (prev > 0) { const r = (e - f) / prev; if (Number.isFinite(r) && r > 0) idx *= r; }
    peak = Math.max(peak, idx); dd = Math.min(dd, idx / peak - 1); curve.push([t, idx]);
    prev = e; t0 = t;
  }
  const added = flows.reduce((t, f) => t + f[1], 0);
  return { ret: idx - 1, maxDD: dd, start: e0, added, invested: e0 + added, profit: value - e0 - added, curve };
}
// ===================================================================
// Practice tools: smart chart alerts, the monthly review of the trade journal, weekly challenges,
// the what-if machine, options practice (Black-Scholes estimates), the dividend calendar and the
// read-only feed for an iPhone Shortcuts widget.
// ===================================================================
const paperWallets = async () => (await rows('select * from wallets where broker is null and not archived order by id')).map(w => w.id);
const botWallets = async () => new Set((await rows('select wid from bots')).map(r => r.wid));
const fmtPct = x => x == null || !Number.isFinite(x) ? 'n/a' : `${x >= 0 ? '+' : ''}${(x * 100).toFixed(1)}%`;
// daily closes with today's live price as the last bar (so indicators move during the day)
function liveCloses(b, sym) {
  const c = b.c.slice(), v = (b.v || []).slice(), p = priceOf(sym), today = etDate();
  if (p > 0) { if (b.d[b.d.length - 1] === today) c[c.length - 1] = p; else { c.push(p); v.push(null); } }
  return { c, v, i: c.length - 1 };
}

// ---------- smart chart alerts ----------
const IND_ALERTS = {
  rsiBelow: { title: v => `RSI falls below ${v}`, test: (c, i, v) => IND.rsi(c, 14, i) < v, def: 30 },
  rsiAbove: { title: v => `RSI rises above ${v}`, test: (c, i, v) => IND.rsi(c, 14, i) > v, def: 70 },
  above50: { title: () => 'Crosses above its 50-day average', test: (c, i) => c[i] > IND.sma(c, 50, i) },
  below50: { title: () => 'Crosses below its 50-day average', test: (c, i) => c[i] < IND.sma(c, 50, i) },
  above200: { title: () => 'Crosses above its 200-day average', test: (c, i) => i >= 200 && c[i] > IND.sma(c, 200, i) },
  below200: { title: () => 'Crosses below its 200-day average', test: (c, i) => i >= 200 && c[i] < IND.sma(c, 200, i) },
  golden: { title: () => 'Golden cross (50-day over 200-day)', test: (c, i) => i >= 200 && IND.sma(c, 50, i) > IND.sma(c, 200, i) },
  death: { title: () => 'Death cross (50-day under 200-day)', test: (c, i) => i >= 200 && IND.sma(c, 50, i) < IND.sma(c, 200, i) },
  high52: { title: () => 'Hits a new 52-week high', test: (c, i) => i >= 252 && c[i] >= Math.max(...c.slice(i - 251, i)) },
  low52: { title: () => 'Hits a new 52-week low', test: (c, i) => i >= 252 && c[i] <= Math.min(...c.slice(i - 251, i)) },
  volSpike: { title: v => `Volume over ${v}× normal`, test: (c, i, v, vol) => { const k = vol.length - 1; if (!(vol[k] > 0) || k < 21) return false; let s = 0; for (let j = k - 20; j < k; j++) s += vol[j] || 0; return vol[k] > v * s / 20; }, def: 2 },
};
const indTitle = ind => IND_ALERTS[ind?.t]?.title(ind.v) || 'Chart signal';
let indBusy = false;
async function indAlertJob() {
  if (indBusy) return; indBusy = true; try { await indAlertRun(); } finally { indBusy = false; }
}
async function indAlertRun() {
  const list = await rows("select * from alert where kind='ind' and not fired").catch(() => []); if (!list.length) return;
  const bySym = {}; for (const a of list) (bySym[a.sym] ||= []).push(a);
  for (const [sym, as] of Object.entries(bySym)) {
    if (!isCrypto(sym) && market().state === 'closed' && etParts().h > 9) continue; // stocks: only while something trades
    const b = await dailyBars(sym).catch(() => null); if (!b) continue;
    const { c, v, i } = liveCloses(b, sym);
    for (const a of as) {
      const ind = jsonOr(a.ind, {}), def = IND_ALERTS[ind.t]; if (!def) continue;
      let on = false; try { on = !!def.test(c, i, num(ind.v, def.def), v); } catch (e) {}
      if (!on) { if (!a.armed) await Q('update alert set armed=true where id=$1', [a.id]); continue; }
      if (!a.armed || etDate(tsMs(a.last_fired) || 0) === etDate()) continue; // fire when it becomes true, at most once a day
      const rsi = IND.rsi(c, 14, i);
      const won = await row(a.rep ? 'update alert set armed=false, last_fired=now() where id=$1 and armed returning id' : 'update alert set fired=true, armed=false, last_fired=now() where id=$1 and armed and not fired returning id', [a.id]);
      if (!won) continue; // another check already fired it
      await notify('alert', `${sym}: ${indTitle(ind)}`, `${sym} is at ${pxStr(c[i])}. RSI ${rsi != null ? rsi.toFixed(0) : 'n/a'}, 50-day ${pxStr(IND.sma(c, 50, i) || 0)}.${a.label ? ' ' + a.label : ''}`, { sym, data: { kind: 'alert' } });
    }
  }
}

// ---------- monthly review ----------
async function monthReview(wid, m) {
  if (!/^\d{4}-\d{2}$/.test(m)) fail('Pick a month');
  const [y, mo] = m.split('-').map(Number), from = new Date(Date.UTC(y, mo - 1, 1, 4)), to = new Date(Date.UTC(mo === 12 ? y + 1 : y, mo === 12 ? 0 : mo, 1, 4));
  const tr = await rows("select * from trd where wid=$1 and ts >= $2 and ts < $3 and side in ('buy','sell') order by id", [wid, from, to]);
  const sells = tr.filter(t => t.side === 'sell' && t.pnl != null), wins = sells.filter(t => +t.pnl > 0), losses = sells.filter(t => +t.pnl <= 0);
  const sum = a => a.reduce((t, x) => t + +x.pnl, 0);
  const holds = await rows('select r.acquired, t.ts from realized r join trd t on t.id=r.trd_id where t.wid=$1 and t.ts >= $2 and t.ts < $3', [wid, from, to]).catch(() => []);
  const avgHold = holds.length ? holds.reduce((t, r) => t + (tsMs(r.ts) - tsMs(r.acquired)), 0) / holds.length / 864e5 : null;
  const byTag = {}; for (const t of sells) for (const g of String(t.tags || '').split(',').filter(Boolean)) { const x = byTag[g] ||= { n: 0, pnl: 0, wins: 0 }; x.n++; x.pnl += +t.pnl; if (+t.pnl > 0) x.wins++; }
  const byDay = {}; for (const t of sells) { const d = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][new Date(etDate(tsMs(t.ts)) + 'T12:00').getDay()]; const x = byDay[d] ||= { n: 0, pnl: 0 }; x.n++; x.pnl += +t.pnl; }
  const bySym = {}; for (const t of sells) { const x = bySym[t.sym] ||= { n: 0, pnl: 0 }; x.n++; x.pnl += +t.pnl; }
  const e0 = await row('select eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [wid, from]), e1 = await row('select eq from snap where wid=$1 and ts < $2 order by ts desc limit 1', [wid, to]);
  let spy = null; try { const b = await dailyBars('SPY'), i0 = lastIndexOnOrBefore(b.d, etDate(from.getTime() - 864e5)), i1 = lastIndexOnOrBefore(b.d, etDate(to.getTime() - 864e5)); if (i0 >= 0 && i1 > i0) spy = b.c[i1] / b.c[i0] - 1; } catch (e) {}
  const best = sells.slice().sort((a, b) => +b.pnl - +a.pnl)[0], worst = sells.slice().sort((a, b) => +a.pnl - +b.pnl)[0];
  const noted = tr.filter(t => t.note).length;
  const tj = t => t && { id: t.id, sym: t.sym, pnl: +t.pnl, ts: tsMs(t.ts), note: t.note, tags: t.tags };
  const saved = jsonOr(await kvGet(`review:${wid}:${m}`).catch(() => null), null);
  return { month: m, trades: tr.length, buys: tr.length - sells.length, sells: sells.length, wins: wins.length, losses: losses.length, winRate: sells.length ? wins.length / sells.length : null,
    realized: sum(sells), avgWin: wins.length ? sum(wins) / wins.length : null, avgLoss: losses.length ? sum(losses) / losses.length : null, avgHold, best: tj(best), worst: tj(worst),
    ret: e0 && e1 ? +e1.eq / +e0.eq - 1 : null, spy, noted, tags: Object.entries(byTag).map(([k, x]) => ({ tag: k, ...x })).sort((a, b) => b.n - a.n), days: byDay, syms: Object.entries(bySym).map(([k, x]) => ({ sym: k, ...x })).sort((a, b) => b.pnl - a.pnl),
    notes: tr.filter(t => t.note).slice(-12).map(t => ({ sym: t.sym, side: t.side, pnl: t.pnl == null ? null : +t.pnl, note: String(t.note).slice(0, 200), tags: t.tags })), coach: saved };
}
async function reviewCoach(wid, m) {
  const r = await monthReview(wid, m);
  const facts = `Month ${m}. ${r.trades} trades (${r.buys} buys, ${r.sells} sells). Closed: ${r.wins} wins, ${r.losses} losses${r.winRate != null ? `, win rate ${Math.round(r.winRate * 100)}%` : ''}. Realized ${usdS(r.realized)}. Average win ${usdS(r.avgWin)}, average loss ${usdS(r.avgLoss)}. Average hold ${r.avgHold != null ? r.avgHold.toFixed(1) + ' days' : 'n/a'}.
Wallet return ${fmtPct(r.ret)} vs S&P 500 ${fmtPct(r.spy)}.
Best trade: ${r.best ? `${r.best.sym} ${usdS(r.best.pnl)}` : 'none'}. Worst: ${r.worst ? `${r.worst.sym} ${usdS(r.worst.pnl)}` : 'none'}.
By tag: ${r.tags.map(t => `${t.tag} ${t.n} trades ${usdS(t.pnl)}`).join('; ') || 'no tags'}.
By weekday: ${Object.entries(r.days).map(([d, x]) => `${d} ${x.n} ${usdS(x.pnl)}`).join('; ') || 'n/a'}.
Journal notes (${r.noted} of ${r.trades} trades have one):
${r.notes.map(n => `- ${n.side} ${n.sym}${n.pnl != null ? ` (${usdS(n.pnl)})` : ''}: ${n.note}${n.tags ? ` [${n.tags}]` : ''}`).join('\n') || '- none'}`;
  let text;
  if (analystReady()) { const a = await aiText('review', 'You are a friendly trading coach reviewing one month of paper trades for someone learning to trade. Using only the facts, write 4 short paragraphs: how the month went (with numbers, against the S&P 500), the strongest habit to keep, the pattern that cost the most money (look at tags, weekdays, hold times and the notes), and one specific thing to try next month. Be honest and kind. No advice about real money.', facts, { max: 450 }); text = a.ok ? a.text : null; }
  if (!text) text = [r.trades ? `${r.trades} trades, ${r.wins} wins and ${r.losses} losses, ${usdS(r.realized)} realized.` : 'No trades this month.', r.ret != null ? `The wallet did ${fmtPct(r.ret)} against ${fmtPct(r.spy)} for the S&P 500.` : '', r.avgWin != null && r.avgLoss != null ? `Average win ${usdS(r.avgWin)} vs average loss ${usdS(r.avgLoss)}: ${Math.abs(r.avgWin) > Math.abs(r.avgLoss) ? 'your wins are bigger than your losses, keep that.' : 'your losses are bigger than your wins; try cutting losers sooner.'}` : '', r.noted < r.trades / 2 && r.trades ? `Only ${r.noted} of ${r.trades} trades have a journal note. Writing why you trade makes these reviews much more useful.` : ''].filter(Boolean).join('\n\n');
  const out = { ts: Date.now(), text, ai: analystReady() };
  await kvSet(`review:${wid}:${m}`, JSON.stringify(out)); return out;
}
const usdS = x => x == null || !Number.isFinite(+x) ? 'n/a' : `${+x < 0 ? '−' : ''}$${Math.abs(+x).toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
const prevMonth = () => { const d = etDate(), [y, m] = d.split('-').map(Number); return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`; };
GET('/api/review', auth, async req => { chalBump('review'); return monthReview(await activeWalletId(req), String(req.query.m || etDate().slice(0, 7))); });
POST('/api/review/coach', auth, async req => reviewCoach(await activeWalletId(req), String(req.body?.m || etDate().slice(0, 7))));
GET('/api/review/months', auth, async req => (await rows("select distinct to_char(ts at time zone 'America/New_York','YYYY-MM') m from trd where wid=$1 and side in ('buy','sell') order by m desc limit 36", [await activeWalletId(req)])).map(r => r.m));
async function reviewJob() {
  const d = etDate(); if (+d.slice(8) > 3 || etParts().h < 9) return; const m = prevMonth();
  if ((await kvGet('review_sent').catch(() => null)) === m) return; await kvSet('review_sent', m);
  const wid = S.defaultWallet || 1, r = await monthReview(wid, m).catch(() => null); if (!r?.trades) return;
  await reviewCoach(wid, m).catch(() => {});
  await notify('summary', `📓 Your ${new Date(m + '-15').toLocaleString('en-US', { month: 'long' })} review`, `${r.trades} trades, ${r.wins} wins, ${usdS(r.realized)} realized. Tap for your coach's notes.`, { url: '/#/review/' + m });
}

// ---------- weekly challenges ----------
const chalWeek = (t = Date.now()) => { const d = new Date(etDate(t) + 'T12:00Z'), wd = (d.getUTCDay() + 6) % 7; d.setUTCDate(d.getUTCDate() - wd); return d.toISOString().slice(0, 10); };
const chalCount = async (k, wk = chalWeek()) => +(await kvGet(`chal:${wk}:${k}`).catch(() => 0) || 0);
async function chalBump(k) { const wk = chalWeek(); await kvSet(`chal:${wk}:${k}`, String((await chalCount(k, wk)) + 1)).catch(() => {}); }


// ---------- the what-if machine ----------
POST('/api/whatif', auth, async req => {
  const sym = cleanSym(req.body?.sym); if (!sym) fail('Pick a stock or coin');
  const usd = clamp(num(req.body?.usd, 1000), 1, 1e9), date = String(req.body?.date || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || date >= etDate()) fail('Pick a date in the past');
  const b = await dailyBars(sym), i0 = b.d.findIndex(d => d >= date); if (i0 < 0) fail(`No prices for ${sym} after ${date}`);
  const now = priceOf(sym) || b.c[b.c.length - 1], sh = usd / b.c[i0], value = sh * now;
  let spy = null; try { const s = await dailyBars('SPY'), j = s.d.findIndex(d => d >= b.d[i0]); if (j >= 0) spy = usd * (priceOf('SPY') || s.c[s.c.length - 1]) / s.c[j]; } catch (e) {}
  const step = Math.max(1, Math.floor((b.c.length - i0) / 120)), points = []; for (let k = i0; k < b.c.length; k += step) points.push([Date.parse(b.d[k] + 'T21:00Z'), sh * b.c[k]]); points.push([Date.now(), value]);
  let peak = 0, dd = 0; for (let k = i0; k < b.c.length; k++) { const v = sh * b.c[k]; peak = Math.max(peak, v); dd = Math.min(dd, v / peak - 1); }
  chalBump('whatif');
  return { sym, usd, from: b.d[i0], fromPx: b.c[i0], now, shares: sh, value, ret: value / usd - 1, spy, spyRet: spy ? spy / usd - 1 : null, maxDD: dd, years: (Date.now() - Date.parse(b.d[i0])) / 31557600000, points, first: b.d[0], adjusted: true };
});
// your own sells: what they'd be worth if you'd held on
GET('/api/whatif/sells', auth, async req => {
  const wid = await activeWalletId(req), list = await rows("select * from trd where wid=$1 and side='sell' order by id desc limit 25", [wid]);
  return list.map(t => { const now = priceOf(t.sym); return { id: t.id, sym: t.sym, ts: tsMs(t.ts), qty: +t.qty, sold: +t.price, now, diff: now ? (now - +t.price) * +t.qty : null, pct: now ? now / +t.price - 1 : null }; }).filter(x => x.now);
});

// ---------- option pricing: Black-Scholes from recent volatility (the options themselves live in 09i-options.js) ----------
const OPT_START = 10000, OPT_FEE = 0.65, RATE = 0.04;
const ncdf = x => { const t = 1 / (1 + 0.2316419 * Math.abs(x)), d = 0.3989423 * Math.exp(-x * x / 2), p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274)))); return x > 0 ? 1 - p : p; };
function bs(kind, S0, K, T, iv) {
  if (T <= 0 || !(iv > 0)) return { px: Math.max(0, kind === 'call' ? S0 - K : K - S0), delta: kind === 'call' ? (S0 > K ? 1 : 0) : (S0 < K ? -1 : 0) };
  const sq = iv * Math.sqrt(T), d1 = (Math.log(S0 / K) + (RATE + iv * iv / 2) * T) / sq, d2 = d1 - sq;
  const px = kind === 'call' ? S0 * ncdf(d1) - K * Math.exp(-RATE * T) * ncdf(d2) : K * Math.exp(-RATE * T) * ncdf(-d2) - S0 * ncdf(-d1);
  const theta = (-(S0 * Math.exp(-d1 * d1 / 2) / Math.sqrt(2 * Math.PI) * iv) / (2 * Math.sqrt(T)) - (kind === 'call' ? 1 : -1) * RATE * K * Math.exp(-RATE * T) * ncdf(kind === 'call' ? d2 : -d2)) / 365;
  return { px: Math.max(0.01, px), delta: kind === 'call' ? ncdf(d1) : ncdf(d1) - 1, theta };
}
const yearsTo = exp => Math.max(0, (etToUtc(...exp.split('-').map(Number), 16, 0) - Date.now()) / 31557600000);
async function baseVol(sym) {
  return cached('iv:' + sym, 3600e3, async () => { const b = await dailyBars(sym), i = b.c.length - 1; const v20 = IND.vol(b.c, 20, i), v60 = IND.vol(b.c, 60, i); return clamp(((v20 || 0.015) * 0.5 + (v60 || v20 || 0.015) * 0.5) * Math.sqrt(252) * 1.1, 0.08, 2.5); });
}
const ivAt = (iv, S0, K) => { const m = Math.log(K / S0); return iv * (1 + 1.2 * m * m - (m < 0 ? 0.15 * m : 0)); }; // a gentle smile, puts a little richer
function expiries() {
  const out = [], p = etParts(); let d = etDate();
  for (let k = 0; k < 120 && out.length < 8; k++, d = addIso(d, 1)) { const wd = new Date(d + 'T12:00Z').getUTCDay(), [y, m, dd] = d.split('-').map(Number); if (wd !== 5 || !isTradingDay(y, m, dd) || (d === etDate() && p.h >= 16)) continue; const third = dd >= 15 && dd <= 21; if (out.length < 5 || third) out.push(d); }
  return out;
}
const strikeStep = px => px < 25 ? 1 : px < 100 ? 2.5 : px < 250 ? 5 : px < 1000 ? 10 : 25;
// ---------- dividend calendar ----------
const weekdayOf = t => { const wd = new Date(t).getUTCDay(); return t - (wd === 6 ? 1 : wd === 0 ? 2 : 0) * 864e5; }; // estimated dates land on a weekday
GET('/api/dividends/calendar', auth, async req => {
  const wid = await activeWalletId(req), pos = await positions(wid), watch = (await rows('select sym from watch')).map(r => r.sym);
  const syms = [...new Set([...pos.map(p => p.sym), ...watch])].filter(s => !isCrypto(s)).slice(0, 40), out = [], horizon = Date.now() + 120 * 864e5;
  for (const s of syms) {
    const ins = await assetInsights(s).catch(() => ({})), divs = (ins.dividends || []).slice().sort((a, b) => a.date - b.date); if (!divs.length) continue;
    const yr = divs.filter(d => d.date > Date.now() - 400 * 864e5); if (!yr.length) continue; // stopped paying
    const gaps = []; for (let k = 1; k < yr.length; k++) gaps.push(yr[k].date - yr[k - 1].date);
    const gap = gaps.length ? gaps.reduce((t, x) => t + x, 0) / gaps.length : 365 * 864e5, last = yr[yr.length - 1], held = pos.find(p => p.sym === s);
    for (let t = last.date + gap, k = 0; t < horizon && k < 6; t += gap, k++) if (t > Date.now() - 2 * 864e5) out.push({ sym: s, date: weekdayOf(Math.round(t)), amount: last.amount, qty: held?.qty || 0, est: held ? last.amount * held.qty : 0, held: !!held, perYear: Math.round(365 * 864e5 / gap), estimated: true });
    if (last.date > Date.now() - 2 * 864e5) out.push({ sym: s, date: last.date, amount: last.amount, qty: held?.qty || 0, est: held ? last.amount * held.qty : 0, held: !!held, estimated: false });
  }
  out.sort((a, b) => a.date - b.date);
  return { events: out, next30: out.filter(e => e.held && e.date < Date.now() + 30 * 864e5).reduce((t, e) => t + e.est, 0), next90: out.filter(e => e.held && e.date < Date.now() + 90 * 864e5).reduce((t, e) => t + e.est, 0) };
});

// ---------- Shortcuts widget feed (read-only, secret link) ----------
async function widgetToken(renew = false) { let t = await kvGet('widget_token').catch(() => null); if (!t || renew) { t = crypto.randomBytes(18).toString('hex'); await kvSet('widget_token', t); } return t; }
GET('/api/widget-link', auth, async () => ({ token: await widgetToken() }));
POST('/api/widget-link', auth, async () => ({ token: await widgetToken(true) }));
GET('/api/widget/:token', async (req, res) => {
  const t = await kvGet('widget_token').catch(() => null);
  if (!t || String(req.params.token).length !== t.length || !crypto.timingSafeEqual(Buffer.from(String(req.params.token)), Buffer.from(t))) fail('Not found', 404);
  const ws = (await walletsSummary()).filter(w => !w.archived && !w.broker), main = ws.find(w => w.id === (S.defaultWallet || 1)) || ws[0];
  const pos = main ? await positions(main.id) : [], movers = pos.map(p => { const q = prices.get(p.sym); return q?.p && q.prev ? { sym: p.sym, pct: round((q.p / q.prev - 1) * 100, 2) } : null; }).filter(Boolean).sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct)).slice(0, 3);
  const bots = await rows("select b.name, b.wid from bots b join wallets w on w.id=b.wid where b.status='running' and w.broker is null");
  const brief = jsonOr(await kvGet('brief').catch(() => null), null);
  const dayAbs = main?.day?.change ?? null, dayPct = main?.day?.pct ?? null;
  const out = { wallet: main?.name || 'Main', value: round(main?.value || 0, 2), day: dayAbs != null ? round(dayAbs, 2) : null, dayPct: dayPct != null ? round(dayPct, 2) : null, total: round(ws.reduce((t, w) => t + w.value, 0), 2), market: market().state, movers, bots: bots.length, brief: brief?.day === etDate() ? brief.text.split('\n')[0] : null, ts: new Date().toISOString() };
  if (req.query.f === 'text') { res.setHeader('content-type', 'text/plain; charset=utf-8'); res.end(`🥭 ${out.wallet} $${out.value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${out.dayPct != null ? ` (${out.dayPct >= 0 ? '+' : ''}${out.dayPct.toFixed(2)}% today)` : ''}\n${movers.map(m => `${m.sym} ${m.pct >= 0 ? '+' : ''}${m.pct.toFixed(1)}%`).join('  ')}\n${out.bots} bot${out.bots === 1 ? '' : 's'} running · market ${out.market}`); return; }
  return out;
});
// =====================================================================
// Options in wallets: approval levels, collateral, OCC expiration and assignment
// =====================================================================
// Prices are Black-Scholes estimates from recent volatility (bs, baseVol, ivAt, expiries, strikeStep live in the
// practice file), with a bid and ask that widen away from the money. Positions live in real wallets, count toward
// the wallet's value, and tie up shares or cash the way a broker does.
const OPT_MULT = 100;
const OPT_LEVELS = [
  { n: 0, name: 'No options', allows: 'Options trading is off for this wallet' },
  { n: 1, name: 'Level 1', short: 'Covered calls and secured puts', allows: 'Covered calls and cash-secured puts' },
  { n: 2, name: 'Level 2', short: 'Buy calls and puts', allows: 'Buying calls and puts' },
  { n: 3, name: 'Level 3', short: 'Spreads', allows: 'Spreads: buy one strike, sell another' },
  { n: 4, name: 'Level 4', short: 'Naked options', allows: 'Naked calls and puts: selling without shares or cash behind them' },
];
const NAKED_MIN_EQ = 25000, L2_TRADES = 10;
// cash-settled index options (European style, never assigned early). The index level comes from SPY × 10.
const INDEX_OPT = { SPX: { base: 'SPY', mult: 10, name: 'S&P 500 index' } };
const undOf = sym => INDEX_OPT[sym]?.base || sym;
const spotOf = sym => INDEX_OPT[sym] ? (priceOf(INDEX_OPT[sym].base) ? priceOf(INDEX_OPT[sym].base) * INDEX_OPT[sym].mult : null) : priceOf(sym);
const isIndexOpt = sym => !!INDEX_OPT[sym];
// regulatory and clearing fees per contract (commission-free brokers still pass these on); sells add the FINRA fee
const optFee = (n, side) => Math.max(0.01, round(n * 0.04 + (side === 'sell' ? n * 0.00279 : 0), 2));
const IVC = new Map(); // volatility per symbol, so wallet values can be marked without waiting
async function ivFor(sym) { const v = await baseVol(undOf(sym)); IVC.set(sym, v); return v; }
const fmtExp = exp => { const [y, m, d] = exp.split('-'); return `${m}/${d}/${y.slice(2)}`; };
const optLabel = o => `${o.sym} ${fmtExp(o.exp)} $${+o.strike} ${o.kind === 'call' ? 'Call' : 'Put'}`;
const occCode = o => `${o.sym.padEnd(6, ' ')}${o.exp.slice(2).replace(/-/g, '')}${o.kind === 'call' ? 'C' : 'P'}${String(Math.round(+o.strike * 1000)).padStart(8, '0')}`;
// a quote for one contract: the model's middle, and a bid and ask around it
function optQuote2(sym, kind, K, exp) {
  const S0 = spotOf(sym); if (!S0) return null;
  const base = IVC.get(sym) || 0.3, iv = ivAt(base, S0, K), T = yearsTo(exp), g = bs(kind, S0, K, T, iv);
  const intr = Math.max(0, kind === 'call' ? S0 - K : K - S0);
  const mid = Math.max(T > 0 ? g.px : intr, intr + (isIndexOpt(sym) ? 0 : 0)); // American options never trade under what exercising pays
  const m = Math.abs(Math.log(K / S0)), adv = assets.get(undOf(sym))?.adv || 0, liq = isIndexOpt(sym) || adv > 2e7 ? 0.3 : adv > 5e6 ? 0.6 : 1; // busy underlyings have tight option markets
  const half = clamp((0.005 + 0.012 * Math.max(0, mid - intr) + 0.004 * mid * (1 + 4 * m)) * liq, 0.005, Math.max(0.03, mid * 0.2));
  let bid = Math.max(0, round(mid - half, 2)), ask = round(Math.max(mid + half, bid + 0.01), 2);
  if (mid < 0.015) { bid = 0; ask = Math.max(0.01, ask); }
  return { spot: S0, iv, T, mid, bid, ask, delta: g.delta, theta: g.theta || 0, intr, ext: Math.max(0, mid - intr), days: Math.ceil(T * 365.25) };
}

// ---------- the book: every open contract, kept in memory so wallet values stay instant ----------
const OPTBOOK = new Map(); // wid -> [rows]
const optRow = r => ({ id: r.id, wid: r.wid, sym: r.sym, kind: r.kind, strike: +r.strike, exp: r.exp, qty: +r.qty, avg: +r.avg, coll: r.coll || null, spread: r.spread || null, opened: tsMs(r.opened), why: r.why || null, bot: r.bot_id || null });
async function loadOptBook() {
  OPTBOOK.clear();
  for (const r of await rows('select * from optpos where closed is null order by id').catch(() => [])) { const o = optRow(r); (OPTBOOK.get(o.wid) || OPTBOOK.set(o.wid, []).get(o.wid)).push(o); }
  for (const s of new Set([...OPTBOOK.values()].flat().map(o => o.sym))) if (!IVC.has(s)) ivFor(s).catch(() => {});
}
const optOpen = wid => OPTBOOK.get(wid) || [];
// what the wallet's options are worth right now (short contracts count against it)
function optMark(wid) {
  let v = 0;
  for (const o of optOpen(wid)) { const q = optQuote2(o.sym, o.kind, o.strike, o.exp); v += o.qty * OPT_MULT * (q ? q.mid : o.avg); }
  return v;
}
// what each short contract ties up: shares for covered calls, cash for puts and spreads, a margin requirement for naked ones
function optReq(o, list = null) {
  if (o.qty >= 0) return { cash: 0, shares: 0 };
  const n = -o.qty, S0 = spotOf(o.sym) || o.strike;
  if (o.coll === 'covered') return { cash: 0, shares: n * OPT_MULT, sym: undOf(o.sym) };
  if (o.coll === 'cash') return { cash: o.strike * OPT_MULT * n, shares: 0 };
  if (o.coll === 'spread') {
    const leg = (list || optOpen(o.wid)).find(x => x.spread === o.spread && x.id !== o.id);
    if (!leg) return nakedReq(o, S0, n);
    const width = o.kind === 'call' ? Math.max(0, leg.strike - o.strike) : Math.max(0, o.strike - leg.strike);
    return { cash: width * OPT_MULT * Math.min(n, leg.qty), shares: 0 };
  }
  return nakedReq(o, S0, n);
}
// FINRA's rule for uncovered options: 20% of the stock (15% for broad indexes) minus how far out of the money it is, at least 10% of the strike, plus the option's value
function nakedReq(o, S0, n) {
  const q = optQuote2(o.sym, o.kind, o.strike, o.exp), mark = q ? q.mid : o.avg, pct = isIndexOpt(o.sym) ? 0.15 : 0.20;
  const otm = Math.max(0, o.kind === 'call' ? o.strike - S0 : S0 - o.strike);
  const per = Math.max(pct * S0 - otm, 0.10 * (o.kind === 'call' ? S0 : o.strike)) + mark;
  return { cash: per * OPT_MULT * n, shares: 0, naked: true };
}
function optHolds(wid) {
  const out = { cash: 0, shares: {} }, list = optOpen(wid);
  for (const o of list) { const r = optReq(o, list); out.cash += r.cash; if (r.shares) out.shares[r.sym] = (out.shares[r.sym] || 0) + r.shares; }
  return out;
}

// ---------- approval levels ----------
const quizPassed = async () => jsonOr(await kvGet('opt_quiz', '{}').catch(() => '{}'), {});
function optCap(w) {
  if (!w || !isReal(w) || w.broker) return 0;
  if (isIra(w)) return 1;
  return w.acct === 'margin' ? 4 : 2;
}
const optLevel = w => Math.min(+w.opt_level || 0, optCap(w));
function ageProblem() {
  const a = ageOn(); if (a == null) return 'Add your date of birth first (Settings → Your profile). Options need you to be 18 or older.';
  if (a < 18) return 'Options need you to be 18 or older.'; return null;
}
async function levelNeeds(w, n) {
  const quiz = await quizPassed(), out = [];
  const age = ageProblem(); out.push({ label: '18 or older', ok: !age, have: age ? (ageOn() == null ? 'No date of birth yet' : 'Under 18') : 'Yes' });
  if (!isReal(w)) out.push({ label: 'Wallet on broker rules', ok: false, have: 'Practice rules' });
  if (n >= 1) out.push({ label: 'Level 1 quiz', ok: !!quiz[1], have: quiz[1] ? 'Passed' : 'Not yet', quiz: 1 });
  if (n >= 2) {
    if (isIra(w)) out.push({ label: 'Not an IRA', ok: false, have: 'Mango keeps IRAs at level 1' });
    const t = +(await row("select count(*) n from trd where wid=$1 and side in ('buy','sell')", [w.id])).n;
    out.push({ label: `${L2_TRADES} stock trades in this wallet`, ok: t >= L2_TRADES, have: `${t} so far` });
    out.push({ label: 'Level 2 quiz', ok: !!quiz[2], have: quiz[2] ? 'Passed' : 'Not yet', quiz: 2 });
  }
  if (n >= 3) { out.push({ label: 'Margin account', ok: w.acct === 'margin', have: ACCT_LABEL(w.acct) }); out.push({ label: 'Level 3 quiz', ok: !!quiz[3], have: quiz[3] ? 'Passed' : 'Not yet', quiz: 3 }); }
  if (n >= 4) {
    const eq = (await accountInfo(w.id, { w })).equity;
    out.push({ label: `${money(NAKED_MIN_EQ)} or more in the wallet`, ok: eq >= NAKED_MIN_EQ, have: money(eq) });
    out.push({ label: 'Level 4 quiz', ok: !!quiz[4], have: quiz[4] ? 'Passed' : 'Not yet', quiz: 4 });
  }
  return out;
}
const ACCT_LABEL = a => ({ cash: 'Cash account', margin: 'Margin account', instant: 'Instant account', ira: 'Traditional IRA', roth: 'Roth IRA' }[a] || a);
// four questions per level; every answer has to be right (you can retry)
const OPT_QUIZ = {
  1: [
    ['You sell 1 covered call on your 100 KO shares at a $65 strike. KO closes at $70 on expiration day. What happens?', ['Your 100 shares are sold at $65 each, and you keep the premium', 'You keep the shares and the premium', 'You pay $500 and keep the shares', 'Nothing, it just expires'], 0],
    ['What does a cash-secured put tie up in your wallet?', ['Enough cash to buy 100 shares at the strike, for each contract', 'Only the premium', '100 shares of the stock', 'Nothing until it expires'], 0],
    ['How many shares does one stock option contract usually cover?', ['100', '1', '10', '1,000'], 0],
    ['You sell a call for $1.20. What is the premium you collect?', ['$120 (1.20 × 100), yours to keep whatever happens', '$1.20', '$12', '$1,200'], 0],
  ],
  2: [
    ['You buy a call for $2.00. What is the most you can lose?', ['$200, what you paid', '$2', 'Unlimited', 'The price of 100 shares'], 0],
    ['If the stock does not move, what happens to an option\'s value as expiration gets closer?', ['It shrinks a little every day (time decay)', 'It grows', 'It stays the same', 'It doubles on the last day'], 0],
    ['Your call ends $0.50 in the money and you don\'t close it. What happens at expiration?', ['It\'s exercised automatically: you buy 100 shares at the strike', 'It expires worthless', 'You get $0.50 in cash', 'The broker cancels it'], 0],
    ['A delta of 0.30 roughly means…', ['About a 30% chance of ending in the money, and the option moves about $0.30 per $1 in the stock', 'It costs 30% of the stock', 'You make 30% if the stock rises', 'It expires in 30 days'], 0],
  ],
  3: [
    ['You buy the $100 call and sell the $105 call (same expiration) for $2.00. What is the most you can lose?', ['$200', '$500', '$300', 'Unlimited'], 0],
    ['Same spread: what is the most you can make?', ['$300: the $5 width minus the $2 you paid, × 100', '$500', '$200', 'Unlimited'], 0],
    ['You sell the $50 put and buy the $45 put for a $1.50 credit. What does the broker hold?', ['$500, the width, of which $150 came from the credit, so $350 is yours at risk', '$5,000', '$150', 'Nothing'], 0],
    ['Your short leg gets assigned early. What still protects you?', ['The long leg: you can sell it or exercise it', 'Nothing', 'The premium', 'The broker reverses it'], 0],
  ],
  4: [
    ['You sell a call without owning the shares (naked). How much can you lose?', ['There is no limit: the stock can keep rising', 'The premium', 'The strike × 100', '$25,000'], 0],
    ['Why do brokers want a margin account and money behind naked options?', ['Losses can be far bigger than the premium, so the account needs a cushion', 'To pay the commission', 'It\'s a tax rule', 'They don\'t'], 0],
    ['The money held for a naked put changes when…', ['The stock moves: it\'s recalculated all the time', 'Never', 'Only at expiration', 'Only on Mondays'], 0],
    ['A stock you sold naked $50 calls on jumps to $80 overnight on a buyout. Roughly what do you owe per contract?', ['About $3,000 (the $30 above the strike × 100), minus the premium you got', 'The premium only', '$50', 'Nothing until expiration'], 0],
  ],
};
GET('/api/opt/quiz/:n', auth, async req => {
  const n = clamp(Math.round(num(req.params.n)), 1, 4), q = OPT_QUIZ[n];
  // shuffle each question's answers so the right one isn't always first; the client sends back the text it picked
  return { level: n, questions: q.map(([t, a]) => ({ q: t, a: a.map((x, i) => ({ x, r: Math.random() + i * 0 })).sort((p, q2) => p.r - q2.r).map(y => y.x) })) };
});
POST('/api/opt/quiz/:n', auth, async req => {
  const n = clamp(Math.round(num(req.params.n)), 1, 4), q = OPT_QUIZ[n], picks = Array.isArray(req.body?.answers) ? req.body.answers : [];
  const wrong = q.map(([t, a, k], i) => (picks[i] === a[k] ? null : i)).filter(x => x != null);
  if (wrong.length) return { ok: false, wrong, explain: wrong.map(i => ({ q: q[i][0], right: q[i][1][q[i][2]] })) };
  const done = await quizPassed(); done[n] = Date.now(); await kvSet('opt_quiz', JSON.stringify(done));
  return { ok: true };
});
POST('/api/opt/unlock', auth, async req => {
  const w = await walletRow(await activeWalletId(req)), n = clamp(Math.round(num(req.body?.level)), 0, 4);
  if (n <= optLevel(w)) { await Q('update wallets set opt_level=$1 where id=$2', [n, w.id]); await loadWalletMeta(); return { ok: true, level: n }; } // stepping down is always fine
  if (n > optCap(w)) fail(!isReal(w) ? 'Options need a wallet on Robinhood or Alpaca rules.' : w.broker ? 'Options on a wallet linked to your broker go through the broker.' : isIra(w) ? 'Mango keeps IRA wallets at level 1: covered calls and cash-secured puts.' : `Level ${n} needs a margin account. Change the account type in the wallet's settings.`);
  const need = await levelNeeds(w, n), miss = need.filter(x => !x.ok);
  if (miss.length) fail(`Not yet: ${miss.map(x => `${x.label.replace(/^./, c => c.toLowerCase())} (${x.have.toLowerCase()})`).join('; ')}.`);
  await Q('update wallets set opt_level=$1 where id=$2', [n, w.id]); await loadWalletMeta();
  return { ok: true, level: n };
});

// ---------- the chain ----------
function optExps(sym) {
  const out = expiries();
  if (isIndexOpt(sym)) return out; // SPX lists Fridays like stocks here (the real one also has daily expirations)
  return out;
}
GET('/api/opt/chain/:sym', auth, async req => {
  const sym = cleanSym(req.params.sym); if (!sym || isCrypto(sym)) fail('Options are for stocks, ETFs and the S&P 500 index (SPX)');
  const und = undOf(sym); if (!assets.has(und)) await trackAsset(und);
  const S0 = spotOf(sym); if (!S0) fail('No live price yet for ' + sym);
  await ivFor(sym);
  const exps = optExps(sym), exp = exps.includes(req.query.exp) ? req.query.exp : exps.find(e => yearsTo(e) * 365.25 >= 25) || exps[exps.length - 1];
  const st = isIndexOpt(sym) ? 25 : strikeStep(S0), mid = Math.round(S0 / st) * st, T = yearsTo(exp);
  const strikes = [];
  for (let k = -10; k <= 10; k++) {
    const K = round(mid + k * st, 2); if (K <= 0) continue;
    const c = optQuote2(sym, 'call', K, exp), p = optQuote2(sym, 'put', K, exp);
    strikes.push({ K, call: { bid: c.bid, ask: c.ask, mid: round(c.mid, 2), delta: round(c.delta, 2) }, put: { bid: p.bid, ask: p.ask, mid: round(p.mid, 2), delta: round(p.delta, 2) } });
  }
  const earn = isIndexOpt(sym) ? null : (await earningsFor(sym).catch(() => null))?.next || null;
  const div = isIndexOpt(sym) ? null : await nextExDiv(sym).catch(() => null);
  return { sym, und, index: isIndexOpt(sym), indexName: INDEX_OPT[sym]?.name || null, spot: S0, iv: round(IVC.get(sym), 3), exp, exps, days: Math.ceil(T * 365.25), strikes, step: st,
    earnings: earn && earn.date <= exp ? earn.date : null, exDiv: div && div.date <= exp ? div : null, settle: isIndexOpt(sym) ? 'cash' : 'shares', style: isIndexOpt(sym) ? 'European' : 'American' };
});
// the next ex-dividend date, estimated from the payment history (tests can pin one)
const OPT_TEST_DIV = new Map();
async function nextExDiv(sym) {
  if (OPT_TEST_DIV.has(sym)) return OPT_TEST_DIV.get(sym);
  const ins = (await Promise.race([assetInsights(sym), sleep(4000).then(() => null)]).catch(() => null)) || {}, divs = (ins.dividends || []).slice().sort((a, b) => a.date - b.date);
  const yr = divs.filter(d => d.date > Date.now() - 400 * 864e5); if (yr.length < 2) return null;
  const gaps = []; for (let k = 1; k < yr.length; k++) gaps.push(yr[k].date - yr[k - 1].date);
  const gap = gaps.reduce((t, x) => t + x, 0) / gaps.length, last = yr[yr.length - 1];
  let t = last.date + gap; while (t < Date.now() - 864e5) t += gap;
  return { date: etDate(t), amount: last.amount, estimated: true };
}

// ---------- pricing an order before it goes in ----------
// legs: [{ kind, strike, exp, side: 'buy'|'sell', qty, secure: 'cash'|'naked' }] — one leg, or two for a vertical spread
async function optPlan(w, sym, legsIn) {
  if (!Array.isArray(legsIn) || !legsIn.length || legsIn.length > 2) fail('Pick one option, or two for a spread');
  sym = cleanSym(sym); if (!sym || isCrypto(sym)) fail('Options are for stocks, ETFs and SPX');
  if (!isReal(w)) fail('Options need a wallet on Robinhood or Alpaca rules. Switch the rules in the wallet\'s settings.');
  if (w.broker) fail('Options on a wallet linked to your broker go through the broker.');
  if (w.archived) fail('That wallet is archived. Restore it first.');
  const age = ageProblem(); if (age) fail(age);
  const sess = sessionAt(profOf(w)); if (sess !== 'regular') fail('Options trade only in regular hours, 9:30 am to 4:00 pm Eastern.');
  const halt = haltOf(undOf(sym)); if (halt) fail(halt.why + ' Its options are halted too.');
  await ivFor(sym);
  const exps = optExps(sym), legs = legsIn.map(l => ({ kind: l.kind === 'put' ? 'put' : 'call', strike: round(num(l.strike), 2), exp: String(l.exp || ''), side: l.side === 'sell' ? 'sell' : 'buy', qty: clamp(Math.round(num(l.qty, 1)), 1, 500), secure: l.secure === 'naked' ? 'naked' : 'cash' }));
  for (const l of legs) { if (!exps.includes(l.exp)) fail('Pick an expiration from the list'); if (!(l.strike > 0)) fail('Pick a strike'); }
  const level = optLevel(w), pos = await positions(w.id), acct = await accountInfo(w.id, { w, pos }), open = optOpen(w.id);
  for (const l of legs) { const ex = open.find(o => o.sym === sym && o.kind === l.kind && o.strike === l.strike && o.exp === l.exp && (o.qty > 0) !== (l.side === 'buy')); if (ex) fail(`You ${ex.qty > 0 ? 'own' : 'are short'} the ${optLabel(ex)}. Close it from your positions instead.`); }
  let spread = null;
  if (legs.length === 2) {
    const [a, b] = legs;
    if (a.kind !== b.kind || a.exp !== b.exp || a.side === b.side || a.qty !== b.qty || a.strike === b.strike) fail('A spread here is two strikes of the same type and expiration: buy one, sell the other, same number of contracts.');
    const long = a.side === 'buy' ? a : b, short = a.side === 'buy' ? b : a;
    const credit = long.kind === 'call' ? short.strike < long.strike : short.strike > long.strike;
    spread = { long, short, width: Math.abs(long.strike - short.strike), credit };
  }
  // price each leg: buys at the ask, sells at the bid
  let debit = 0, credit = 0, fees = 0, needLevel = 0, hold = 0, holdShares = 0, notes = [];
  for (const l of legs) {
    const q = optQuote2(sym, l.kind, l.strike, l.exp); if (!q) fail('No live price yet');
    l.q = q; l.px = l.side === 'buy' ? q.ask : q.bid; l.fee = optFee(l.qty, l.side);
    if (l.side === 'sell' && !(l.px > 0)) fail(`The ${sym} $${l.strike} ${l.kind} has no bid (it's nearly worthless), so there's nothing to sell.`);
    if (l.side === 'buy') debit += l.px * OPT_MULT * l.qty; else credit += l.px * OPT_MULT * l.qty;
    fees += l.fee;
    if (l.side === 'buy') { if (!spread) needLevel = Math.max(needLevel, 2); continue; }
    if (spread) { l.coll = 'spread'; needLevel = Math.max(needLevel, 3); continue; }
    if (l.kind === 'call') {
      const und = undOf(sym), p = pos.find(x => x.sym === und), free = (p ? p.qty : 0) - (acct.holdsShares[und] || 0);
      if (!isIndexOpt(sym) && free >= l.qty * OPT_MULT - 1e-9) { l.coll = 'covered'; needLevel = Math.max(needLevel, 1); holdShares += l.qty * OPT_MULT; }
      else { l.coll = 'naked'; needLevel = Math.max(needLevel, 4); notes.push(isIndexOpt(sym) ? 'Index calls can\'t be covered with shares, so selling one alone is naked.' : `Covering ${l.qty} call${l.qty > 1 ? 's' : ''} needs ${l.qty * OPT_MULT} ${und} shares that aren't already tied up (you have ${+Math.max(0, free).toFixed(4)}). Without them it's a naked call.`); }
    } else if (l.secure === 'naked' && level >= 4) { l.coll = 'naked'; needLevel = Math.max(needLevel, 4); }
    else { l.coll = 'cash'; needLevel = Math.max(needLevel, 1); }
    const r = optReq({ wid: w.id, sym, kind: l.kind, strike: l.strike, exp: l.exp, qty: -l.qty, coll: l.coll });
    hold += r.cash;
  }
  if (spread && spread.credit) hold += spread.width * OPT_MULT * spread.short.qty;
  // best and worst case, and where it breaks even (per the whole order)
  const net = debit - credit; let maxLoss, maxGain, be = null;
  const one = legs[0], n = one.qty;
  if (spread) {
    const w0 = spread.width * OPT_MULT * n;
    if (net > 0) { maxLoss = net + fees; maxGain = w0 - net - fees; } else { maxGain = -net - fees; maxLoss = w0 + net + fees; }
    const perSh = net / (OPT_MULT * n); be = spread.long.kind === 'call' ? (net > 0 ? spread.long.strike + perSh : spread.short.strike - perSh) : (net > 0 ? spread.long.strike - perSh : spread.short.strike + perSh);
  } else if (one.side === 'buy') { maxLoss = debit + fees; maxGain = one.kind === 'call' ? Infinity : (one.strike - one.px) * OPT_MULT * n - fees; be = one.kind === 'call' ? one.strike + one.px : one.strike - one.px; }
  else if (one.kind === 'call') { maxGain = credit - fees; maxLoss = one.coll === 'covered' ? null : Infinity; be = one.strike + one.px; }
  else { maxGain = credit - fees; maxLoss = (one.strike - one.px) * OPT_MULT * n + fees; be = one.strike - one.px; }
  // money check: options can't be bought on margin, so only cash counts
  const need = Math.max(0, hold + debit + fees - credit), have = acct.bpCrypto;
  const problems = [];
  if (needLevel > level) problems.push(level === 0 ? `This wallet isn't approved for options yet. ${needLevel === 1 ? 'Level 1' : 'Level ' + needLevel} is needed.` : `This needs options level ${needLevel}: ${OPT_LEVELS[needLevel].allows.replace(/^./, c => c.toLowerCase())}. The wallet is at level ${level}.`);
  if (needLevel === 4 && acct.equity < NAKED_MIN_EQ) problems.push(`Naked options need at least ${money(NAKED_MIN_EQ)} in the wallet (it has ${money(acct.equity)}).`);
  if (need > have + 0.005) problems.push(`Not enough cash. This needs ${money(need)}${hold ? ` (${money(hold)} held as collateral)` : ''} and the wallet has ${money(have)} to use. Options can't be bought with margin.`);
  return { sym, und: undOf(sym), index: isIndexOpt(sym), spot: one.q.spot, legs: legs.map(l => ({ kind: l.kind, strike: l.strike, exp: l.exp, side: l.side, qty: l.qty, px: l.px, bid: l.q.bid, ask: l.q.ask, mid: round(l.q.mid, 2), delta: round(l.q.delta, 2), fee: l.fee, coll: l.coll || null, days: l.q.days, label: optLabel({ sym, ...l }) })),
    spread: spread ? { width: spread.width, credit: spread.credit } : null, debit, credit, net, fees, hold, holdShares, need, have, maxLoss: maxLoss === Infinity ? null : maxLoss, maxGain: maxGain === Infinity ? null : maxGain, unlimitedLoss: maxLoss === Infinity, unlimitedGain: maxGain === Infinity,
    be, level, needLevel, notes, problems, ok: !problems.length, _legs: legs };
}
POST('/api/opt/preview', auth, async req => { const w = await walletRow(await activeWalletId(req)), p = await optPlan(w, req.body?.sym, req.body?.legs); delete p._legs; return p; });
async function optOpenOrder(wid, sym, legsIn, o = {}) {
  const w = await walletRow(wid), plan = await optPlan(w, sym, legsIn);
  if (!plan.ok) fail(plan.problems.join(' '));
  sym = plan.sym;
  const prof = profOf(w), made = [];
  await tx(async c => {
    await c.query('select 1 from wallets where id=$1 for update', [wid]);
    let spreadId = null;
    for (const l of plan._legs) {
      const signed = l.side === 'buy' ? l.qty : -l.qty;
      // premium per share, with the fee folded in (paid adds to the cost, received is net of it)
      const avg = l.side === 'buy' ? (l.px * OPT_MULT * l.qty + l.fee) / (OPT_MULT * l.qty) : (l.px * OPT_MULT * l.qty - l.fee) / (OPT_MULT * l.qty);
      const same = !plan.spread ? (await c.query('select * from optpos where wid=$1 and sym=$2 and kind=$3 and strike=$4 and exp=$5 and closed is null and spread is null and sign(qty)=sign($6::int) and coalesce(coll,\'\')=coalesce($7,\'\') for update', [wid, sym, l.kind, l.strike, l.exp, signed, l.coll || null])).rows[0] : null;
      let id;
      if (same) { const nq = +same.qty + signed, na = (Math.abs(+same.qty) * +same.avg + l.qty * avg) / Math.abs(nq); await c.query('update optpos set qty=$1, avg=$2 where id=$3', [nq, na, same.id]); id = same.id; }
      else id = (await c.query('insert into optpos(wid,sym,kind,strike,exp,qty,avg,coll,spread,why,bot_id) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) returning id', [wid, sym, l.kind, l.strike, l.exp, signed, avg, l.coll || null, spreadId, o.why || 'manual', o.bot || null])).rows[0].id;
      if (plan.spread && !spreadId) { spreadId = id; await c.query('update optpos set spread=$1 where id=$1', [id]); }
      const cash = l.side === 'buy' ? -(l.px * OPT_MULT * l.qty + l.fee) : l.px * OPT_MULT * l.qty - l.fee;
      await c.query('update wallets set cash=cash+$1 where id=$2', [round(cash, 6), wid]);
      if (l.side === 'sell' && w.acct === 'cash') await c.query('insert into settle(wid,amount,settles) values($1,$2,$3)', [wid, round(cash, 6), new Date(settleTime(prof))]);
      const t = (await c.query('insert into opttrd(wid,pos_id,sym,kind,strike,exp,action,qty,price,fee,why,note,bid,ask) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) returning id', [wid, id, sym, l.kind, l.strike, l.exp, l.side === 'buy' ? 'bto' : 'sto', l.qty, l.px, l.fee, o.why || 'manual', o.note ? String(o.note).slice(0, 300) : null, l.q.bid, l.q.ask])).rows[0];
      made.push({ id, trade: t.id, label: optLabel({ sym, ...l }), side: l.side, qty: l.qty, px: l.px, fee: l.fee, coll: l.coll || null });
    }
  });
  await loadOptBook(); refreshUserSyms().catch(() => {}); broadcast({ t: 'evt', e: 'opt', wid });
  return { ok: true, legs: made, net: plan.net, fees: plan.fees, hold: plan.hold };
}
POST('/api/opt/order', auth, async req => { const wid = await activeWalletId(req); return optOpenOrder(wid, req.body?.sym, req.body?.legs, { note: req.body?.note }); });

// ---------- closing, exercising, and what happens at expiration ----------
// tax lots for options: closing a long is a sale, closing a short is the purchase that ends it (always short-term)
async function optRealize(c, o, n, closePx, fee, how) {
  const gross = closePx * OPT_MULT * n;
  const long = o.qty > 0, opened = new Date(o.opened || Date.now()), now = new Date();
  const proceeds = long ? gross - fee : o.avg * OPT_MULT * n, basis = long ? o.avg * OPT_MULT * n : gross + fee;
  const term = long && now - opened > 365 * 864e5 ? 'long' : 'short';
  await c.query('insert into realized(wid,sym,qty,proceeds,basis,acquired,sold,term,crypto) values($1,$2,$3,$4,$5,$6,$7,$8,false)', [o.wid, optLabel(o), n, round(proceeds, 6), round(basis, 6), opened, now, term]);
  return proceeds - basis;
}
// shrink or close one position inside a transaction; returns the profit
async function optReduce(c, o, n, closePx, fee, action, note = null) {
  const pnl = await optRealize(c, o, n, closePx, fee, action);
  const left = Math.abs(o.qty) - n;
  if (left <= 0) await c.query('update optpos set closed=now(), close_px=$1, how=$2, pnl=coalesce(pnl,0)+$3, qty=$4 where id=$5', [closePx, action, round(pnl, 6), o.qty, o.id]);
  else await c.query('update optpos set qty=$1, pnl=coalesce(pnl,0)+$2 where id=$3', [o.qty > 0 ? left : -left, round(pnl, 6), o.id]);
  const q = optQuote2(o.sym, o.kind, o.strike, o.exp);
  await c.query('insert into opttrd(wid,pos_id,sym,kind,strike,exp,action,qty,price,fee,pnl,note,bid,ask) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)', [o.wid, o.id, o.sym, o.kind, o.strike, o.exp, action, n, closePx, fee, round(pnl, 6), note, q?.bid ?? null, q?.ask ?? null]);
  return pnl;
}
async function optClose(wid, ids, o = {}) {
  const list = optOpen(wid).filter(x => ids.includes(x.id)); if (!list.length) fail('That position is already closed', 404);
  const w = await walletRow(wid);
  if (!o.force) {
    const sess = sessionAt(profOf(w)); if (sess !== 'regular') fail('Options trade only in regular hours, 9:30 am to 4:00 pm Eastern.');
    const halt = haltOf(undOf(list[0].sym)); if (halt) fail(halt.why + ' Its options are halted too.');
  }
  // closing the long half of a spread on its own would leave the short one naked
  for (const x of list) if (x.spread && x.qty > 0) { const sh = optOpen(wid).find(y => y.spread === x.spread && y.id !== x.id); if (sh && !ids.includes(sh.id) && optLevel(w) < 4) fail('Closing only the bought leg would leave the sold one naked. Close both legs together, or the sold leg first.'); }
  const out = [];
  await tx(async c => {
    await c.query('select 1 from wallets where id=$1 for update', [wid]);
    for (const x of list) {
      const q = optQuote2(x.sym, x.kind, x.strike, x.exp); if (!q) fail('No live price yet');
      const n = Math.abs(x.qty), side = x.qty > 0 ? 'sell' : 'buy', px = o.px != null ? o.px : side === 'sell' ? q.bid : q.ask, fee = px > 0 ? optFee(n, side) : 0;
      const cash = side === 'sell' ? px * OPT_MULT * n - fee : -(px * OPT_MULT * n + fee);
      await c.query('update wallets set cash=cash+$1 where id=$2', [round(cash, 6), wid]);
      if (side === 'sell' && w.acct === 'cash' && cash > 0) await c.query('insert into settle(wid,amount,settles) values($1,$2,$3)', [wid, round(cash, 6), new Date(settleTime(profOf(w)))]);
      const pnl = await optReduce(c, x, n, px, fee, o.action || (side === 'sell' ? 'stc' : 'btc'), o.note || null);
      out.push({ id: x.id, label: optLabel(x), side, px, fee, pnl });
    }
  });
  await loadOptBook(); broadcast({ t: 'evt', e: 'opt', wid });
  return out;
}
POST('/api/opt/close', auth, async req => { const wid = await activeWalletId(req), ids = (Array.isArray(req.body?.ids) ? req.body.ids : [req.body?.id]).map(Number).filter(Boolean); return { ok: true, closed: await optClose(wid, ids) }; });
// Turn one contract into shares (or cash for SPX). Used for exercise, assignment and expiration.
// The premium folds into the stock's cost or sale price, the way the IRS treats it.
async function optDeliver(x, S, how, o = {}) {
  const n = Math.abs(x.qty), w = await walletRow(x.wid), long = x.qty > 0, intr = Math.max(0, x.kind === 'call' ? S - x.strike : x.strike - S);
  const cashSettle = async (why) => {
    await tx(async c => {
      const amt = (long ? 1 : -1) * intr * OPT_MULT * n;
      await c.query('update wallets set cash=cash+$1 where id=$2', [round(amt, 6), x.wid]);
      await optReduce(c, x, n, intr, 0, how === 'expired' ? 'expired' : 'cash', why);
    });
    return { how: 'cash', amount: (long ? 1 : -1) * intr * OPT_MULT * n, why };
  };
  if (intr <= 0) { await tx(async c => { await optReduce(c, x, n, 0, 0, 'expired', null); }); return { how: 'expired', amount: 0 }; }
  if (isIndexOpt(x.sym)) return cashSettle(`${x.sym} options settle in cash: ${money(intr)} a share × 100.`);
  if (x.coll === 'spread' || (long && x.spread)) return cashSettle('Both legs of the spread were settled together, so the shares in and out net to cash.');
  const und = undOf(x.sym), shares = n * OPT_MULT, prem = x.avg; // per share, net of fees
  // stock side: who buys and who sells at the strike
  const buys = (long && x.kind === 'call') || (!long && x.kind === 'put');
  if (!buys) { // selling shares at the strike: long put exercised, or short call assigned
    const p = (await positions(x.wid)).find(y => y.sym === und);
    if (!p || p.qty < shares - 1e-9) return cashSettle(long ? `You didn't own ${shares} ${und} shares to deliver, so it was settled at what it's worth (${money(intr)} a share).` : `Mango doesn't hold short stock, so it bought ${shares} ${und} at ${pxStr(S)} and delivered them at the ${pxStr(x.strike)} strike right away.`);
  }
  // close the contract first so its collateral is free, then trade the shares at the strike
  await tx(async c => { await c.query('update optpos set closed=now(), close_px=$1, how=$2, pnl=0 where id=$3', [intr, how, x.id]); await c.query('insert into opttrd(wid,pos_id,sym,kind,strike,exp,action,qty,price,fee,pnl,note) values($1,$2,$3,$4,$5,$6,$7,$8,$9,0,null,$10)', [x.wid, x.id, x.sym, x.kind, x.strike, x.exp, how, n, x.strike, `${buys ? 'Bought' : 'Sold'} ${shares} ${und} at ${pxStr(x.strike)}`]); });
  await loadOptBook();
  let fill;
  try { fill = await execTrade(x.wid, und, buys ? 'buy' : 'sell', { qty: shares, px: x.strike, force: true, auto: true, optx: true, noImp: true, why: how === 'exercised' ? 'exercise' : 'assigned', otype: 'option', note: optLabel(x) }); }
  catch (e) { // couldn't take the shares (no money for them): undo and settle the value instead
    await Q("update optpos set closed=null, close_px=null, how=null where id=$1", [x.id]); await Q("delete from opttrd where pos_id=$1 and action=$2 and ts > now() - interval '1 minute'", [x.id, how]); await loadOptBook();
    return cashSettle(`There wasn't enough money to ${buys ? 'buy' : 'sell'} the shares (${e.message.replace(/\.$/, '')}), so it was settled at what it's worth.`);
  }
  // fold the premium into the stock: paid premium raises a purchase's cost or lowers a sale; received premium does the reverse
  const adj = prem * shares * (long ? 1 : -1); // + adds cost / removes proceeds
  await tx(async c => {
    if (buys) {
      await c.query('update lots set basis=basis+$1 where trd_id=$2', [adj / shares, fill.id]);
      const pr = (await c.query('select qty, avg from pos where wid=$1 and sym=$2', [x.wid, und])).rows[0];
      if (pr) await c.query('update pos set avg=avg+$1 where wid=$2 and sym=$3', [adj / +pr.qty, x.wid, und]);
    } else {
      const rs = (await c.query('select id, qty from realized where trd_id=$1', [fill.id])).rows, tot = rs.reduce((t, r) => t + +r.qty, 0) || 1;
      for (const r of rs) await c.query('update realized set proceeds=proceeds-$1 where id=$2', [adj * +r.qty / tot, r.id]);
      await c.query('update trd set pnl=coalesce(pnl,0)-$1 where id=$2', [adj, fill.id]);
    }
  });
  return { how: 'shares', side: buys ? 'buy' : 'sell', shares, price: x.strike, fill: fill.id };
}
POST('/api/opt/exercise', auth, async req => {
  const wid = await activeWalletId(req), x = optOpen(wid).find(o => o.id === +req.body?.id); if (!x) fail('That position is closed', 404);
  if (x.qty <= 0) fail('Only options you bought can be exercised.');
  if (isIndexOpt(x.sym)) fail('SPX options are European style: they can only be exercised at expiration, and they settle in cash.');
  const S = spotOf(x.sym), q = optQuote2(x.sym, x.kind, x.strike, x.exp);
  if (!(q.intr > 0)) fail('It\'s out of the money, so exercising would mean paying more than the market price.');
  if (sessionAt(profOf(await walletRow(wid))) !== 'regular') fail('Exercise requests go in during regular hours.');
  const r = await optDeliver(x, S, 'exercised');
  await notify('order', `Exercised ${optLabel(x)}`, r.how === 'shares' ? `You ${r.side === 'buy' ? 'bought' : 'sold'} ${r.shares} ${undOf(x.sym)} at ${pxStr(x.strike)}.${q.ext > 0.05 ? ` It still had ${money(q.ext * OPT_MULT * x.qty)} of time value that exercising gave up.` : ''}` : r.why, { wid, url: '/#/options' });
  broadcast({ t: 'evt', e: 'opt', wid });
  return { ok: true, ...r, gaveUp: q.ext * OPT_MULT * x.qty };
});
// the closing price on the expiration day
async function settlePx(sym, exp) {
  const und = undOf(sym), mult = INDEX_OPT[sym]?.mult || 1;
  if (exp < etDate()) { try { const b = await dailyBars(und), i = lastIndexOnOrBefore(b.d, exp); if (i >= 0 && b.d[i] === exp) return b.c[i] * mult; } catch (e) {} }
  return spotOf(sym);
}
async function optExpire(x, S) {
  const n = Math.abs(x.qty), intr = Math.max(0, x.kind === 'call' ? S - x.strike : x.strike - S), label = optLabel(x);
  if (intr < 0.01) {
    await optDeliver(x, S, 'expired');
    await notify('order', `${label} expired worthless`, x.qty > 0 ? `${x.sym} closed at ${pxStr(S)}. You paid ${money(x.avg * OPT_MULT * n)}.` : `${x.sym} closed at ${pxStr(S)}, so it wasn't assigned. You keep the ${money(x.avg * OPT_MULT * n)} premium.`, { wid: x.wid, url: '/#/options' });
    return 'expired';
  }
  // OCC exercises anything a cent or more in the money; short contracts in the money get assigned
  const r = await optDeliver(x, S, x.qty > 0 ? 'exercised' : 'assigned');
  const und = undOf(x.sym);
  await notify('order', `${label} ${x.qty > 0 ? 'was exercised' : 'was assigned'}`, r.how === 'shares' ? `${x.sym} closed at ${pxStr(S)}, in the money. You ${r.side === 'buy' ? 'bought' : 'sold'} ${r.shares} ${und} at ${pxStr(x.strike)}.` : `${x.sym} closed at ${pxStr(S)}. ${r.why} ${r.amount >= 0 ? 'You got' : 'It cost'} ${money(Math.abs(r.amount))}.`, { wid: x.wid, url: '/#/options' });
  return r.how;
}
// Each afternoon: pin-risk warnings, selling what can't be exercised, then expiration after the close and any early assignment.
async function optJob({ force = false, asOf = null } = {}) {
  if (!OPTBOOK.size) return;
  const today = asOf || etDate(), p = etParts(), mins = p.h * 60 + p.mi, after = force || mins >= 16 * 60 + 5 || today < etDate();
  for (const s of new Set([...OPTBOOK.values()].flat().map(o => o.sym))) await ivFor(s).catch(() => {});
  const all = [...OPTBOOK.values()].flat();
  // 1. on the last day: warn about pin risk, and sell anything in the money that the wallet can't afford to exercise
  if (!force && mins >= 14 * 60 && mins < 16 * 60) for (const x of all.filter(o => o.exp === today)) {
    const S = spotOf(x.sym); if (!S) continue;
    if (x.qty < 0 && Math.abs(S / x.strike - 1) < 0.01 && (await kvGet('pin:' + x.id)) !== today) {
      await kvSet('pin:' + x.id, today);
      await notify('order', `Pin risk · ${optLabel(x)}`, `${x.sym} is at ${pxStr(S)}, right by the strike, on expiration day. It might or might not be assigned, and you won't know until tomorrow. Buying it back now removes the doubt.`, { wid: x.wid, url: '/#/options' });
    }
    if (x.qty > 0 && mins >= 15 * 60 + 30 && !isIndexOpt(x.sym)) {
      const q = optQuote2(x.sym, x.kind, x.strike, x.exp); if (!(q?.intr >= 0.01) || x.spread) continue;
      const n = x.qty, acct = await accountInfo(x.wid), und = undOf(x.sym), have = (await positions(x.wid)).find(y => y.sym === und)?.qty || 0;
      const can = x.kind === 'call' ? acct.bp >= x.strike * OPT_MULT * n : have >= n * OPT_MULT;
      if (can) continue;
      try { const r = await optClose(x.wid, [x.id], { note: 'Sold before expiration: not enough to exercise' }); await notify('order', `Sold ${optLabel(x)} before it expired`, `It's in the money, but the wallet ${x.kind === 'call' ? `doesn't have ${money(x.strike * OPT_MULT * n)} to buy the shares` : `doesn't own the ${n * OPT_MULT} shares to sell`}, so Mango sold it at ${money(r[0].px)} a share like a broker would.`, { wid: x.wid, url: '/#/options' }); } catch (e) { warn('opt pre-expiry', e.message); }
    }
  }
  if (!after) return;
  // 2. expiration: everything that expired today (after the close) or earlier
  for (const x of [...OPTBOOK.values()].flat().filter(o => o.exp < today || (o.exp === today && after))) {
    const S = await settlePx(x.sym, x.exp); if (!S) continue;
    try { await optExpire(x, S); } catch (e) { warn('opt expire', x.id, e.message); }
    await loadOptBook();
  }
  // 3. early assignment (American options only): short calls the day before an ex-dividend date when the dividend is worth more than the time value left, and deep short puts with no time value
  if ((await kvGet('opt_early')) === today && !force) return;
  await kvSet('opt_early', today);
  const [ny, nm, nd] = nextTradingDay(p.y, p.m, p.d), tomorrow = ymd([ny, nm, nd]);
  for (const x of [...OPTBOOK.values()].flat().filter(o => o.qty < 0 && !isIndexOpt(o.sym) && o.exp > today)) {
    const q = optQuote2(x.sym, x.kind, x.strike, x.exp); if (!q || q.intr < 0.01) continue;
    let why = null;
    if (x.kind === 'call') { const d = await nextExDiv(x.sym).catch(() => null); if (d && d.date === tomorrow && d.amount > q.ext) why = `${x.sym} goes ex-dividend tomorrow (${money(d.amount)} a share), and that's worth more than the ${money(q.ext)} of time value left, so the owner exercised early to collect it.`; }
    else if (q.ext < 0.02 && q.intr > x.strike * 0.05) why = `It's deep in the money with almost no time value left, so the owner exercised early.`;
    if (!why) continue;
    const label = optLabel(x), r = await optDeliver(x, q.spot, 'assigned').catch(e => { warn('early assign', e.message); return null; }); if (!r) continue;
    await notify('order', `Assigned early · ${label}`, `${why} ${r.how === 'shares' ? `You ${r.side === 'buy' ? 'bought' : 'sold'} ${r.shares} ${undOf(x.sym)} at ${pxStr(x.strike)}.` : r.why}`, { wid: x.wid, url: '/#/options' });
    await loadOptBook();
  }
}

// ---------- the options screen ----------
function optPosJson(o, list) {
  const q = optQuote2(o.sym, o.kind, o.strike, o.exp), n = Math.abs(o.qty), mark = q ? q.mid : o.avg, val = o.qty * OPT_MULT * mark;
  const cost = o.qty * OPT_MULT * o.avg, req = optReq(o, list), today = etDate(), S = q?.spot;
  return { id: o.id, sym: o.sym, und: undOf(o.sym), kind: o.kind, strike: o.strike, exp: o.exp, qty: o.qty, avg: o.avg, label: optLabel(o), occ: occCode(o), coll: o.coll, spread: o.spread,
    bid: q?.bid ?? null, ask: q?.ask ?? null, mark: round(mark, 4), value: val, pl: val - cost, plPct: cost ? (val - cost) / Math.abs(cost) : 0, delta: q ? round(q.delta * o.qty * OPT_MULT, 1) : null, theta: q ? round(q.theta * o.qty * OPT_MULT, 2) : null,
    days: q ? q.days : 0, spot: S, itm: q ? q.intr > 0 : false, intr: q ? q.intr : 0, ext: q ? q.ext : 0, hold: req.cash, holdShares: req.shares, naked: !!req.naked,
    pin: o.exp === today && o.qty < 0 && S ? Math.abs(S / o.strike - 1) < 0.01 : false, index: isIndexOpt(o.sym), opened: o.opened, bot: o.bot };
}
GET('/api/opt/state', auth, async req => {
  const w = await walletRow(await activeWalletId(req)), list = optOpen(w.id);
  for (const s of new Set(list.map(o => o.sym))) await ivFor(s).catch(() => {});
  const lvl = optLevel(w), next = Math.min(optCap(w), lvl + 1), quiz = await quizPassed();
  const acct = isReal(w) && !w.broker ? await accountInfo(w.id, { w }) : null;
  const hist = await rows('select * from opttrd where wid=$1 order by id desc limit 40', [w.id]);
  return {
    wallet: { id: w.id, name: w.name, emoji: w.emoji, acct: w.acct, real: isReal(w), broker: !!w.broker }, level: lvl, cap: optCap(w), levels: OPT_LEVELS, quiz,
    next: next > lvl ? { n: next, needs: await levelNeeds(w, next) } : null, age: ageProblem(),
    open: list.map(o => optPosJson(o, list)), value: optMark(w.id), holds: optHolds(w.id), bp: acct ? acct.bpCrypto : 0, equity: acct ? acct.equity : null,
    history: hist.map(t => ({ id: t.id, ts: tsMs(t.ts), label: optLabel(t), action: t.action, qty: +t.qty, price: +t.price, fee: +t.fee || 0, pnl: t.pnl == null ? null : +t.pnl, note: t.note, why: t.why })),
    nakedMin: NAKED_MIN_EQ, l2Trades: L2_TRADES, indexes: Object.keys(INDEX_OPT),
  };
});
// every wallet's options in one list (for the home screen)
const optSummary = wid => { const l = optOpen(wid); return l.length ? { n: l.length, value: optMark(wid) } : null; };

// ---------- one time: the old options sandbox becomes a real wallet ----------
async function migrateOptSandbox() {
  if (await kvGet('opt_migrated')) return;
  const old = await rows('select * from opt_pos order by id').catch(() => []), cash0 = await kvGet('opt_cash').catch(() => null);
  if (old.length || cash0 != null) {
    const cash = num(cash0, OPT_START);
    await tx(async c => {
      const w = (await c.query(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,opt_level) values('Options practice','🎯','sky',$1,$2,(select coalesce(max(sort),0)+1 from wallets),'robinhood','margin',2) returning *`, [cash, OPT_START])).rows[0];
      for (const o of old) {
        if (o.closed) { await c.query("insert into opttrd(wid,ts,sym,kind,strike,exp,action,qty,price,fee,pnl,note) values($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10,'From the old options sandbox')", [w.id, o.closed, o.sym, o.kind, o.strike, o.exp, o.how === 'sold' ? 'stc' : o.how === 'exercised' ? 'cash' : 'expired', o.qty, +o.close_px || 0, (+o.close_px - +o.prem) * 100 * o.qty]); continue; }
        await c.query("insert into optpos(wid,sym,kind,strike,exp,qty,avg,opened,why) values($1,$2,$3,$4,$5,$6,$7,$8,'sandbox')", [w.id, o.sym, o.kind, o.strike, o.exp, o.qty, +o.prem, o.opened]);
      }
    });
    log(`Moved the options sandbox (${old.filter(o => !o.closed).length} open) into a new "Options practice" wallet`);
  }
  await kvSet('opt_migrated', '1');
}
if (ENV.MANGO_FAST_JOBS) {
  POST('/api/test/opt-job', auth, async req => { await optJob({ force: true, asOf: req.body?.asOf || null }); return { ok: true }; });
  POST('/api/test/opt-div', auth, async req => { const p = etParts(), t = ymd(nextTradingDay(p.y, p.m, p.d)); OPT_TEST_DIV.set(req.body.sym, { date: req.body.date || t, amount: num(req.body.amount) }); return { tomorrow: t }; });
}

// ---------- the covered-call bot's options step (runs after it buys or holds its lots) ----------
async function covcallRun(bot, w, log, dry) {
  const p = bot.params, lvl = optLevel(w), today = etDate();
  if (lvl < 1) { log.skips.push(ageProblem() || 'Covered calls need options level 1 on this wallet: open Options, pass the level 1 quiz and approve the wallet.'); return; }
  if (sessionAt(profOf(w)) !== 'regular') { log.skips.push('Calls are sold in regular hours; the shares are handled, the calls wait for the next session.'); return; }
  for (const s of bot.syms) await ivFor(s).catch(() => {});
  // 1. manage the calls it already sold: take profit early, roll or let go near expiration, drop ones on stocks it no longer trades
  for (const o of optOpen(bot.wid).filter(x => x.kind === 'call' && x.qty < 0 && x.coll === 'covered')) {
    const q = optQuote2(o.sym, o.kind, o.strike, o.exp); if (!q) continue;
    const earned = o.avg > 0 ? 1 - q.ask / o.avg : 0, label = optLabel(o);
    let why = null;
    if (!bot.syms.includes(o.sym)) why = `${o.sym} isn't in the bot's list any more`;
    else if (p.takePct > 0 && earned >= p.takePct / 100) why = `it kept ${Math.round(earned * 100)}% of the premium already`;
    else if (p.atExp === 'roll' && q.days <= 5 && q.intr > 0) why = `it's in the money with ${q.days} day${q.days === 1 ? '' : 's'} left, so it rolls to a later month instead of giving up the shares`;
    if (!why) { if (q.days <= 5 && q.intr > 0) log.skips.push(`${label} is in the money: the shares will likely be called away at ${pxStr(o.strike)}.`); continue; }
    if (dry) { log.skips.push(`Would buy back ${label}: ${why}.`); continue; }
    try { const r = await optClose(bot.wid, [o.id], { note: 'Bot: ' + why }); log.orders.push({ sym: o.sym, side: 'opt', short: `bought back ${o.sym} call`, text: `Bought back ${label} at ${money(r[0].px)} a share`, label: `because ${why} (${money(r[0].pnl, { sign: true })})` }); }
    catch (e) { log.skips.push(`Couldn't buy back ${label}: ${e.message}`); }
  }
  // 2. sell a call on every free lot of 100
  const pos = await positions(bot.wid), acct = await accountInfo(bot.wid, { w, pos }), exps = expiries();
  const mid = (p.dteMin + p.dteMax) / 2, dte = e => yearsTo(e) * 365.25;
  const inWin = exps.filter(e => dte(e) >= p.dteMin && dte(e) <= p.dteMax), exp = (inWin.length ? inWin : exps.filter(e => dte(e) >= p.dteMin)).sort((a, b) => Math.abs(dte(a) - mid) - Math.abs(dte(b) - mid))[0];
  for (const s of bot.syms) {
    const ps = pos.find(x => x.sym === s); if (!ps) continue;
    const n = Math.floor((ps.qty - (acct.holdsShares[s] || 0)) / 100 + 1e-9);
    if (n < 1) { if (ps.qty < 100) log.skips.push(`${s}: ${+ps.qty.toFixed(2)} shares; a covered call needs 100.`); continue; }
    if (!exp) { log.skips.push('No expiration in range.'); break; }
    if (p.skipEarn === 'on') { const e = (await earningsFor(s).catch(() => null))?.next; if (e && e.date >= today && e.date <= exp) { log.skips.push(`${s}: earnings on ${e.date}, before the ${exp} expiration, so no call this time.`); continue; } }
    if (p.skipDiv === 'on') { const d = await nextExDiv(s).catch(() => null); if (d && d.date > today && d.date <= exp) { log.skips.push(`${s}: ex-dividend about ${d.date}, before expiration. A call in the money then could be assigned early to take the dividend, so it waits.`); continue; } }
    const S0 = spotOf(s), st = strikeStep(S0), want = p.delta / 100;
    let best = null;
    for (let K = Math.ceil(S0 / st) * st, k = 0; k < 25; k++, K = round(K + st, 2)) { const q = optQuote2(s, 'call', K, exp); if (!q || q.bid < 0.05) break; const d = Math.abs(q.delta - want); if (!best || d < best.d) best = { K, d, q }; }
    if (!best) { log.skips.push(`${s}: no out-of-the-money call worth selling.`); continue; }
    const label = optLabel({ sym: s, kind: 'call', strike: best.K, exp });
    if (dry) { log.skips.push(`Would sell ${n} ${label} for about ${money(best.q.bid * 100 * n)}.`); continue; }
    try { await optOpenOrder(bot.wid, s, [{ kind: 'call', strike: best.K, exp, side: 'sell', qty: n }], { why: 'bot', bot: bot.id }); log.orders.push({ sym: s, side: 'opt', short: `sold ${n} ${s} call${n > 1 ? 's' : ''}`, text: `Sold ${n} ${label} for ${money(best.q.bid * 100 * n)}`, label: `delta ${best.q.delta.toFixed(2)}, ${Math.round(dte(exp))} days` }); }
    catch (e) { log.skips.push(`Couldn't sell the ${s} call: ${e.message}`); }
  }
}
// =====================================================================
// Bot lab 2: sector rotation by the economic cycle, a filings bot, crossbred bots,
// walk-forward testing, Monte Carlo, strategy decay alerts and "bot vs you"
// =====================================================================

// ---------- sector rotation by the economic cycle ----------
// The classic business-cycle map (which sectors tend to lead in each phase), with the phase read from the market itself:
// the S&P 500's long trend, consumer discretionary vs staples (spending appetite) and long Treasuries (where rates are heading).
const CYCLE = {
  early: { name: 'Early recovery', sectors: ['XLF', 'XLY', 'XLI', 'XLRE', 'XLB', 'XLK'], why: 'Stocks are turning up from a slump and rates are falling: banks, consumer spending, industrials and real estate usually lead.' },
  mid: { name: 'Mid-cycle expansion', sectors: ['XLK', 'XLC', 'XLI', 'XLB', 'XLY'], why: 'A steady uptrend with people still spending: technology, communication and industrials usually lead.' },
  late: { name: 'Late cycle', sectors: ['XLE', 'XLB', 'XLV', 'XLP', 'XLI'], why: 'Still in an uptrend, but spending appetite is fading and prices are hot: energy, materials and health care usually hold up best.' },
  slow: { name: 'Slowdown', sectors: ['XLP', 'XLU', 'XLV'], why: 'Below the long-term trend with spending appetite falling: staples, utilities and health care, which people buy anyway, usually fall least.' },
};
const CYCLE_SYMS = ['SPY', 'XLK', 'XLF', 'XLE', 'XLV', 'XLY', 'XLP', 'XLI', 'XLU', 'XLB', 'XLC', 'XLRE', 'TLT'];
function cyclePhase(ctx) {
  const c = s => ctx.idx[s] != null ? ctx.data[s]?.c : null, i = s => ctx.idx[s];
  if (!c('SPY') || !c('XLY') || !c('XLP')) return null;
  const spy = c('SPY'), si = i('SPY'), sma = IND.sma(spy, 200, si); if (sma == null) return null;
  const up = spy[si] > sma, y1 = IND.ret(spy, 252, si);
  const ratio = k => c('XLY')[i('XLY') - k] / c('XLP')[i('XLP') - k];
  const appetite = i('XLY') >= 63 && i('XLP') >= 63 ? ratio(0) / ratio(63) - 1 : 0;
  const rates = c('TLT') ? IND.ret(c('TLT'), 63, i('TLT')) : null; // TLT up = long rates falling
  let k;
  if (!up) k = appetite > 0 ? 'early' : 'slow';
  else if (y1 != null && y1 < 0.05 && rates != null && rates > 0) k = 'early';
  else k = appetite >= 0 ? 'mid' : 'late';
  return { k, up, y1, appetite, rates, sma };
}
STRATEGIES.cycle = {
  name: 'Economic cycle sectors', short: 'Cycle', icon: 'radar', rebalance: 'monthly', markets: 'stocks',
  overlays: { council: 'advise', mood: false, corr: false, exits: false, tighten: false, news: 'severe', review: false, stops: false },
  desc: 'Reads where the economy is in its cycle from the market itself: whether the S&P 500 is above its 200-day average, whether people are buying more luxuries than necessities (consumer discretionary vs staples), and whether long-term rates are falling. Then it holds the strongest of the sectors that usually lead in that phase. Once a month.',
  params: [{ k: 'top', label: 'Sectors to hold', def: 3, min: 1, max: 5 }, FUND_P, SAFE_P],
  grid: { top: [2, 3, 4] }, need: () => 253,
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, ph = cyclePhase(ctx);
    const park = p.safe && p.safe !== 'cash' && ctx.idx[p.safe] != null ? p.safe : null;
    if (!ph) { out.regime = 'Needs SPY, XLY (consumer discretionary), XLP (staples) and a year of history to read the cycle.'; if (park) out.weights[park] = 1; return out; }
    const C = CYCLE[ph.k], mom = s => { const c = ctx.data[s]?.c, i = ctx.idx[s]; return c && i != null ? IND.ret(c, 63, i) : null; };
    const ranked = C.sectors.filter(s => ctx.syms.includes(s) && mom(s) != null).sort((a, b) => mom(b) - mom(a));
    // in a slowdown it still holds the defensive sectors; otherwise only sectors that are actually rising
    const pick = ranked.filter(s => ph.k === 'slow' || mom(s) > 0).slice(0, p.top), cap = p.fullFund !== 'off' ? 1 : ctx.maxW;
    const each = pick.length ? Math.min(cap, 1 / pick.length) : 0;
    for (const s of pick) { out.weights[s] = each; out.notes[s] = `${C.name}: ${s} is up ${(mom(s) * 100).toFixed(1)}% over 3 months.`; }
    const used = pick.length * each; if (park && used < 0.99) out.weights[park] = 1 - used;
    out.regime = `${C.name}. ${C.why} (S&P 500 ${ph.up ? 'above' : 'below'} its 200-day average; discretionary vs staples ${ph.appetite >= 0 ? 'up' : 'down'} ${(Math.abs(ph.appetite) * 100).toFixed(1)}% in 3 months${ph.rates != null ? `; long Treasuries ${ph.rates >= 0 ? 'up' : 'down'} ${(Math.abs(ph.rates) * 100).toFixed(1)}%` : ''}.)`;
    out.phase = ph.k;
    return out;
  },
};
UNIVERSES.cycle = { name: 'Sector ETFs for the cycle', syms: CYCLE_SYMS };

// ---------- the filings bot: acts only on official SEC filings ----------
STRATEGIES.filings = {
  name: 'Filings follower', short: 'Filings', icon: 'note', rebalance: 'none', markets: 'stocks', live: true,
  overlays: { council: 'advise', mood: false, corr: false, exits: false, tighten: false, news: 'severe', review: false, stops: true },
  desc: 'Buys companies where several insiders just bought shares with their own money on the open market (Form 4 filings), or where a famous investor opened a new position in their latest 13F. It never acts on headlines, only filings. Each position is held about 3 months with a stop loss, and idle money waits in T-bills. Live only: the backtester has no filings history, so a test shows it waiting.',
  params: [{ k: 'minBuyers', label: 'Insiders buying in 30 days, at least', def: 2, min: 1, max: 6 }, { k: 'minValue', label: 'Their buys add up to at least ($)', def: 100000, min: 0, max: 1e8 },
    { k: 'funds', label: 'Famous investors\' new positions', def: 'on', choices: [['on', 'Count them'], ['off', 'Ignore']] }, { k: 'holdDays', label: 'Hold for (trading days)', def: 60, min: 10, max: 250 },
    { k: 'maxPositions', label: 'Most positions', def: 5, min: 1, max: 12 }, { k: 'stopPct', label: 'Stop loss (%)', def: 12, min: 3, max: 40 }, SAFE_P],
  grid: {}, need: () => 21,
  async prep(bot) {
    const p = bot.params, since = etDate(Date.now() - 30 * 864e5), signals = {};
    for (const s of bot.syms.filter(x => x !== p.safe && !isCrypto(x))) {
      let score = 0; const why = [];
      const ins = await insiders(s).catch(() => null);
      if (ins) {
        const buys = ins.list.filter(x => x.code === 'P' && x.date >= since), people = new Set(buys.map(x => x.name)), val = buys.reduce((t, x) => t + x.value, 0);
        if (people.size >= p.minBuyers && val >= p.minValue) { score += people.size + Math.log10(Math.max(10, val)) / 3; why.push(`${people.size} insider${people.size > 1 ? 's' : ''} bought ${val >= 1e6 ? '$' + (val / 1e6).toFixed(1) + 'M' : '$' + Math.round(val / 1e3) + 'K'} on the market (Form 4)`); }
      }
      if (p.funds === 'on') for (const f of FUND13F) { const d = fundCached(f); if (!d || d.filed < etDate(Date.now() - 75 * 864e5)) continue; const hh = d.holdings.find(x => x.sym === s && x.change === 'new'); if (hh) { score += 2; why.push(`${f.who} opened a new position (13F filed ${d.filed})`); } }
      if (score > 0) signals[s] = { score, why: why.join('; ') };
    }
    return { signals, at: Date.now() };
  },
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, sig = ctx.extra?.signals;
    const park = p.safe && p.safe !== 'cash' && ctx.idx[p.safe] != null ? p.safe : null;
    if (!sig) { out.regime = 'It acts on live SEC filings, which the backtester doesn\'t have, so a test shows it waiting.'; if (park) out.weights[park] = 1; return out; }
    const keep = Object.entries(ctx.held).filter(([s, h]) => s !== park && ctx.syms.includes(s) && ((h.days || 0) < p.holdDays || sig[s])).sort((a, b) => (sig[b[0]]?.score || 0) - (sig[a[0]]?.score || 0)).map(([s]) => s);
    const fresh = Object.entries(sig).filter(([s]) => !ctx.held[s] && ctx.syms.includes(s) && ctx.idx[s] != null).sort((a, b) => b[1].score - a[1].score).map(([s]) => s);
    const pick = [...keep, ...fresh].slice(0, p.maxPositions), each = Math.min(ctx.maxW, 1 / p.maxPositions);
    for (const s of pick) {
      out.weights[s] = each; const h = ctx.held[s], px = ctx.data[s].c[ctx.idx[s]];
      out.stops[s] = { price: (h ? h.entry : px) * (1 - p.stopPct / 100) };
      out.notes[s] = sig[s] ? sig[s].why : `Holding: day ${h?.days || 0} of ${p.holdDays}.`;
    }
    const used = pick.length * each; if (park && used < 0.99) out.weights[park] = 1 - used;
    out.regime = Object.keys(sig).length ? `${Object.keys(sig).length} fresh filing signal${Object.keys(sig).length > 1 ? 's' : ''}: ${Object.keys(sig).slice(0, 4).join(', ')}.` : 'No fresh filing signals: waiting.';
    return out;
  },
};

// ---------- crossbreeding: two parents, one child ----------
// Same strategy: the child mixes their settings. Different strategies: the child runs both, each on its own schedule, and splits the money.
const HYB_MEMO = new Map();
function dueOn(reb, ctx) {
  if (reb !== 'monthly' && reb !== 'weekly') return true;
  const s = ctx.syms.find(x => ctx.idx[x] != null && ctx.idx[x] > 0); if (!s) return true;
  const d = ctx.data[s].d, i = ctx.idx[s];
  return reb === 'monthly' ? d[i].slice(0, 7) !== d[i - 1].slice(0, 7) : new Date(d[i] + 'T12:00:00Z').getUTCDay() < new Date(d[i - 1] + 'T12:00:00Z').getUTCDay() || daysBetween(d[i - 1], d[i]) > 3;
}
const todayOf = ctx => { const s = ctx.syms.find(x => ctx.idx[x] != null); return s ? ctx.data[s].d[ctx.idx[s]] : null; };
STRATEGIES.hybrid = {
  name: 'Crossbreed', short: 'Crossbreed', icon: 'flask', rebalance: 'none', markets: 'both', hidden: true,
  overlays: { council: 'advise', mood: false, corr: false, exits: false, tighten: false, news: 'severe', review: false, stops: true },
  desc: 'A child of two strategies. Each parent keeps its own settings, symbols and schedule, and the money is split between them.',
  params: [{ k: 'mix', label: 'Share for the first parent (%)', def: 50, min: 10, max: 90 }],
  grid: {}, need: p => Math.max(STRATEGIES[p?.a]?.need(p?.pa || {}) || 64, STRATEGIES[p?.b]?.need(p?.pb || {}) || 64),
  decide(ctx) {
    const p = ctx.params, out = { weights: {}, stops: {}, notes: {} }, today = todayOf(ctx), regs = [];
    for (const [side, k, pp, syms, share] of [['A', p.a, p.pa, p.sa, p.mix / 100], ['B', p.b, p.pb, p.sb, 1 - p.mix / 100]]) {
      const st = STRATEGIES[k]; if (!st || st.auto || k === 'hybrid') continue;
      const sub = { ...ctx, params: pp || {}, syms: ctx.syms.filter(s => (syms || []).includes(s)), maxW: Math.min(1, ctx.maxW / share) };
      const key = JSON.stringify([k, pp, syms]), m = HYB_MEMO.get(key);
      let dec;
      if (m && !dueOn(st.rebalance, sub) && m.date < today && daysBetween(m.date, today) < 40) dec = m.dec;
      else { try { dec = st.decide(sub); } catch (e) { dec = { weights: {}, stops: {}, notes: {} }; } HYB_MEMO.set(key, { date: today, dec }); if (HYB_MEMO.size > 400) HYB_MEMO.delete(HYB_MEMO.keys().next().value); }
      for (const [s, wt] of Object.entries(dec.weights || {})) if (wt > 0) { out.weights[s] = (out.weights[s] || 0) + wt * share; out.notes[s] = `${st.short || st.name}: ${dec.notes?.[s] || 'holds it'}`; }
      for (const [s, sp] of Object.entries(dec.stops || {})) if (sp && (!out.stops[s] || sp.price > out.stops[s].price)) out.stops[s] = sp;
      if (dec.regime) regs.push(`${st.short || st.name}: ${dec.regime}`);
    }
    out.regime = regs.join(' ');
    return out;
  },
};
// mixing settings: numbers between the parents' values, choices from one or the other, plus a small mutation
function mixParams(key, pa, pb, R = Math.random) {
  const st = STRATEGIES[key], out = {};
  for (const d of st.params) {
    const a = pa[d.k] ?? d.def, b = pb[d.k] ?? d.def;
    if (d.choices) out[d.k] = R() < 0.5 ? a : b;
    else { const t = R(), v = a + (b - a) * t, jitter = (d.max - d.min) * 0.04 * (R() - 0.5), step = Number.isInteger(d.def) && Number.isInteger(d.min) ? 1 : 0.01; out[d.k] = clamp(Math.round((v + jitter) / step) * step, d.min, d.max); }
  }
  return out;
}
async function parentOf(x) {
  if (x?.bot) { const b = await row('select * from bots where id=$1', [+x.bot]); if (!b) fail('That bot is gone', 404); const bj = botJson(b); return { key: bj.strategy, params: bj.params, syms: bj.syms, name: bj.name, bot: bj.id }; }
  const key = x?.strategy; if (!STRATEGIES[key]) fail('Pick a strategy for each parent');
  return { key, params: cleanParams(key, x.params || {}), syms: (x.syms || []).map(cleanSym).filter(Boolean), name: STRATEGIES[key].name };
}
POST('/api/lab/breed', auth, async req => {
  botsOn();
  const A = await parentOf(req.body?.a), B = await parentOf(req.body?.b), years = clamp(num(req.body?.years, 3), 1, 8), capital = clamp(num(req.body?.capital, 10000), 100, 1e8);
  for (const x of [A, B]) if (STRATEGIES[x.key].auto || STRATEGIES[x.key].live || x.key === 'hybrid') fail(`${STRATEGIES[x.key].name} can't be crossbred (it can't be backtested the same way).`);
  const test = async (key, params, syms) => { const r = await backtest({ strategy: key, params, syms, years, capital, robust: false, maxPosPct: 100 }); return { ret: r.stats?.ret, cagr: r.stats?.cagr, maxDD: r.stats?.maxDD, sharpe: r.stats?.sharpe, trades: r.trades, curve: downsamplePts(r.curve, 90), benchRet: r.bench?.ret, start: r.start, end: r.end }; };
  const [ra, rb] = [await test(A.key, A.params, A.syms), await test(B.key, B.params, B.syms)];
  const kids = [];
  if (A.key === B.key) {
    const syms = [...new Set([...A.syms, ...B.syms])].slice(0, 30);
    for (let n = 0; n < 5; n++) { const params = mixParams(A.key, A.params, B.params); try { kids.push({ strategy: A.key, params, syms, ...(await test(A.key, params, syms)) }); } catch (e) {} }
  } else {
    const syms = [...new Set([...A.syms, ...B.syms])].slice(0, 40);
    for (const mix of [30, 50, 70]) { const params = { mix, a: A.key, b: B.key, pa: A.params, pb: B.params, sa: A.syms, sb: B.syms }; try { kids.push({ strategy: 'hybrid', params, syms, ...(await test('hybrid', params, syms)) }); } catch (e) { warn('breed', e.message); } }
  }
  const score = x => (x.cagr ?? -1) - Math.abs(x.maxDD ?? 1) * 0.35 + Math.min(x.sharpe ?? 0, 3) * 0.02;
  kids.sort((x, y) => score(y) - score(x));
  const best = kids[0], beats = best && score(best) > Math.max(score(ra), score(rb));
  return { a: { ...A, ...ra }, b: { ...B, ...rb }, kids: kids.slice(0, 3), same: A.key === B.key, beats,
    verdict: !best ? 'The children couldn\'t be tested.' : beats ? 'The best child beat both parents on growth for the risk taken in this test. Check it on a walk-forward test before trusting it.' : 'Neither child beat the better parent here. Sometimes the parent is just better.' };
});

// ---------- walk-forward: tune on a year, trade the next 3 months, roll forward ----------
async function walkForward(o) {
  const key = o.strategy, st = STRATEGIES[key]; if (!st || st.auto || st.live) fail('That strategy can\'t be walk-forward tested.');
  const params = cleanParams(key, o.params), syms = withPark([...new Set((o.syms || []).map(cleanSym).filter(Boolean))].slice(0, 30), params);
  const years = clamp(num(o.years, 5), 3, 10), capital = clamp(num(o.capital, 10000), 100, 1e8), train = clamp(Math.round(num(o.train, 12)), 6, 36), testM = clamp(Math.round(num(o.test, 3)), 1, 12);
  const bench = benchFor(syms), { data } = await loadUniverse([...new Set([...syms, bench])]), usable = syms.filter(s => data[s]); if (!usable.length) fail('No price history for those symbols');
  const end = usable.map(s => data[s].d[data[s].d.length - 1]).sort().pop(), start = addIso(end, -Math.round(years * 365.25)), P = ppy(usable);
  const args = { key, params, syms: usable, data, capital, bench, maxPosPct: 100 }, combos = Object.keys(st.grid || {}).length ? gridCombos(st.grid).slice(0, 12) : [{}];
  const windows = []; let ts = addIso(start, Math.round(train * 30.44));
  while (ts < end) {
    const te = addIso(ts, Math.round(testM * 30.44)) > end ? end : addIso(ts, Math.round(testM * 30.44)), tr0 = addIso(ts, -Math.round(train * 30.44));
    let best = null;
    for (const g of combos) { const pp = { ...params, ...g }; if (key === 'trend' && pp.fast >= pp.slow) continue; const s0 = curveStats(runBacktest({ ...args, params: pp, start: tr0, end: ts }).curve, P); if (s0 && (!best || (s0.sharpe ?? -9) > (best.s.sharpe ?? -9))) best = { g, s: s0 }; }
    const tuned = runBacktest({ ...args, params: { ...params, ...(best?.g || {}) }, start: ts, end: te }), fixed = combos.length > 1 ? runBacktest({ ...args, start: ts, end: te }) : tuned;
    const r = c => c.length > 1 ? c[c.length - 1][1] / c[0][1] - 1 : 0;
    windows.push({ from: ts, to: te, tuned: best?.g || {}, inSharpe: best?.s?.sharpe ?? null, inRet: best?.s?.cagr ?? null, ret: r(tuned.curve), fixedRet: r(fixed.curve), benchRet: tuned.bcurve ? r(tuned.bcurve) : null, dd: curveStats(tuned.curve, P)?.maxDD ?? 0 });
    ts = te; if (te === end) break;
  }
  if (!windows.length) fail('Not enough history for a walk-forward test. Try more years.');
  // stitch the unseen windows together
  const chain = k => windows.reduce((t, w) => t * (1 + (w[k] ?? 0)), 1) - 1, yrs = daysBetween(windows[0].from, windows[windows.length - 1].to) / 365.25;
  const oos = chain('ret'), fixedAll = chain('fixedRet'), benchAll = chain('benchRet'), cagr = Math.pow(1 + oos, 1 / Math.max(0.25, yrs)) - 1;
  const inAvg = windows.filter(w => w.inRet != null).reduce((t, w, _, a) => t + w.inRet / a.length, 0);
  const eff = inAvg > 0 ? cagr / inAvg : null, wins = windows.filter(w => w.ret > 0).length, beat = windows.filter(w => w.benchRet != null && w.ret > w.benchRet).length;
  const pass = cagr > 0 && (eff == null || eff >= 0.5) && wins / windows.length >= 0.5;
  return { strategy: key, name: st.name, syms: usable, bench, train, test: testM, windows, oos, cagr, fixedRet: fixedAll, benchRet: benchAll, efficiency: eff, wins, beat, tunable: combos.length > 1, pass,
    verdict: pass ? `Held up: on ${windows.length} stretches it had never seen, it made money ${wins} times and kept ${eff == null ? 'most' : Math.round(eff * 100) + '%'} of the return it showed while tuning.` : cagr <= 0 ? 'On data it hadn\'t seen, it lost money overall. The backtest was probably fitted to the past.' : `Weak: it kept only ${eff == null ? 'a little' : Math.round(eff * 100) + '%'} of its tuned return on unseen data, or lost in most stretches.` };
}
// ---------- Monte Carlo: reshuffle the backtest's days thousands of times ----------
function monteCarlo(curve, { sims = 1000, block = 10, ppyN = 252, R = Math.random } = {}) {
  const rets = []; for (let i = 1; i < curve.length; i++) rets.push(curve[i][1] / curve[i - 1][1] - 1);
  const n = rets.length; if (n < 60) fail('Not enough history for a Monte Carlo test.');
  const finals = [], dds = [], paths = [], marks = Array.from({ length: 41 }, (_, k) => Math.round(k * n / 40));
  for (let s = 0; s < sims; s++) {
    let v = 1, peak = 1, dd = 0, t = 0; const path = [1];
    while (t < n) { const st0 = Math.floor(R() * (n - block)); for (let k = 0; k < block && t < n; k++, t++) { v *= 1 + rets[st0 + k]; peak = Math.max(peak, v); dd = Math.min(dd, v / peak - 1); if (marks.includes(t + 1)) path.push(v); } }
    finals.push(v - 1); dds.push(dd); paths.push(path);
  }
  const q = (arr, p) => { const a = arr.slice().sort((x, y) => x - y); return a[Math.min(a.length - 1, Math.max(0, Math.round(p * (a.length - 1))))]; };
  const years = n / ppyN, cg = r => Math.pow(1 + r, 1 / years) - 1;
  const fan = marks.map((m, k) => { const col = paths.map(pp => pp[Math.min(k, pp.length - 1)]); return { t: m, p5: q(col, 0.05), p25: q(col, 0.25), p50: q(col, 0.5), p75: q(col, 0.75), p95: q(col, 0.95) }; });
  return { sims, years, final: { p5: q(finals, 0.05), p25: q(finals, 0.25), p50: q(finals, 0.5), p75: q(finals, 0.75), p95: q(finals, 0.95) }, cagr: { p5: cg(q(finals, 0.05)), p50: cg(q(finals, 0.5)), p95: cg(q(finals, 0.95)) },
    dd: { p50: q(dds, 0.5), p5: q(dds, 0.05), worst: Math.min(...dds) }, lossChance: finals.filter(x => x < 0).length / sims, dd30Chance: dds.filter(x => x <= -0.3).length / sims, fan,
    actual: curve[curve.length - 1][1] / curve[0][1] - 1 };
}
const labCache = new Map();
const labCached = async (k, fn) => { const h0 = labCache.get(k); if (h0 && Date.now() - h0.t < 30 * 60e3) return h0.v; const v = await fn(); labCache.set(k, { t: Date.now(), v }); if (labCache.size > 30) labCache.delete(labCache.keys().next().value); return v; };
POST('/api/lab/walkforward', auth, async req => { botsOn(); const b = req.body || {}; learnSeen('walkforward'); return labCached(JSON.stringify(['wf', b.strategy, b.params, b.syms, b.years, b.capital, b.train, b.test]), () => walkForward(b)); });
POST('/api/lab/montecarlo', auth, async req => {
  botsOn(); const b = req.body || {};
  return labCached(JSON.stringify(['mc', b.strategy, b.params, b.syms, b.years, b.capital]), async () => {
    const st = STRATEGIES[b.strategy]; if (!st || st.live) fail('That strategy can\'t be tested this way.');
    const params = cleanParams(b.strategy, b.params), syms = withPark([...new Set((b.syms || []).map(cleanSym).filter(Boolean))].slice(0, 40), params);
    const years = clamp(num(b.years, 5), 1, 10), capital = clamp(num(b.capital, 10000), 100, 1e8), bench = benchFor(syms), { data } = await loadUniverse([...new Set([...syms, bench])]);
    const usable = syms.filter(s => data[s]); if (!usable.length) fail('No price history for those symbols');
    const end = usable.map(s => data[s].d[data[s].d.length - 1]).sort().pop(), r = runBacktest({ key: b.strategy, params, syms: usable, data, start: addIso(end, -Math.round(years * 365.25)), end, capital, bench, maxPosPct: 100 });
    return { strategy: b.strategy, name: st.name, capital, ...monteCarlo(r.curve, { ppyN: ppy(usable) }) };
  });
});

// ---------- strategy decay: is the bot (or its strategy) still doing what the backtest promised? ----------
async function decayCheck(b) {
  const bot = botJson(b), st = STRATEGIES[bot.strategy]; if (!st || st.auto || st.live) return null;
  const days = (Date.now() - bot.created) / 864e5; if (days < 60) return null;
  const from = etDate(bot.created), r = await backtest({ strategy: bot.strategy, params: bot.params, syms: bot.syms, years: 3, robust: false, maxPosPct: bot.mandate.maxPosPct });
  const full = r.curve, P = ppy(r.syms);
  // 1. the strategy itself: the last 6 months against its own 3-year record
  const six = etDate(Date.now() - 183 * 864e5), recent = curveStats(full.filter(x => x[0] >= six), P), long = curveStats(full, P);
  const benchRecent = r.bcurve ? curveStats(r.bcurve.filter(x => x[0] >= six), P) : null;
  const edgeGone = long && recent && (long.sharpe ?? 0) > 0.4 && (recent.sharpe ?? 0) < 0 && benchRecent && recent.ret < benchRecent.ret - 0.05;
  // 2. the bot: what it actually made since it started, against what the backtest made over the same days
  const w = await walletRow(bot.wid), value = +w.cash + marketValue(await positions(bot.wid)), live = await flowPerf(bot.wid, bot.created, value).catch(() => null);
  const same = full.filter(x => x[0] >= from), btRet = same.length > 1 ? same[same.length - 1][1] / same[0][1] - 1 : null, liveRet = live?.ret ?? null;
  const drift = liveRet != null && btRet != null && liveRet < btRet - Math.max(0.05, Math.abs(btRet) * 0.5);
  if (!edgeGone && !drift) return { ok: true, liveRet, btRet, recent: recent?.sharpe, long: long?.sharpe };
  return { ok: false, kind: edgeGone ? 'edge' : 'drift', liveRet, btRet, recentRet: recent?.ret, recentSharpe: recent?.sharpe, longSharpe: long?.sharpe, benchRecent: benchRecent?.ret,
    why: edgeGone ? `Its strategy has stopped working lately: over the last 6 months the backtest shows ${(recent.ret * 100).toFixed(1)}% (Sharpe ${(recent.sharpe ?? 0).toFixed(2)}) against ${(benchRecent.ret * 100).toFixed(1)}% for ${r.benchSym}, after a 3-year Sharpe of ${long.sharpe.toFixed(2)}.`
      : `It has made ${(liveRet * 100).toFixed(1)}% since it started, while its own backtest over the same days made ${(btRet * 100).toFixed(1)}%. Something live (fills, vetoes, news filters, small-wallet limits) is costing it.` };
}
async function decayJob() {
  if (S.decayAlerts === false) return;
  for (const b of await rows("select * from bots where status='running'")) {
    const st0 = jsonOr(b.state, {}); if (st0._decay?.ts > Date.now() - 7 * 864e5 || st0._decayAck > Date.now() - 30 * 864e5) continue;
    const r = await decayCheck(b).catch(e => { warn('decay', b.id, e.message); return null; }); if (!r) continue;
    const st = jsonOr((await row('select state from bots where id=$1', [b.id]))?.state, {});
    st._decay = { ts: Date.now(), ...r }; await Q('update bots set state=$1 where id=$2', [JSON.stringify(st), b.id]);
    if (r.ok) continue;
    await Q("update bots set status='paused' where id=$1", [b.id]);
    await botLog(b.id, 'risk', r.kind === 'edge' ? 'Paused: its strategy stopped working lately' : 'Paused: falling behind its own backtest', r.why + ' Resume it if you still want it running (it won\'t pause for this again for 30 days).');
    await notify('bot', `📉 ${b.name} paused: ${r.kind === 'edge' ? 'strategy decay' : 'behind its backtest'}`, r.why, { url: '/#/bot/' + b.id });
  }
}
POST('/api/bots/:id/decay', auth, async req => { const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404); const r = await decayCheck(b); if (!r) return { ok: true, young: true }; const st = jsonOr(b.state, {}); st._decay = { ts: Date.now(), ...r }; await Q('update bots set state=$1 where id=$2', [JSON.stringify(st), b.id]); return r; });

// ---------- bot vs you: the same money, the same deposits, a strategy instead of you ----------
POST('/api/lab/vs', auth, async req => {
  botsOn();
  const b = req.body || {}, w = await walletRow(+b.wallet || await activeWalletId(req)), key = b.strategy, st = STRATEGIES[key];
  if (!st || st.auto || st.live || key === 'hybrid') fail('Pick a strategy that can be backtested');
  const params = cleanParams(key, b.params || {}), syms = withPark([...new Set((b.syms?.length ? b.syms : (SUGGEST[key] || UNIVERSES.megacap.syms)).map(cleanSym).filter(Boolean))].slice(0, 30), params);
  const since = tsMs(w.season_start) || tsMs(w.created), from = etDate(since);
  const snaps = await rows("select ts, eq from snap where wid=$1 and ts >= $2 order by ts", [w.id, new Date(since)]);
  const byDay = new Map(); for (const s of snaps) byDay.set(etDate(tsMs(s.ts)), +s.eq);
  const mine = [...byDay.entries()];
  if (mine.length < 5) fail(`${w.name} needs at least a week of history to compare. Check back in a few days.`);
  // money in and out since the start, on the day it arrived
  const fl = await rows("select ts, price from trd where wid=$1 and side='cash' and ts >= $2", [w.id, new Date(since)]);
  const flows = {}; let netIn = 0; for (const f of fl) { const d = etDate(tsMs(f.ts)), a = +f.price; flows[d] = (flows[d] || 0) + a; netIn += a; }
  const pos = await positions(w.id), now = +w.cash + marketValue(pos), cap0 = mine[0][1];
  const bench = benchFor(syms), { data } = await loadUniverse([...new Set([...syms, bench])]), usable = syms.filter(s => data[s]); if (!usable.length) fail('No price history for those symbols');
  const r = runBacktest({ key, params, syms: usable, data, start: mine[0][0], end: null, capital: cap0, bench, maxPosPct: 100, flows });
  const botMoney = r.money.length ? r.money[r.money.length - 1][1] : cap0;
  const P = ppy(usable), myStats = curveStats(mine, P), botStats = curveStats(r.curve, P);
  const myTwr = await flowPerf(w.id, since, now).catch(() => null);
  const myTrades = +(await row("select count(*) n from trd where wid=$1 and side in ('buy','sell') and ts >= $2", [w.id, new Date(since)])).n;
  const lead = now - botMoney;
  return { wallet: { id: w.id, name: w.name, emoji: w.emoji }, strategy: key, name: st.name, syms: usable, from, start: cap0, netIn,
    you: { money: now, ret: myTwr?.ret ?? (myStats?.ret ?? null), maxDD: myStats?.maxDD ?? null, trades: myTrades, curve: downsamplePts(mine, 120) },
    bot: { money: botMoney, ret: botStats?.ret ?? null, maxDD: botStats?.maxDD ?? null, trades: r.trades.length, curve: downsamplePts(r.money, 120), holds: r.open.map(o => o.sym) },
    lead, verdict: Math.abs(lead) < Math.max(5, cap0 * 0.002) ? 'Neck and neck.' : lead > 0 ? `You're ahead of the bot by ${money(lead)}.` : `The bot is ahead of you by ${money(-lead)}.`,
    note: `The bot starts with ${money(cap0)} on ${new Date(from + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' })}, the day ${w.name}'s current season started, and gets your deposits and withdrawals on the days they arrived. It trades at each day's close with the same fees and spreads.` };
});
// =====================================================================
// Learning and fun: story mode (Academy chapters and the Time machine), compound interest and DCA,
// the risk profile meter, the daily call, new achievements, and the days HQ celebrates
// =====================================================================
// ---------- things you've looked at (for lessons that ask you to try a tool) ----------
let SEEN = null;
async function learnSeen(k) { if (!SEEN) SEEN = jsonOr(await kvGet('learn_seen', '{}').catch(() => '{}'), {}); if (SEEN[k]) return; SEEN[k] = Date.now(); await kvSet('learn_seen', JSON.stringify(SEEN)).catch(() => {}); }
const seenAll = async () => SEEN || (SEEN = jsonOr(await kvGet('learn_seen', '{}').catch(() => '{}'), {}));
POST('/api/learn/seen', auth, async req => { const k = String(req.body?.k || '').replace(/[^a-z0-9_]/g, '').slice(0, 30); if (k) await learnSeen(k); return { ok: true }; });

// ---------- Academy: chapters you finish by doing the real thing ----------
const BONDS = new Set(['TLT', 'IEF', 'SHY', 'BND', 'AGG', 'SGOV', 'BIL', 'TIP', 'VGIT', 'VGLT', 'GOVT', 'SCHO']);
const ACADEMY = [
  { k: 'basics', title: 'Your first trades', icon: 'sparkle', steps: [
    { k: 'buy', t: 'Buy your first stock or fund', go: 'search', lesson: 'A share is a tiny slice of a company. Its price moves as people buy and sell it. Buying in dollars ("$50 of Apple") works the same as buying in shares.', test: c => c.buys >= 1 },
    { k: 'watch', t: 'Add something to your watchlist', go: 'market', lesson: 'Watching before buying is free. Star a few things you\'re curious about and see how they move for a week.', test: c => c.watch >= 1 },
    { k: 'note', t: 'Write a note on one of your trades', go: 'activity', lesson: 'Write down why you bought. Later, compare it with what happened. It\'s the fastest way to learn what works for you.', test: c => c.notes >= 1 },
  ] },
  { k: 'diversify', title: 'Don\'t put all your eggs in one basket', icon: 'pie', steps: [
    { k: 'index', t: 'Buy an index fund, like SPY or VOO', go: 'asset/SPY', lesson: 'An index fund owns hundreds of companies at once. If one fails, it barely matters. Most professional investors don\'t beat it over time.', test: c => c.fundBuy },
    { k: 'three', t: 'Hold 3 or more different things at once', go: 'portfolio', lesson: 'Different things go up and down at different times, so together they swing less than any one of them.', test: c => c.maxHeld >= 3 },
    { k: 'bond', t: 'Own a bond or T-bill fund (TLT, IEF, BND, AGG or SGOV)', go: 'asset/SGOV', lesson: 'Bonds are loans that pay interest. They usually move less than stocks, and often hold up when stocks fall.', test: c => c.bond },
  ] },
  { k: 'protect', title: 'Protect your downside', icon: 'shield', steps: [
    { k: 'stop', t: 'Set a stop loss on something you own', go: 'portfolio', lesson: 'A stop loss sells automatically if the price falls to a level you choose, so one bad day can\'t turn into a disaster while you\'re not looking.', test: c => c.stops },
    { k: 'alert', t: 'Set a price alert', go: 'portfolio', lesson: 'An alert doesn\'t trade, it just tells you. Handy for "buy if it drops to $X" without watching the screen.', test: c => c.alerts >= 1 },
    { k: 'take', t: 'Set a take profit', go: 'portfolio', lesson: 'A take profit sells when the price reaches your target. Deciding in advance beats deciding in the excitement of the moment.', test: c => c.takes },
  ] },
  { k: 'orders', title: 'Order types', icon: 'list', steps: [
    { k: 'limit', t: 'Place a limit order', go: 'search', lesson: 'A limit order only fills at your price or better. You may not get filled, but you never pay more than you meant to.', test: c => c.limits },
    { k: 'ext', t: 'Trade before the open or after the close', go: 'search', lesson: 'Extended hours have fewer traders, so spreads are wider and prices jumpier. Limit orders only.', test: c => c.ext },
    { k: 'frac', t: 'Buy a fraction of a share', go: 'search', lesson: 'With fractions, $10 buys a slice of even the most expensive stock. They trade as market or day orders, in regular hours.', test: c => c.frac },
  ] },
  { k: 'patience', title: 'Time in the market', icon: 'clock', steps: [
    { k: 'hold30', t: 'Hold one position for 30 days', go: 'portfolio', lesson: 'The market\'s best days often come right after its worst. Missing a handful of them cuts long-run returns a lot.', test: c => c.longestHold >= 30 * 864e5 },
    { k: 'div', t: 'Get paid a dividend', go: 'dividends', lesson: 'Many companies pay you a share of their profits just for owning them. Reinvested, dividends have been a big part of the market\'s long-run return.', test: c => c.divs > 0 },
    { k: 'compound', t: 'Try the compound interest calculator', go: 'compound', lesson: 'Money that earns money, which then earns money too. Time does most of the work, which is why starting early matters more than starting big.', test: c => c.seen.compound },
  ] },
  { k: 'risk', title: 'Know your risk', icon: 'radar', steps: [
    { k: 'profile', t: 'Take the risk profile quiz', go: 'risk', lesson: 'How much of a drop could you sit through without selling? Your answer should shape what you own.', test: c => c.riskDone },
    { k: 'overlap', t: 'Compare two funds for overlap', go: 'overlap', lesson: 'Owning three tech funds isn\'t diversification if they hold the same stocks.', test: c => c.seen.overlap },
    { k: 'research', t: 'Read a stock\'s research', go: 'asset/AAPL', lesson: 'Insider buys, earnings history and dividend safety are public. Checking them takes a minute.', test: c => c.seen.research },
  ] },
  { k: 'bots', title: 'Put a robot to work', icon: 'bot', steps: [
    { k: 'backtest', t: 'Backtest a strategy', go: 'lab', lesson: 'A backtest replays a set of rules over past prices. It shows how rules behaved, not what will happen next.', test: c => c.seen.backtest },
    { k: 'bot', t: 'Start a bot', go: 'newbot', lesson: 'A bot follows its rules every day without fear or greed. You set the limits it can\'t cross.', test: c => c.bots >= 1 },
    { k: 'wf', t: 'Run a walk-forward test', go: 'lab', lesson: 'It\'s easy to find rules that fit the past perfectly and fail tomorrow. Testing on data the rules never saw shows if they\'re real.', test: c => c.seen.walkforward },
  ] },
  { k: 'options', title: 'Options, carefully', icon: 'target', steps: [
    { k: 'quiz', t: 'Pass the level 1 options quiz', go: 'options', lesson: 'An option is the right to buy or sell 100 shares at a set price by a set date. Brokers make you show you understand the risks first.', test: c => c.optQuiz },
    { k: 'sell', t: 'Sell a covered call or a cash-secured put', go: 'options', lesson: 'Selling an option pays you now, in exchange for an obligation later. Covered by shares or cash, the risk is the same as owning the stock.', test: c => c.optSold },
    { k: 'close', t: 'Close an option or let one expire', go: 'options', lesson: 'Most options are closed before expiration. If one expires in the money, it turns into shares automatically.', test: c => c.optClosed },
  ] },
  { k: 'history', title: 'Learn from history', icon: 'calendar', steps: [
    { k: 'replay', t: 'Finish a Time machine replay', go: 'replay', lesson: 'Trading through a real crash, one day at a time and without knowing the ending, teaches more than reading about it.', test: c => c.replays >= 1 },
    { k: 'beat', t: 'Beat buy-and-hold in a replay', go: 'replay', lesson: 'Beating "just buy and hold" is harder than it looks. Try it and see what you were tempted to do.', test: c => c.replayBeat },
    { k: 'calls', t: 'Make 5 daily calls', go: 'games', lesson: 'Guess whether the S&P 500 closes up or down. After a few dozen, most people land near 50/50, which is the point.', test: c => c.calls >= 5 },
  ] },
];
const ACH_BASE = achievementContext; // the original facts; the version with the new ones is set below
async function learnCtx(base = null) {
  const c = base || await ACH_BASE(), s = await seenAll(), quiz = jsonOr(await kvGet('opt_quiz', '{}').catch(() => '{}'), {});
  const buys = await rows("select sym, qty, sess from trd where side='buy'");
  const optT = await rows('select action from opttrd').catch(() => []), rp = await replayStats();
  return { ...c, seen: s, buys: buys.length, fundBuy: buys.some(b => FUNDS.has(b.sym)), frac: buys.some(b => !isCrypto(b.sym) && Math.abs(+b.qty - Math.round(+b.qty)) > 1e-6), ext: buys.some(b => b.sess && !['regular', 'closed'].includes(b.sess)) || (await row("select 1 from trd where side='sell' and sess in ('pre','post','overnight') limit 1")) != null,
    bond: (await rows('select distinct sym from pos')).some(r => BONDS.has(r.sym)) || buys.some(b => BONDS.has(b.sym)),
    watch: +(await row('select count(*) n from watch')).n, stops: (c.why['stop loss'] || 0) > 0 || (await row("select 1 from ord where kind in ('stop','trail') limit 1")) != null,
    takes: (c.why['take profit'] || 0) > 0 || (await row("select 1 from ord where kind='take' limit 1")) != null, limits: (c.why['limit order'] || 0) > 0 || (await row('select 1 from lim limit 1')) != null,
    riskDone: !!(await kvGet('risk_profile').catch(() => null)), bots: +(await row('select count(*) n from bots')).n, optQuiz: !!quiz[1],
    optSold: optT.some(t => t.action === 'sto'), optClosed: optT.some(t => ['btc', 'stc', 'expired', 'assigned', 'exercised', 'cash'].includes(t.action)), optTrades: optT.length,
    replays: rp.done, replayBeat: rp.beat, calls: +(await row("select count(*) n from dcall").catch(() => ({ n: 0 }))).n, callBest: await dcallBest() };
}
async function academy() {
  const c = await learnCtx();
  const ch = ACADEMY.map(x => { const steps = x.steps.map(s => ({ k: s.k, t: s.t, lesson: s.lesson, go: s.go, done: !!s.test(c) })); return { k: x.k, title: x.title, icon: x.icon, steps, done: steps.every(s => s.done), n: steps.filter(s => s.done).length }; });
  const next = ch.find(x => !x.done);
  return { chapters: ch, done: ch.filter(x => x.done).length, next: next ? { k: next.k, title: next.title, step: next.steps.find(s => !s.done) } : null };
}
GET('/api/learn', auth, async () => academy());
const learnDone = async () => (await academy().catch(() => ({ done: 0 }))).done;

// ---------- the Time machine: trade through a real market, one day at a time ----------
const REPLAYS = {
  covid: { name: 'The 2020 crash', blurb: 'February to June 2020: a new virus, the fastest bear market ever, and an even faster rebound.', start: '2020-02-14', end: '2020-06-30', syms: ['SPY', 'QQQ', 'TLT', 'GLD', 'AAPL', 'AMZN', 'ZM', 'AAL', 'XOM', 'JPM'],
    events: [['2020-02-19', 'The S&P 500 closes at a record high.'], ['2020-02-24', 'Coronavirus spreads outside China. The Dow falls more than 1,000 points.'], ['2020-03-03', 'The Fed makes an emergency half-point rate cut.'], ['2020-03-09', 'Oil crashes and stocks fall 7% at the open: the first market-wide circuit breaker since 1997.'], ['2020-03-11', 'The World Health Organization declares a pandemic.'], ['2020-03-12', 'Another circuit breaker. The worst day since 1987.'], ['2020-03-16', 'The Fed cut rates to zero on Sunday. Stocks fall 12% anyway, with a third circuit breaker.'], ['2020-03-23', 'The Fed says it will buy bonds without limit.'], ['2020-03-27', 'The $2 trillion CARES Act is signed.'], ['2020-04-20', 'US oil futures go below zero for the first time.'], ['2020-05-08', 'The jobs report shows 20 million jobs lost in April.'], ['2020-06-05', 'A surprise: the economy added jobs in May.']] },
  gfc: { name: 'The 2008 financial crisis', blurb: 'September 2008 to June 2009: Lehman falls, banks wobble, and stocks lose half their value before the bottom.', start: '2008-09-02', end: '2009-06-30', syms: ['SPY', 'QQQ', 'TLT', 'GLD', 'AAPL', 'JPM', 'C', 'XOM', 'WMT', 'AMZN'],
    events: [['2008-09-08', 'The government takes over Fannie Mae and Freddie Mac.'], ['2008-09-15', 'Lehman Brothers files for bankruptcy. Merrill Lynch sells itself to Bank of America.'], ['2008-09-16', 'The Fed rescues AIG.'], ['2008-09-29', 'The House votes down the bank bailout. The Dow falls 778 points.'], ['2008-10-03', 'The $700 billion bailout (TARP) becomes law.'], ['2008-10-08', 'Central banks around the world cut rates together.'], ['2008-10-13', 'The Dow jumps 936 points, its biggest point gain yet.'], ['2008-11-20', 'The S&P 500 closes at its lowest since 1997.'], ['2008-11-24', 'The government steps in to save Citigroup.'], ['2008-12-16', 'The Fed cuts rates to near zero.'], ['2009-02-17', 'A $787 billion stimulus is signed.'], ['2009-03-09', 'Stocks are down more than 50% from the 2007 peak.'], ['2009-03-18', 'The Fed says it will buy $1 trillion more in bonds.'], ['2009-05-07', 'The bank stress test results are out.']] },
  dotcom: { name: 'The dot-com bust', blurb: 'March 2000 to March 2001: the internet bubble pops and the Nasdaq loses more than half.', start: '2000-03-01', end: '2001-03-30', syms: ['SPY', 'QQQ', 'MSFT', 'CSCO', 'INTC', 'ORCL', 'AMZN', 'WMT', 'XOM', 'JNJ'],
    events: [['2000-03-10', 'The Nasdaq closes above 5,000 for the first time.'], ['2000-04-14', 'The Nasdaq falls almost 10% in a day, ending its worst week ever.'], ['2000-05-16', 'The Fed raises rates by half a point.'], ['2000-11-08', 'The presidential election is too close to call.'], ['2000-12-13', 'The election is settled after the Supreme Court ruling.'], ['2001-01-03', 'The Fed makes a surprise half-point rate cut.'], ['2001-03-12', 'The Nasdaq falls below 2,000.']] },
  bear22: { name: 'The 2022 bear market', blurb: 'All of 2022: inflation at a 40-year high, the fastest rate hikes in decades, and stocks and bonds falling together.', start: '2022-01-03', end: '2022-12-30', syms: ['SPY', 'QQQ', 'TLT', 'GLD', 'SGOV', 'AAPL', 'META', 'TSLA', 'XOM', 'NFLX'],
    events: [['2022-01-03', 'The S&P 500 closes at a record high.'], ['2022-02-24', 'Russia invades Ukraine.'], ['2022-03-16', 'The Fed raises rates for the first time since 2018.'], ['2022-05-04', 'The Fed raises rates by half a point.'], ['2022-06-13', 'The S&P 500 enters a bear market: 20% below its high.'], ['2022-06-15', 'The Fed raises rates by three quarters of a point, the most since 1994.'], ['2022-08-26', 'Powell warns that fighting inflation will bring "some pain".'], ['2022-09-13', 'Inflation comes in hotter than expected. The S&P 500 falls 4%.'], ['2022-11-10', 'Inflation comes in cooler than expected. The S&P 500 jumps 5.5%.']] },
};
const RP_SPREAD = 0.0005; // half a cent on the dollar each way, no commissions
async function replayData(k) {
  const sc = REPLAYS[k];
  return cached('replay:' + k, 7 * 864e5, async () => {
    const p1 = Math.floor(Date.parse(addIso(sc.start, -120) + 'T00:00:00Z') / 1000), p2 = Math.floor(Date.parse(addIso(sc.end, 3) + 'T00:00:00Z') / 1000), data = {};
    for (const s of sc.syms) { try { const b = parseYahooDaily(await yahooChart(s, `period1=${p1}&period2=${p2}&interval=1d&events=div,split`), s); if (b.d.length > 20) data[s] = b; } catch (e) {} }
    if (!data.SPY) fail('Couldn\'t load the price history for this replay. Try again in a minute.');
    const days = data.SPY.d.filter(d => d >= sc.start && d <= sc.end);
    return { data, days };
  });
}
const RP_BLANK = { k: null };
async function replayState() { return jsonOr(await kvGet('replay').catch(() => null), RP_BLANK) || RP_BLANK; }
const pxOnDay = (b, d) => { const i = lastIndexOnOrBefore(b.d, d); return i >= 0 ? b.c[i] : null; };
async function replayView(g) {
  if (!g.k) return { game: null };
  const sc = REPLAYS[g.k], { data, days } = await replayData(g.k), day = days[g.i], last = g.i >= days.length - 1;
  const value = g.cash + Object.entries(g.pos).reduce((t, [s, h]) => t + h.qty * (pxOnDay(data[s], day) || 0), 0);
  const spy0 = pxOnDay(data.SPY, days[0]), spyNow = pxOnDay(data.SPY, day);
  const assets = sc.syms.filter(s => data[s]).map(s => {
    const b = data[s], i = lastIndexOnOrBefore(b.d, day), prev = i > 0 ? b.c[i - 1] : b.c[i], i0 = Math.max(0, i - 60);
    return { sym: s, px: b.c[i], day: b.c[i] / prev - 1, since: b.c[i] / pxOnDay(b, days[0]) - 1, held: g.pos[s]?.qty || 0, cost: g.pos[s]?.cost || 0, spark: b.c.slice(i0, i + 1).filter((_, j, a) => j % 2 === 0 || j === a.length - 1) };
  });
  const news = sc.events.filter(([d]) => d <= day).reverse().slice(0, 5).map(([d, t]) => ({ d, t, fresh: d > (g.lastSeen || '') }));
  return { game: { k: g.k, name: sc.name, day, i: g.i, of: days.length, start: days[0], end: days[days.length - 1], cash: g.cash, value, ret: value / g.start - 1, spy: spyNow / spy0 - 1, assets, news, trades: g.trades.slice(-12).reverse(), done: !!g.result, result: g.result || null, last } };
}
GET('/api/replay', auth, async () => {
  const g = await replayState(), best = jsonOr(await kvGet('replay_best', '{}').catch(() => '{}'), {});
  return { scenarios: Object.entries(REPLAYS).map(([k, s]) => ({ k, name: s.name, blurb: s.blurb, start: s.start, end: s.end, best: best[k] || null })), ...(await replayView(g)) };
});
POST('/api/replay/start', auth, async req => {
  const k = req.body?.k; if (!REPLAYS[k]) fail('Pick a replay');
  await replayData(k);
  const g = { k, i: 0, cash: 10000, start: 10000, pos: {}, trades: [], started: Date.now(), lastSeen: '' };
  await kvSet('replay', JSON.stringify(g)); return replayView(g);
});
POST('/api/replay/trade', auth, async req => {
  const g = await replayState(); if (!g.k || g.result) fail('Start a replay first');
  const { data, days } = await replayData(g.k), day = days[g.i], sym = cleanSym(req.body?.sym), side = req.body?.side === 'sell' ? 'sell' : 'buy';
  const b = data[sym]; if (!b) fail('That isn\'t in this replay'); const px = pxOnDay(b, day); if (!px) fail(`${sym} has no price on this day`);
  let usd = num(req.body?.usd);
  if (side === 'buy') {
    if (req.body?.all) usd = g.cash; if (!(usd > 0)) fail('Enter an amount'); usd = Math.min(usd, g.cash); if (usd < 1) fail('Not enough cash');
    const fill = px * (1 + RP_SPREAD), qty = usd / fill, h = g.pos[sym] || { qty: 0, cost: 0 };
    h.qty += qty; h.cost += usd; g.pos[sym] = h; g.cash -= usd; g.trades.push({ d: day, sym, side, qty, px: fill, usd });
  } else {
    const h = g.pos[sym]; if (!h) fail(`You don't own ${sym}`);
    const fill = px * (1 - RP_SPREAD), qty = req.body?.all || !(usd > 0) ? h.qty : Math.min(h.qty, usd / fill), part = qty / h.qty, got = qty * fill;
    g.trades.push({ d: day, sym, side, qty, px: fill, usd: got, pnl: got - h.cost * part }); h.qty -= qty; h.cost -= h.cost * part; g.cash += got; if (h.qty < 1e-9) delete g.pos[sym];
  }
  await kvSet('replay', JSON.stringify(g)); return replayView(g);
});
POST('/api/replay/next', auth, async req => {
  const g = await replayState(); if (!g.k || g.result) fail('Start a replay first');
  const { data, days } = await replayData(g.k), step = clamp(Math.round(num(req.body?.days, 1)), 1, 66);
  g.lastSeen = days[g.i]; g.i = Math.min(days.length - 1, g.i + step);
  if (g.i >= days.length - 1 || req.body?.finish) { // the end: how did you do against just holding the S&P 500, and against cash?
    g.i = days.length - 1; const end = days[g.i];
    const value = g.cash + Object.entries(g.pos).reduce((t, [s, h]) => t + h.qty * (pxOnDay(data[s], end) || 0), 0), ret = value / g.start - 1, spy = pxOnDay(data.SPY, end) / pxOnDay(data.SPY, days[0]) - 1;
    const beat = ret > spy, grade = ret > spy + 0.1 ? 'A' : ret > spy ? 'B' : ret > spy - 0.1 ? 'C' : 'D';
    g.result = { value, ret, spy, beat, grade, trades: g.trades.length, worst: g.trades.filter(t => t.side === 'sell' && t.pnl < 0).sort((a, b) => a.pnl - b.pnl)[0] || null };
    const best = jsonOr(await kvGet('replay_best', '{}').catch(() => '{}'), {}); if (!best[g.k] || ret > best[g.k].ret) best[g.k] = { ret, spy, grade, ts: Date.now() };
    await kvSet('replay_best', JSON.stringify(best));
    const done = jsonOr(await kvGet('replay_done', '[]').catch(() => '[]'), []); done.push({ k: g.k, ret, spy, beat, ts: Date.now() }); await kvSet('replay_done', JSON.stringify(done.slice(-50)));
    checkAchievementsSoon();
  }
  await kvSet('replay', JSON.stringify(g)); return replayView(g);
});
POST('/api/replay/quit', auth, async () => { await kvSet('replay', JSON.stringify(RP_BLANK)); return { game: null }; });
async function replayStats() { const d = jsonOr(await kvGet('replay_done', '[]').catch(() => '[]'), []); return { done: d.length, beat: d.some(x => x.beat), kinds: new Set(d.map(x => x.k)).size }; }

// ---------- compound interest, and dollar-cost averaging on real prices ----------
GET('/api/learn/dca', auth, async req => {
  const sym = cleanSym(req.query.sym || 'SPY'), years = clamp(num(req.query.years, 10), 1, 10), monthly = clamp(num(req.query.monthly, 500), 1, 1e6);
  learnSeen('compound');
  const b = await dailyBars(sym), end = b.d[b.d.length - 1], start = addIso(end, -Math.round(years * 365.25));
  const i0 = b.d.findIndex(d => d >= start); if (i0 < 0) fail('Not enough history');
  // buy on the first trading day of each month; the lump sum puts the same total in on day one
  let units = 0, put = 0, month = null; const curve = [], months = [];
  for (let i = i0; i < b.d.length; i++) { const m = b.d[i].slice(0, 7); if (m !== month) { month = m; units += monthly / b.c[i]; put += monthly; months.push(i); } if (i % 5 === 0 || i === b.d.length - 1) curve.push([b.d[i], units * b.c[i], put]); }
  const total = put, lumpUnits = total / b.c[i0], lump = []; for (let i = i0; i < b.d.length; i += 5) lump.push([b.d[i], lumpUnits * b.c[i]]); lump.push([end, lumpUnits * b.c[b.d.length - 1]]);
  const dcaEnd = units * b.c[b.d.length - 1], lumpEnd = lumpUnits * b.c[b.d.length - 1];
  let worst = 0, peak = 0; for (const [, v, p0] of curve) { peak = Math.max(peak, v / p0); worst = Math.min(worst, v / p0 / peak - 1); }
  return { sym, years, monthly, start: b.d[i0], end, invested: total, dca: dcaEnd, lump: lumpEnd, months: months.length, curve: curve.map(([d, v, p0]) => [d, v, p0]), lumpCurve: lump, lumpWins: lumpEnd > dcaEnd };
});

// ---------- the risk profile meter: what you can stomach vs what you own ----------
const RISK_Q = [
  ['When will you need this money?', [['Within 2 years', 1], ['In 2 to 5 years', 4], ['In 5 to 10 years', 7], ['In more than 10 years', 10]]],
  ['Your investments fall 25% in a month. What do you do?', [['Sell everything', 1], ['Sell some', 4], ['Wait it out', 7], ['Buy more', 10]]],
  ['Which matters more to you?', [['Not losing money', 1], ['Mostly safety, some growth', 4], ['Mostly growth, some safety', 7], ['Growth, even with big swings', 10]]],
  ['How steady is your income?', [['Not steady', 2], ['Somewhat steady', 5], ['Very steady', 8], ['Steady, with savings for emergencies', 10]]],
  ['How much investing have you done?', [['None', 3], ['A little', 5], ['Some, through ups and downs', 8], ['A lot', 10]]],
];
const RISK_LABEL = s => s <= 2.5 ? 'Very careful' : s <= 4.5 ? 'Careful' : s <= 6.5 ? 'Balanced' : s <= 8.5 ? 'Growth' : 'Aggressive';
async function portfolioRisk(wid) {
  const pos = await positions(wid), w = await walletRow(wid), acct = await accountInfo(wid, { w, pos }).catch(() => null);
  const eq = acct?.equity || (+w.cash + marketValue(pos)); if (!(eq > 0)) return null;
  let vol = 0, crypto = 0, top = 0, topSym = null;
  for (const p of pos) {
    const v = p.qty * (priceOf(p.sym) || p.avg), wt = v / eq; if (wt > top) { top = wt; topSym = p.sym; }
    if (isCrypto(p.sym)) crypto += wt;
    let sv = null; try { const b = await dailyBars(p.sym); sv = IND.vol(b.c, 63, b.c.length - 1) * Math.sqrt(isCrypto(p.sym) ? 365 : 252); } catch (e) {}
    vol += wt * (sv ?? (isCrypto(p.sym) ? 0.7 : 0.3));
  }
  const lev = acct?.debit > 0 ? acct.debit / eq : 0, shortOpt = optOpen(wid).filter(o => o.qty < 0 && o.coll === 'naked').length;
  vol *= 0.85; // holdings don't all move together; a rough allowance for diversification
  vol *= 1 + lev;
  // a 1-10 score: 5% a year swing is 1, 50% is 10
  const score = clamp(1 + (vol - 0.05) / 0.05, 1, 10) + (shortOpt ? 1 : 0), cash = Math.max(0, (acct?.cash ?? +w.cash) / eq);
  return { score: Math.min(10, score), label: RISK_LABEL(Math.min(10, score)), vol, crypto, top, topSym, cash, lev, naked: shortOpt, n: pos.length };
}
GET('/api/learn/risk', auth, async req => {
  const saved = jsonOr(await kvGet('risk_profile').catch(() => null), null), wid = await activeWalletId(req), pr = await portfolioRisk(wid).catch(() => null);
  const you = saved ? { score: saved.score, label: RISK_LABEL(saved.score), ts: saved.ts } : null;
  const gap = you && pr ? pr.score - you.score : null;
  return { questions: RISK_Q.map(([q, a]) => ({ q, a: a.map(x => x[0]) })), answers: saved?.answers || null, you, portfolio: pr, gap,
    verdict: gap == null ? null : gap > 2 ? `This wallet is riskier than you said you're comfortable with. ${pr.top > 0.3 ? `${pr.topSym} alone is ${Math.round(pr.top * 100)}% of it. ` : ''}${pr.crypto > 0.2 ? `Crypto is ${Math.round(pr.crypto * 100)}%. ` : ''}Spreading out or adding bonds would bring it closer.` : gap < -2 ? 'You\'re playing it safer than you need to for your goals. That\'s fine, but a little more in stocks could grow more over time.' : 'What you own fits what you said you\'re comfortable with.' };
});
POST('/api/learn/risk', auth, async req => {
  const a = Array.isArray(req.body?.answers) ? req.body.answers : []; if (a.length !== RISK_Q.length) fail('Answer every question');
  const pts = a.map((x, i) => { const o = RISK_Q[i][1].find(o2 => o2[0] === x); if (!o) fail('Answer every question'); return o[1]; });
  // the drop question counts double: it's what decides whether people sell at the bottom
  const score = (pts.reduce((t, x) => t + x, 0) + pts[1]) / (RISK_Q.length + 1);
  await kvSet('risk_profile', JSON.stringify({ answers: a, score: round(score, 2), ts: Date.now() })); checkAchievementsSoon();
  return { ok: true, score, label: RISK_LABEL(score) };
});

// ---------- the daily call: will the S&P 500 close up or down? ----------
// A call made before 9:30 am is for today's close; after the open it's for the next trading day.
// Three house bots call too: Trend (above its 20-day average means up), Contrarian (the opposite of yesterday) and Always up.
function callDay(t = Date.now()) {
  const p = etParts(t), today = ymd([p.y, p.m, p.d]), open = isTradingDay(p.y, p.m, p.d) && p.h * 60 + p.mi < 570;
  return open ? today : ymd(nextTradingDay(p.y, p.m, p.d));
}
POST('/api/dcall', auth, async req => {
  const pick = req.body?.pick === 'down' ? 'down' : req.body?.pick === 'up' ? 'up' : null; if (!pick) fail('Pick up or down');
  const d = callDay();
  const ex = await row('select * from dcall where d=$1', [d]); if (ex?.result) fail('That day is already decided');
  await Q('insert into dcall(d,pick) values($1,$2) on conflict(d) do update set pick=excluded.pick, made=now()', [d, pick]);
  return dcallJson();
});
async function houseCalls(d) {
  const b = await dailyBars('SPY').catch(() => null); if (!b) return {};
  const i = lastIndexOnOrBefore(b.d, addIso(d, -1)); if (i < 21) return {};
  const sma = IND.sma(b.c, 20, i), y = b.c[i] / b.c[i - 1] - 1;
  return { trend: b.c[i] > sma ? 'up' : 'down', contra: y >= 0 ? 'down' : 'up', bull: 'up' };
}
async function dcallJob() {
  const p = etParts(), today = etDate(), after = p.h * 60 + p.mi >= 16 * 60 + 5;
  for (const c of await rows("select * from dcall where result is null and (d < $1 or (d = $1 and $2))", [today, after]).catch(() => [])) {
    const b = await dailyBars('SPY').catch(() => null); let close = null, prev = null;
    if (b) { const i = b.d.indexOf(c.d); if (i > 0) { close = b.c[i]; prev = b.c[i - 1]; } }
    if (close == null && c.d === today) { const q = prices.get('SPY'); if (q?.p && q.prev) { close = q.p; prev = q.prev; } }
    if (close == null) { if (c.d < addIso(today, -5)) await Q("update dcall set result='void' where d=$1", [c.d]); continue; }
    const move = close / prev - 1, res = Math.abs(move) < 0.00005 ? 'push' : (move > 0) === (c.pick === 'up') ? 'win' : 'loss', house = await houseCalls(c.d);
    await Q('update dcall set close=$1, prev=$2, result=$3, house=$4 where d=$5', [close, prev, res, JSON.stringify(house), c.d]);
    await notify('game', res === 'win' ? '🎯 You called today right' : res === 'loss' ? 'Today went the other way' : 'Flat day', `The S&P 500 (SPY) closed ${move >= 0 ? 'up' : 'down'} ${(Math.abs(move) * 100).toFixed(2)}%. You said ${c.pick}.`, { url: '/#/games' });
    checkAchievementsSoon();
  }
}
async function dcallBest() { const l = await rows("select result from dcall where result in ('win','loss') order by d").catch(() => []); let best = 0, cur = 0; for (const r of l) { cur = r.result === 'win' ? cur + 1 : 0; best = Math.max(best, cur); } return best; }
async function dcallJson() {
  const list = await rows('select * from dcall order by d desc limit 120').catch(() => []), done = list.filter(c => c.result === 'win' || c.result === 'loss');
  let streak = 0; for (const c of done) { if (c.result === 'win') streak++; else break; }
  const tally = { you: [0, 0], trend: [0, 0], contra: [0, 0], bull: [0, 0] };
  for (const c of done) { const up = +c.close > +c.prev; tally.you[0] += c.result === 'win'; tally.you[1]++; const hs = jsonOr(c.house, {}); for (const k of ['trend', 'contra', 'bull']) if (hs[k]) { tally[k][0] += (hs[k] === 'up') === up; tally[k][1]++; } }
  const next = callDay(), pend = list.find(c => c.d === next && !c.result);
  return { next, pick: pend?.pick || null, open: market().state === 'open', streak, best: await dcallBest(), tally,
    history: list.slice(0, 20).map(c => ({ d: c.d, pick: c.pick, result: c.result, move: c.close ? +c.close / +c.prev - 1 : null, house: jsonOr(c.house, {}) })) };
}
GET('/api/dcall', auth, async () => dcallJson());

// ---------- new achievements ----------
ACHIEVEMENTS.push(
  ['opt_first', '🎟️', 'Options trader', 'Make your first options trade', c => c.optTrades >= 1],
  ['opt_income', '🧾', 'Rent collector', 'Sell a covered call or cash-secured put', c => c.optSold],
  ['academy_1', '🎓', 'Student', 'Finish an Academy chapter', c => c.chapters >= 1],
  ['academy_all', '🏛️', 'Graduate', 'Finish every Academy chapter', c => c.chapters >= ACADEMY.length],
  ['replay_1', '⏳', 'Time traveler', 'Finish a Time machine replay', c => c.replays >= 1],
  ['replay_beat', '🦸', 'Crash survivor', 'Beat buy-and-hold in a replay', c => c.replayBeat],
  ['replay_all', '🗺️', 'Historian', 'Finish all four replays', c => c.replayKinds >= Object.keys(REPLAYS).length],
  ['call_5', '📞', 'Caller', 'Make 5 daily calls', c => c.calls >= 5],
  ['call_streak', '🎯', 'Hot hand', 'Call 5 days in a row right', c => c.callBest >= 5],
  ['risk_done', '🧭', 'Know thyself', 'Take the risk profile quiz', c => c.riskDone],
  ['bots_3', '🤖', 'Robot army', 'Run 3 bots', c => c.bots >= 3],
  ['researcher', '🔬', 'Did the homework', 'Read a stock\'s research', c => c.seen?.research],
);
achievementContext = async function () { // the extra facts for the new achievements
  const lc = await learnCtx(await ACH_BASE()), rp = await replayStats();
  return { ...lc, replayKinds: rp.kinds, chapters: ACADEMY.filter(x => x.steps.every(st => st.test(lc))).length };
};

// ---------- days HQ celebrates ----------
function dayEvents(iso = etDate()) {
  const [y, m, d] = iso.split('-').map(Number), out = [], hol = holidayName(y, m, d), wd = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  if (hol) out.push({ k: 'holiday', text: `🏖 ${hol}: the stock market is closed` });
  for (const e of ECON.filter(e => e.date === iso)) out.push({ k: e.k === 'fomc' ? 'fed' : 'data', text: e.k === 'fomc' ? `🏛 Fed day: the rate decision is at ${e.time}` : `📊 ${e.name} out at ${e.time}` });
  if ([3, 6, 9, 12].includes(m) && wd === 5 && d === nthWeekday(y, m, 5, 3)) out.push({ k: 'witch', text: '🧙 Quadruple witching: stock and index options and futures all expire today' });
  else if (wd === 5 && d === nthWeekday(y, m, 5, 3)) out.push({ k: 'opex', text: '🎟 Monthly options expiration day' });
  if (m === 10 && d === 31) out.push({ k: 'halloween', text: '🎃 Happy Halloween' });
  if (m === 12 && (d === 24 || d === 25)) out.push({ k: 'xmas', text: '🎄 Merry Christmas' });
  if ((m === 12 && d === 31) || (m === 1 && d === 1)) out.push({ k: 'newyear', text: '🎆 Happy New Year' });
  if (m === 7 && d === 4) out.push({ k: 'july4', text: '🎇 Happy Fourth of July' });
  if (m === 11 && wd === 4 && d === nthWeekday(y, 11, 4, 4)) out.push({ k: 'thanks', text: '🦃 Happy Thanksgiving' });
  if (m === 2 && d === 14) out.push({ k: 'valentine', text: '💘 Happy Valentine\'s Day' });
  if (m === 3 && d === 14) out.push({ k: 'pi', text: '🥧 Happy Pi Day' });
  if (m === 4 && d === 15 && !hol) out.push({ k: 'tax', text: '🧾 Tax day: returns are due' });
  const lastTrading = (() => { let dd = new Date(Date.UTC(y, m, 0)).getUTCDate(); while (dd > 0 && !isTradingDay(y, m, dd)) dd--; return dd; })();
  if (d === lastTrading && !out.some(x => x.k === 'witch')) out.push({ k: 'monthend', text: '📅 Last trading day of the month' });
  return out;
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
  stepCheck(req); // a fresh Face ID check for real money and account changes, once Face ID is set up
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
  fees: jsonOr(t.fees), bid: t.bid == null ? null : +t.bid, ask: t.ask == null ? null : +t.ask, otype: t.otype || null, tif: t.tif || null, sess: t.sess || null, settle: tsMs(t.settle), liq: t.liq || null, via: t.via || null, broker: !!t.ext_id, imp: t.imp == null ? null : +t.imp });
function rulesJson(w) {
  const prof = profOf(w), sess = sessionAt(prof);
  return { profile: prof.key, name: prof.name, session: sess, sessionLabel: SESSION_LABEL[sess], pre: prof.pre, post: prof.post, overnight: prof.overnight, gtcDays: prof.gtcDays, minUsd: prof.minUsd,
    instant: w.acct === 'cash' ? 0 : prof.instant[w.gold ? 'gold' : 'std'], achDays: prof.achDays, settleNext: settleTime(prof), real: isReal(w) };
}
const brokerJson = () => BROKER ? { on: true, env: BROKER.env, feed: BROKER.feed, ok: BROKER.ok, err: BROKER.err, dataOk: BROKER.dataOk, wid: BROKER.wid, lastSync: BROKER.lastSync } : { on: false };
const alertJson = a => ({ ind: a.ind ? { ...jsonOr(a.ind, {}), title: indTitle(jsonOr(a.ind, {})) } : null, id: a.id, sym: a.sym, kind: a.kind || 'price', dir: a.dir, price: +a.price, pct: a.pct == null ? null : +a.pct, rep: a.rep, fired: a.fired, armed: a.armed, label: a.label, ts: tsMs(a.ts), last: tsMs(a.last_fired) });
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
    v: BUILD, appVersion: APP_VERSION, now: Date.now(), market: market(), mstate: mstateJson(), calNote: calendarNote(), days: dayEvents(), profile: profileJson(), user: APP_USER,
    wallet: w, wallets, pos, ord, lims, account, xfers, options: optSummary(wid), rules: rulesJson(wr), broker: brokerJson(), trd: trd.map(tradeJson), watch: watch.map(x => x.sym), alerts: alerts.map(alertJson),
    preds: preds.map(p => ({ id: p.id, sym: p.sym, dir: p.dir, start: +p.start_px, startTs: tsMs(p.start_ts), end: tsMs(p.end_ts) })),
    assets: assetsJson(syms), prices: pricesJson(), spark: sp, eq1d: eq,
    lists: { stocks: STOCK_DEFAULT, indexes: INDEXES, crypto: CRYPTO_DEFAULT, extra: [...userSyms.extra] },
    settings: S, unread, counts: { trades: +counts.trades, events: +counts.events, conditional: ord.length + lims.length, seasons: +seasons[0].n },
    stats: { day: dayChange(pos), diversity: diversity(pos), protected: protectedPct(wid, pos), invested: marketValue(pos) },
    feeds: { crypto: { ok: feeds.crypto.ok, last: feeds.crypto.last }, stocks: { ok: feeds.stocks.ok, last: feeds.stocks.last, key: !!FINNHUB_KEY, live: feeds.stocks.ws } },
    push: { key: VAPID_PUB, on: pushOn }, standby: standbyJson(), night: nightJson(), analyst: { key: !!AI_KEY, on: S.aiOn !== false, debate: S.aiDebate !== false, bench: AI_BENCH.map(m => ({ k: m.k, name: m.name, on: S[m.set] !== false })) }, councilNames: await getCouncilNames(),
  };
  if (tokenExpiry(req.token) - Date.now() < (TOKEN_DAYS - 30) * 864e5) out.token = newToken(); // keeps you logged in while you use the app
  return out;
});
GET('/api/band/:sym', auth, req => { const sym = cleanSym(req.params.sym); const h = haltOf(sym); return { sym, halt: h ? { kind: h.kind, why: h.why, until: h.until } : null, band: isCrypto(sym) || !luldOn() ? null : luldOf(sym) }; });
GET('/api/calendar', auth, () => { const y = etParts().y; return { days: calendarJson([y, y + 1]), today: market(), mstate: mstateJson(), note: calendarNote() }; });
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
  const plan = { ...o, auto: false, noImp: true };
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
        out.tax = { short: st, long: lt, washRisk, est: isIra(w) ? 0 : saleTaxEst(Math.max(0, st), Math.max(0, lt)), sheltered: isIra(w) };
      }
    }
  } catch (e) { out.err = e.message; }
  if (real && !crypto && otype === 'market' && sess !== 'regular') { out.queued = true; out.opensAt = market().opensAt; out.warn.push(`Market orders wait for the regular session. This will be placed at the open (${new Date(market().opensAt).toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'short', hour: 'numeric', minute: '2-digit' })} ET) at whatever the price is then.`); }
  if (real && !crypto && (otype === 'stop' || otype === 'stoplimit' || otype === 'trail') && sess !== 'regular') out.warn.push('Stop orders only trigger during regular hours (9:30 AM to 4:00 PM ET).');
  if (real && !crypto && out.quote && !out.quote.live) out.warn.push('Bid and ask are estimated from the last trade. Connect Alpaca in Settings for live stock quotes.');
  if (out.deep) out.warn.push('This order is bigger than the visible order book, so the price is an estimate.');
  const halt = haltOf(sym); if (halt) { out.halt = { kind: halt.kind, why: halt.why, until: halt.until }; if (otype === 'market') { out.queued = true; out.opensAt = halt.until; } }
  if (real && !crypto) { const fp = fracProblem(w, { sym, kind: otype, units: mode === 'qty' ? 'qty' : mode === 'pct' ? 'pct' : 'usd', amt: amount, tif: b.tif === 'day' ? 'day' : 'gtc', sess: b.sess }); if (fp) out.frac = fp; out.fracOk = !fp; }
  if (real && !crypto && otype === 'market' && out.improveNote !== false && profOf(w).pi) out.improve = 'Market orders usually fill slightly better than the bid or ask (price improvement). The receipt shows how much.';
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
  // the "are you sure?" checks, once per order: the app asks, you confirm
  if (!b.confirm) {
    const acct0 = await accountInfo(wid, { w }).catch(() => null), px0 = otype === 'limit' ? num(b.price) : otype === 'stoplimit' ? num(b.lpx) : priceOf(sym) || 0;
    const notional = mode === 'usd' ? amount : mode === 'qty' ? amount * px0 : 0;
    const checks = await sanityChecks(w, { sym, side, otype, price: b.price, lpx: b.lpx, notional, equity: acct0?.equity || 0 });
    if (side === 'sell' && w.acct === 'cash' && cashRulesOn()) {
      const p0 = (await positions(wid)).find(x => x.sym === sym), q = !p0 ? 0 : mode === 'qty' ? amount : mode === 'pct' ? p0.qty * Math.min(100, amount) / 100 : amount / (priceOf(sym) || p0.avg);
      const risk = await gfvRisk(pool, w, sym, q), n = risk ? +(await row("select count(*) n from cashviol where wid=$1 and kind='gfv' and ts > now() - interval '12 months'", [wid])).n : 0;
      if (risk) checks.unshift({ k: 'gfv', text: `Selling now is a good faith violation: you bought ${+risk.qty.toFixed(4)} of these shares with money from a sale that settles ${new Date(risk.settles).toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long' })}. You have ${n} in the last 12 months; at 3 the wallet can only use settled cash for 90 days. Wait until then to sell for free.` });
    }
    if (checks.length) return { confirm: true, checks };
  }
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
  const halt = haltOf(sym);
  if (halt) { const r = await addLimit(wid, sym, { side, kind: 'moo', amount, units, sl: b.sl, tp: b.tp }); return { queued: true, id: r.id, opensAt: halt.until, halt: { kind: halt.kind, why: halt.why, until: halt.until } }; }
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
  if (b.kind === 'ind') {
    const t = String(b.ind?.t || ''), def = IND_ALERTS[t]; if (!def) fail('Pick a chart signal');
    const v = def.def != null ? clamp(num(b.ind?.v, def.def), t === 'volSpike' ? 1.2 : 5, t === 'volSpike' ? 10 : 95) : null;
    await Q('insert into alert(sym,kind,dir,price,rep,label,ind) values($1,$2,$3,$4,$5,$6,$7)', [sym, 'ind', 'both', priceOf(sym) || 0, !!b.rep, String(b.label || '').slice(0, 80) || null, JSON.stringify({ t, v })]);
    await loadAlerts(); await refreshUserSyms(); indAlertJob().catch(() => {}); return { ok: true };
  }
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
const COLORS = ['mango', 'leaf', 'sky', 'grape', 'rose', 'slate'], PROFILE_KEYS = ['robinhood', 'alpaca', 'practice'], ACCTS = ['cash', 'instant', 'margin', 'ira', 'roth'], LOTS = ['fifo', 'lifo', 'hifo'];
GET('/api/wallets', auth, () => walletsSummary());
POST('/api/wallets', auth, async req => {
  const b = req.body, name = String(b.name || '').trim().slice(0, 30) || 'New wallet';
  const start = clamp(num(b.start, 10000), 100, 1e9);
  if (+(await row('select count(*) n from wallets')).n >= 25) fail('You can have up to 25 wallets');
  if (b.acct === 'margin' && isMinor()) fail('Margin accounts need you to be 18 or older. Pick a cash or instant account.');
  if (IRA_ACCTS.includes(b.acct)) { if (!PROFILE?.dob) fail('Add your date of birth first: IRA rules depend on your age.'); if (b.profile === 'practice') b.profile = 'robinhood'; }
  const r = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,gold,drip,lot_method,crypto_route,margin_max) values($1,$2,$3,$4,$4,(select coalesce(max(sort),0)+1 from wallets),$5,$6,$7,$8,$9,$10,$11) returning *`,
    [name, String(b.emoji || '💼').slice(0, 8), COLORS.includes(b.color) ? b.color : 'sky', start, PROFILE_KEYS.includes(b.profile) ? b.profile : 'robinhood', ACCTS.includes(b.acct) ? b.acct : 'instant', !!b.gold, !!b.drip, LOTS.includes(b.lotMethod) ? b.lotMethod : 'fifo', b.cryptoRoute === 'exchange' ? 'exchange' : 'mm', num(b.marginMax) > 0 ? num(b.marginMax) : null]);
  if (IRA_ACCTS.includes(r.acct)) await Q('update wallets set ira_opened=now(), ira_basis=$1 where id=$2', [r.acct === 'roth' ? start : 0, r.id]); // the starting money counts as a rollover, not this year's contribution
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
    if (acct !== w.acct && (IRA_ACCTS.includes(acct) || IRA_ACCTS.includes(w.acct))) fail('An IRA is its own kind of account. Open a new IRA wallet instead of switching this one.');
    if (acct === 'margin' && w.acct !== 'margin' && isMinor()) fail('Margin accounts need you to be 18 or older.');
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
    for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid', 'lots', 'realized', 'settle', 'xfer', 'splits', 'optpos', 'opttrd']) await c.query(`delete from ${t} where wid=$1`, [wid]);
    await c.query('update wallets set cash=$1, start_bal=$1, season_start=now(), margin_accrued=0, call_amt=0, call_since=null where id=$2', [start, wid]);
  });
  lastSnap.delete(wid); await loadOrders(); await snapshot(wid); await refreshUserSyms();
}
POST('/api/wallets/:id/reset', auth, async req => { const w = await walletRow(+req.params.id); await resetWallet(w.id, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
POST('/api/reset', auth, async req => { const wid = await activeWalletId(req); const w = await walletRow(wid); await resetWallet(wid, clamp(num(req.body.start, +w.start_bal), 100, 1e9)); });
DEL('/api/wallets/:id', auth, async req => {
  const w = await walletRow(+req.params.id);
  if (+(await row('select count(*) n from wallets where id<>$1', [w.id])).n === 0) fail("You can't delete your only wallet");
  await tx(async c => { for (const t of ['pos', 'ord', 'lim', 'trd', 'snap', 'divpaid', 'seasons', 'lots', 'realized', 'settle', 'xfer', 'splits', 'optpos', 'opttrd']) await c.query(`delete from ${t} where wid=$1`, [w.id]); await c.query('delete from wallets where id=$1', [w.id]); });
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
    if (IRA_ACCTS.includes(w.acct)) fail('An IRA balance only changes with deposits (which count toward the yearly limit) and withdrawals.');
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
  if (isIra(fw) && isIra(tw)) fail('Moving money between IRAs is a rollover with its own paperwork. In Mango, withdraw and deposit instead.');
  if (isIra(tw)) await checkContribution(pool, tw, a);
  let cost = null;
  await tx(async c => { await moveCash(c, from, -a, 'transfer out'); await moveCash(c, to, a, 'transfer in'); if (isIra(tw)) await noteContribution(c, tw, a, 'transfer in'); if (isIra(fw)) cost = await noteWithdrawal(c, fw, a); });
  if (cost) return { ok: true, ira: cost };
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
  version: BUILD, appVersion: APP_VERSION, up: Date.now() - BOOT, market: market(), finnhubKey: !!FINNHUB_KEY, wsMax: WS_MAX,
  feeds: { crypto: { ...feeds.crypto, subs: cbSubs.size }, stocks: { ...feeds.stocks, subs: [...fhSubs] } },
  queue: fhQueue.length, tracked: assets.size, broker: brokerJson(), clients: wss.clients.size, db: { schema: +(await kvGet('schema', '0')), snaps: +(await row('select count(*) n from snap')).n },
}));

// ---------- backup ----------
const BACKUP_TABLES = ['wallets', 'pos', 'ord', 'lim', 'trd', 'snap', 'extra', 'watch', 'alert', 'divpaid', 'seasons', 'preds', 'achv', 'meta', 'lots', 'realized', 'settle', 'xfer', 'splits', 'optpos', 'opttrd'];
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
    for (const t of ['wallets', 'ord', 'lim', 'trd', 'alert', 'seasons', 'preds', 'notifs', 'lots', 'realized', 'settle', 'xfer', 'optpos', 'opttrd']) await c.query(`select setval(pg_get_serial_sequence('${t}','id'), greatest((select coalesce(max(id),1) from ${t}),1))`);
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
  const b = req.body || {}, key = JSON.stringify([b.strategy, b.params, b.syms, b.years, b.capital, b.profile, b.maxPosPct, b.robust, b.addWeekly || 0]);
  const hit = btCache.get(key); if (hit && Date.now() - hit.t < 30 * 60e3) return hit.v;
  const v = await backtest(b); learnSeen('backtest');
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
function healthOf(bot, state, extra = {}) {
  const hh = state._health || {}, issues = [], p = etParts();
  if (hh.missing?.length) issues.push({ k: 'history', text: `No price history for ${hh.missing.slice(0, 5).join(', ')}${hh.missing.length > 5 ? ` and ${hh.missing.length - 5} more` : ''}, so ${hh.missing.length === 1 ? 'it was' : 'they were'} left out.` });
  if (hh.saved?.length) issues.push({ k: 'saved', text: `Used saved prices for ${hh.saved.slice(0, 5).join(', ')} because the price-history source didn't answer.`, minor: true });
  if (hh.noPrice?.length) issues.push({ k: 'price', text: `No live price for ${hh.noPrice.slice(0, 5).join(', ')} at decision time.`, minor: hh.noPrice.length < 3 });
  if (hh.news && hh.news.wanted && hh.news.checked < hh.news.wanted * 0.6) issues.push({ k: 'news', text: `The news check only reached ${hh.news.checked} of ${hh.news.wanted} symbols.`, minor: true });
  if (bot.status === 'running' && bot.lastDay) { const last = prevTradingDate(p.y, p.m, p.d); const onlyC = bot.syms.every(isCrypto); if (bot.lastDay < (onlyC ? ymd(addDays(p.y, p.m, p.d, -1)) : last)) issues.push({ k: 'missed', text: `Its last decision was on ${bot.lastDay}. It catches up at the next chance.` }); }
  if (extra.lastError && Date.now() - extra.lastError.ts < 2 * 864e5) issues.push({ k: 'error', text: `${extra.lastError.title}: ${extra.lastError.body || ''}`.slice(0, 220) });
  if (dataHealth.yahoo.ok === false && dataHealth.yahoo.fails > 2) issues.push({ k: 'yahoo', text: `Yahoo, the price-history source, is failing (${dataHealth.yahoo.err}). Saved prices are used for up to 10 days.` });
  if (FINNHUB_KEY && feeds.stocks.ok === false && market().state === 'open' && bot.syms.some(x => !isCrypto(x))) issues.push({ k: 'stocks', text: 'The live stock price feed is down right now.' });
  return { ok: !issues.some(i => !i.minor), issues, checked: hh.t || null };
}
// What a bot is doing right now, in plain words: one headline, one detail line, and a tone for its color.
function tradingDaysToMonthEnd() { const p = etParts(); let [y, m, d] = [p.y, p.m, p.d], n = 0; for (let i = 0; i < 25; i++) { const nx = nextTradingDay(y, m, d); if (nx[1] !== p.m) break; [y, m, d] = nx; n++; } return n; }
const listSyms = (a, n = 3) => a.slice(0, n).join(', ') + (a.length > n ? ` +${a.length - n}` : '');
function botActivity(bot, ctx) {
  const { holding, last, queued, state, invested, value, health, deciding } = ctx, st = STRATEGIES[bot.strategy] || {};
  const rows = last?.data?.rows || [], today = etDate(), ranToday = bot.lastDay === today;
  const skipped = rows.filter(r => (r.why || []).some(w => /^Skipped buying/.test(w)));
  const skipWhy = r => { const w = (r.why || []).find(x => /^Skipped buying/.test(x)) || ''; return /earnings/.test(w) ? 'earnings' : /serious news/.test(w) ? 'bad news' : /mood/.test(w) ? 'gloomy mood' : 'news'; };
  const skipText = skipped.length ? `Skipped ${skipped.slice(0, 2).map(r => `${r.sym} (${skipWhy(r)})`).join(', ')}${skipped.length > 2 ? ` +${skipped.length - 2}` : ''}` : '';
  const pctIn = value > 0 ? Math.round(invested / value * 100) : 0;
  const A = (k, text, sub, tone = 'up', pulse = false) => ({ k, text, sub, tone, pulse });
  // states that override everything
  if (deciding) return A('deciding', 'Deciding right now', `Reading the charts${bot.mandate.newsFilter ? ' and the news' : ''} for ${bot.syms.length} symbol${bot.syms.length === 1 ? '' : 's'}`, 'ac', true);
  if (bot.status === 'killed') return A('killed', 'Stopped itself', bot.killed ? bot.killed.split('. ')[0] + '.' : 'It hit its drop limit. Restart it when you are ready.', 'dn');
  if (ctx.broker && S.realHalt) return A('halted', 'Stopped by the kill switch', holding.length ? `Still holding ${listSyms(holding)}; resume in Alpaca controls` : 'Resume in Alpaca controls', 'dn');
  if (bot.status === 'paused') return A('paused', 'Paused', holding.length ? `Holding ${listSyms(holding)} but making no new decisions` : 'In cash, making no new decisions', '');
  if (health && !health.ok) { const miss = health.issues.find(i => i.k === 'missed'); if (miss) return A('catchup', 'Catching up on a missed day', 'It decides again at the next chance', 'ac'); }
  if (queued.length) return A('queued', `Waiting for the open to ${queued.every(q => q.side === 'buy') ? 'buy' : queued.every(q => q.side === 'sell') ? 'sell' : 'trade'} ${listSyms([...new Set(queued.map(q => q.sym))])}`, 'Its orders go in when the market opens', 'ac');
  if (!bot.lastDay) return A('new', 'Getting ready', 'Its first decision is coming up', 'ac');
  if (/daily limit/.test(last?.body || '') && ranToday) return A('cooldown', 'Taking a breather', 'It hit today\'s loss limit, so it is only selling until tomorrow', 'dn');
  // Autopilot
  if (st.auto) {
    const alloc = state._alloc || {}, n = Object.keys(alloc).length, risk = state._risk || {};
    if (!n) return A('sitout', 'Sitting out in cash', 'None of its strategies has a good enough record right now', '');
    if (risk.brake) return A('brake', 'Brake on, holding half', `It is ${Math.round(Math.abs(bot.peak ? value / bot.peak - 1 : 0) * 100)}% below its high and waits to recover`, 'dn');
    const top = (state._board || []).filter(t => t.alloc > 0).sort((a, b) => b.alloc - a.alloc)[0];
    const def = risk.stocksOff || risk.cryptoOff ? ` · safety switch on for ${[risk.stocksOff && 'stocks', risk.cryptoOff && 'crypto'].filter(Boolean).join(' and ')}` : risk.scale < 0.999 ? ` · holding ${Math.round(risk.scale * 100)}% of the plan, markets are choppy` : '';
    return A('auto', `Running ${n} strateg${n === 1 ? 'y' : 'ies'}, ${pctIn}% invested`, `Biggest: ${top ? top.label + ' ' + Math.round(top.alloc * 100) + '%' : '–'}${def}`, risk.stocksOff || risk.cryptoOff ? 'ac' : 'up');
  }
  // what the strategy is doing, by strategy
  const has = holding.length > 0, hs = listSyms(holding), regime = last?.data?.regime || '';
  const did = (last?.title || '').replace(/^(Daily decision|Manual run): /, ''), sub = skipText || (ranToday ? (/^No changes/.test(did) || !did ? 'Checked today, nothing to change' : 'Today: ' + did.replace(/^./, c => c.toLowerCase()).replace(/^bought/, 'bought')) : '');
  switch (bot.strategy) {
    case 'trend': return has ? A('hold', `Riding the uptrend in ${hs}`, sub || 'Trailing stops follow each one up', 'up') : A('wait', 'In cash, waiting for an uptrend', sub || `Nothing in its list is trending up right now`, '');
    case 'momentum': return has ? A('hold', `Holding this month's leaders: ${hs}`, sub || 'It reshuffles on the first trading day of next month', 'up') : A('wait', /below/.test(regime) ? 'Playing defense in cash' : 'In cash, nothing is rising', /below/.test(regime) ? `The market filter says stay out: ${regime}` : (sub || 'It checks again next month'), '');
    case 'rsi2': return has ? A('hold', `Waiting for ${hs} to bounce`, sub || 'It sells on the first bounce or after 10 days', 'up') : A('wait', 'Watching for a sharp dip to buy', sub || `Nothing in ${bot.syms.length} symbols has dipped hard enough yet`, '');
    case 'snapback': return has ? A('hold', `Waiting for ${hs} to snap back`, sub || 'It sells when the price gets back to its average', 'up') : A('wait', 'Watching for an overreaction', sub || 'It buys when something in an uptrend falls far below its band', '');
    case 'breakout': return has ? A('hold', `Riding the breakout in ${hs}`, sub || 'It sells if price falls to its 20-day low or its stop', 'up') : A('wait', 'Watching for a breakout', sub || 'It buys a close above the 55-day high', '');
    case 'drift': return has ? A('hold', `Riding the news jump in ${hs}`, sub || 'It holds each one about a month', 'up') : A('wait', 'Watching for big news jumps', sub || 'It buys a strong jump on heavy volume', '');
    case 'dual': return has ? (holding.every(x => SAFE.has(x)) ? A('hold', `Playing defense in ${hs}`, sub || 'Stocks are not beating bonds right now', 'ac') : A('hold', `Risk on: holding ${hs}`, sub || 'The strongest asset is beating bonds', 'up')) : A('wait', 'Sitting in cash', sub || 'Stocks and bonds are both falling', '');
    case 'lowvol': return has ? A('hold', `Holding the calm ones: ${hs}`, sub || 'It reshuffles next month', 'up') : A('wait', 'In cash, nothing calm is rising', sub || 'It checks again next month', '');
    case 'tom': { const left = tradingDaysToMonthEnd(); return has ? A('hold', `In the month-turn window with ${hs}`, 'It sells a few days into the new month', 'up') : A('wait', 'Waiting for the turn of the month', left <= 0 ? 'The window opens today' : `The window opens in ${left} trading day${left === 1 ? '' : 's'}`, ''); }
    case 'hold': return has ? A('hold', `Holding ${hs}`, sub || 'It rebalances once a month', 'up') : A('wait', 'Buying the mix next', sub || 'Waiting for prices', '');
    default: return has ? A('hold', `Holding ${hs}`, sub, 'up') : A('wait', 'In cash, watching', sub, '');
  }
}
async function botCard(b) {
  const bot = botJson(b), w = await row('select * from wallets where id=$1', [b.wid]);
  if (!w) return { ...bot, wallet: null };
  const snaps = (await rows("select ts, eq from snap where wid=$1 and ts > now() - interval '30 days' order by ts", [b.wid])).map(r => [tsMs(r.ts), +r.eq]);
  const pos = await positions(b.wid), value = +w.cash + marketValue(pos);
  const mine = pos.filter(p => bot.syms.includes(p.sym));
  const st0 = jsonOr(b.state, {}), perf = await flowPerf(b.wid, bot.created, value, st0._start0 || null).catch(() => null);
  const out = { ...bot, wallet: { id: w.id, name: w.name, emoji: w.emoji, color: w.color, profile: w.profile, archived: w.archived, broker: w.broker || null }, value, start: +w.start_bal, ret: perf ? perf.ret * 100 : (value / +w.start_bal - 1) * 100, added: perf?.added || 0, profit: perf ? perf.profit : value - +w.start_bal, day: dayChange(pos),
    holding: mine.map(p => p.sym), next: nextRunOf(bot), health: healthOf(bot, jsonOr(b.state, {}), { lastError: await row("select ts, title, body from bot_log where bot_id=$1 and kind='error' order by id desc limit 1", [b.id]).then(r => r && { ts: tsMs(r.ts), title: r.title, body: r.body }) }), invested: mine.reduce((t, p) => t + p.qty * (priceOf(p.sym) || p.avg), 0), cash: +w.cash, dd: bot.peak ? value / bot.peak - 1 : 0, spark: downsamplePts(snaps.concat([[Date.now(), value]]), 32).map(x => round(x[1], 2)) };
  out.decay = st0._decay ? { ...st0._decay, ack: st0._decayAck || null } : null;
  // plain-words status
  const lastR = await row("select * from bot_log where bot_id=$1 and kind='decision' order by id desc limit 1", [b.id]);
  out.activity = botActivity(bot, { holding: out.holding, last: lastR && logJson(lastR), queued: [...limOrders.values()].flat().filter(l => l.wid === b.wid && bot.syms.includes(l.sym)), state: jsonOr(b.state, {}), invested: out.invested, value, health: out.health, deciding: running.has(b.id), broker: !!w.broker });
  return out;
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
  const auto = STRATEGIES[card.strategy]?.auto ? { since: state._since || null, alloc: state._alloc || {}, board: state._board || [], boardT: state._boardT || null, month: state._allocMonth || null, risk: state._risk || null, report: (await row("select * from bot_log where bot_id=$1 and kind='report' order by id desc limit 1", [b.id]).then(r => r && logJson(r))) || null } : null;
  const sources = { prices: { ...dataHealth }, news: buzzStatus, stocks: { ok: feeds.stocks.ok, err: feeds.stocks.err || null }, crypto: { ok: feeds.crypto.ok }, broker: BROKER ? { env: BROKER.env, ok: BROKER.ok, err: BROKER.err } : null };
  let spy = null; try { spy = await dailyBars('SPY'); } catch (e) {}
  const report = STRATEGIES[card.strategy]?.auto ? null : await botReport(b, spy).catch(() => null), tune = state._tune || null;
  return { ...card, positions: pos, log, last: last ? logJson(last) : null, queued, auto, sources, report, tune, tunable: !STRATEGIES[card.strategy]?.auto && !!Object.keys(STRATEGIES[card.strategy]?.grid || {}).length, broker: !!card.wallet?.broker, live: !!card.wallet?.broker && BROKER?.env === 'live', strategyInfo: strategiesJson()[card.strategy] || null };
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
  if (FINNHUB_KEY && S.f_buzz !== false) earningsCal().catch(() => {});
  const params = cleanParams(b.strategy, b.params), mandate = cleanMandate({ ...(st.auto ? { maxTradesDay: 30, maxPosPct: 20 } : {}), ...(st.optLevel ? { maxPosPct: 100 } : {}), ...(b.mandate || {}) });
  let syms;
  if (st.auto) { // Autopilot trades the tryouts' symbols; prices are fetched in the background
    syms = apSyms(params.markets);
    for (const s of syms) if (!assets.has(s)) addAsset(s, AP_CRYPTO.includes(s) || CRYPTO_DEFAULT.includes(s) ? 'crypto' : 'stock');
    Promise.allSettled(syms.map(s => trackAsset(s, AP_CRYPTO.includes(s) ? 'crypto' : 'stock'))).catch(() => {});
  } else syms = withPark(await cleanBotSyms(b.syms, b.kinds || {}), params);
  let wid = +b.wallet || 0;
  if (!wid) {
    if (+(await row('select count(*) n from wallets')).n >= 25) fail('You have 25 wallets already. Pick an existing wallet for this bot or archive one.');
    const start = clamp(num(b.start, 10000), 100, 1e9);
    const w = await row(`insert into wallets(name,emoji,color,cash,start_bal,sort,profile,acct,lot_method,crypto_route) values($1,'🤖',$2,$3,$3,(select coalesce(max(sort),0)+1 from wallets),$4,$5,'fifo','mm') returning *`,
      [name.slice(0, 30), COLORS.includes(b.color) ? b.color : 'grape', start, PROFILE_KEYS.includes(b.profile) ? b.profile : 'robinhood', ACCTS.includes(b.acct) ? b.acct : 'instant']);
    wid = w.id;
    if (st.optLevel && !ageProblem() && (await quizPassed())[st.optLevel] && optCap(w) >= st.optLevel) await Q('update wallets set opt_level=$1 where id=$2', [st.optLevel, wid]); // its own wallet gets the options level it needs, if you've passed that quiz
    await loadWalletMeta(); await snapshot(wid);
  } else {
    const w = await walletRow(wid); if (w.archived) fail('That wallet is archived');
    if (w.broker) {
      if (!BROKER) fail('Add your Alpaca keys in Railway first.');
      if (!(mandate.budget > 0)) fail('On your Alpaca account, give the bot a budget: the most money it can use.');
      if (BROKER.env === 'live') {
        if (b.confirm !== 'REAL MONEY') fail('This is your real-money Alpaca account. Type REAL MONEY to let a bot trade it.');
        if (!(mandate.liveCap > 0)) fail('Set a real-money cap: the most the bot can ever have invested.');
        mandate.allowLive = true;
      }
    }
    const others = (await rows("select * from bots where wid=$1 and status<>'killed'", [wid])).map(botJson);
    const overlap = others.flatMap(o => o.syms.filter(s => syms.includes(s)));
    if (overlap.length) fail(`Another bot in this wallet already trades ${[...new Set(overlap)].join(', ')}. Give each bot its own symbols or its own wallet.`);
  }
  const acct = await accountInfo(wid);
  const r = await row('insert into bots(name,wid,strategy,params,syms,mandate,status,peak,day_start,day_date,state) values($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$10) returning *',
    [name, wid, b.strategy, JSON.stringify(params), JSON.stringify(syms), JSON.stringify(mandate), b.paused ? 'paused' : 'running', acct.equity, etDate(), JSON.stringify({ _spy0: priceOf('SPY') || null, _start0: acct.equity, _sv: (STRAT_VER[b.strategy] || []).slice(-1)[0]?.v || 1 })]);
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
  if (x.name !== undefined) {
    const nm = String(x.name).trim().slice(0, 30) || bot.name; set('name', nm);
    // A bot's own wallet is named after it; keep the two in step.
    if (x.renameWallet !== false) await Q('update wallets set name=$1 where id=$2 and name=$3', [nm, bot.wid, bot.name]).then(r => r.rowCount && loadWalletMeta()).catch(() => {});
  }
  if (x.params) { const np = cleanParams(bot.strategy, { ...bot.params, ...x.params }); set('params', JSON.stringify(np)); if (STRATEGIES[bot.strategy]?.auto) set('syms', JSON.stringify(apSyms(np.markets))); else if (!x.syms) { const ws = withPark(bot.syms, np); if (ws.length !== bot.syms.length) set('syms', JSON.stringify(ws)); } }
  if (x.mandate) {
    const nm = cleanMandate(x.mandate, bot.mandate), w = await row('select broker from wallets where id=$1', [bot.wid]);
    if (w?.broker && BROKER?.env === 'live' && nm.allowLive && !bot.mandate.allowLive && x.confirm !== 'REAL MONEY') fail('Type REAL MONEY to let this bot trade real money.');
    if (w?.broker && BROKER?.env === 'live' && nm.allowLive && !(nm.liveCap > 0)) fail('Set a real-money cap first.');
    set('mandate', JSON.stringify(nm));
  }
  if (x.syms && !STRATEGIES[bot.strategy]?.auto) set('syms', JSON.stringify(withPark(await cleanBotSyms(x.syms, x.kinds || {}), { ...bot.params, ...(x.params || {}) })));
  const changes = [];
  if (x.status && ['running', 'paused'].includes(x.status) && x.status !== bot.status) {
    set('status', x.status); changes.push(x.status === 'running' ? 'Resumed' : 'Paused');
    if (x.status === 'running') { const stt = jsonOr(b.state, {}); if (stt._decay && !stt._decay.ok) { stt._decayAck = Date.now(); set('state', JSON.stringify(stt)); } }
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

// Test-only helpers (only with MANGO_FAST_JOBS, which Railway never sets).
if (ENV.MANGO_FAST_JOBS) {
  POST('/api/test/weekly', auth, async () => { await kvSet('job_botweekly', ''); const p = etParts(); return weeklySummary({ ...p, y: 2026, m: 10, d: 23 }, '16:30', 'test-' + Date.now()); });
  POST('/api/test/night/:id', auth, async req => runBot(+req.params.id, { night: true }));
  POST('/api/test/nightsched', auth, async req => { const [h, mi] = String(req.body.hm).split(':').map(Number); const p = { ...etParts(), h, mi }; await nightScheduler(p, req.body.hm, etDate()); return { ok: true }; });
  POST('/api/test/clear-cache', auth, async req => { for (const k of [...cache.keys()]) if (!req.body?.prefix || k.startsWith(req.body.prefix)) cache.delete(k); return { ok: true }; });
}

// ---------- the Alpaca side: controls, kill switch, moving a bot over ----------
GET('/api/real', auth, async () => {
  if (BROKER && Date.now() - (BROKER.lastSync || 0) > 20e3) await brokerSync().catch(() => {});
  const w = BROKER ? await brokerWallet() : null, a = BROKER?.account;
  const bots = w ? (await rows('select * from bots where wid=$1 order by id', [w.id])) : [];
  return {
    on: !!BROKER, env: BROKER?.env || null, ok: !!BROKER?.ok, err: BROKER?.err || null, wid: w?.id || null, halted: !!S.realHalt, dailyLossPct: S.realDailyLossPct,
    account: a ? { equity: +a.equity, last: +a.last_equity, day: realDay(), cash: +a.cash, bp: +a.buying_power, status: a.status, number: a.account_number } : null,
    orders: (BROKER?.orders || []).length, bots: await Promise.all(bots.map(botCard)), maxOrder: ALP.maxOrder || null,
    blocked: (() => { try { realGuard(); return null; } catch (e) { return e.message; } })(),
  };
});
POST('/api/real/kill', auth, async req => {
  if (!BROKER) fail('Alpaca is not connected');
  await saveSettings({ realHalt: true });
  const w = await brokerWallet(), out = { paused: 0, cancelled: false, closed: false };
  if (w) { const r = await Q("update bots set status='paused' where wid=$1 and status='running'", [w.id]); out.paused = r.rowCount; }
  try { await apca('/v2/orders', { method: 'DELETE' }); out.cancelled = true; } catch (e) { out.cancelErr = e.message; }
  if (req.body?.flatten) { try { await apca('/v2/positions?cancel_orders=true', { method: 'DELETE' }); out.closed = true; } catch (e) { out.closeErr = e.message; } }
  brokerSyncSoon(); broadcast({ t: 'evt', e: 'settings' });
  await notify('real', `${BROKER.env === 'live' ? '🔴 LIVE' : '🔵 Alpaca'} · Kill switch on`, `All Alpaca trading is stopped. ${out.paused} bot${out.paused === 1 ? '' : 's'} paused, open orders cancelled${out.closed ? ', every position sold' : ''}. Resume it in Alpaca controls.`, { url: '/#/real' });
  return out;
});
POST('/api/real/resume', auth, async () => { await saveSettings({ realHalt: false }); broadcast({ t: 'evt', e: 'settings' }); return { ok: true }; });
POST('/api/bots/:id/promote', auth, async req => {
  if (!BROKER) fail('Connect Alpaca first: add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway, then connect it in Alpaca controls.');
  const w = await brokerWallet(); if (!w) fail('Connect your Alpaca account in Alpaca controls first.');
  const src = await row('select * from bots where id=$1', [+req.params.id]); if (!src) fail('That bot is gone', 404);
  const bot = botJson(src), b = req.body || {};
  if (src.wid === w.id) fail('This bot already trades on Alpaca.');
  const budget = num(b.budget), cap = num(b.liveCap);
  if (!(budget > 0)) fail('Give it a budget: the most money it can use on Alpaca.');
  if (BROKER.env === 'live') { if (b.confirm !== 'REAL MONEY') fail('Type REAL MONEY to move this bot to real money.'); if (!(cap > 0)) fail('Set a real-money cap.'); }
  if (BROKER.env === 'live' && S.promoGate !== false) { const g = await promoGate(src); if (!g.pass) fail(`It hasn't earned real money yet: ${g.checks.filter(c => !c.ok).map(c => `${c.label.replace(/^./, x => x.toLowerCase())} (${c.have})`).join('; ')}. Keep it on paper, or change the rules in Settings.`); }
  const others = (await rows("select * from bots where wid=$1 and status<>'killed'", [w.id])).map(botJson);
  const overlap = others.flatMap(o => o.syms.filter(s => bot.syms.includes(s)));
  if (overlap.length) fail(`Another Alpaca bot already trades ${[...new Set(overlap)].join(', ')}. Each bot needs its own symbols on one account.`);
  const card = await botCard(src);
  const mandate = cleanMandate({ ...bot.mandate, budget, liveCap: cap, allowLive: BROKER.env === 'live', notify: 'trades' });
  const r = await row('insert into bots(name,wid,strategy,params,syms,mandate,status,peak,day_start,day_date,state) values($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,$10) returning *',
    [String(b.name || bot.name).slice(0, 30), w.id, bot.strategy, JSON.stringify(bot.params), JSON.stringify(bot.syms), JSON.stringify(mandate), b.start ? 'running' : 'paused', (await accountInfo(w.id)).equity, etDate(),
      JSON.stringify({ _sv: (STRAT_VER[bot.strategy] || []).slice(-1)[0]?.v || 1, _spy0: priceOf('SPY') || null, _start0: budget, _from: { id: src.id, name: bot.name, ret: card.ret, days: Math.round((Date.now() - bot.created) / 864e5) }, ...(STRATEGIES[bot.strategy]?.auto ? Object.fromEntries(Object.entries(jsonOr(src.state, {})).filter(([k]) => ['_since', '_histStart', '_alloc', '_allocMonth'].includes(k))) : {}) })]);
  await botLog(r.id, 'info', `Moved over from paper`, `${bot.name} made ${pct(card.ret)} in ${Math.round((Date.now() - bot.created) / 864e5)} days on paper. On Alpaca it can use up to ${money(budget)}${cap ? `, with at most ${money(cap)} invested in real money` : ''}. The paper bot keeps running so you can compare.`);
  await refreshUserSyms();
  return { bot: await botCard(r) };
});

// =====================================================================
// Face ID (passkeys, WebAuthn) and backups
// Passkeys: the phone keeps a private key behind Face ID; the server stores only the public key and
// checks a signature over a one-time challenge. No password crosses the network.
// =====================================================================
// ---------- a small CBOR reader (enough for WebAuthn) ----------
function cborDecode(buf, pos = 0) {
  const head = buf[pos++], major = head >> 5, info = head & 31;
  const readLen = () => { if (info < 24) return info; if (info === 24) return buf[pos++]; if (info === 25) { const v = buf.readUInt16BE(pos); pos += 2; return v; } if (info === 26) { const v = buf.readUInt32BE(pos); pos += 4; return v; } if (info === 27) { const v = Number(buf.readBigUInt64BE(pos)); pos += 8; return v; } throw new Error('CBOR length'); };
  if (major === 0) { const v = readLen(); return [v, pos]; }
  if (major === 1) { const v = readLen(); return [-1 - v, pos]; }
  if (major === 2) { const n = readLen(); const v = buf.subarray(pos, pos + n); return [Buffer.from(v), pos + n]; }
  if (major === 3) { const n = readLen(); const v = buf.toString('utf8', pos, pos + n); return [v, pos + n]; }
  if (major === 4) { const n = readLen(), a = []; for (let i = 0; i < n; i++) { const [v, p] = cborDecode(buf, pos); a.push(v); pos = p; } return [a, pos]; }
  if (major === 5) { const n = readLen(), m = new Map(); for (let i = 0; i < n; i++) { const [k, p1] = cborDecode(buf, pos); const [v, p2] = cborDecode(buf, p1); m.set(k, v); pos = p2; } return [m, pos]; }
  if (major === 6) { readLen(); return cborDecode(buf, pos); }
  if (major === 7) { if (info === 20) return [false, pos]; if (info === 21) return [true, pos]; if (info === 22 || info === 23) return [null, pos]; if (info === 25) return [0, pos + 2]; if (info === 26) return [buf.readFloatBE(pos), pos + 4]; if (info === 27) return [buf.readDoubleBE(pos), pos + 8]; }
  throw new Error('CBOR type ' + major);
}
const b64u = b => Buffer.from(b).toString('base64url');
const fromB64u = s => Buffer.from(String(s || ''), 'base64url');
function coseToJwk(cose) {
  const kty = cose.get(1), alg = cose.get(3);
  if (kty === 2) { if (cose.get(-1) !== 1) throw new Error('Only P-256 keys are supported'); return { alg: alg || -7, jwk: { kty: 'EC', crv: 'P-256', x: b64u(cose.get(-2)), y: b64u(cose.get(-3)) } }; }
  if (kty === 3) return { alg: alg || -257, jwk: { kty: 'RSA', n: b64u(cose.get(-1)), e: b64u(cose.get(-2)) } };
  throw new Error('Unsupported key type');
}
function parseAuthData(ad) {
  const out = { rpIdHash: ad.subarray(0, 32), flags: ad[32], counter: ad.readUInt32BE(33) };
  if (out.flags & 0x40) {
    const credLen = ad.readUInt16BE(53), credId = ad.subarray(55, 55 + credLen);
    const [cose] = cborDecode(ad, 55 + credLen);
    out.credId = credId; out.cose = cose;
  }
  return out;
}
// The site's name for passkeys (its host name) and the exact origin the browser must report.
function rpOf(req) {
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  const name = host.replace(/:\d+$/, ''), local = /^(localhost|127\.0\.0\.1)$/.test(name);
  const proto = local ? 'http' : 'https';
  return { id: name, origins: [`${proto}://${host}`, `https://${name}`] };
}
const pkChallenges = new Map(); // challenge -> { kind, t }
function newChallenge(kind) { const c = b64u(crypto.randomBytes(32)); pkChallenges.set(c, { kind, t: Date.now() }); for (const [k, v] of pkChallenges) if (Date.now() - v.t > 5 * 60e3) pkChallenges.delete(k); return c; }
function checkClient(req, clientDataJSON, kind) {
  const cd = JSON.parse(fromB64u(clientDataJSON).toString('utf8')), rp = rpOf(req);
  if (cd.type !== (kind === 'reg' ? 'webauthn.create' : 'webauthn.get')) fail('That Face ID response was for something else.');
  const ch = pkChallenges.get(cd.challenge); if (!ch || (kind === 'reg') !== (ch.kind === 'reg')) fail('That Face ID request expired. Try again.', 401);
  pkChallenges.delete(cd.challenge);
  if (!rp.origins.includes(cd.origin)) fail('Face ID was used from a different website.', 401);
  return { rp, kind: ch.kind };
}
const passkeyCount = async () => +(await row('select count(*) n from passkeys')).n;
GET('/api/passkey/available', async () => ({ count: await passkeyCount() }));
GET('/api/passkeys', auth, async () => (await rows('select id, name, created, last_used from passkeys order by created')).map(r => ({ id: r.id, name: r.name, created: tsMs(r.created), last: tsMs(r.last_used) })));
POST('/api/passkey/register-options', auth, async req => {
  if (await passkeyCount() >= 10) fail('You can have up to 10 passkeys');
  const rp = rpOf(req), existing = (await rows('select id from passkeys')).map(r => ({ type: 'public-key', id: r.id }));
  return { challenge: newChallenge('reg'), rp: { name: 'Mango', id: rp.id }, user: { id: b64u(Buffer.from('mango-' + APP_USER)), name: APP_USER, displayName: 'Mango' }, exclude: existing };
});
POST('/api/passkey/register', auth, async req => {
  const b = req.body || {}, { rp } = checkClient(req, b.clientDataJSON, 'reg');
  const [att] = cborDecode(fromB64u(b.attestationObject));
  const ad = parseAuthData(att.get('authData'));
  if (!ad.rpIdHash.equals(crypto.createHash('sha256').update(rp.id).digest())) fail('That passkey is for a different website.');
  if (!(ad.flags & 0x01) || !(ad.flags & 0x04)) fail('Face ID didn\'t confirm it was you. Try again.');
  if (!ad.credId) fail('No key came back from Face ID.');
  const { alg, jwk } = coseToJwk(ad.cose);
  crypto.createPublicKey({ key: jwk, format: 'jwk' }); // throws if the key is malformed
  const id = b64u(ad.credId), name = String(b.name || 'This iPhone').slice(0, 40);
  await Q('insert into passkeys(id,pub,alg,counter,name,rp) values($1,$2,$3,$4,$5,$6) on conflict(id) do nothing', [id, JSON.stringify(jwk), alg, ad.counter, name, rp.id]);
  await refreshPasskeysOn();
  return { ok: true, id };
});
async function verifyAssertion(req, b, kind) {
  checkClient(req, b.clientDataJSON, kind);
  const pk = await row('select * from passkeys where id=$1', [b.id]); if (!pk) fail('That passkey isn\'t set up on Mango anymore. Log in with your password.', 401);
  const rp = rpOf(req), ad = fromB64u(b.authenticatorData), info = parseAuthData(ad);
  if (!info.rpIdHash.equals(crypto.createHash('sha256').update(rp.id).digest())) fail('That passkey is for a different website.', 401);
  if (!(info.flags & 0x01) || !(info.flags & 0x04)) fail('Face ID didn\'t confirm it was you.', 401);
  const data = Buffer.concat([ad, crypto.createHash('sha256').update(fromB64u(b.clientDataJSON)).digest()]);
  const key = crypto.createPublicKey({ key: JSON.parse(pk.pub), format: 'jwk' });
  const ok = crypto.verify('sha256', data, key, fromB64u(b.signature));
  if (!ok) fail('Face ID check failed.', 401);
  if (+pk.counter > 0 && info.counter > 0 && info.counter <= +pk.counter) fail('That passkey looks copied. Log in with your password.', 401);
  await Q('update passkeys set counter=$1, last_used=now() where id=$2', [info.counter, pk.id]);
  return pk;
}
POST('/api/passkey/login-options', async req => { rateLimit(clientIp(req)); return { challenge: newChallenge('login'), rpId: rpOf(req).id }; });
POST('/api/passkey/login', async req => {
  const ip = clientIp(req); rateLimit(ip);
  await verifyAssertion(req, req.body || {}, 'login');
  tries.delete(ip);
  return { token: newToken(), step: newStep() };
});
// Step-up: a fresh Face ID check, good for 10 minutes, for anything touching real money or the account.
const STEP_MS = 10 * 60e3;
const newStep = () => sign('s' + (Date.now() + STEP_MS));
function stepValid(t) { t = String(t || ''); const e = t.split('.')[0]; if (!/^s\d+$/.test(e) || +e.slice(1) < Date.now()) return false; return safeEq(sign(e), t); }
POST('/api/passkey/step-options', auth, async req => ({ challenge: newChallenge('step'), rpId: rpOf(req).id, allow: (await rows('select id from passkeys')).map(r => r.id) }));
POST('/api/passkey/step', auth, async req => { await verifyAssertion(req, req.body || {}, 'step'); return { step: newStep(), until: Date.now() + STEP_MS }; });
DEL('/api/passkeys/:id', auth, async req => { await Q('delete from passkeys where id=$1', [req.params.id]); await refreshPasskeysOn(); return { ok: true }; });
// Which requests need a fresh Face ID check (only once a passkey exists and the setting is on).
let passkeysOn = false;
async function refreshPasskeysOn() { passkeysOn = (await passkeyCount().catch(() => 0)) > 0; }
setInterval(() => refreshPasskeysOn(), 60e3);
function needsStep(req) {
  if (req.method === 'GET') return false;
  const p = req.path, w = walletMeta.get(+(req.query.w || req.body?.w || 0));
  if (/^\/api\/passkey\//.test(p) || p === '/api/login' || p === '/api/real/kill') return false; // the kill switch never waits
  if (p.startsWith('/api/passkeys/') || p === '/api/backup/restore' || p === '/api/password' || p === '/api/logout-all' || p === '/api/import' || p === '/api/real/resume' || p === '/api/broker/link' || /^\/api\/bots\/\d+\/promote$/.test(p)) return true;
  if (p === '/api/settings' && (req.body?.faceReal === false || req.body?.faceReal === 'false')) return true;
  if (p === '/api/settings' && (req.body?.promoGate === false || req.body?.promoGate === 'false')) return true; // switching off the real-money gate
  if (/^\/api\/bots\/\d+$/.test(p) && req.method === 'PATCH' && req.body?.mandate?.allowLive) return true;
  if (w?.broker && ['/api/trade', '/api/order', '/api/limit'].includes(p) && req.method === 'POST') return true;
  if (w?.broker && /^\/api\/order\/[^/]+$/.test(p) && req.method === 'PATCH') return true;
  return false;
}
function stepCheck(req) {
  if (!passkeysOn || S.faceReal === false || !needsStep(req)) return;
  if (stepValid(req.headers['x-step'])) return;
  throw Object.assign(new HttpError(428, 'Confirm with Face ID to do this.'), { step: true });
}

// ---------- backups ----------
const BACKUP_SKIP = new Set(['backups', 'bars', 'prices']); // price data comes back on its own
async function backupTables() { return (await rows("select table_name t from information_schema.tables where table_schema='public' and table_type='BASE TABLE' order by 1")).map(r => r.t).filter(t => !BACKUP_SKIP.has(t)); }
async function makeBackup() {
  const out = { app: 'mango', v: 1, made: new Date().toISOString(), schema: +(await kvGet('schema', '0')), tables: {} };
  for (const t of await backupTables()) {
    const cols = (await rows("select column_name c from information_schema.columns where table_schema='public' and table_name=$1 order by ordinal_position", [t])).map(r => r.c);
    const data = (await pool.query(`select * from "${t}"`)).rows;
    out.tables[t] = { cols, rows: data.map(r => cols.map(c => r[c] instanceof Date ? r[c].toISOString() : Buffer.isBuffer(r[c]) ? { $b: r[c].toString('base64') } : r[c])) };
  }
  return zlib.gzipSync(Buffer.from(JSON.stringify(out)), { level: 9 });
}
async function saveBackup(kind = 'auto', note = null) {
  const gz = await makeBackup();
  await Q('insert into backups(kind,size,data,note) values($1,$2,$3,$4)', [kind, gz.length, gz.toString('base64'), note]); // stored as text so any Postgres client handles it
  await Q("delete from backups where kind='auto' and id not in (select id from backups where kind='auto' order by id desc limit 7)");
  await Q("delete from backups where kind<>'auto' and id not in (select id from backups where kind<>'auto' order by id desc limit 5)");
  return gz.length;
}
async function restoreBackup(gz) {
  let d; try { d = JSON.parse(zlib.gunzipSync(gz).toString('utf8')); } catch (e) { fail('That file isn\'t a Mango backup.'); }
  if (d.app !== 'mango' || !d.tables) fail('That file isn\'t a Mango backup.');
  const cur = +(await kvGet('schema', '0'));
  if (d.schema > cur) fail('That backup is from a newer version of Mango. Update the app first.');
  await saveBackup('before-restore', 'Saved automatically before a restore');
  const tables = await backupTables();
  await tx(async c => {
    for (const t of tables) if (d.tables[t]) await c.query(`delete from "${t}"`);
    for (const [t, v] of Object.entries(d.tables)) {
      if (!tables.includes(t) || !v.rows.length) continue;
      const have = new Set((await c.query("select column_name c from information_schema.columns where table_schema='public' and table_name=$1", [t])).rows.map(r => r.c));
      const idx = v.cols.map((c, i) => [c, i]).filter(([c]) => have.has(c));
      for (let i = 0; i < v.rows.length; i += 400) {
        const chunk = v.rows.slice(i, i + 400), vals = [], ph = [];
        for (const r of chunk) { ph.push('(' + idx.map(([, k]) => { const x = r[k]; vals.push(x && typeof x === 'object' && x.$b ? Buffer.from(x.$b, 'base64') : x !== null && typeof x === 'object' ? JSON.stringify(x) : x); return '$' + vals.length; }).join(',') + ')'); }
        await c.query(`insert into "${t}"(${idx.map(([c]) => `"${c}"`).join(',')}) values ${ph.join(',')}`, vals);
      }
      // keep id counters past the restored rows
      const seq = have.has('id') ? (await c.query("select pg_get_serial_sequence($1, 'id') s", [t])).rows[0]?.s : null;
      if (seq) await c.query(`select setval($1, greatest((select coalesce(max(id), 0) from "${t}"), 1))`, [seq]);
    }
  });
  await migrate(); // brings an older backup up to today's version
  await loadSettings(); await loadOrders(); await refreshUserSyms(); await refreshPasskeysOn();
  return { tables: Object.keys(d.tables).length, made: d.made };
}
GET('/api/backups', auth, async () => ({ on: S.autoBackup !== false, list: (await rows('select id, ts, kind, size, note from backups order by id desc')).map(r => ({ id: r.id, ts: tsMs(r.ts), kind: r.kind, size: r.size, note: r.note })) }));
POST('/api/backups', auth, async () => ({ size: await saveBackup('manual', 'Made by you') }));
GET('/api/backup/download', auth, async (req, res) => {
  const saved = req.query.id ? (await row('select data from backups where id=$1', [+req.query.id]))?.data : null, gz = req.query.id ? (saved ? Buffer.from(saved, 'base64') : null) : await makeBackup();
  if (!gz) fail('That backup is gone', 404);
  send(req, res, 200, gz, 'application/gzip', { 'Content-Encoding': 'identity', 'Content-Disposition': `attachment; filename="mango-backup-${etDate()}.json.gz"` });
});
POST('/api/backup/restore', auth, async req => {
  const b = req.body || {};
  if (b.confirm !== 'RESTORE') fail('Type RESTORE to confirm. Everything in Mango is replaced by the backup.');
  let gz;
  if (b.id) { const d = (await row('select data from backups where id=$1', [+b.id]))?.data; gz = d ? Buffer.from(d, 'base64') : null; }
  else if (b.file) gz = Buffer.from(String(b.file), 'base64');
  if (!gz) fail('Pick a backup to restore');
  const r = await restoreBackup(gz);
  broadcast({ t: 'evt', e: 'settings' });
  return r;
});
// Every night around 3 AM New York time.
async function backupJob() {
  if (S.autoBackup === false) return;
  const p = etParts(), today = etDate();
  if (p.h < 3 || (await kvGet('job_backup')) === today) return;
  await kvSet('job_backup', today);
  const size = await saveBackup('auto');
  log(`Backup saved (${Math.round(size / 1024)} KB)`);
}

// =====================================================================
// The Council and Mango HQ
// Before a bot buys (or sells), six characters each look at one kind of real evidence and vote.
// Big Mango, the chair, announces what the bot actually does: its strategy and limits decide,
// and an optional "council veto" lets a majority NO stop a buy. Every vote is checked a week later,
// so "Who called it?" shows which kind of evidence has really been right.
// =====================================================================
const COUNCIL = [
  { k: 'surfer', name: 'Surfer', role: 'Trend', color: '#4EA8FF' },
  { k: 'rocket', name: 'Rocket', role: 'Momentum', color: '#FF7A45' },
  { k: 'scoop', name: 'Scoop', role: 'News', color: '#F5C542' },
  { k: 'shield', name: 'Shield', role: 'Risk', color: '#9AA6B8' },
  { k: 'prof', name: 'Professor', role: 'Value stats', color: '#3DDC97' },
  { k: 'doubt', name: 'Doubt', role: 'Contrarian', color: '#B48CFF' },
];
const pick = (arr, seed) => arr[Math.abs(seed) % arr.length];
const seedOf = s => [...String(s)].reduce((a, c) => a * 31 + c.charCodeAt(0), 7) >>> 0;
// Each member looks at its own evidence and says YES, NO or PASS, with a line in its own voice.
function councilVotes(sym, b, ctx = {}) {
  const i = b?.c?.length - 1, out = [], sd = seedOf(sym + (b?.d?.[i] || ''));
  if (!(i >= 30)) return COUNCIL.map(m => ({ k: m.k, vote: 'pass', conf: 0, line: 'Not enough history to say.', c: 'nodata' }));
  const c = b.c[i], s50 = IND.sma(b.c, 50, i), s200 = i >= 200 ? IND.sma(b.c, 200, i) : null, atr = IND.atr(b, 14, i, b.memo) || c * 0.02;
  const r63 = IND.ret(b.c, 63, i), r5 = IND.ret(b.c, 5, i), rsi = IND.rsi(b.c, 14, i, b.memo);
  const v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : null;
  const m20 = IND.sma(b.c, 20, i); let dev = 0; for (let k = i - 19; k <= i; k++) dev += (b.c[k] - m20) ** 2; const z = (c - m20) / (Math.sqrt(dev / 20) || 1);
  const bz = ctx.buzz, up = ctx.earnings;
  // Surfer: is it trending up?
  if (s200 && c > s200 && s50 > s200) out.push({ k: 'surfer', vote: 'yes', conf: clamp((c / s200 - 1) * 5, 0.3, 1), line: pick([`Above the 200-day by ${pf(c / s200 - 1, 0)}. Ride the wave.`, `Clean uptrend. Surf's up.`, `Fast average over slow, price over both. I'm in.`], sd), c: 'trend_up', v: { p: pf(c / s200 - 1, 0) } });
  else if (s200 && c < s200) out.push({ k: 'surfer', vote: 'no', conf: clamp((1 - c / s200) * 5, 0.3, 1), line: pick([`Under its 200-day. No wave to ride.`, `Downtrend. I don't paddle into rocks.`], sd), c: 'trend_down', v: { p: pf(c / s200 - 1, 0) } });
  else out.push({ k: 'surfer', vote: c > s50 ? 'yes' : 'pass', conf: 0.3, line: c > s50 ? 'Above the 50-day, trend is young.' : 'Flat water. Nothing to ride yet.', c: c > s50 ? 'trend_young' : 'trend_flat' });
  // Rocket: momentum, absolute and against the market
  const bm = ctx.benchRet63;
  if (r63 != null && r63 > 0.04 && (bm == null || r63 > bm)) out.push({ k: 'rocket', vote: 'yes', conf: clamp(r63 * 4, 0.3, 1), line: pick([`${pf(r63, 0)} in 3 months${bm != null ? `, beating the market's ${pf(bm, 0)}` : ''}. To the moon.`, `Strong 3-month run. Winners keep winning.`], sd + 1), c: 'mom_up', v: { p: pf(r63, 0), m: bm != null ? pf(bm, 0) : '' } });
  else if (r63 != null && r63 < -0.03) out.push({ k: 'rocket', vote: 'no', conf: clamp(-r63 * 4, 0.3, 1), line: `${pf(r63, 0)} in 3 months. That's no rocket.`, c: 'mom_down', v: { p: pf(r63, 0) } });
  else out.push({ k: 'rocket', vote: 'pass', conf: 0.2, line: r63 == null ? 'Too new to judge.' : `Only ${pf(r63, 1)} in 3 months. Meh.`, c: r63 == null ? 'nodata' : 'mom_flat', v: { p: r63 == null ? '' : pf(r63, 1) } });
  // Scoop: the news
  if (!bz || bz.count < 3) out.push({ k: 'scoop', vote: 'pass', conf: 0, line: pick(['Quiet out there. No story.', 'Nothing on the wire.'], sd + 2), c: 'news_quiet' });
  else if (bz.flags.includes('severe_news')) out.push({ k: 'scoop', vote: 'no', conf: 1, line: `Breaking: "${(bz.events.find(e => e.k === 'severe')?.title || 'serious trouble').slice(0, 70)}". Stay away.`, c: 'news_severe', v: { t: (bz.events.find(e => e.k === 'severe')?.title || 'serious trouble').slice(0, 70) } });
  else if (bz.sentiment <= -0.2) out.push({ k: 'scoop', vote: 'no', conf: clamp(-bz.sentiment * 2, 0.3, 1), line: `The mood is ${bz.sentiment.toFixed(2)} across ${bz.count} stories. People are gloomy.`, c: 'news_gloomy', v: { s: bz.sentiment.toFixed(2), n: String(bz.count) } });
  else if (bz.sentiment >= 0.15) out.push({ k: 'scoop', vote: 'yes', conf: clamp(bz.sentiment * 2, 0.3, 1), line: `Good press: ${bz.sentiment >= 0 ? '+' : ''}${bz.sentiment.toFixed(2)} across ${bz.count} stories${bz.events[0] ? `, ${bz.events[0].label.toLowerCase()}` : ''}.`, c: 'news_good', v: { s: '+' + bz.sentiment.toFixed(2), n: String(bz.count), e: bz.events[0] ? bz.events[0].label.toLowerCase() : '' } });
  else out.push({ k: 'scoop', vote: 'pass', conf: 0.2, line: `Mixed headlines (${bz.sentiment.toFixed(2)}). No scoop.`, c: 'news_mixed', v: { s: bz.sentiment.toFixed(2) } });
  // Shield: what could hurt
  const hot = v250 && v20 > v250 * 1.6, earnSoon = up && up.tradingDays <= 3;
  if (earnSoon) out.push({ k: 'shield', vote: 'no', conf: 0.9, line: `Earnings ${up.tradingDays <= 0 ? 'today' : `in ${up.tradingDays} day${up.tradingDays === 1 ? '' : 's'}`}. Coin flip with a big move. Pass.`, c: 'earnings', v: { d: String(Math.max(0, up.tradingDays)) } });
  else if (hot) out.push({ k: 'shield', vote: 'no', conf: clamp(v20 / v250 - 1, 0.3, 1), line: `Swinging ${(v20 / v250).toFixed(1)}x harder than usual. Too wild.`, c: 'wild', v: { x: (v20 / v250).toFixed(1) } });
  else if (ctx.weight > 0.3) out.push({ k: 'shield', vote: 'no', conf: 0.6, line: `That would be ${Math.round(ctx.weight * 100)}% of the wallet in one name. Too much.`, c: 'heavy', v: { p: Math.round(ctx.weight * 100) + '%' } });
  else out.push({ k: 'shield', vote: 'yes', conf: 0.4, line: pick(['Risk looks normal. Stop loss ready.', 'Calm enough. I can protect this one.'], sd + 3), c: 'safe' });
  // Professor: stretched or cheap?
  if (rsi != null && rsi > 75) out.push({ k: 'prof', vote: 'no', conf: clamp((rsi - 70) / 20, 0.3, 1), line: `RSI ${rsi.toFixed(0)}. Statistically overbought.`, c: 'overbought', v: { r: rsi.toFixed(0) } });
  else if (z < -1.5) out.push({ k: 'prof', vote: 'yes', conf: clamp(-z / 3, 0.3, 1), line: `${Math.abs(z).toFixed(1)} standard deviations under its average. Cheap, by the numbers.`, c: 'cheap', v: { z: Math.abs(z).toFixed(1) } });
  else if (z > 2) out.push({ k: 'prof', vote: 'no', conf: clamp(z / 3, 0.3, 1), line: `${z.toFixed(1)} standard deviations above average. Expensive today.`, c: 'expensive', v: { z: z.toFixed(1) } });
  else out.push({ k: 'prof', vote: rsi != null && rsi < 60 ? 'yes' : 'pass', conf: 0.3, line: `RSI ${rsi == null ? '–' : rsi.toFixed(0)}, ${z >= 0 ? '+' : ''}${z.toFixed(1)}σ. Fair price.`, c: 'fair', v: { r: rsi == null ? '–' : rsi.toFixed(0), z: (z >= 0 ? '+' : '') + z.toFixed(1) } });
  // Doubt: against the crowd
  if (r5 != null && r5 * c > 2.5 * atr) out.push({ k: 'doubt', vote: 'no', conf: 0.6, line: pick([`Up ${pf(r5, 0)} in a week. We're chasing.`, `Everyone already bought this. Who's left?`], sd + 4), c: 'chasing', v: { p: pf(r5, 0) } });
  else if (bz?.flags?.includes('attention_spike') && bz.sentiment > 0.2) out.push({ k: 'doubt', vote: 'no', conf: 0.5, line: `${bz.attention}x the usual hype. Smells like a top.`, c: 'hype', v: { x: String(bz.attention) } });
  else if (r5 != null && r5 < 0 && s200 && c > s200 && -r5 * c > 1.5 * atr) out.push({ k: 'doubt', vote: 'yes', conf: 0.6, line: `Everyone's scared after a ${pf(r5, 0)} week, but the trend is fine. I like it.`, c: 'fear_buy', v: { p: pf(r5, 0) } });
  else out.push({ k: 'doubt', vote: 'pass', conf: 0.2, line: pick(['Nothing to argue with. Suspicious.', 'I have no objection. For now.'], sd + 5), c: 'no_view' });
  return out.map(v => ({ ...v, conf: round(v.conf, 2) }));
}
const tally = votes => ({ yes: votes.filter(v => v.vote === 'yes').length, no: votes.filter(v => v.vote === 'no').length });
// Run a council session for one decision. Returns what the bot will record and show.
function councilSession(bot, d, series, buzz, earnings, m, capital) {
  const votes = councilVotes(d.sym, series[d.sym], { buzz, earnings, weight: d.want, benchRet63: series.SPY ? IND.ret(series.SPY.c, 63, series.SPY.c.length - 1) : null });
  const t = tally(votes);
  return { votes, yes: t.yes, no: t.no };
}
// ---------- the night crew ----------
const NIGHT_CREW = [
  { k: 'watch', name: 'Night Watch', role: 'Night shift · trend and risk', color: '#7A8CFF', night: true },
  { k: 'owl', name: 'Night Owl', role: 'Night shift · AI', color: '#C792EA', night: true, ai: true },
];
// the Night Watch: one rule-based vote that folds trend and risk together, plus the overnight move
function watchVote(sym, b) {
  const i = b?.c?.length - 1; if (!(i >= 30)) return { k: 'watch', vote: 'pass', conf: 0, line: 'Not enough history to say.', c: 'nodata' };
  const c = b.c[i], s50 = IND.sma(b.c, 50, i), s200 = i >= 200 ? IND.sma(b.c, 200, i) : null, r1 = IND.ret(b.c, 1, i), v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : null;
  if (r1 != null && r1 < -0.06) return { k: 'watch', vote: 'no', conf: clamp(-r1 * 8, 0.5, 1), line: `Down ${(-r1 * 100).toFixed(1)}% since yesterday's close. Not on my watch.`, c: 'nw_drop', v: { p: (-r1 * 100).toFixed(1) + '%' } };
  if (v250 && v20 > v250 * 1.6) return { k: 'watch', vote: 'no', conf: clamp(v20 / v250 - 1, 0.4, 1), line: `Swinging ${(v20 / v250).toFixed(1)}x harder than usual. Too rough for the night.`, c: 'nw_wild', v: { x: (v20 / v250).toFixed(1) } };
  if (c > s50 && (!s200 || c > s200)) return { k: 'watch', vote: 'yes', conf: s200 ? clamp((c / s200 - 1) * 5, 0.3, 1) : 0.4, line: `Above its 50-day${s200 ? ' and 200-day' : ''} averages. All quiet, trend intact.`, c: 'nw_up', v: {} };
  if (s200 && c < s200) return { k: 'watch', vote: 'no', conf: clamp((1 - c / s200) * 5, 0.3, 1), line: 'Under its 200-day average. The trend is down.', c: 'nw_down', v: {} };
  return { k: 'watch', vote: 'pass', conf: 0.2, line: 'Nothing moving either way.', c: 'nw_flat', v: {} };
}
async function nightSession(d, series, bz, up, ctx) {
  const b = series[d.sym], side = d.have > 0 && !(d.target > 0) ? 'sell' : 'buy';
  const votes = [watchVote(d.sym, b)], owl = await owlVote(d.sym, b, { side, target: d.target }, bz, up, ctx, votes[0]).catch(() => null);
  if (owl) votes.push(owl);
  const t = tally(votes); return { votes, yes: t.yes, no: t.no, night: true };
}
// ---------- weighted votes: some members count for more than others ----------
// A vote's weight comes from three things, multiplied and kept between 0.5 and 2 so nobody decides alone:
//   track record: members who have been right more often (20+ graded votes) count more, the rest less;
//   the situation: members count more when it's their area (news on a big news day, risk when it's wild);
//   confidence: a sure vote counts more than a shrug. A pass always weighs 0.
let memberRec = { at: 0, map: {} };
async function councilRecord() {
  if (Date.now() - memberRec.at < 10 * 60e3) return memberRec.map;
  const map = {};
  for (const r of await rows('select right_ from council where settled order by id desc limit 2000').catch(() => [])) for (const [k, ok] of Object.entries(jsonOr(r.right_, {}) || {})) { const m = map[k] ||= { calls: 0, right: 0 }; m.calls++; if (ok) m.right++; }
  memberRec = { at: Date.now(), map }; return map;
}
const recFactor = r => { if (!r || r.calls < 20) return { f: 1 }; const acc = (r.right + 5) / (r.calls + 10); return { f: clamp(1 + (acc - 0.5) * 3, 0.6, 1.6), acc: r.right / r.calls }; };
function situation(b, bz, up) {
  const i = b?.c?.length - 1; if (!(i >= 30)) return {};
  const c = b.c[i], s200 = i >= 200 ? IND.sma(b.c, 200, i) : null, v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : null, vr = v250 ? v20 / v250 : 1;
  const rsi = IND.rsi(b.c, 14, i, b.memo), r5 = IND.ret(b.c, 5, i), atr = IND.atr(b, 14, i, b.memo) || c * 0.02;
  return { news: !!bz && (bz.count >= 8 || bz.flags?.includes('severe_news') || bz.flags?.includes('attention_spike')), earn: !!up && up.tradingDays <= 5, quiet: !bz || bz.count < 3,
    wild: vr > 1.3, trend: vr < 1.05 && s200 && Math.abs(c / s200 - 1) > 0.05, stretched: rsi != null && (rsi > 70 || rsi < 30), crowd: (r5 != null && Math.abs(r5 * c) > 2.5 * atr) || !!bz?.flags?.includes('attention_spike') };
}
const SIT = [ // [condition, members, factor, why]
  ['news', ['hawk', 'scoop'], 1.35, 'big news day'], ['earn', ['hawk', 'scoop', 'shield', 'guard'], 1.25, 'earnings close'], ['quiet', ['hawk', 'scoop'], 0.8, 'no real news'],
  ['wild', ['shield', 'guard'], 1.35, 'market is wild'], ['wild', ['surfer', 'rocket'], 0.8, 'choppy, trends break'],
  ['trend', ['surfer', 'rocket', 'chart'], 1.25, 'clean trend'], ['stretched', ['prof', 'chart'], 1.2, 'price is stretched'], ['crowd', ['doubt'], 1.25, 'crowd piling in'],
];
function weighVotes(votes, sit, rec) {
  let wy = 0, wn = 0;
  for (const v of votes) {
    if (v.vote === 'pass') { v.w = 0; continue; }
    const why = [], R = recFactor(rec[v.k]); let f = R.f;
    if (R.acc != null && Math.abs(R.f - 1) > 0.04) why.push(`right ${Math.round(R.acc * 100)}%`);
    for (const [cond, ks, x, t] of SIT) if (sit[cond] && ks.includes(v.k)) { f *= x; why.push(t); }
    const cf = 0.6 + 0.8 * clamp(num(v.conf, 0.5), 0, 1); f *= cf; if (v.conf >= 0.8) why.push('very sure'); else if (v.conf < 0.35) why.push('unsure');
    v.w = round(clamp(f, 0.5, 2), 2); if (why.length) v.wr = why.slice(0, 3);
    if (v.vote === 'yes') wy += v.w; else wn += v.w;
  }
  return { wy: round(wy, 2), wn: round(wn, 2) };
}
const weightedOn = () => S.councilWeighted !== false;
// which way the council leans: by weight when that's on (and the votes have weights), otherwise by head count
const leansNo = s => weightedOn() && s.wy != null ? s.wn > s.wy : s.no > s.yes;
const tallyText = s => weightedOn() && s.wy != null ? `${s.wy.toFixed(1)}–${s.wn.toFixed(1)}` : `${s.yes}–${s.no}`;
function chairLine(d, s) {
  const t = tallyText(s);
  if (d.action === 'buy' || d.action === 'add') return leansNo(s) ? `Overruled ${t}: the strategy says buy, and it's within the limits.` : `Approved ${t}. Buying ${Math.round(d.target * 100)}% of the wallet.`;
  if (d.action === 'sell' || d.action === 'trim') { const w = (d.why || []).find(x => /^(Selling|Trimming)/.test(x)) || d.why?.[0] || d.note || 'The strategy says it is time.'; return `${d.action === 'trim' ? 'Trimming by half' : 'Selling'}. ${w.replace(/^(Selling|Trimming by half): /, '').replace(/^./, c => c.toUpperCase())}`; }
  const skip = (d.why || []).find(w => /^Skipped|^Not buying|^No new buys/.test(w));
  if (skip) return `Rejected ${t}. ${skip.replace(/^Skipped buying: /, '').replace(/^./, c => c.toUpperCase())}`;
  return `Holding ${t}. Nothing to do.`;
}
async function saveCouncil(botId, d, s, px) {
  const settle = Date.now() + 7 * 864e5;
  const r = await row('insert into council(bot_id,sym,action,price,votes,yes,no,chair,settle_at) values($1,$2,$3,$4,$5,$6,$7,$8,$9) returning id', [botId, d.sym, d.action === 'hold' && d.want > d.target + 1e-6 ? 'skip' : d.action, px, JSON.stringify(s.votes), s.yes, s.no, s.chair, new Date(settle)]).catch(e => { warn('council', e.message); return null; });
  if (r) broadcast({ t: 'evt', e: 'hq', ev: { kind: 'council', botId, id: r.id, sym: d.sym } });
  return r?.id;
}
// A week later: was each member right? YES is right if the price went up, NO if it didn't.
async function settleCouncil() {
  for (const r of await rows("select * from council where not settled and settle_at <= now() order by id limit 200")) {
    const now = priceOf(r.sym); if (!now || !(+r.price > 0)) continue;
    const ret = now / +r.price - 1, votes = jsonOr(r.votes, []), right = {};
    for (const v of votes) if (v.vote !== 'pass') right[v.k] = v.vote === 'yes' ? ret > 0 : ret <= 0;
    await Q('update council set settled=true, result=$1, right_=$2 where id=$3', [round(ret, 5), JSON.stringify(right), r.id]);
    memberRec.at = 0; // track records changed
  }
}
const wsum = (votes, side) => votes.some(v => v.w != null) ? round(votes.filter(v => v.vote === side).reduce((t, v) => t + (v.w ?? 1), 0), 2) : null;
const councilJson = r => ({ wy: wsum(jsonOr(r.votes, []), 'yes'), wn: wsum(jsonOr(r.votes, []), 'no'), id: r.id, ts: tsMs(r.ts), botId: r.bot_id, sym: r.sym, action: r.action, price: +r.price, votes: jsonOr(r.votes, []), yes: r.yes, no: r.no, chair: r.chair, settled: r.settled, result: r.result == null ? null : +r.result, right: jsonOr(r.right_, null), settleAt: tsMs(r.settle_at) });
GET('/api/council', auth, async req => {
  const bot = +req.query.bot || null, lim = clamp(num(req.query.limit, 20), 1, 100), before = +req.query.before || 2e9;
  return (await rows(`select * from council where id < $1 ${bot ? 'and bot_id=$3' : ''} order by id desc limit $2`, bot ? [before, lim, bot] : [before, lim])).map(councilJson);
});
GET('/api/council/board', auth, async req => {
  const bot = +req.query.bot || null;
  const list = (await rows(`select * from council where settled ${bot ? 'and bot_id=$1' : ''} order by id desc limit 2000`, bot ? [bot] : [])).map(councilJson);
  const members = [...COUNCIL, ...AI_ALL, ...NIGHT_CREW].filter(m => (!m.ai && !m.night) || aiMembersOn().includes(m) || list.some(s => s.right && m.k in s.right)).map(m => { let calls = 0, right = 0; for (const s of list) if (s.right && m.k in s.right) { calls++; if (s.right[m.k]) right++; } return { ...m, calls, right, acc: calls ? right / calls : null }; });
  // What a council veto would have done: buys it would have blocked, and skips it wanted to buy.
  const blocked = list.filter(s => (s.action === 'buy' || s.action === 'add') && leansNo(s)), wanted = list.filter(s => s.action === 'skip' && !leansNo(s) && (s.wy != null && weightedOn() ? s.wy > s.wn : s.yes > s.no));
  const avg = a => a.length ? a.reduce((t, s) => t + s.result, 0) / a.length : null;
  const pending = +(await row(`select count(*) n from council where not settled ${bot ? 'and bot_id=$1' : ''}`, bot ? [bot] : [])).n;
  // scorecard: did the weighted majority call the move better than counting heads?
  const card = { weighted: { calls: 0, right: 0 }, heads: { calls: 0, right: 0 } };
  for (const s of list) { if (s.result == null || s.wy == null) continue; const up = s.result > 0;
    if (s.yes !== s.no) { card.heads.calls++; if ((s.yes > s.no) === up) card.heads.right++; }
    if (s.wy !== s.wn) { card.weighted.calls++; if ((s.wy > s.wn) === up) card.weighted.right++; } }
  const rec = await councilRecord(); for (const m of members) m.weight = round(recFactor(rec[m.k]).f, 2);
  return { weighted: weightedOn(), card, members: members.sort((a, b) => (b.acc ?? -1) - (a.acc ?? -1)), settled: list.length, pending, veto: { blocked: blocked.length, blockedAvg: avg(blocked), wanted: wanted.length, wantedAvg: avg(wanted) }, chair: { calls: list.filter(s => s.action !== 'hold').length } };
});
GET('/api/council/members', auth, () => councilMembers());

// ---------- Mango HQ: everything the floor needs in one call ----------
GET('/api/hq', auth, async () => {
  const list = await rows("select * from bots order by id");
  const cards = await Promise.all(list.map(botCard));
  const events = (await rows("select l.*, b.name bname from bot_log l join bots b on b.id=l.bot_id where l.ts > now() - interval '3 days' order by l.id desc limit 80")).map(r => { const j = logJson(r), d = j.data || {}; return { ...j, botId: r.bot_id, bot: r.bname, side: d.side || null, qty: d.fill?.qty ?? null, price: d.fill?.price ?? null, pnl: d.fill?.pnl ?? null, data: undefined }; });
  const council = (await rows('select * from council order by id desc limit 12')).map(councilJson);
  let totalPL = 0, day = 0; for (const c of cards) { totalPL += (c.value || 0) - (c.start || 0); day += c.day?.change || 0; }
  const spy = prices.get('SPY'), btc = prices.get('BTC');
  const held = [...new Set(cards.flatMap(c => c.holding || []))].slice(0, 16);
  const tick = ['SPY', 'QQQ', 'BTC', ...held].filter((s, i, a) => a.indexOf(s) === i).map(s => { const q = prices.get(s); return q?.p ? { sym: s, p: q.p, ch: q.prev ? (q.p / q.prev - 1) * 100 : 0 } : null; }).filter(Boolean);
  const trades = +(await row("select count(*) n from bot_log where kind='trade' and ts > $1", [new Date(etToUtc(...etDate().split('-').map(Number), 0, 0))])).n;
  // levels: experience from trades, winning sales, days on the job and profit
  const tc = new Map((await rows("select wid, count(*) filter (where side in ('buy','sell')) n, count(*) filter (where side='sell' and pnl > 0) w from trd group by wid")).map(r => [r.wid, r]));
  const xpOf = (c, b) => { const t = tc.get(b.wid), days = Math.max(0, (Date.now() - tsMs(b.created)) / 864e5); return Math.round((+t?.n || 0) * 4 + (+t?.w || 0) * 10 + days * 2 + Math.max(0, c.ret || 0) * 5); };
  const lvOf = xp => { let lv = 1, need = 40; while (xp >= need && lv < 30) { xp -= need; lv++; need = Math.round(need * 1.25); } return { lv, into: xp, need }; };
  let earnToday = []; if (FINNHUB_KEY) { try { const cal = await Promise.race([earningsCal(), sleep(1500).then(() => ({}))]); earnToday = held.filter(sym => cal[sym]?.date === etDate()); } catch (e) {} }
  return {
    bots: cards.map(c => ({ id: c.id, name: c.name, strategy: c.strategy, status: c.status, activity: c.activity, holding: c.holding, value: c.value, ret: c.ret, day: c.day, spark: c.spark, broker: !!c.wallet?.broker, auto: !!STRATEGIES[c.strategy]?.auto, deciding: running.has(c.id), next: c.next, created: tsMs(list.find(x => x.id === c.id)?.created), xp: xpOf(c, list.find(x => x.id === c.id)), ...(() => { const L = lvOf(xpOf(c, list.find(x => x.id === c.id))); return { level: L.lv, lvInto: L.into, lvNeed: L.need }; })(), tryouts: STRATEGIES[c.strategy]?.auto ? (jsonOr(list.find(x => x.id === c.id)?.state, {})._board || []).map(t => ({ k: t.k, label: t.label, alloc: t.alloc, eligible: t.eligible })) : null })),
    events, council, members: councilMembers(), night: { ...nightJson(), crew: nightCrewOn() }, names: await getCouncilNames(), tick, trades,
    market: { ...market(), spy: spy?.prev ? (spy.p / spy.prev - 1) * 100 : null, btc: btc?.prev ? (btc.p / btc.prev - 1) * 100 : null },
    vault: { total: totalPL, day }, earnToday, alarm: { halt: !!S.realHalt, killed: cards.filter(c => c.status === 'killed').map(c => c.name), brake: cards.filter(c => c.activity?.k === 'brake').map(c => c.name), cooldown: cards.filter(c => c.activity?.k === 'cooldown').map(c => c.name) },
  };
});

// ---------- your own names for the Council (and the chair), optionally per theme ----------
const NAME_KEYS = ['surfer', 'rocket', 'scoop', 'shield', 'prof', 'doubt', 'ai', 'hawk', 'guard', 'chart', 'watch', 'owl', 'chair'], NAME_THEMES = ['office', 'space', 'village', 'island', 'underwater', 'castle', 'cyber', 'mars'];
let councilNames = null;
async function getCouncilNames() { if (!councilNames) councilNames = jsonOr(await kvGet('council_names').catch(() => null), null) || { sync: true, all: {}, themes: {} }; return councilNames; }
const cleanName = v => String(v ?? '').replace(/[\u0000-\u001f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 24);
const cleanNameSet = o => { const out = {}; for (const k of NAME_KEYS) { const v = cleanName(o?.[k]); if (v) out[k] = v; } return out; };
GET('/api/council/names', auth, () => getCouncilNames());
POST('/api/council/names', auth, async req => {
  const b = req.body || {}, cur = await getCouncilNames(), next = { sync: b.sync !== false, all: b.all !== undefined ? cleanNameSet(b.all) : cur.all, themes: { ...cur.themes } };
  if (b.themes) for (const t of NAME_THEMES) if (b.themes[t] !== undefined) next.themes[t] = cleanNameSet(b.themes[t]);
  councilNames = next; await kvSet('council_names', JSON.stringify(next));
  broadcast({ t: 'evt', e: 'settings' });
  return next;
});
// =====================================================================
// Standby copy: a second Mango on another host (e.g. Render's free plan) that you only switch on when
// your main server is down. Set MANGO_STANDBY=1 on that host. While it's off, bots don't trade and no
// Alpaca orders go out, so the two copies can never trade twice. While it's on, it pings its own public
// address every 10 minutes so free hosts don't put it to sleep.
// =====================================================================
const STANDBY = /^(1|true|yes)$/i.test(ENV.MANGO_STANDBY || '');
const SELF_URL = (ENV.RENDER_EXTERNAL_URL || ENV.PUBLIC_URL || (ENV.KOYEB_PUBLIC_DOMAIN ? 'https://' + ENV.KOYEB_PUBLIC_DOMAIN : '')).replace(/\/$/, '');
const standbyPaused = () => STANDBY && S.standbyActive !== true;
const standbyJson = () => ({ standby: STANDBY, active: STANDBY ? S.standbyActive === true : true, since: S.standbyActiveAt || null, url: SELF_URL || null, awake: standbyAwake });
let standbyAwake = null;
async function standbyPing() {
  if (!STANDBY || standbyPaused() || !SELF_URL) return;
  try { const r = await fetch(SELF_URL + '/api/health', { signal: AbortSignal.timeout(15e3) }); standbyAwake = { ok: r.ok, ts: Date.now() }; } catch (e) { standbyAwake = { ok: false, ts: Date.now(), err: e.message }; }
}
GET('/api/standby', auth, () => standbyJson());
POST('/api/standby', auth, async req => {
  if (!STANDBY) fail('This is your main server, not a standby copy.');
  const on = !!req.body.on;
  await saveSettings({ standbyActive: on, standbyActiveAt: on ? Date.now() : 0 });
  log(on ? 'Standby switched ON: bots and keep-awake running' : 'Standby switched OFF: bots paused, allowed to sleep');
  if (on) standbyPing();
  broadcast({ t: 'evt', e: 'settings' });
  return standbyJson();
});
// =====================================================================
// The AI members of the Council, backed by Claude (Haiku). The Analyst For each buy or sell the bots consider, it
// reads the real evidence (trend, momentum, RSI, volatility, earnings, headlines) and votes like the
// others. Its vote counts in the tally and the council veto, it can shrink a buy (never grow one), and
// "Who called it?" grades it a week later like everyone else. Needs ANTHROPIC_API_KEY. If the key is
// missing, the daily cap is hit, or the reply is unusable, it simply passes and the bot carries on.
// =====================================================================
const AI_KEY = (ENV.ANTHROPIC_API_KEY || '').trim();
const AI_MODEL = (ENV.ANALYST_MODEL || 'claude-haiku-5-5').trim();
const AI_BASE = (ENV.ANTHROPIC_BASE || 'https://api.anthropic.com').replace(/\/$/, '');
const AI_PRICE = { in: +ENV.ANALYST_PRICE_IN || 0.10, out: +ENV.ANALYST_PRICE_OUT || 0.50 }; // dollars per million tokens
// The AI bench: three specialists who each see only their own slice of the evidence, and the Analyst,
// who goes last, reads every vote (rule-based and AI), picks the argument it disagrees with most,
// rebuts it out loud, and then votes. All share one key and one daily cap.
const ANALYST = { k: 'ai', name: 'Analyst', role: 'AI · debate', color: '#FF9F1C', ai: true };
const AI_BENCH = [
  { k: 'hawk', name: 'News Hawk', role: 'AI · news', color: '#E85D75', ai: true, set: 'aiHawk' },
  { k: 'guard', name: 'Risk Officer', role: 'AI · risk', color: '#5BC0BE', ai: true, set: 'aiGuard' },
  { k: 'chart', name: 'Chartist', role: 'AI · charts', color: '#C3E88D', ai: true, set: 'aiChart' },
];
const AI_ALL = [ANALYST, ...AI_BENCH], AI_KEYS = AI_ALL.map(m => m.k);
const analystReady = () => !!AI_KEY && S.aiOn !== false;
const benchOn = m => analystReady() && S[m.set] !== false;
const aiMembersOn = () => analystReady() ? [ANALYST, ...AI_BENCH.filter(benchOn)] : [];
const councilMembers = () => [...COUNCIL, ...aiMembersOn()];
let aiUsage = null;
async function aiUse() { if (!aiUsage) aiUsage = jsonOr(await kvGet('ai_usage').catch(() => null), {}) || {}; const d = etDate(), m = d.slice(0, 7); if (aiUsage.day !== d) Object.assign(aiUsage, { day: d, calls: 0, inT: 0, outT: 0, errors: 0, by: {} }); if (aiUsage.month !== m) Object.assign(aiUsage, { month: m, mCalls: 0, mIn: 0, mOut: 0 }); aiUsage.by ||= {}; return aiUsage; }
const aiCost = (i, o) => (i * AI_PRICE.in + o * AI_PRICE.out) / 1e6;
const aiCache = new Map(); // k|sym|day|side -> vote, so several bots asking about the same stock cost one call each
const f1 = (x, d = 1) => x == null || !Number.isFinite(x) ? 'n/a' : (+x).toFixed(d);
const pcf = x => x == null || !Number.isFinite(x) ? 'n/a' : (x * 100).toFixed(1) + '%';
const proposal = (sym, d, ctx) => `Symbol: ${sym}${isCrypto(sym) ? ' (crypto)' : ''}
Proposed: ${d.side === 'buy' ? `BUY up to ${Math.round(d.target * 100)}% of the wallet` : 'SELL the position'} (bot strategy: ${ctx.strategy})`;
const headlines = (bz, n = 6) => (bz?.items || []).filter(x => x.src === 'news' || x.title).slice(0, n).map(x => `- ${String(x.title || x.headline || '').slice(0, 140)}${x.score != null ? ` (mood ${f1(x.score, 2)})` : ''}`).join('\n') || '- none';
function aiFacts(sym, b, d, bz, up, ctx) {
  const i = b.c.length - 1, c = b.c[i], s50 = IND.sma(b.c, 50, i), s200 = i >= 200 ? IND.sma(b.c, 200, i) : null, rsi = IND.rsi(b.c, 14, i, b.memo);
  const v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : null;
  return `${proposal(sym, d, ctx)}
Price ${c.toFixed(2)}. 5-day ${pcf(IND.ret(b.c, 5, i))}, 1-month ${pcf(IND.ret(b.c, 21, i))}, 3-month ${pcf(IND.ret(b.c, 63, i))}${ctx.bench != null ? ` (S&P 500 3-month ${pcf(ctx.bench)})` : ''}
50-day avg ${f1(s50, 2)}, 200-day avg ${f1(s200, 2)}. RSI(14) ${f1(rsi, 0)}. Volatility now vs normal: ${v250 ? f1(v20 / v250, 2) + 'x' : 'n/a'}
Earnings: ${up ? `in ${up.tradingDays} trading day(s)` : 'none in the next 2 weeks'}
News mood ${bz ? f1(bz.sentiment, 2) : 'n/a'} across ${bz?.count || 0} stories${bz?.flags?.length ? `, flags: ${bz.flags.join(', ')}` : ''}
Headlines:
${headlines(bz)}`;
}
// each specialist only gets its own evidence, so they genuinely disagree
const BENCH_FACTS = {
  hawk: (sym, b, d, bz, up, ctx) => `${proposal(sym, d, ctx)}
Price move: 5-day ${pcf(IND.ret(b.c, 5, b.c.length - 1))}
Earnings: ${up ? `in ${up.tradingDays} trading day(s)` : 'none in the next 2 weeks'}
News mood ${bz ? f1(bz.sentiment, 2) : 'n/a'} across ${bz?.count || 0} stories${bz?.flags?.length ? `, flags: ${bz.flags.join(', ')}` : ''}
Headlines (newest first):
${headlines(bz, 10)}`,
  guard: (sym, b, d, bz, up, ctx) => { const i = b.c.length - 1, c = b.c[i], n = Math.min(i, 63), hi = Math.max(...b.c.slice(i - n, i + 1)); let worst = 0; for (let j = Math.max(1, i - 60); j <= i; j++) worst = Math.min(worst, b.c[j] / b.c[j - 1] - 1);
    const v20 = IND.vol(b.c, 20, i), v250 = i > 252 ? IND.vol(b.c, 250, i) : null, atr = IND.atr(b, 14, i, b.memo);
    return `${proposal(sym, d, ctx)}
Position: currently ${pcf(ctx.have)} of the wallet, would go to ${pcf(d.target)}. Max allowed per position ${ctx.maxPos != null ? ctx.maxPos + '%' : 'n/a'}. Positions held: ${ctx.positions ?? 'n/a'}. Stop losses ${ctx.stops ? 'on' : 'off'}.
Volatility (20-day, yearly) ${pcf(v20)}${v250 ? `, normal ${pcf(v250)}` : ''}. Average daily range ${atr ? pcf(atr / c) : 'n/a'} of price.
Down ${pcf(1 - c / hi)} from its 3-month high. Worst single day in 60 days: ${pcf(worst)}.
Earnings: ${up ? `in ${up.tradingDays} trading day(s)` : 'none in the next 2 weeks'}`; },
  chart: (sym, b, d, bz, up, ctx) => { const i = b.c.length - 1, k = Math.min(30, i), closes = b.c.slice(i - k, i + 1).map(x => +x.toPrecision(5)).join(', '), vols = b.v ? b.v.slice(i - 20, i + 1) : [], vAvg = vols.length ? vols.slice(0, -1).reduce((t, x) => t + x, 0) / Math.max(1, vols.length - 1) : 0;
    return `${proposal(sym, d, ctx)}
Last ${k + 1} daily closes (oldest first): ${closes}
20-day high ${f1(Math.max(...b.h.slice(i - 19, i + 1)), 2)}, low ${f1(Math.min(...b.l.slice(i - 19, i + 1)), 2)}. 50-day avg ${f1(IND.sma(b.c, 50, i), 2)}, 200-day avg ${f1(i >= 200 ? IND.sma(b.c, 200, i) : null, 2)}. RSI(14) ${f1(IND.rsi(b.c, 14, i, b.memo), 0)}.
Volume today vs 20-day average: ${vAvg ? f1(vols[vols.length - 1] / vAvg, 2) + 'x' : 'n/a'}`; },
};
const JSON_SHAPE = `Reply with ONLY a JSON object, no other text:
{"vote":"yes"|"no"|"pass","confidence":0.0-1.0,"size":0.25-1.0,"reason":"one plain sentence, max 110 characters, citing the key evidence"}
"size" is how much of the proposed buy you'd actually take: 1 = as proposed, less than 1 when you see real risk, up to 1.5 only when the evidence is unusually strong and aligned. For sells, "yes" means agree with selling.`;
const BENCH_SYSTEM = {
  hawk: `You are the News Hawk on a trading council for an automated paper-trading bot. You see ONLY the news, headlines and earnings timing, not the charts. Judge whether the news flow supports the proposed trade over roughly the next week: catalysts, surprises, risks hiding in the headlines, and whether the story is already priced in. Say "pass" when there is no real news. ${JSON_SHAPE}`,
  guard: `You are the Risk Officer on a trading council for an automated paper-trading bot. You see ONLY risk facts: position size, volatility, drawdowns, worst days and earnings timing. Your job is to protect the wallet: vote "no" when the downside or the position size is too much for the likely reward, "yes" when the risk is reasonable. You are skeptical by nature but not a pessimist. ${JSON_SHAPE}`,
  chart: `You are the Chartist on a trading council for an automated paper-trading bot. You see ONLY price action: recent closes, highs and lows, averages, RSI and volume. Read the chart in plain words (trend, support and resistance, breakouts, exhaustion) and judge the proposed trade over roughly the next week. ${JSON_SHAPE}`,
};
const AI_SYSTEM = `You are the Analyst on a trading council for an automated paper-trading bot. The bot's own strategy has already proposed a trade; you give an independent second opinion on whether it is a good idea over roughly the next week, using only the facts given. Be decisive but honest: say "pass" only when the evidence is genuinely balanced. ${JSON_SHAPE}`;
const AI_DEBATE = `You are the Analyst on a trading council for an automated paper-trading bot, and you speak last. You have all the facts and every other member's vote with their reason. First, pick the ONE member whose argument you find weakest or most dangerous (usually on the winning side) and rebut it directly in one sentence addressed to them. Then cast your own vote, weighing everything. Don't just follow the majority; don't be contrarian for its own sake either. Reply with ONLY a JSON object, no other text:
{"challenge":"<member key>","argument":"one sentence to that member, max 120 characters","vote":"yes"|"no"|"pass","confidence":0.0-1.0,"size":0.25-1.0,"reason":"one plain sentence, max 110 characters"}
"size" is how much of the proposed buy you'd actually take: 1 = as proposed, less than 1 for risk, up to 1.5 only when the case is unusually strong. For sells, "yes" means agree with selling.`;
const clean = (t, n) => String(t || '').replace(/\s+/g, ' ').trim().slice(0, n);
// one call to Claude for member k; returns a vote, or a pass explaining why it couldn't
async function aiCall(k, system, user, side) {
  const u = await aiUse(); u.calls++; u.mCalls++; u.by[k] = (u.by[k] || 0) + 1;
  try {
    const r = await fetch(AI_BASE + '/v1/messages', { method: 'POST', signal: AbortSignal.timeout(20e3), headers: { 'content-type': 'application/json', 'x-api-key': AI_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 260, system, messages: [{ role: 'user', content: user }] }) });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j?.error?.message || 'HTTP ' + r.status);
    u.inT += j.usage?.input_tokens || 0; u.outT += j.usage?.output_tokens || 0; u.mIn += j.usage?.input_tokens || 0; u.mOut += j.usage?.output_tokens || 0;
    const text = (j.content || []).map(x => x.text || '').join(''), m = text.match(/\{[\s\S]*\}/), o = m ? JSON.parse(m[0]) : null;
    if (!o || !['yes', 'no', 'pass'].includes(o.vote)) throw new Error('unreadable answer');
    return { k, vote: o.vote, conf: round(clamp(num(o.confidence, 0.5), 0, 1), 2), size: side === 'buy' ? round(clamp(num(o.size, 1), 0.25, 1.5), 2) : 1, line: clean(o.reason, 140) || 'No reason given.', c: 'ai', _o: o };
  } catch (e) { u.errors++; warn('analyst', k + ': ' + e.message); u.lastError = { ts: Date.now(), msg: String(e.message).slice(0, 160) }; return { k, vote: 'pass', conf: 0, line: "Couldn't reach the AI, sitting this one out.", c: 'ai_err' }; }
  finally { kvSet('ai_usage', JSON.stringify(u)).catch(() => {}); }
}
const capPass = k => ({ k, vote: 'pass', conf: 0, line: 'Daily AI limit reached, sitting this one out.', c: 'ai_cap' });
const cacheGet = key => aiCache.has(key) ? { ...aiCache.get(key), cached: true } : null;
const cacheSet = (key, v) => { if (v.c === 'ai') { aiCache.set(key, v); if (aiCache.size > 800) aiCache.delete(aiCache.keys().next().value); } };
const nameOf = k => (COUNCIL.find(m => m.k === k) || AI_ALL.find(m => m.k === k))?.name || k;
// The whole AI round for one decision: specialists in parallel, then the Analyst (debating if on).
// `rule` is the six rule-based votes. Returns the AI votes in speaking order.
async function aiRound(sym, b, d, bz, up, ctx = {}, rule = []) {
  if (!analystReady() || !b || !(b.c?.length > 30)) return [];
  const side = d.side, day = etDate(), u = await aiUse(), max = S.aiDailyMax ?? 120;
  const bench = AI_BENCH.filter(benchOn);
  // the cap always leaves room for the Analyst: specialists sit out first (decided up front, since they run at once)
  const aKey = `ai|${sym}|${day}|${side}|${S.aiDebate !== false ? 'd' : 's'}`;
  let left = max - u.calls - (aiCache.has(aKey) ? 0 : 1);
  const plan = bench.map(m => { const key = `${m.k}|${sym}|${day}|${side}`; if (aiCache.has(key)) return { m, key }; if (left > 0) { left--; return { m, key, call: true }; } return { m, key, cap: true }; });
  const out = await Promise.all(plan.map(async ({ m, key, call, cap }) => {
    if (cap) return capPass(m.k); if (!call) return cacheGet(key);
    const v = await aiCall(m.k, BENCH_SYSTEM[m.k], BENCH_FACTS[m.k](sym, b, d, bz, up, ctx), side); delete v._o; cacheSet(key, v); return v;
  }));
  const debate = S.aiDebate !== false, key = aKey;
  let a = cacheGet(key);
  if (!a) {
    if (u.calls >= max) a = capPass('ai');
    else if (!debate) { a = await aiCall('ai', AI_SYSTEM, aiFacts(sym, b, d, bz, up, ctx), side); delete a._o; }
    else {
      const all = [...rule, ...out].filter(v => v && v.c !== 'ai_cap' && v.c !== 'ai_err');
      const table = all.map(v => `- ${v.k} (${nameOf(v.k)}, ${(COUNCIL.find(m => m.k === v.k) || AI_ALL.find(m => m.k === v.k))?.role || ''}): ${v.vote.toUpperCase()} — ${v.line}`).join('\n');
      a = await aiCall('ai', AI_DEBATE, `${aiFacts(sym, b, d, bz, up, ctx)}\n\nThe council's votes so far (key, name, role):\n${table || '- none'}`, side);
      const o = a._o; delete a._o;
      if (o && all.some(v => v.k === o.challenge)) { a.vs = o.challenge; a.arg = clean(o.argument, 150); a.vsVote = all.find(v => v.k === o.challenge).vote; }
    }
    cacheSet(key, a);
  }
  return [...out, a];
}
// the Night Owl: the only AI call on the night shift. It sees everything the Analyst would, plus the Night Watch's vote.
const OWL_SYSTEM = `You are the Night Owl, working the overnight shift on a trading council for an automated paper-trading bot. The day council is asleep; it's just you and the Night Watch, and only crypto trades at night. The bot's strategy has proposed a trade; give your own judgement over roughly the next day or two, using only the facts given. Overnight crypto can be thin and jumpy, so be a little more careful than in the day, but don't be timid when the evidence is clear. ${JSON_SHAPE}`;
const nightCrewOn = () => NIGHT_CREW.filter(m => m.k === 'watch' || analystReady());
async function owlVote(sym, b, d, bz, up, ctx, watch) {
  if (!analystReady() || !b || !(b.c?.length > 30)) return null;
  const u = await aiUse(), key = `owl|${sym}|${etDate()}|${etParts().h}|${d.side}`, hit = cacheGet(key); if (hit) return hit;
  if (u.calls >= (S.aiDailyMax ?? 120)) return capPass('owl');
  const v = await aiCall('owl', OWL_SYSTEM, `${aiFacts(sym, b, d, bz, up, ctx)}\n\nThe Night Watch voted ${watch.vote.toUpperCase()}: ${watch.line}`, d.side); delete v._o; cacheSet(key, v); return v;
}
// the daily position check: once a day the Analyst looks at each position a bot is holding (that nobody is voting on)
// and says hold, trim (sell half) or sell. Holding is the default; it only acts when the case has turned.
const REVIEW_SYSTEM = `You are the Analyst on a trading council, doing the daily position check for an automated paper-trading bot. The bot already holds this position and its strategy wants to keep it. Decide whether to keep holding, trim (sell half) or sell it all, looking roughly a week ahead and using only the facts given. Holding is the default: only say trim or sell when the evidence has clearly turned against the position (a broken trend, bad news, or a stretched run with risk building). Reply with ONLY a JSON object, no other text:
{"vote":"yes"|"no","action":"hold"|"trim"|"sell","confidence":0.0-1.0,"reason":"one plain sentence, max 110 characters, citing the key evidence"}
"vote" is "yes" to keep the position as is and "no" to trim or sell.`;
async function aiReview(sym, b, bz, up, ctx, botId) {
  if (!analystReady() || S.aiReview === false || !b || !(b.c?.length > 30)) return null;
  const u = await aiUse(), key = `review|${botId}|${sym}|${etDate()}`, hit = cacheGet(key); if (hit) return hit;
  if (u.calls >= (S.aiDailyMax ?? 120)) return null;
  const market = aiFacts(sym, b, { side: 'sell', target: 0 }, bz, up, ctx).split('\n').filter(l => !/^Proposed:/.test(l)).join('\n');
  const pos = `Holding: bought at ${f1(ctx.entry, 2)}, now ${f1(priceOf(sym), 2)} (${pcf(ctx.pnl)}), held ${ctx.days ?? 'n/a'} trading days, ${pcf(ctx.weight)} of the wallet. Highest since buying ${f1(ctx.high, 2)}.${ctx.stopPx ? ` Stop loss at ${f1(ctx.stopPx, 2)}.` : ''}`;
  const v = await aiCall('ai', REVIEW_SYSTEM, `${market}\n${pos}`, 'sell'), o = v._o; delete v._o;
  if (v.c === 'ai') { v.action = ['hold', 'trim', 'sell'].includes(o?.action) ? o.action : 'hold'; v.review = true; }
  cacheSet(key, v); return v;
}
// kept for older callers: the Analyst alone
async function analystVote(sym, b, d, bz, up, ctx = {}) { const r = await aiRound(sym, b, d, bz, up, ctx, []); return r[r.length - 1] || null; }
// one tiny call to check the key works (costs about a hundredth of a cent and counts toward today's calls)
POST('/api/analyst/test', auth, async () => {
  if (!AI_KEY) return { ok: false, msg: 'No ANTHROPIC_API_KEY on the server.' };
  const t0 = Date.now(), v = await aiCall('test', 'You are checking that an API key works. Reply with ONLY this JSON: {"vote":"pass","confidence":0,"size":1,"reason":"Key works."}', 'Ping.', 'buy');
  return v.c === 'ai' ? { ok: true, ms: Date.now() - t0, model: AI_MODEL, reply: v.line } : { ok: false, msg: (await aiUse()).lastError?.msg || v.line, model: AI_MODEL };
});
GET('/api/analyst', auth, async () => { const u = await aiUse(); return { key: !!AI_KEY, on: S.aiOn !== false, sizing: S.aiSize !== false, debate: S.aiDebate !== false, bench: AI_BENCH.map(m => ({ k: m.k, name: m.name, role: m.role, on: S[m.set] !== false })), model: AI_MODEL, dailyMax: S.aiDailyMax ?? 120, today: { calls: u.calls, cost: aiCost(u.inT, u.outT), errors: u.errors, by: u.by || {} }, month: { calls: u.mCalls, cost: aiCost(u.mIn, u.mOut) }, lastError: u.lastError || null }; });
// ===================================================================
// More AI: the morning brief, Ask the Council, the weekly letter from the chair, chatting with a bot,
// post-mortems on losing trades and the news explainer. Everything shares the daily AI cap, and
// everything that can has a plain rule-based version when there's no key (or the cap is hit).
// ===================================================================
const TEXT_RULE = 'Reply in plain text only: no markdown, no headings, no bullet symbols, no emoji unless asked.';
// one free-text call. Returns { ok, text } or { ok: false, msg }. Counts toward the same daily cap as the Council.
async function aiText(k, system, messages, { max = 400, force = false } = {}) {
  if (!AI_KEY) return { ok: false, msg: 'No ANTHROPIC_API_KEY on the server.' };
  if (S.aiOn === false && !force) return { ok: false, msg: 'AI members are switched off in Settings.' };
  const u = await aiUse(); if (u.calls >= (S.aiDailyMax ?? 120)) return { ok: false, msg: 'Today\'s AI limit is used up. It resets at midnight New York time.' };
  u.calls++; u.mCalls++; u.by[k] = (u.by[k] || 0) + 1;
  const msgs = typeof messages === 'string' ? [{ role: 'user', content: messages }] : messages;
  try {
    const r = await fetch(AI_BASE + '/v1/messages', { method: 'POST', signal: AbortSignal.timeout(30e3), headers: { 'content-type': 'application/json', 'x-api-key': AI_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: max, system: system + '\n' + TEXT_RULE, messages: msgs }) });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j?.error?.message || 'HTTP ' + r.status);
    u.inT += j.usage?.input_tokens || 0; u.outT += j.usage?.output_tokens || 0; u.mIn += j.usage?.input_tokens || 0; u.mOut += j.usage?.output_tokens || 0;
    const text = (j.content || []).map(x => x.text || '').join('').replace(/\*\*|^#+\s*/gm, '').trim();
    if (!text) throw new Error('empty answer');
    return { ok: true, text };
  } catch (e) { u.errors++; warn('ai', k + ': ' + e.message); u.lastError = { ts: Date.now(), msg: String(e.message).slice(0, 160) }; return { ok: false, msg: "Couldn't reach the AI: " + String(e.message).slice(0, 100) }; }
  finally { kvSet('ai_usage', JSON.stringify(u)).catch(() => {}); }
}
const aiReadyFor = flag => analystReady() && S[flag] !== false;
const memberName = async k => { const n = await getCouncilNames().catch(() => null); return n?.all?.[k] || (k === 'chair' ? 'Big Mango' : nameOf(k)); };
const prevClose = s => prices.get(s)?.prev || null;
const pctS = x => x == null || !Number.isFinite(x) ? 'n/a' : `${x >= 0 ? '+' : ''}${(x * 100).toFixed(1)}%`;
const usd = x => '$' + (+x || 0).toLocaleString('en-US', { maximumFractionDigits: 0 });

// ---------- facts about the paper side, shared by the brief and the letter ----------
async function botsFacts(sinceMs) {
  const out = [];
  for (const b of await rows("select b.*, w.broker from bots b join wallets w on w.id=b.wid where b.status <> 'killed' order by b.id")) {
    if (b.broker) continue;
    const bot = botJson(b), w = await row('select cash from wallets where id=$1', [b.wid]); if (!w) continue;
    const pos = await positions(b.wid), value = +w.cash + marketValue(pos);
    const back = await row('select eq from snap where wid=$1 and ts <= $2 order by ts desc limit 1', [b.wid, new Date(sinceMs)]);
    const tr = await rows('select sym, side, pnl from trd where wid=$1 and ts >= $2 order by id', [b.wid, new Date(sinceMs)]);
    const last = await row("select title from bot_log where bot_id=$1 and kind='decision' order by id desc limit 1", [b.id]);
    out.push({ id: b.id, name: bot.name, strategy: STRATEGIES[bot.strategy]?.name || bot.strategy, status: bot.status, value, ret: back ? value / +back.eq - 1 : null,
      holds: pos.map(p => p.sym), buys: tr.filter(t => t.side === 'buy').map(t => t.sym), sells: tr.filter(t => t.side === 'sell').map(t => ({ sym: t.sym, pnl: +t.pnl || 0 })), last: last?.title?.replace(/^(Daily decision|Manual run): /, '') || null });
  }
  return out;
}
async function marketFacts() {
  const L = [];
  for (const s of ['SPY', 'QQQ', 'BTC']) {
    const b = await dailyBars(s).catch(() => null); if (!b) continue;
    const i = b.c.length - 1, md = s !== 'QQQ' ? moodOf(b, s === 'BTC' ? 'Bitcoin' : 'The S&P 500') : null;
    L.push(`${s}: ${f1(priceOf(s) || b.c[i], 2)}, 1 day ${pctS(IND.ret(b.c, 1, i))}, 1 week ${pctS(IND.ret(b.c, 5, i))}, 1 month ${pctS(IND.ret(b.c, 21, i))}${md ? `, mood ${md.k}` : ''}`);
  }
  return L;
}
const holdSyms = async () => [...new Set((await rows("select distinct p.sym from pos p join wallets w on w.id=p.wid where w.broker is null and not w.archived").catch(() => [])).map(r => r.sym))];

// ---------- the morning brief ----------
async function morningBrief({ manual = false } = {}) {
  const today = etDate(), bots = await botsFacts(Date.now() - 864e5), mkt = await marketFacts();
  const held = (await holdSyms()).slice(0, 30), earn = [];
  for (const s of held.filter(x => !isCrypto(x))) { const bz = await Promise.race([buzzFor(s), sleep(8000).then(() => null)]).catch(() => null); const up = bz?.upcoming?.find(u => u.k === 'earnings'); if (up && up.tradingDays <= 1) earn.push(`${s} ${up.tradingDays === 0 ? 'today' : 'tomorrow'}`); }
  const movers = held.map(s => { const p = priceOf(s), pv = prevClose(s); return p && pv ? { s, ch: p / pv - 1 } : null; }).filter(Boolean).sort((a, b) => Math.abs(b.ch) - Math.abs(a.ch)).slice(0, 3);
  const facts = [`Date: ${today} (${['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][new Date(today + 'T12:00').getDay()]})`, 'Markets:', ...mkt.map(x => '- ' + x),
    `Earnings for things we hold: ${earn.join(', ') || 'none today or tomorrow'}`, `Biggest overnight moves in holdings: ${movers.map(m => `${m.s} ${pctS(m.ch)}`).join(', ') || 'none'}`,
    'Bots (name, strategy, last day, what they hold, latest decision):', ...bots.map(b => `- ${b.name} (${b.strategy}, ${b.status}): ${pctS(b.ret)}, holds ${b.holds.join(', ') || 'cash'}${b.last ? `; last: ${b.last}` : ''}`)].join('\n');
  let text = null, ai = false;
  if (aiReadyFor('briefOn')) {
    const r = await aiText('brief', `You write a short morning brief for someone learning to trade with paper-money bots. Using only the facts given, write 4 to 6 short lines, each on its own line: first the market mood in one line, then what matters today (earnings, big moves in what they hold), then one line on the bots worth watching, and finish with one line on what to keep an eye on. Plain, friendly, specific, with numbers. Never give financial advice or tell them to buy or sell.`, facts, { max: 320 });
    if (r.ok) { text = r.text; ai = true; }
  }
  if (!text) { // the rule-based brief
    const spy = mkt.find(x => x.startsWith('SPY')), best = bots.filter(b => b.ret != null).sort((a, b) => b.ret - a.ret)[0];
    text = [spy ? `Market: ${spy.replace(/^SPY: /, 'S&P 500 ')}.` : 'Market data is still loading.', earn.length ? `Earnings: ${earn.join(', ')}.` : 'No earnings today for anything you hold.', movers.length ? `Biggest moves in your holdings: ${movers.map(m => `${m.s} ${pctS(m.ch)}`).join(', ')}.` : '', best ? `Best bot yesterday: ${best.name} ${pctS(best.ret)}.` : bots.length ? '' : 'No bots running yet.'].filter(Boolean).join('\n');
  }
  const brief = { ts: Date.now(), day: today, text, ai };
  await kvSet('brief', JSON.stringify(brief));
  if (!manual) await notify('brief', '☀️ Morning brief', text.split('\n')[0].slice(0, 180), { url: '/#/brief', tag: 'brief-' + today });
  return brief;
}
async function briefJob() {
  if (S.briefOn === false) return;
  const p = etParts(), hm = String(p.h).padStart(2, '0') + ':' + String(p.mi).padStart(2, '0'), today = etDate();
  if (!isTradingDay(p.y, p.m, p.d) || hm < (S.briefAt || '08:45') || hm > '11:00') return;
  if ((await kvGet('brief_day').catch(() => null)) === today) return;
  await kvSet('brief_day', today); await morningBrief();
}
GET('/api/brief', auth, async () => jsonOr(await kvGet('brief').catch(() => null), null));
POST('/api/brief', auth, async () => morningBrief({ manual: true }));

// ---------- Ask the Council about any ticker ----------
const askCache = new Map();
POST('/api/council/ask', auth, async req => {
  const sym = cleanSym(req.body?.sym); if (!sym) fail('Which symbol?');
  const side = req.body?.side === 'sell' ? 'sell' : 'buy', key = `${sym}|${side}|${etDate()}`;
  chalBump('ask');
  if (askCache.has(key) && Date.now() - askCache.get(key).ts < 30 * 60e3) return { ...askCache.get(key), cached: true };
  const b = await dailyBars(sym).catch(() => null); if (!b || b.c.length < 30) fail(`Not enough price history for ${sym}.`);
  const spy = await dailyBars('SPY').catch(() => null), bench = spy ? IND.ret(spy.c, 63, spy.c.length - 1) : null;
  const bz = S.f_buzz !== false ? await Promise.race([buzzFor(sym), sleep(12000).then(() => null)]).catch(() => null) : null, up = bz?.upcoming?.find(u => u.k === 'earnings') || null;
  const votes = councilVotes(sym, b, { buzz: bz, earnings: up, weight: 0.1, benchRet63: bench });
  const ais = await aiRound(sym, b, { side, target: 0.1 }, bz, up, { strategy: 'a question from the owner (no bot is trading this; judge it on its merits)', bench, have: 0, positions: 0, maxPos: 25, stops: true }, votes).catch(() => []);
  votes.push(...ais);
  const s = { votes, yes: votes.filter(v => v.vote === 'yes').length, no: votes.filter(v => v.vote === 'no').length };
  Object.assign(s, weighVotes(votes, situation(b, bz, up), await councilRecord()));
  const t = tallyText(s), lean = leansNo(s) ? 'no' : (s.wy ?? s.yes) > (s.wn ?? s.no) ? 'yes' : 'split';
  const chair = `${await memberName('chair')}: ${lean === 'yes' ? `The Council would back a ${side} of ${sym}, ${t}.` : lean === 'no' ? `The Council would not ${side} ${sym} today, ${t}.` : `The Council is split on ${sym}, ${t}.`}`;
  const out = { ts: Date.now(), sym, side, votes: votes.map(v => ({ k: v.k, vote: v.vote, conf: v.conf, line: v.line, w: v.w, wr: v.wr, c: v.c, vs: v.vs, arg: v.arg })), yes: s.yes, no: s.no, wy: s.wy, wn: s.wn, lean, chair, price: priceOf(sym) || b.c[b.c.length - 1] };
  askCache.set(key, out); if (askCache.size > 100) askCache.delete(askCache.keys().next().value);
  return out;
});

// ---------- the weekly letter from the chair ----------
async function weeklyLetter({ manual = false } = {}) {
  const since = Date.now() - 7 * 864e5, bots = await botsFacts(since), mkt = await marketFacts(), chair = await memberName('chair');
  const settled = await rows("select votes, right_, result, sym from council where settled and ts >= now() - interval '14 days'").catch(() => []);
  const score = {}; for (const r of settled) for (const [k, ok] of Object.entries(jsonOr(r.right_, {}) || {})) { const m = score[k] ||= { n: 0, r: 0 }; m.n++; if (ok) m.r++; }
  const calls = (await Promise.all(Object.entries(score).filter(([, m]) => m.n >= 3).map(async ([k, m]) => ({ k, name: await memberName(k), acc: m.r / m.n, n: m.n })))).sort((a, b) => b.acc - a.acc);
  const sells = bots.flatMap(b => b.sells.map(s => ({ ...s, bot: b.name }))), best = sells.sort((a, b) => b.pnl - a.pnl)[0], worst = sells[sells.length - 1];
  const ranked = bots.filter(b => b.ret != null).sort((a, b) => b.ret - a.ret);
  const facts = ['Markets this week:', ...mkt.map(x => '- ' + x), 'Bots this week (return, trades):', ...[...ranked, ...bots.filter(b => b.ret == null)].map(b => `- ${b.name} (${b.strategy}): ${pctS(b.ret)}, ${b.buys.length} buys, ${b.sells.length} sells, holds ${b.holds.join(', ') || 'cash'}`),
    best ? `Best closed trade: ${best.bot} on ${best.sym}, ${usd(best.pnl)}` : 'No closed trades.', worst && worst !== best ? `Worst closed trade: ${worst.bot} on ${worst.sym}, ${usd(worst.pnl)}` : '',
    calls.length ? `Council members who called it best (last 2 weeks, graded a week after each vote): ${calls.slice(0, 3).map(c => `${c.name} ${Math.round(c.acc * 100)}% of ${c.n}`).join(', ')}; worst: ${calls.slice(-2).map(c => `${c.name} ${Math.round(c.acc * 100)}%`).join(', ')}` : 'Too few graded Council votes this week.'].filter(Boolean).join('\n');
  let text = null, ai = false;
  if (aiReadyFor('letterOn')) {
    const r = await aiText('letter', `You are ${chair}, the chair (the king) of a council of trading bots in a playful game called Mango HQ. Write this week's letter to the owner, in character: warm, a bit regal and funny, but every claim must come from the facts given. 120 to 180 words, in 3 short paragraphs: how the week went for the kingdom (markets and the bots, with numbers), who in the court earned praise or a scolding (best and worst bot, Council members who called it), and what the court will watch next week. Sign it with your name. Never give financial advice.`, facts, { max: 450 });
    if (r.ok) { text = r.text; ai = true; }
  }
  if (!text) text = [`To the owner, from ${chair}.`, ranked.length ? `This week ${ranked[0].name} led the court at ${pctS(ranked[0].ret)}${ranked.length > 1 ? ` and ${ranked[ranked.length - 1].name} trailed at ${pctS(ranked[ranked.length - 1].ret)}` : ''}.` : bots.length ? `${bots.length} bot${bots.length === 1 ? ' is' : 's are'} at work, too new to rank yet.` : 'The court had no bots at work this week.', calls.length ? `${calls[0].name} called it best, right ${Math.round(calls[0].acc * 100)}% of the time.` : '', mkt[0] ? `The realm: ${mkt[0].replace(/^SPY: /, 'S&P 500 ')}.` : '', `— ${chair}`].filter(Boolean).join('\n\n');
  const letter = { id: Date.now(), ts: Date.now(), week: etDate(), from: chair, text, ai };
  const list = jsonOr(await kvGet('letters').catch(() => null), []) || []; list.unshift(letter); await kvSet('letters', JSON.stringify(list.slice(0, 20)));
  if (!manual) await notify('letter', `👑 A letter from ${chair}`, text.replace(/\s+/g, ' ').slice(0, 160), { url: '/#/letters', tag: 'letter-' + letter.week });
  return letter;
}
async function letterJob() {
  if (S.letterOn === false) return;
  const p = etParts(), today = etDate(); if (new Date(today + 'T12:00').getDay() !== 5 || p.h < 17) return; // Friday after 5 PM ET
  if ((await kvGet('letter_day').catch(() => null)) === today) return;
  await kvSet('letter_day', today); await weeklyLetter();
}
GET('/api/letters', auth, async () => jsonOr(await kvGet('letters').catch(() => null), []) || []);
POST('/api/letters', auth, async () => weeklyLetter({ manual: true }));

// ---------- chat with a bot ----------
const PERSONA = { trend: 'calm and patient, a surfer who rides waves', momentum: 'restless and competitive, always chasing the strongest runner', rsi2: 'a bargain hunter who loves a dip', breakout: 'excitable, lives for new highs', hold: 'sleepy and unbothered', dual: 'a careful planner', lowvol: 'cautious and sensible', drift: 'a news junkie', tom: 'a creature of habit who loves the calendar', snapback: 'a contrarian who bets on bounces', earnings: 'an earnings-season nerd', autopilot: 'a manager running a team of strategies' };
POST('/api/bots/:id/chat', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  if (!AI_KEY) fail('Chat needs an ANTHROPIC_API_KEY on the server.');
  if (S.aiChat === false) fail('Chat is switched off in Settings.');
  const msg = clean(req.body?.msg, 500); if (!msg) fail('Say something first.');
  const bot = botJson(b), st = STRATEGIES[bot.strategy] || {}, w = await row('select cash from wallets where id=$1', [b.wid]), pos = await positions(b.wid), value = +w.cash + marketValue(pos);
  const last = await row("select * from bot_log where bot_id=$1 and kind='decision' order by id desc limit 1", [b.id]), rowsW = jsonOr(last?.data, {})?.rows || [];
  const log = (await rows('select kind, title, body, ts from bot_log where bot_id=$1 order by id desc limit 12', [b.id])).map(l => `- ${etDate(tsMs(l.ts))} ${l.kind}: ${l.title}${l.body ? ' — ' + String(l.body).split('\n')[0].slice(0, 120) : ''}`).join('\n');
  const tr = (await rows('select ts, sym, side, qty, price, pnl from trd where wid=$1 order by id desc limit 10', [b.wid])).map(t => `- ${etDate(tsMs(t.ts))} ${t.side} ${t.sym} at ${f1(+t.price, 2)}${t.pnl != null ? ` (P/L ${usd(+t.pnl)})` : ''}`).join('\n');
  const m = { ...MANDATE_DEF, ...bot.mandate };
  const facts = `Your name: ${bot.name}. Strategy: ${st.name || bot.strategy} — ${String(st.desc || '').slice(0, 400)}
Settings: ${JSON.stringify(bot.params)}. Trades: ${bot.syms.join(', ')}. Status: ${bot.status}.
Limits: max ${m.maxPosPct}% per position, max ${m.maxTradesDay} trades a day, stops ${m.stops ? 'on' : 'off'}, drawdown stop ${m.maxDDPct}%, take half profit at +${m.takeProfitPct}%, time stop ${m.timeStopDays} days.
Wallet: ${usd(value)} (cash ${usd(w.cash)}), started with ${usd(+(await row('select start_bal from wallets where id=$1', [b.wid]))?.start_bal || 0)}.
Holding: ${pos.map(p => `${p.sym} ${f1(p.qty, 3)} @ ${f1(p.avg, 2)} now ${f1(priceOf(p.sym), 2)} (${pctS((priceOf(p.sym) || p.avg) / p.avg - 1)})`).join('; ') || 'nothing, all cash'}
Latest decision: ${last?.title || 'none yet'}
${rowsW.slice(0, 10).map(r => `- ${r.sym}: ${r.action}. ${(r.note || '')} ${(r.why || []).join(' ')}`.slice(0, 300)).join('\n')}
Recent trades:\n${tr || '- none'}
Journal:\n${log || '- empty'}`;
  const hist = (Array.isArray(req.body?.history) ? req.body.history : []).slice(-8).map(x => ({ role: x.role === 'bot' ? 'assistant' : 'user', content: clean(x.text, 600) })).filter(x => x.content);
  while (hist.length && hist[0].role !== 'user') hist.shift();
  const r = await aiText('chat', `You are ${bot.name}, a paper-trading bot in the Mango app, talking with your owner. Personality: ${PERSONA[bot.strategy] || 'friendly and straightforward'}. Stay in character but be genuinely useful: explain your decisions, your rules and your holdings using only the facts below, and say plainly when you don't know. Keep answers to 1 to 4 short sentences. You cannot place trades or change settings from this chat; if asked, tell them which button in the app does it (Decide now, Pause, Change, Sell everything). Never give financial advice about real money.\n\nFacts about you:\n${facts}`, [...hist, { role: 'user', content: msg }], { max: 300 });
  if (!r.ok) fail(r.msg);
  return { text: r.text };
});

// ---------- post-mortems on losing trades ----------
async function postmortemJob() {
  if (S.aiPostmortem === false) return;
  const lastId = +(await kvGet('pm_last').catch(() => 0) || 0);
  const list = await rows("select t.*, b.id as bot_id, b.name as bot_name, b.strategy from trd t join bots b on b.wid=t.wid where t.side='sell' and t.pnl < 0 and t.id > $1 and t.ts > now() - interval '3 days' order by t.id limit 3", [lastId]).catch(() => []);
  for (const t of list) {
    await kvSet('pm_last', String(t.id));
    const buy = await row("select * from trd where wid=$1 and sym=$2 and side='buy' and id < $3 order by id desc limit 1", [t.wid, t.sym, t.id]);
    const cv = buy ? await row("select * from council where bot_id=$1 and sym=$2 and ts <= $3::timestamptz + interval '1 hour' and ts >= $3::timestamptz - interval '3 days' order by id desc limit 1", [t.bot_id, t.sym, buy.ts]) : null;
    const votes = jsonOr(cv?.votes, []), lossPct = buy ? +t.price / +buy.price - 1 : null, days = buy ? Math.max(1, Math.round((tsMs(t.ts) - tsMs(buy.ts)) / 864e5)) : null;
    const b = await dailyBars(t.sym).catch(() => null); let path = '';
    if (b && buy) { const i0 = lastIndexOnOrBefore(b.d, etDate(tsMs(buy.ts))), i1 = lastIndexOnOrBefore(b.d, etDate(tsMs(t.ts))); if (i0 >= 0 && i1 > i0) { let hi = 0, lo = Infinity; for (let k = i0; k <= i1; k++) { hi = Math.max(hi, b.h[k]); lo = Math.min(lo, b.l[k]); } path = `While held, high ${f1(hi, 2)} (${pctS(hi / +buy.price - 1)}), low ${f1(lo, 2)} (${pctS(lo / +buy.price - 1)}).`; } }
    const spy = await dailyBars('SPY').catch(() => null); let mk = '';
    if (spy && buy) { const i0 = lastIndexOnOrBefore(spy.d, etDate(tsMs(buy.ts))), i1 = lastIndexOnOrBefore(spy.d, etDate(tsMs(t.ts))); if (i0 >= 0 && i1 >= i0) mk = `S&P 500 over the same days: ${pctS(spy.c[i1] / spy.c[i0] - 1)}.`; }
    const facts = `Bot ${t.bot_name} (${STRATEGIES[t.strategy]?.name || t.strategy}) lost ${usd(Math.abs(+t.pnl))} on ${t.sym}${lossPct != null ? ` (${pctS(lossPct)})` : ''}${days ? ` over ${days} day(s)` : ''}.
Bought${buy ? ` at ${f1(+buy.price, 2)} on ${etDate(tsMs(buy.ts))}` : ''}. Why it bought: ${clean(buy?.why, 300) || 'n/a'}
Sold at ${f1(+t.price, 2)} on ${etDate(tsMs(t.ts))}. Why it sold: ${clean(t.why, 300) || 'n/a'}
${path} ${mk}
Council vote on the buy: ${votes.length ? votes.filter(v => v.vote !== 'pass').map(v => `${nameOf(v.k)} ${v.vote}: ${clean(v.line, 90)}`).join(' | ') : 'not recorded'}`;
    let text = null, ai = false;
    if (aiReadyFor('aiPostmortem')) { const r = await aiText('postmortem', 'You write a short post-mortem on one losing paper trade by an automated bot, for someone learning to trade. Using only the facts, in 2 or 3 sentences: what went wrong (bad luck, the whole market fell, a stop that was too tight, a signal that was late, ignored warnings), whether the bot followed its own rules, and one lesson. If the Council members who voted no were right, say who. Be honest: sometimes a loss is just the normal cost of a sound strategy.', facts, { max: 220 }); if (r.ok) { text = r.text; ai = true; } }
    if (!text) { const noes = votes.filter(v => v.vote === 'no').map(v => nameOf(v.k)); text = `Lost ${lossPct != null ? (Math.abs(lossPct) * 100).toFixed(1) + '%' : usd(Math.abs(+t.pnl))}${days ? ` in ${days} day${days === 1 ? '' : 's'}` : ''}. ${mk ? mk + ' ' : ''}${noes.length ? `${noes.join(', ')} voted no on the buy and turned out right.` : 'Nobody on the Council saw it coming.'}`; }
    await botLog(t.bot_id, 'lesson', `Lesson from ${t.sym}: lost ${usd(Math.abs(+t.pnl))}`, text, { ai, trade: t.id }, t.sym);
  }
}

// ---------- the news explainer ----------
const explainCache = new Map();
GET('/api/explain/:sym', auth, async req => {
  const sym = cleanSym(req.params.sym); if (!sym) fail('Which symbol?');
  const hit = explainCache.get(sym); if (hit && Date.now() - hit.ts < 60 * 60e3 && !req.query.fresh) return hit;
  const bz = await Promise.race([buzzFor(sym), sleep(15000).then(() => null)]).catch(() => null);
  const p = priceOf(sym), pv = prevClose(sym), b = await dailyBars(sym).catch(() => null), i = b ? b.c.length - 1 : -1;
  const facts = `${sym}${isCrypto(sym) ? ' (crypto)' : ''}: price ${f1(p, 2)}, today ${p && pv ? pctS(p / pv - 1) : 'n/a'}, 1 week ${b ? pctS(IND.ret(b.c, 5, i)) : 'n/a'}, 1 month ${b ? pctS(IND.ret(b.c, 21, i)) : 'n/a'}.
News mood ${bz ? f1(bz.sentiment, 2) : 'n/a'} across ${bz?.count || 0} items this week${bz?.flags?.length ? `; flags ${bz.flags.join(', ')}` : ''}.${bz?.upcoming?.length ? ` Coming up: ${bz.upcoming.map(u => `${u.k} in ${u.tradingDays ?? '?'} trading days`).join(', ')}.` : ''}
Headlines, newest first:
${(bz?.items || []).filter(x => x.src === 'news' || x.title).slice(0, 12).map(x => `- ${new Date(x.t).toISOString().slice(0, 10)} ${String(x.title).slice(0, 160)}`).join('\n') || '- none'}`;
  if (!(bz?.count > 0)) return { ts: Date.now(), sym, text: `There's almost no news on ${sym} this week, so any move is probably the wider market or normal noise.`, ai: false };
  if (!aiReadyFor('aiExplain')) {
    const top = (bz.items || []).filter(x => x.src === 'news').slice(0, 3).map(x => x.title);
    return { ts: Date.now(), sym, ai: false, text: `${buzzLine(bz)}.${top.length ? ` Top stories: ${top.join('; ')}.` : ''}` };
  }
  const r = await aiText('explain', 'You explain, in plain English for a beginner, what the news says about one stock or coin and whether it explains the price move. Use only the facts. 3 or 4 short sentences: the main story, whether the move fits the news (or looks like the wider market or noise), and anything coming up worth knowing. If the headlines are thin or contradictory, say so. No advice to buy or sell.', facts, { max: 260 });
  if (!r.ok) fail(r.msg);
  const out = { ts: Date.now(), sym, text: r.text, ai: true }; explainCache.set(sym, out); if (explainCache.size > 200) explainCache.delete(explainCache.keys().next().value);
  return out;
});
// ===================================================================
// Real-money safety and running costs: the promotion gate, the real-vs-paper twin, the panic button
// (also from a Shortcut), nightly offsite backups to a private GitHub repo, the uptime monitor
// between the main server and the standby, and the cost dashboard.
// ===================================================================

// ---------- the promotion gate: a paper bot earns its way to real money ----------
async function promoGate(b) {
  const bot = botJson(b), card = await botCard(b), days = Math.round((Date.now() - bot.created) / 864e5);
  const sells = await rows("select pnl from trd where wid=$1 and side='sell' and pnl is not null and ts >= $2", [b.wid, new Date(bot.created)]);
  let spy = null; try { const s = await dailyBars('SPY'), i = lastIndexOnOrBefore(s.d, etDate(bot.created)); const s0 = jsonOr(b.state, {})._spy0 || (i >= 0 ? s.c[i] : null); if (s0) spy = (priceOf('SPY') || s.c[s.c.length - 1]) / s0 - 1; } catch (e) {}
  const ret = card.ret / 100, dd = Math.abs(Math.min(0, card.dd || 0)) * 100;
  const checks = [
    { k: 'days', label: `At least ${S.promoMinDays} days on paper`, ok: days >= S.promoMinDays, have: `${days} days` },
    { k: 'trades', label: `At least ${S.promoMinTrades} closed trades`, ok: sells.length >= S.promoMinTrades, have: `${sells.length} closed` },
    { k: 'spy', label: 'Ahead of the S&P 500 since it started', ok: spy == null ? false : ret > spy, have: spy == null ? 'no S&P data yet' : `${pct(ret * 100)} vs ${pct(spy * 100)}`, off: !S.promoBeatSpy },
    { k: 'dd', label: `Worst drop under ${S.promoMaxDD}%`, ok: dd < S.promoMaxDD, have: `${dd.toFixed(1)}%` },
    { k: 'status', label: 'Not stopped by its own limits', ok: bot.status !== 'killed', have: bot.status },
  ].filter(c => !c.off);
  return { on: S.promoGate !== false, live: BROKER?.env === 'live', pass: checks.every(c => c.ok), checks, days, ret, spy };
}
GET('/api/bots/:id/gate', auth, async req => { const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404); return promoGate(b); });

// ---------- real vs paper twin ----------
GET('/api/bots/:id/twin', auth, async req => {
  const b = await row('select * from bots where id=$1', [+req.params.id]); if (!b) fail('That bot is gone', 404);
  const st = jsonOr(b.state, {}), from = st._from; if (!from?.id) return null;
  const p = await row('select * from bots where id=$1', [from.id]); if (!p) return { gone: true, name: from.name };
  const since = new Date(tsMs(b.created)), side = async x => {
    const w = await row('select cash from wallets where id=$1', [x.wid]), pos = await positions(x.wid), now = +w.cash + marketValue(pos);
    const pts = (await rows('select ts, eq from snap where wid=$1 and ts >= $2 order by ts', [x.wid, since])).map(r => [tsMs(r.ts), +r.eq]);
    const base = (await row('select eq from snap where wid=$1 and ts <= $2 order by ts desc limit 1', [x.wid, since]))?.eq ?? pts[0]?.[1] ?? now;
    pts.push([Date.now(), now]);
    const tr = await rows("select side, pnl, fee, qty, price from trd where wid=$1 and ts >= $2 and side in ('buy','sell')", [x.wid, since]);
    return { name: x.name, ret: now / +base - 1, trades: tr.length, fees: tr.reduce((t, r) => t + (+r.fee || 0), 0), realized: tr.reduce((t, r) => t + (+r.pnl || 0), 0), idx: downsample(pts.map(([t, v]) => [t, round(100 * v / +base, 3)]), 200) };
  };
  const real = await side(b), paper = await side(p);
  // the same trade on both sides: how much worse did the real fill come in?
  const rf = await rows("select sym, side, price, ts from trd where wid=$1 and ts >= $2 and side in ('buy','sell') order by id desc limit 40", [b.wid, since]);
  const slips = [];
  for (const t of rf) {
    const m = await row("select price from trd where wid=$1 and sym=$2 and side=$3 and ts between $4::timestamptz - interval '2 hours' and $4::timestamptz + interval '2 hours' order by abs(extract(epoch from ts - $4::timestamptz)) limit 1", [p.wid, t.sym, t.side, t.ts]);
    if (m) slips.push((t.side === 'buy' ? 1 : -1) * (+t.price / +m.price - 1));
  }
  return { since: tsMs(b.created), real, paper, gap: real.ret - paper.ret, slip: slips.length ? slips.reduce((t, x) => t + x, 0) / slips.length : null, matched: slips.length, paperId: p.id };
});

// ---------- the panic button ----------
// Stops everything that can trade: every bot paused, the Alpaca kill switch on, open Alpaca orders cancelled.
// It never buys anything. Selling everything at Alpaca is only possible from inside the app.
async function panic({ flatten = false, via = 'app' } = {}) {
  const out = { paused: 0, alpaca: false, cancelled: false, closed: false };
  out.paused = (await Q("update bots set status='paused' where status='running'")).rowCount;
  await saveSettings({ realHalt: true, panicAt: Date.now() });
  if (BROKER) {
    out.alpaca = true;
    try { await apca('/v2/orders', { method: 'DELETE' }); out.cancelled = true; } catch (e) { out.cancelErr = e.message; }
    if (flatten) { try { await apca('/v2/positions?cancel_orders=true', { method: 'DELETE' }); out.closed = true; } catch (e) { out.closeErr = e.message; } }
    brokerSyncSoon();
  }
  broadcast({ t: 'evt', e: 'settings' }); broadcast({ t: 'evt', e: 'bot', id: 0 });
  await notify('real', '🛑 Panic button pressed', `${out.paused} bot${out.paused === 1 ? '' : 's'} paused${out.alpaca ? `, Alpaca trading stopped${out.cancelled ? ' and open orders cancelled' : ''}${out.closed ? ', every Alpaca position sold' : ''}` : ''}. ${via === 'shortcut' ? 'From your Shortcut. ' : ''}Resume bots one by one, and Alpaca in Alpaca controls.`, { url: '/#/real' });
  log(`PANIC (${via}): ${JSON.stringify(out)}`);
  return out;
}
POST('/api/panic', auth, async req => panic({ flatten: !!req.body?.flatten }));
async function panicToken(renew = false) { let t = await kvGet('panic_token').catch(() => null); if (!t || renew) { t = crypto.randomBytes(18).toString('hex'); await kvSet('panic_token', t); } return t; }
GET('/api/panic-link', auth, async () => ({ token: await panicToken() }));
POST('/api/panic-link', auth, async () => ({ token: await panicToken(true) }));
// for a Shortcut: POST only, so a link preview or a stray tap in a browser can't set it off
POST('/api/panic/:token', async req => {
  const t = await kvGet('panic_token').catch(() => null), got = String(req.params.token);
  if (!t || got.length !== t.length || !crypto.timingSafeEqual(Buffer.from(got), Buffer.from(t))) fail('Not found', 404);
  const r = await panic({ via: 'shortcut' });
  return { ok: true, message: `Stopped. ${r.paused} bots paused${r.alpaca ? ', Alpaca halted' : ''}.` };
});

// ---------- offsite backups: one file a night in a private GitHub repo ----------
const GH_TOKEN = (ENV.GITHUB_BACKUP_TOKEN || '').trim(), GH_REPO = (ENV.GITHUB_BACKUP_REPO || '').trim().replace(/^https:\/\/github\.com\//, '').replace(/\.git$/, '');
let ghStatus = { on: !!(GH_TOKEN && GH_REPO), repo: GH_REPO || null, last: null };
const gh = async (p, o = {}) => { const r = await fetch('https://api.github.com' + p, { ...o, signal: AbortSignal.timeout(60e3), headers: { authorization: 'Bearer ' + GH_TOKEN, accept: 'application/vnd.github+json', 'user-agent': 'mango-backup', 'x-github-api-version': '2022-11-28', ...(o.body ? { 'content-type': 'application/json' } : {}) } }); const j = await r.json().catch(() => ({})); if (!r.ok && r.status !== 404) throw new Error(j.message || 'GitHub HTTP ' + r.status); return r.status === 404 ? null : j; };
async function offsiteBackup() {
  if (!ghStatus.on) fail('Add GITHUB_BACKUP_TOKEN and GITHUB_BACKUP_REPO on your server first.');
  try {
    const repo = await gh(`/repos/${GH_REPO}`); if (!repo) throw new Error(`Can't see the repo ${GH_REPO}. Check the name and that the token can access it.`);
    if (!repo.private) throw new Error(`${GH_REPO} is public. Backups hold your whole account, so they only go to a private repo.`);
    const gz = await makeBackup(), name = `backups/mango-${etDate()}.json.gz`, old = await gh(`/repos/${GH_REPO}/contents/${name}`);
    await gh(`/repos/${GH_REPO}/contents/${name}`, { method: 'PUT', body: JSON.stringify({ message: `Mango backup ${etDate()}`, content: gz.toString('base64'), ...(old?.sha ? { sha: old.sha } : {}) }) });
    const list = (await gh(`/repos/${GH_REPO}/contents/backups`)) || [], keep = Math.max(3, S.offsiteKeep || 14);
    for (const f of list.filter(f => /^mango-\d{4}-\d\d-\d\d\.json\.gz$/.test(f.name)).sort((a, b) => b.name.localeCompare(a.name)).slice(keep)) await gh(`/repos/${GH_REPO}/contents/${f.path}`, { method: 'DELETE', body: JSON.stringify({ message: 'Remove old backup', sha: f.sha }) }).catch(() => {});
    ghStatus.last = { ok: true, ts: Date.now(), size: gz.length, name }; await kvSet('gh_backup', JSON.stringify(ghStatus.last));
    return ghStatus.last;
  } catch (e) {
    ghStatus.last = { ok: false, ts: Date.now(), err: String(e.message).slice(0, 200) }; await kvSet('gh_backup', JSON.stringify(ghStatus.last)).catch(() => {});
    await notify('real', '⚠️ Offsite backup failed', ghStatus.last.err, { url: '/#/settings' }); fail(ghStatus.last.err, 400);
  }
}
async function offsiteJob() {
  if (!ghStatus.on || STANDBY || S.offsiteOn === false) return;
  const p = etParts(), today = etDate(); if (p.h < 4 || (await kvGet('job_gh_backup').catch(() => null)) === today) return;
  await kvSet('job_gh_backup', today); await offsiteBackup().catch(e => warn('offsite backup', e.message));
}
GET('/api/offsite', auth, async () => { if (!ghStatus.last) ghStatus.last = jsonOr(await kvGet('gh_backup').catch(() => null), null); return ghStatus; });
POST('/api/offsite', auth, async () => offsiteBackup());

// ---------- uptime monitor: the main server keeps the standby awake, the standby watches the main ----------
const MAIN_URL = (ENV.MAIN_URL || '').trim().replace(/\/$/, ''), STANDBY_URL = (ENV.STANDBY_URL || '').trim().replace(/\/$/, '');
let uptime = { role: STANDBY ? 'standby' : 'main', watching: STANDBY ? MAIN_URL || null : STANDBY_URL || null, state: 'unknown', fails: 0, last: null, since: null, downAt: null };
async function uptimeJob() {
  if (!STANDBY) { // main: ping the standby so a host that sleeps stays awake to watch us
    if (!STANDBY_URL) return;
    try { const r = await fetch(STANDBY_URL + '/api/health', { signal: AbortSignal.timeout(20e3) }); uptime.last = { ok: r.ok, ts: Date.now() }; uptime.state = r.ok ? 'up' : 'down'; } catch (e) { uptime.last = { ok: false, ts: Date.now(), err: e.message }; uptime.state = 'down'; }
    return;
  }
  if (!MAIN_URL) return;
  let ok = false, err = null; try { const r = await fetch(MAIN_URL + '/api/health', { signal: AbortSignal.timeout(20e3) }); ok = r.ok; if (!ok) err = 'HTTP ' + r.status; } catch (e) { err = e.message; }
  uptime.last = { ok, ts: Date.now(), err };
  if (ok) {
    if (uptime.state === 'down') await notify('real', '✅ Main server is back', `It was down for about ${Math.max(1, Math.round((Date.now() - uptime.downAt) / 60e3))} minutes.${S.standbyActive ? ' The standby is still ON: switch it off once you have restored a fresh backup on the main server.' : ''}`, { url: '/#/settings' });
    uptime.fails = 0; if (uptime.state !== 'up') uptime.since = Date.now(); uptime.state = 'up'; return;
  }
  uptime.fails++;
  if (uptime.fails >= 3 && uptime.state !== 'down') {
    uptime.state = 'down'; uptime.downAt = Date.now(); uptime.since = Date.now();
    await notify('real', '🚨 Main server looks down', `Your standby couldn't reach it 3 times in a row (${String(err || '').slice(0, 80)}). Open the standby to switch it on if it stays down.`, { url: (SELF_URL || '') + '/#/settings' });
  }
  if (uptime.state === 'down' && SELF_URL) fetch(SELF_URL + '/api/health', { signal: AbortSignal.timeout(15e3) }).catch(() => {}); // keep ourselves awake while the main is down
}
GET('/api/uptime', auth, () => uptime);

// ---------- cost dashboard ----------
let cpu0 = { t: Date.now(), u: process.cpuUsage() }, cpuAvg = null;
setInterval(() => { const now = Date.now(), u = process.cpuUsage(), used = (u.user + u.system - cpu0.u.user - cpu0.u.system) / 1e3 / (now - cpu0.t); cpuAvg = cpuAvg == null ? used : cpuAvg * 0.8 + used * 0.2; cpu0 = { t: now, u }; }, 5 * 60e3).unref?.();
GET('/api/costs', auth, async () => {
  const u = await aiUse(), d = etParts(), dim = new Date(Date.UTC(d.y, d.m, 0)).getUTCDate();
  const perCall = u.calls ? aiCost(u.inT, u.outT) / u.calls : aiCost(450, 70), month = aiCost(u.mIn, u.mOut);
  const ramGB = process.memoryUsage().rss / 1073741824, vcpu = cpuAvg ?? (process.cpuUsage().user + process.cpuUsage().system) / 1e3 / (Date.now() - BOOT);
  const RAM = num(ENV.COST_RAM_GB_MONTH, 10), CPU = num(ENV.COST_VCPU_MONTH, 20), plan = num(ENV.COST_PLAN_MONTH, 5), credit = num(ENV.COST_PLAN_CREDIT, 5);
  const host = { ramGB: round(ramGB, 3), vcpu: round(vcpu, 4), ram: ramGB * RAM, cpu: vcpu * CPU, plan, credit };
  host.usage = host.ram + host.cpu; host.bill = Math.max(plan, plan + host.usage - credit); // the plan includes some usage; anything past it is billed on top
  return {
    ai: { key: !!AI_KEY, model: AI_MODEL, today: { calls: u.calls, cost: aiCost(u.inT, u.outT) }, month: { calls: u.mCalls, cost: month, projected: month / Math.max(1, d.d) * dim }, cap: S.aiDailyMax ?? 120, capCost: (S.aiDailyMax ?? 120) * perCall * dim, perCall, by: Object.entries(u.by || {}).map(([k, n]) => ({ k, n, cost: n * perCall })).sort((a, b) => b.n - a.n) },
    host, standby: STANDBY, offsite: ghStatus.on, uptime: process.uptime(), estimated: true,
  };
});
// ===================================================================
// Insights: "Why isn't it trading?" for one bot, and "Compare every strategy" for the lab.
// ===================================================================
// what held a buy back, matched against the bot's own decision notes
const WHY_CATS = [
  { k: 'earnings', re: /Skipped buying: earnings/, label: 'Earnings were coming up', fix: 'Change → Limits and news → set "Skip if earnings are within" to 0' },
  { k: 'severe', re: /serious news/, label: 'Serious news about it', fix: 'Change → Limits and news → turn off "Skip stocks with serious news"' },
  { k: 'newsMood', re: /the mood is negative/, label: 'The news mood was negative', fix: 'Change → Limits and news → lower "Skip if the mood is below"' },
  { k: 'tilt', re: /because of the news mood/, label: 'Sized down for the news mood', fix: 'Change → Limits and news → turn off "Size by the mood"' },
  { k: 'aiVeto', re: /vetoed it/, label: 'The Analyst vetoed it', fix: 'Settings → AI Council members → turn off AI veto' },
  { k: 'council', re: /the council voted/, label: 'The council voted no', fix: 'Settings → Weighted Council votes → turn off "Council veto for every bot"' },
  { k: 'aiSize', re: /cut this buy to/, label: 'The AI made the buy smaller', fix: 'Settings → AI Council members → turn off "Let them shrink risky buys"' },
  { k: 'corr', re: /correlation/, label: 'It moved like something it already holds', fix: 'Settings → Portfolio guards → turn off the correlation guard' },
  { k: 'mood', re: /New buys at|choppy market|falling market/, label: 'The market mood switch made the buy smaller', fix: 'Settings → Portfolio guards → turn off the market mood switch' },
  { k: 'stopped', re: /stop loss sold it/, label: 'Its stop had sold it earlier that day', fix: null },
  { k: 'dailyLoss', re: /daily loss limit/, label: 'The daily loss limit was hit', fix: 'Change → raise "Stop buying after a daily loss of"' },
  { k: 'cash', re: /Not enough buying power/, label: 'Not enough cash in the wallet', fix: 'Add money to the wallet, or use "Add every week"' },
  { k: 'shared', re: /all bots together/, label: 'All bots together already had the most allowed in it', fix: 'Settings → Portfolio guards → raise the shared limits' },
];
// what each strategy is waiting for, for the "no signal yet" case
const WAIT_HINT = {
  trend: 'It needs something above its slow average, with the fast average above the slow one and up over 6 months.',
  momentum: 'It only trades on the first trading day of each month, unless new money comes in.',
  rsi2: 'It needs a sharp 1 to 3 day drop in something that is still above its 200-day average. In a calm rising market that can take a few weeks.',
  breakout: 'It needs a close above the highest high of the last 55 days, in an uptrend, on strong volume.',
  hold: 'It buys everything on its first run and then only adds new money and rebalances monthly.',
  dual: 'It only trades on the first trading day of each month.',
  lowvol: 'It only trades on the first trading day of each month.',
  drift: 'It needs a stock to jump at least 1.5 ATRs on heavy volume and hold the move. That is rare, so a long list of stocks helps.',
  tom: 'It only buys on the last trading day of the month and sells a few days into the next one.',
  snapback: 'It needs something in an uptrend to fall well below its 20-day Bollinger band.',
  earnings: 'It needs a company to beat earnings estimates and the price to jump and hold. Most of these come in earnings season.',
};
async function botWhy(id, n = 30) {
  const b = await row('select * from bots where id=$1', [id]); if (!b) fail('That bot is gone', 404);
  const bot = botJson(b), st = STRATEGIES[bot.strategy] || {}, park = bot.params?.safe && bot.params.safe !== 'cash' ? bot.params.safe : null;
  const logs = (await rows("select ts, data from bot_log where bot_id=$1 and kind='decision' order by id desc limit $2", [id, n])).map(r => ({ ts: tsMs(r.ts), d: jsonOr(r.data, {}) }));
  const cats = {}, wantedSyms = {}, closed = { n: 0 };
  let runs = 0, wanted = 0, bought = 0, sold = 0, blocked = 0, parked = 0;
  for (const { d } of logs) {
    runs++; const cap = d.capital || d.equity || 1;
    for (const r of d.rows || []) {
      if (r.sym === park) { if (r.action === 'buy' || r.action === 'add') parked++; continue; }
      if (r.action === 'buy' || r.action === 'add') bought++;
      if (r.action === 'sell' || r.action === 'trim') sold++;
      const wants = r.want * cap > (r.have || 0) + 5;
      if (!wants) { if (/market is closed/.test(r.note || '')) closed.n++; continue; }
      wanted++; wantedSyms[r.sym] = (wantedSyms[r.sym] || 0) + 1;
      if (r.action === 'buy' || r.action === 'add') continue;
      const text = (r.why || []).join(' ');
      const hit = WHY_CATS.filter(c => c.re.test(text));
      if (hit.length) { blocked++; for (const c of hit) cats[c.k] = (cats[c.k] || 0) + 1; }
      else if (r.target * cap > (r.have || 0) + 5) { blocked++; cats.small = (cats.small || 0) + 1; }
    }
  }
  const reasons = Object.entries(cats).sort((a, b) => b[1] - a[1]).map(([k, c]) => { const w = WHY_CATS.find(x => x.k === k); return { k, n: c, label: w ? w.label : `The buy was too small to place (under $${bot.mandate.minTrade})`, fix: w ? w.fix : 'Add money to the wallet, or use "Add every week"' }; });
  let tone = 'ok', head, detail;
  if (!runs) { tone = 'wait'; head = 'No decisions yet'; detail = 'It makes its first decision at its next run time. Tap Run now to see one right away.'; }
  else if (!wanted) { tone = 'wait'; head = `Its rules haven't found a buy in ${runs} run${runs === 1 ? '' : 's'}`; detail = `Nothing is blocking it: ${st.name || 'the strategy'} is waiting for its setup. ${WAIT_HINT[bot.strategy] || ''}${parked ? ` Meanwhile the money earns interest in ${park}.` : ''}`; }
  else if (blocked && blocked >= wanted * 0.5) { tone = 'warn'; head = `${blocked} of ${wanted} buys it wanted were held back`; detail = `Mostly: ${reasons[0].label.toLowerCase()}. ${reasons[0].fix ? 'To allow more of them: ' + reasons[0].fix + '.' : ''}`; }
  else { head = bought ? `Trading normally: ${bought} buy${bought === 1 ? '' : 's'} and ${sold} sell${sold === 1 ? '' : 's'} in ${runs} runs` : `It wanted ${wanted} buy${wanted === 1 ? '' : 's'}`; detail = blocked ? `${blocked} buy${blocked === 1 ? ' was' : 's were'} held back by the safety checks; that's normal.` : 'Nothing was held back.'; }
  const lastRows = logs[0]?.d?.rows || [];
  const now = lastRows.filter(r => r.sym !== park).slice(0, 6).map(r => ({ sym: r.sym, note: r.note }));
  return { runs, wanted, bought, sold, blocked, parked, park, reasons, tone, head, detail, wantedSyms, now, since: logs.length ? logs[logs.length - 1].ts : null, strategy: st.name };
}
GET('/api/bots/:id/why', auth, req => botWhy(+req.params.id));

// every strategy tested side by side on its suggested list, with your money and deposits
const SUGGEST = {
  trend: ['SPY', 'QQQ', 'IWM', 'EFA', 'EEM', 'GLD', 'TLT', 'XLK', 'XLE', 'XLV'], momentum: ['SPY', 'QQQ', 'IWM', 'EFA', 'EEM', 'TLT', 'IEF', 'GLD', 'DBC', 'VNQ'],
  rsi2: DIP_ETFS, breakout: ['SPY', 'QQQ', 'IWM', 'GLD', 'TLT', 'XLK', 'XLE', 'XLF'], hold: ['SPY', 'QQQ', 'GLD', 'TLT'], dual: ['SPY', 'QQQ', 'EFA', 'EEM', 'IEF', 'TLT'],
  lowvol: ['KO', 'PG', 'JNJ', 'PEP', 'WMT', 'MCD', 'COST', 'MSFT'], drift: DRIFT_SYMS, tom: ['SPY', 'QQQ', 'IWM'], snapback: DIP_ETFS, earnings: DRIFT_SYMS.slice(0, 16), cycle: CYCLE_SYMS, covcall: ['KO', 'PFE', 'VZ', 'T', 'CSCO', 'BAC', 'F', 'INTC'],
};
POST('/api/backtest/compare', auth, async req => {
  botsOn();
  const b = req.body || {}, years = clamp(num(b.years, 5), 1, 8), capital = clamp(num(b.capital, 10000), 25, 1e8), addWeekly = clamp(num(b.addWeekly, 0), 0, 1e6), crypto = !!b.crypto;
  const key = JSON.stringify(['cmp', years, capital, addWeekly, crypto]), hit = btCache.get(key); if (hit && Date.now() - hit.t < 30 * 60e3) return hit.v;
  const out = [];
  for (const [k, st] of Object.entries(STRATEGIES)) {
    if (st.auto || !SUGGEST[k]) continue;
    const syms = crypto && st.markets === 'both' ? ['BTC', 'ETH', 'SOL', ...SUGGEST[k].slice(0, 5)] : SUGGEST[k];
    try {
      const r = await backtest({ strategy: k, syms, params: {}, years, capital, addWeekly, maxPosPct: 25 });
      const bc = r.bcurve || [], bret = bc.length ? bc[bc.length - 1][1] / bc[0][1] - 1 : null;
      out.push({ key: k, name: st.name, icon: st.icon, syms: r.syms, ret: r.stats.ret, cagr: r.stats.cagr, maxDD: r.stats.maxDD, sharpe: r.stats.sharpe, trades: r.trades, bench: r.benchSym || r.bench, benchRet: bret, exposure: r.exposure, end: r.dca ? r.dca.end : r.stats.end, invested: r.dca ? r.dca.invested : capital, years: r.stats.years, short: !!r.shortHistory?.length });
    } catch (e) { out.push({ key: k, name: st.name, icon: st.icon, error: e.message }); }
    await sleep(5);
  }
  // a simple overall score: growth, with drops and roughness counted against it
  for (const x of out) if (x.cagr != null) x.score = x.cagr - Math.abs(x.maxDD) * 0.35 + Math.min(x.sharpe, 3) * 0.02;
  out.sort((a, b) => (b.score ?? -9) - (a.score ?? -9));
  const v = { years, capital, addWeekly, list: out, t: Date.now() };
  btCache.set(key, { t: Date.now(), v });
  return v;
});
// ===================================================================
// The bot leaderboard: fair returns (deposits don't count), every period, a 0-100 score that
// rewards beating the S&P 500 steadily and doesn't let a lucky first week top the table,
// rank changes since yesterday, streaks, badges, sparklines, and a by-strategy view.
// ===================================================================
const BOARD_PERIODS = [['all', null], ['30d', 30], ['7d', 7], ['1d', 1]];
// one bot's numbers from its daily fair-return index ([date, idx], idx starts at 1)
function boardMetrics(days, spyOn, spy0, created) {
  const n = days.length, last = days[n - 1], first = days[0];
  const at = d => { let v = first; for (const x of days) { if (x[0] <= d) v = x; else break; } return v; };
  const per = {};
  for (const [k, nd] of BOARD_PERIODS) {
    let base, sBase;
    if (nd == null) { base = [first[0], 1]; sBase = spy0; }
    else if (nd === 1) { base = n > 1 ? days[n - 2] : [first[0], 1]; sBase = n > 1 ? spyOn(base[0]) : spy0; }
    else { const bd = etDate(Date.parse(last[0] + 'T16:00:00-04:00') - nd * 864e5); if (bd < first[0]) { base = [first[0], 1]; sBase = spy0; } else { base = at(bd); sBase = spyOn(base[0]); } }
    const ret = base[1] > 0 ? last[1] / base[1] - 1 : 0, sNow = spyOn(last[0]), sr = sBase && sNow ? sNow / sBase - 1 : null;
    per[k] = { ret, spy: sr, vs: sr == null ? null : ret - sr, short: nd != null && days.length > 1 && base[0] === first[0] && nd > 1 };
  }
  // day by day: against the S&P on the same days
  const rets = [], beat = [];
  for (let i = 1; i < n; i++) {
    const r = days[i][1] / days[i - 1][1] - 1, s0 = spyOn(days[i - 1][0]), s1 = spyOn(days[i][0]); rets.push(r);
    if (s0 && s1) beat.push(r > s1 / s0 - 1 ? 1 : 0);
  }
  const mean = rets.length ? rets.reduce((t, x) => t + x, 0) / rets.length : 0;
  const sd = rets.length > 1 ? Math.sqrt(rets.reduce((t, x) => t + (x - mean) ** 2, 0) / (rets.length - 1)) : 0;
  const vol = sd * Math.sqrt(252), sharpe = rets.length >= 5 && sd > 1e-6 ? mean / sd * Math.sqrt(252) : null;
  let peak = 1, maxDD = 0; for (const [, v] of days) { peak = Math.max(peak, v); maxDD = Math.min(maxDD, v / peak - 1); }
  const offPeak = last[1] / peak - 1, beatPct = beat.length ? beat.reduce((t, x) => t + x, 0) / beat.length : null;
  let streak = 0; for (let i = rets.length - 1; i >= 0; i--) { const up = rets[i] > 0; if (!streak) streak = up ? 1 : rets[i] < 0 ? -1 : 0; else if (up && streak > 0) streak++; else if (!up && rets[i] < 0 && streak < 0) streak--; else break; if (!streak) break; }
  const age = Math.max(1, (Date.now() - created) / 864e5), years = age / 365.25;
  // the score: excess over the S&P (per year once it has some history), risk-adjusted, drops, and how often it beat the market,
  // pulled toward the middle until the bot has a few weeks of record
  const ex = per.all.vs ?? per.all.ret, exYr = years > 0.25 ? ex / Math.sqrt(years * 4) : ex;
  const parts = { excess: Math.tanh(exYr / 0.05), risk: sharpe == null ? 0 : Math.tanh(sharpe / 2), drop: -Math.tanh(Math.abs(maxDD) / 0.12), steady: beatPct == null ? 0 : (beatPct - 0.5) * 2 };
  const conf = age / (age + 21), raw = 0.45 * parts.excess + 0.25 * parts.risk + 0.15 * (parts.drop + 0.3) + 0.15 * parts.steady;
  const score = Math.round(Math.max(1, Math.min(99, 50 + 50 * raw * conf)));
  return { per, vol, sharpe, maxDD, offPeak, beatPct, streak, score, conf, parts, rets: rets.length, age };
}
async function boardData() {
  const list = await rows('select * from bots order by id'), out = [];
  let spy = null; try { spy = await dailyBars('SPY'); } catch (e) {}
  const today = etDate(), spyNow = priceOf('SPY') || spy?.c[spy.c.length - 1];
  const spyOn = d => { if (d >= today && spyNow) return spyNow; if (!spy) return null; const i = lastIndexOnOrBefore(spy.d, d); return i >= 0 ? spy.c[i] : spy.c[0]; };
  for (const b of list) {
    const bot = botJson(b), w = await row('select * from wallets where id=$1', [b.wid]); if (!w) continue;
    const since = new Date(bot.created), sinceD = etDate(bot.created), st0 = jsonOr(b.state, {});
    const pos = await positions(b.wid), value = +w.cash + marketValue(pos);
    const first = await row('select eq from snap where wid=$1 and ts >= $2 order by ts limit 1', [b.wid, since]);
    const perf = await flowPerf(b.wid, since, value, st0._start0 || (etDate(tsMs(w.created)) === sinceD ? null : first ? +first.eq : null));
    // the fair-return index at each day's close (the last snapshot of each New York day), starting at 1 on day one
    const byDay = new Map([[sinceD, 1]]); for (const [t, v] of perf.curve) byDay.set(etDate(t), v);
    const days = [...byDay.entries()].sort((a, c) => a[0].localeCompare(c[0]));
    const spy0 = st0._spy0 || spyOn(sinceD);
    const mx = boardMetrics(days, spyOn, spy0, bot.created);
    // yesterday's score, for the rank arrows
    const prevDays = days.filter(x => x[0] < today), mxPrev = prevDays.length >= 1 && days.length > prevDays.length ? boardMetrics(prevDays, d => d >= today ? null : spyOn(d), spy0, bot.created - 864e5) : null;
    const sells = await rows("select pnl from trd where wid=$1 and side='sell' and pnl is not null and ts >= $2", [b.wid, since]);
    const wins = sells.filter(x => +x.pnl > 0).length, trades = +(await row("select count(*) n from bot_log where bot_id=$1 and kind='trade'", [b.id])).n;
    const rep = await botReport(b, spy).catch(() => null);
    // sparkline: up to 40 daily points, bot and the S&P from the same day
    const pick = days.length > 40 ? days.filter((_, i) => i % Math.ceil(days.length / 40) === 0 || i === days.length - 1) : days;
    const s0 = spy0 || spyOn(pick[0][0]), spark = pick.map(([d, v]) => [d, +v.toFixed(5), s0 ? +((spyOn(d) || s0) / s0).toFixed(5) : null]);
    out.push({
      id: b.id, name: bot.name, strategy: bot.strategy, sname: STRATEGIES[bot.strategy]?.short || bot.strategy, status: bot.status, broker: !!w.broker, since: bot.created,
      days: Math.max(0, Math.round((Date.now() - bot.created) / 864e5)), value, start: perf.start, added: perf.added, profit: perf.profit,
      ret: mx.per.all.ret, spy: mx.per.all.spy, vs: mx.per.all.vs, per: mx.per, maxDD: mx.maxDD, offPeak: mx.offPeak, vol: mx.vol, sharpe: mx.sharpe, beatPct: mx.beatPct, streak: mx.streak,
      score: mx.score, scorePrev: mxPrev?.score ?? null, conf: mx.conf, parts: mx.parts,
      trades, closed: sells.length, win: sells.length ? wins / sells.length : null, payoff: rep?.payoff ?? null, benched: !!rep?.benched, tune: !!st0._tune?.better, spark,
    });
  }
  // ranks by score now and yesterday
  const rankBy = (arr, k) => { const r = {}; [...arr].filter(x => x[k] != null).sort((a, c) => c[k] - a[k] || c.ret - a.ret).forEach((x, i) => { r[x.id] = i + 1; }); return r; };
  for (const space of [false, true]) {
    const grp = out.filter(x => x.broker === space), now = rankBy(grp, 'score'), prev = rankBy(grp, 'scorePrev');
    for (const x of grp) { x.rank = now[x.id]; x.move = prev[x.id] && grp.filter(y => y.scorePrev != null).length === grp.length ? prev[x.id] - now[x.id] : 0; }
    // badges: one standout per kind, among bots with enough record to mean something
    const seasoned = grp.filter(x => x.days >= 5 && x.spark.length >= 4);
    const best = (arr, f, min) => { let top = null; for (const x of arr) { const v = f(x); if (v == null || (min != null && v <= min)) continue; if (!top || v > top.v) top = { x, v }; } return top?.x; };
    const give = (x, k) => { if (x) (x.badges ||= []).push(k); };
    give(best(seasoned, x => x.per['7d'].vs, 0.002), 'hot');
    give(best(seasoned.filter(x => x.vol > 0 && x.ret > 0), x => -x.vol), 'steady');
    give(best(seasoned.filter(x => x.beatPct != null && x.spark.length >= 8), x => x.beatPct, 0.5), 'consistent');
    give(best(seasoned, x => x.streak, 2), 'streak');
    give(best(seasoned.filter(x => x.offPeak > -0.002 && x.maxDD < -0.03), x => -x.maxDD), 'comeback');
    for (const x of grp) { if (x.days < 14) give(x, 'new'); if (x.benched) give(x, 'benched'); }
  }
  // by strategy: the average of its bots
  const strat = {};
  for (const x of out) { const s = strat[`${x.broker ? 1 : 0}:${x.strategy}`] ||= { strategy: x.strategy, name: STRATEGIES[x.strategy]?.name || x.strategy, broker: x.broker, n: 0, score: 0, ret: 0, vs: 0, profit: 0, best: null }; s.n++; s.score += x.score; s.ret += x.ret; s.vs += x.vs ?? 0; s.profit += x.profit; if (!s.best || x.score > s.best.score) s.best = { id: x.id, name: x.name, score: x.score }; }
  const strategies = Object.values(strat).map(s => ({ ...s, score: Math.round(s.score / s.n), ret: s.ret / s.n, vs: s.vs / s.n })).sort((a, c) => c.score - a.score);
  return { bots: out.sort((a, c) => (a.rank || 99) - (c.rank || 99)), strategies, spyNow, asOf: Date.now() };
}
GET('/api/bots/board', auth, () => boardData());
// =====================================================================
// Research: insiders (Form 4), famous investors (13F), Congress, ETF holdings and overlap,
// earnings history, the economic calendar, short volume, dividend safety
// =====================================================================
// Free public sources first: SEC EDGAR (filings), FINRA (daily short volume), Finnhub's free plan.
// Finnhub's paid data (Congress, ETF holdings, short interest) is used when the key has it.
const SEC_UA = () => ENV.SEC_CONTACT ? `MangoPaperTrading/1.0 ${String(ENV.SEC_CONTACT).slice(0, 120)}` : 'MangoPaperTrading/1.0 (self-hosted research)';
async function fetchText(url, { timeout = 15000, headers = {} } = {}) {
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Mango Paper Trading)', ...headers }, signal: AbortSignal.timeout(timeout) });
  if (r.status === 403 || r.status === 401) throw new HttpError(403, 'refused');
  if (r.status === 404) throw new HttpError(404, 'not found');
  if (!r.ok) throw new HttpError(502, `Data source error ${r.status}`);
  return r.text();
}
// SEC asks every app to say who it is (name and email); without it, it may refuse
const secGet = url => fetchText(url, { headers: { 'User-Agent': SEC_UA(), Accept: '*/*' } }).catch(e => { if (e.status === 403) throw new HttpError(503, ENV.SEC_CONTACT ? 'The SEC refused the request. Try again later.' : 'The SEC asks apps to identify themselves. Add a SEC_CONTACT variable in Railway with your name and email (for example "Alex Smith alex@example.com").'); throw e; });
const secJson = async url => JSON.parse(await secGet(url));
const fhPaid = e => /free plan|access|premium/i.test(e?.message || '');
async function fhMaybe(pathq) { // a Finnhub call that may be paid-only: null when the plan doesn't include it
  try { const d = await fhApi(pathq, 2); if (d && typeof d === 'object' && !Array.isArray(d) && d.error) return { paid: true }; return { d }; }
  catch (e) { if (fhPaid(e)) return { paid: true }; throw e; }
}
const xmlTag = (s, t) => { const m = new RegExp(`<(?:\\w+:)?${t}[^>]*>([\\s\\S]*?)</(?:\\w+:)?${t}>`, 'i').exec(s); return m ? m[1].trim() : null; };
const xmlAll = (s, t) => { const out = [], re = new RegExp(`<(?:\\w+:)?${t}[^>]*>([\\s\\S]*?)</(?:\\w+:)?${t}>`, 'gi'); let m; while ((m = re.exec(s))) out.push(m[1]); return out; };
const unXml = s => s == null ? null : s.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;|&apos;/g, "'");
// company names in filings ("APPLE INC") matched to tickers ("Apple Inc.")
const normName = s => String(s || '').toUpperCase().replace(/&/g, ' AND ').replace(/[^A-Z0-9 ]/g, ' ')
  .replace(/\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|PLC|HLDGS|HLDG|HOLDINGS?|GROUP|GRP|CL [A-C]|CLASS [A-C]|CL|COM|COMMON|STOCK|NEW|DEL|THE|SA|NV|AG|LP|LLC|SHS|ORD|SPONSORED|ADR|ADS|REIT|TR|TRUST)\b/g, ' ').replace(/\s+/g, ' ').trim();
async function secTickers() {
  return cached('sec:tickers', 7 * 864e5, async () => {
    const j = await secJson('https://www.sec.gov/files/company_tickers.json'), byName = new Map(), byTicker = new Map();
    for (const v of Object.values(j)) { byTicker.set(v.ticker, { cik: v.cik_str, title: v.title }); const k = normName(v.title); if (k && !byName.has(k)) byName.set(k, v.ticker); }
    return { byName, byTicker };
  });
}
const tickerFor = (t, name) => t.byName.get(normName(name)) || null;

// ---------- famous investors: their latest 13F (quarterly holdings, 45 days after the quarter) ----------
const FUND13F = [
  { k: 'berkshire', cik: 1067983, name: 'Berkshire Hathaway', who: 'Warren Buffett' },
  { k: 'scion', cik: 1649339, name: 'Scion Asset Management', who: 'Michael Burry' },
  { k: 'pershing', cik: 1336528, name: 'Pershing Square', who: 'Bill Ackman' },
  { k: 'ark', cik: 1697748, name: 'ARK Investment Management', who: 'Cathie Wood' },
  { k: 'duquesne', cik: 1536411, name: 'Duquesne Family Office', who: 'Stanley Druckenmiller' },
  { k: 'appaloosa', cik: 1656456, name: 'Appaloosa', who: 'David Tepper' },
  { k: 'baupost', cik: 1061768, name: 'Baupost Group', who: 'Seth Klarman' },
  { k: 'thirdpoint', cik: 1040273, name: 'Third Point', who: 'Dan Loeb' },
  { k: 'bridgewater', cik: 1350694, name: 'Bridgewater Associates', who: 'Ray Dalio\'s firm' },
];
const cik10 = c => String(c).padStart(10, '0');
async function info13f(cik, acc, filed) {
  const base = `https://www.sec.gov/Archives/edgar/data/${cik}/${acc.replace(/-/g, '')}`;
  const idx = await secJson(base + '/index.json'), files = (idx.directory?.item || []).map(x => x.name);
  const xml = files.find(f => /\.xml$/i.test(f) && !/primary_doc/i.test(f)) || files.find(f => /infotable|informationtable/i.test(f));
  if (!xml) throw new HttpError(502, 'No holdings table in that filing');
  const body = await secGet(`${base}/${xml}`), thousands = filed < '2023-01-03'; // values were in thousands of dollars before 2023
  const map = new Map();
  for (const it of xmlAll(body, 'infoTable')) {
    if (xmlTag(it, 'putCall')) continue; // options on a stock aren't owning it
    const cusip = xmlTag(it, 'cusip'), name = unXml(xmlTag(it, 'nameOfIssuer')), val = +xmlTag(it, 'value') * (thousands ? 1000 : 1), sh = +xmlTag(it, 'sshPrnamt') || 0, type = xmlTag(it, 'sshPrnamtType');
    if (type && type !== 'SH') continue;
    const e = map.get(cusip) || { cusip, name, value: 0, shares: 0 }; e.value += val || 0; e.shares += sh; map.set(cusip, e);
  }
  return [...map.values()];
}
async function fund13f(f) {
  return cached('13f:' + f.k, 24 * 3600e3, async () => {
    const sub = await secJson(`https://data.sec.gov/submissions/CIK${cik10(f.cik)}.json`), r = sub.filings?.recent || {};
    const list = (r.form || []).map((form, i) => ({ form, acc: r.accessionNumber[i], filed: r.filingDate[i], period: r.reportDate[i] })).filter(x => x.form === '13F-HR');
    if (!list.length) throw new HttpError(404, 'No 13F filings found');
    const [cur, prev] = list;
    const [now, before] = await Promise.all([info13f(f.cik, cur.acc, cur.filed), prev ? info13f(f.cik, prev.acc, prev.filed).catch(() => null) : null]);
    const t = await secTickers().catch(() => null), total = now.reduce((s, x) => s + x.value, 0) || 1, pm = new Map((before || []).map(x => [x.cusip, x]));
    const holdings = now.map(x => { const p = pm.get(x.cusip); return { ...x, sym: t ? tickerFor(t, x.name) : null, pct: x.value / total, prevShares: p ? p.shares : 0, change: !before ? null : !p ? 'new' : x.shares > p.shares * 1.02 ? 'added' : x.shares < p.shares * 0.98 ? 'trimmed' : 'same' }; }).sort((a, b) => b.value - a.value);
    const sold = before ? before.filter(x => !now.some(y => y.cusip === x.cusip)).map(x => ({ ...x, sym: t ? tickerFor(t, x.name) : null })).sort((a, b) => b.value - a.value) : [];
    return { k: f.k, name: f.name, who: f.who, secName: sub.name, period: cur.period, filed: cur.filed, prevPeriod: prev?.period || null, total, count: holdings.length, holdings, sold, url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${f.cik}&type=13F-HR` };
  });
}
let warming = null;
function warmFunds() { if (!warming) warming = (async () => { for (const f of FUND13F) { await fund13f(f).catch(e => warn('13F', f.k, e.message)); await sleep(400); } })().finally(() => { warming = null; }); return warming; }
const fundCached = f => cache.get('13f:' + f.k)?.v || null;
GET('/api/research/funds', auth, async () => {
  const have = FUND13F.map(fundCached); if (have.some(x => !x)) warmFunds();
  return { funds: FUND13F.map((f, i) => { const d = have[i]; return { k: f.k, name: f.name, who: f.who, ready: !!d, period: d?.period, filed: d?.filed, total: d?.total, count: d?.count, top: d ? d.holdings.slice(0, 3).map(x => x.sym || x.name) : [] }; }), loading: have.some(x => !x), contact: !!ENV.SEC_CONTACT };
});
GET('/api/research/fund/:k', auth, async req => {
  const f = FUND13F.find(x => x.k === req.params.k); if (!f) fail('Unknown fund', 404);
  const d = await fund13f(f);
  return { ...d, holdings: d.holdings.slice(0, 60), sold: d.sold.slice(0, 20) };
});
function fundsHolding(sym) {
  const out = []; let pending = false;
  for (const f of FUND13F) { const d = fundCached(f); if (!d) { pending = true; continue; } const h = d.holdings.find(x => x.sym === sym); if (h) out.push({ k: f.k, name: f.name, who: f.who, shares: h.shares, value: h.value, pct: h.pct, change: h.change, prevShares: h.prevShares, period: d.period }); else { const s = d.sold.find(x => x.sym === sym); if (s) out.push({ k: f.k, name: f.name, who: f.who, shares: 0, value: 0, pct: 0, change: 'sold', prevShares: s.shares, period: d.period }); } }
  if (pending) warmFunds();
  return { funds: out.sort((a, b) => b.value - a.value), pending };
}

// ---------- insiders: Form 4 buys and sells, via Finnhub's free plan ----------
const INSIDER_CODE = { P: 'Bought on the market', S: 'Sold on the market', A: 'Granted (pay)', M: 'Exercised options', F: 'Shares withheld for tax', G: 'Gift', D: 'Returned to the company', C: 'Converted', X: 'Exercised options', J: 'Other' };
async function insiders(sym) {
  return cached('ins4:' + sym, 6 * 3600e3, async () => {
    const d = await fhApi(`stock/insider-transactions?symbol=${encodeURIComponent(sym)}&from=${etDate(Date.now() - 200 * 864e5)}`, 2);
    const list = (d?.data || []).filter(x => x.transactionDate >= etDate(Date.now() - 183 * 864e5)).map(x => ({ name: x.name, date: x.transactionDate, filed: x.filingDate, code: x.transactionCode, what: INSIDER_CODE[x.transactionCode] || x.transactionCode, change: +x.change || 0, price: +x.transactionPrice || 0, after: +x.share || 0, value: Math.abs(+x.change || 0) * (+x.transactionPrice || 0) }))
      .sort((a, b) => b.date.localeCompare(a.date));
    const buys = list.filter(x => x.code === 'P'), sells = list.filter(x => x.code === 'S');
    const people = new Set(buys.map(x => x.name));
    return { list: list.slice(0, 40), buys: buys.length, sells: sells.length, buyValue: buys.reduce((t, x) => t + x.value, 0), sellValue: sells.reduce((t, x) => t + x.value, 0), buyers: people.size,
      cluster: people.size >= 3, note: 'Open-market buys (code P) are the signal people watch: insiders sell for many reasons, but they usually buy for one. Grants, option exercises and tax withholding are routine.' };
  });
}
// ---------- Congress: trades members disclose under the STOCK Act (Finnhub paid data) ----------
async function congress(sym) {
  return cached('cong:' + sym, 12 * 3600e3, async () => {
    const r = await fhMaybe(`stock/congressional-trading?symbol=${encodeURIComponent(sym)}&from=${etDate(Date.now() - 365 * 864e5)}&to=${etDate()}`);
    if (r.paid) return { available: false, why: 'Congress trades are on Finnhub\'s paid plans. With a key that has them, they show up here.' };
    const list = (r.d?.data || []).map(x => ({ name: x.name, position: x.position, owner: x.ownerType, type: x.transactionType, date: x.transactionDate, filed: x.filingDate, from: +x.amountFrom || 0, to: +x.amountTo || 0 })).sort((a, b) => b.date.localeCompare(a.date));
    return { available: true, list: list.slice(0, 30), buys: list.filter(x => /purchase|buy/i.test(x.type)).length, sells: list.filter(x => /sale|sell/i.test(x.type)).length,
      note: 'Members of Congress have up to 45 days to report a trade, and only a dollar range is disclosed.' };
  });
}

// ---------- ETF holdings: Finnhub (paid) or the fund's latest SEC portfolio report (N-PORT) ----------
const ETF_PROXY = { SPY: 'IVV', VOO: 'IVV', SPLG: 'IVV', QQQ: 'QQQM', DIA: null }; // unit trusts don't file N-PORT; these track the same index
async function nportFor(sym) {
  const mf = await cached('sec:mf', 7 * 864e5, () => secJson('https://www.sec.gov/files/company_tickers_mf.json'));
  const fi = mf.fields || ['cik', 'seriesId', 'classId', 'symbol'], row0 = (mf.data || []).find(r => r[fi.indexOf('symbol')] === sym);
  if (!row0) return null;
  const cik = row0[fi.indexOf('cik')], series = row0[fi.indexOf('seriesId')];
  const atom = await secGet(`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${series}&type=NPORT-P&dateb=&owner=include&count=5&output=atom`);
  const acc = xmlTag(atom, 'accession-number') || (/(\d{10}-\d{2}-\d{6})/.exec(atom) || [])[1]; if (!acc) return null;
  const xml = await secGet(`https://www.sec.gov/Archives/edgar/data/${cik}/${acc.replace(/-/g, '')}/primary_doc.xml`);
  const asOf = xmlTag(xml, 'repPdDate') || xmlTag(xml, 'repPdEnd'), t = await secTickers().catch(() => null);
  const rows = xmlAll(xml, 'invstOrSec').map(it => ({ name: unXml(xmlTag(it, 'name') || xmlTag(it, 'title')), cusip: xmlTag(it, 'cusip'), pct: (+xmlTag(it, 'pctVal') || 0) / 100, value: +xmlTag(it, 'valUSD') || 0 }))
    .filter(x => x.pct > 0).map(x => ({ ...x, sym: t ? tickerFor(t, x.name) : null })).sort((a, b) => b.pct - a.pct);
  return { asOf, holdings: rows, source: 'SEC N-PORT' };
}
async function etfHoldings(sym) {
  return cached('etfh:' + sym, 3 * 864e5, async () => {
    if (FINNHUB_KEY) { const r = await fhMaybe('etf/holdings?symbol=' + encodeURIComponent(sym)).catch(() => ({ paid: true })); if (!r.paid && r.d?.holdings?.length) return { sym, asOf: r.d.atDate, source: 'Finnhub', holdings: r.d.holdings.map(x => ({ sym: x.symbol, name: x.name, pct: (+x.percent || 0) / 100, value: +x.value || 0 })).sort((a, b) => b.pct - a.pct) }; }
    let d = await nportFor(sym).catch(e => { if (e.status === 503) throw e; return null; }), via = null;
    if (!d && ETF_PROXY[sym]) { d = await nportFor(ETF_PROXY[sym]).catch(e => { if (e.status === 503) throw e; return null; }); via = ETF_PROXY[sym]; }
    if (!d) fail(`No holdings report found for ${sym}. Funds file their holdings with the SEC every quarter; some (like unit trusts) don't.`, 404);
    return { sym, asOf: d.asOf, source: d.source, via, holdings: d.holdings };
  });
}
const holdKey = x => x.sym || normName(x.name);
function overlapOf(a, b) {
  const mb = new Map(b.holdings.map(x => [holdKey(x), x])); let ov = 0; const both = [];
  for (const x of a.holdings) { const y = mb.get(holdKey(x)); if (!y) continue; const m = Math.min(x.pct, y.pct); ov += m; both.push({ sym: x.sym, name: x.name, a: x.pct, b: y.pct, min: m }); }
  return { overlap: ov, count: both.length, countA: a.holdings.length, countB: b.holdings.length, both: both.sort((p, q) => q.min - p.min).slice(0, 25) };
}
GET('/api/research/etf/:sym', auth, async req => { const d = await etfHoldings(cleanSym(req.params.sym)); return { ...d, count: d.holdings.length, holdings: d.holdings.slice(0, 50), top10: d.holdings.slice(0, 10).reduce((t, x) => t + x.pct, 0) }; });
GET('/api/research/overlap', auth, async req => {
  const a = cleanSym(req.query.a), b = cleanSym(req.query.b); if (!a || !b || a === b) fail('Pick two different funds'); learnSeen('overlap');
  const [A, B] = await Promise.all([etfHoldings(a), etfHoldings(b)]);
  return { a, b, asOfA: A.asOf, asOfB: B.asOf, viaA: A.via || null, viaB: B.via || null, ...overlapOf(A, B) };
});

// ---------- earnings history: beat or miss, and how the stock moved ----------
async function earningsHistory(sym) {
  return cached('eh:' + sym, 12 * 3600e3, async () => {
    const [eps, cal, bars] = await Promise.all([
      fhApi('stock/earnings?symbol=' + encodeURIComponent(sym), 2).catch(() => []),
      fhApi(`calendar/earnings?from=${etDate(Date.now() - 400 * 864e5)}&to=${etDate()}&symbol=${encodeURIComponent(sym)}`, 2).catch(() => null),
      dailyBars(sym).catch(() => null),
    ]);
    const dates = (cal?.earningsCalendar || []).filter(x => x.date <= etDate()).sort((a, b) => b.date.localeCompare(a.date));
    const rows = (Array.isArray(eps) ? eps : []).slice(0, 8).map(x => {
      // the report that covers this quarter is the first one within ~100 days after its end
      const rep = dates.find(d => d.date > x.period && d.date <= addIso(x.period, 100));
      let move = null;
      if (rep && bars?.d?.length) {
        const i = lastIndexOnOrBefore(bars.d, rep.date), amc = rep.hour === 'amc';
        const before = amc ? i : i - 1, after = amc ? i + 1 : i;
        if (before >= 0 && after < bars.c.length && bars.d[i] === rep.date) move = bars.c[after] / bars.c[before] - 1;
      }
      return { period: x.period, q: x.quarter, y: x.year, actual: x.actual, estimate: x.estimate, surprise: x.surprisePercent, beat: x.actual != null && x.estimate != null ? x.actual >= x.estimate : null, date: rep?.date || null, hour: rep?.hour || null, move };
    });
    const known = rows.filter(r => r.beat != null), moves = rows.filter(r => r.move != null).map(r => Math.abs(r.move));
    return { rows, beats: known.filter(r => r.beat).length, of: known.length, avgMove: moves.length ? moves.reduce((t, x) => t + x, 0) / moves.length : null, next: (await assetInsights(sym).catch(() => ({}))).next || null };
  });
}
// ---------- short selling: FINRA's daily short volume (free), and short interest when Finnhub has it ----------
async function finraDay(day) {
  return cached('finra:' + day, 24 * 3600e3, async () => {
    const t = await fetchText(`https://cdn.finra.org/equity/regsho/daily/CNMSshvol${day.replace(/-/g, '')}.txt`, { timeout: 20000 });
    const m = new Map();
    for (const line of t.split('\n')) { const p = line.split('|'); if (p.length < 5 || p[0] === 'Date') continue; m.set(p[1], { short: +p[2] || 0, exempt: +p[3] || 0, total: +p[4] || 0 }); }
    if (!m.size) throw new HttpError(404, 'empty');
    return m;
  });
}
async function shortData(sym) {
  return cached('short:' + sym, 6 * 3600e3, async () => {
    const days = []; let p = etParts(), d = [p.y, p.m, p.d];
    for (let k = 0; k < 12 && days.length < 5; k++) { const iso = ymd(d); if (isTradingDay(...d) && (iso < etDate() || p.h >= 19)) { const m = await finraDay(iso).catch(() => null); if (m) { const x = m.get(sym); if (x) days.push({ date: iso, ...x, ratio: x.total ? x.short / x.total : null }); } } d = addDays(...d, -1); }
    let si = null;
    if (FINNHUB_KEY) { const r = await fhMaybe(`stock/short-interest?symbol=${encodeURIComponent(sym)}&from=${etDate(Date.now() - 120 * 864e5)}&to=${etDate()}`).catch(() => ({ paid: true })); if (!r.paid) { const l = (r.d?.data || []).sort((a, b) => b.settleDate?.localeCompare(a.settleDate)); if (l[0]) si = { date: l[0].settleDate, shares: +l[0].shortInterest || 0 }; } }
    const avg = days.length ? days.reduce((t, x) => t + (x.ratio || 0), 0) / days.length : null;
    return { days, ratio: days[0]?.ratio ?? null, avg5: avg, interest: si,
      note: 'Short volume is the share of each day\'s trades marked "short" on US off-exchange venues. Market makers short all the time to fill buy orders, so 40 to 50% is normal; it is not the same as short interest (shares sold short and still open).' };
  });
}
// ---------- dividend safety: payout, growth, debt and its own payment history ----------
async function dividendSafety(sym) {
  return cached('dsafe:' + sym, 12 * 3600e3, async () => {
    const [met, ins] = await Promise.all([cached('met:' + sym, 6 * 3600e3, () => fhApi('stock/metric?symbol=' + encodeURIComponent(sym) + '&metric=all', 2)).catch(() => null), assetInsights(sym).catch(() => ({}))]);
    const m = met?.metric || {}, divs = (ins.dividends || []).slice().sort((a, b) => a.date - b.date);
    if (!divs.length && !(m.dividendYieldIndicatedAnnual > 0)) return { pays: false };
    const pick = (...k) => { for (const x of k) if (m[x] != null && Number.isFinite(+m[x])) return +m[x]; return null; };
    const payout = pick('payoutRatioTTM', 'payoutRatioAnnual'), growth5 = pick('dividendGrowthRate5Y'), eps = pick('epsGrowthTTMYoy', 'epsGrowth3Y'), de = pick('totalDebt/totalEquityQuarterly', 'totalDebt/totalEquityAnnual'), yld = pick('dividendYieldIndicatedAnnual', 'currentDividendYieldTTM');
    let cuts = 0; for (let i = 1; i < divs.length; i++) if (divs[i].amount < divs[i - 1].amount * 0.9) cuts++;
    const checks = [];
    const add = (label, ok, have, why, w) => checks.push({ label, ok, have, why, w });
    if (payout != null) add('Pays out a safe share of profits', payout <= 60 ? 1 : payout <= 80 ? 0.6 : payout <= 100 ? 0.25 : 0, `${Math.round(payout)}% of earnings`, 'Under 60% leaves room to keep paying in a bad year. Over 100% means it pays more than it earns.', 30);
    if (growth5 != null) add('Has been raising it', growth5 > 2 ? 1 : growth5 >= 0 ? 0.6 : 0, `${growth5 >= 0 ? '+' : ''}${growth5.toFixed(1)}% a year over 5 years`, 'Companies that keep raising their dividend rarely cut it.', 20);
    add('No cuts in recent years', cuts === 0 ? 1 : cuts === 1 ? 0.3 : 0, cuts ? `${cuts} cut${cuts > 1 ? 's' : ''} in ${divs.length} payments` : `${divs.length} payments, none cut`, 'A past cut is the clearest warning sign.', 25);
    if (eps != null) add('Earnings growing', eps > 0 ? 1 : eps > -10 ? 0.5 : 0, `${eps >= 0 ? '+' : ''}${eps.toFixed(1)}% over the last year`, 'Shrinking profits squeeze the dividend.', 15);
    if (de != null) add('Manageable debt', de < 1 ? 1 : de < 2 ? 0.6 : 0.2, `${de.toFixed(2)} debt for each $1 of equity`, 'Heavy debt competes with dividends for cash.', 10);
    if (yld != null && yld > 8) add('Yield not suspiciously high', 0, `${yld.toFixed(1)}%`, 'A very high yield often means the market expects a cut.', 15);
    const wt = checks.reduce((t, c) => t + c.w, 0) || 1, score = Math.round(checks.reduce((t, c) => t + c.ok * c.w, 0) / wt * 100);
    const thin = checks.length < 3; // one or two numbers aren't enough to call it
    return { pays: true, score: thin ? null : score, grade: thin ? 'Not enough data' : score >= 75 ? 'Safe' : score >= 50 ? 'Okay' : 'At risk', yield: yld, checks, note: 'A rough guide from public numbers, not a promise. Funds pass through what they hold, so this is most useful for single companies.' };
  });
}

// ---------- the economic calendar ----------
// Official 2026–27 schedules from the Federal Reserve, BLS and BEA (all at 8:30 am Eastern unless noted). They change rarely; Finnhub's calendar is used too when the key has it.
const ECON = [
  ...[['2026-10-28', 0], ['2026-12-09', 1], ['2027-01-27', 0], ['2027-03-17', 1], ['2027-04-28', 0], ['2027-06-09', 1], ['2027-07-28', 0], ['2027-09-15', 1], ['2027-10-27', 0], ['2027-12-08', 1]].map(([d, sep]) => ({ date: d, time: '2:00 pm', k: 'fomc', name: 'Fed interest rate decision', why: `The Federal Reserve sets its interest rate${sep ? ', with new forecasts and the "dot plot"' : ''}. The chair speaks at 2:30 pm. Stocks often swing in the hour after.` })),
  ...['2026-11-10', '2026-12-10', '2027-01-13', '2027-02-11', '2027-03-10', '2027-04-13', '2027-05-12', '2027-06-10', '2027-07-14', '2027-08-11', '2027-09-14', '2027-10-14', '2027-11-10', '2027-12-10'].map(d => ({ date: d, time: '8:30 am', k: 'cpi', name: 'Inflation (CPI)', why: 'Consumer prices for last month. Hotter than expected inflation usually means higher rates for longer, which tends to hit growth stocks and bonds.' })),
  ...['2026-11-06', '2026-12-04', '2027-01-08', '2027-02-05', '2027-03-05', '2027-04-02', '2027-05-07', '2027-06-04', '2027-07-02', '2027-08-06', '2027-09-03', '2027-10-08', '2027-11-05', '2027-12-03'].map(d => ({ date: d, time: '8:30 am', k: 'jobs', name: 'Jobs report', why: 'Jobs added and the unemployment rate. A big surprise either way moves stocks, bonds and the dollar.' })),
  { date: '2026-10-29', time: '8:30 am', k: 'gdp', name: 'GDP, Q3 first estimate', why: 'How fast the economy grew last quarter.' },
  { date: '2026-11-25', time: '8:30 am', k: 'gdp', name: 'GDP, Q3 second estimate', why: 'A revised look at last quarter\'s growth.' },
  { date: '2026-12-23', time: '8:30 am', k: 'gdp', name: 'GDP, Q3 third estimate', why: 'The final revision for last quarter.' },
  ...['2026-10-29', '2026-11-25', '2026-12-23'].map(d => ({ date: d, time: '8:30 am', k: 'pce', name: 'Spending and PCE inflation', why: 'Consumer spending, and the inflation measure the Fed watches most.' })),
  { date: '2026-12-15', time: '8:30 am', k: 'ppi', name: 'Producer prices (PPI)', why: 'What businesses pay. Often hints at where consumer inflation is going.' },
  { date: '2026-12-01', time: '10:00 am', k: 'jolts', name: 'Job openings (JOLTS)', why: 'How many jobs employers are trying to fill.' },
];
GET('/api/research/calendar', auth, async () => {
  const today = etDate(), until = etDate(Date.now() + 75 * 864e5);
  let events = ECON.filter(e => e.date >= today && e.date <= until).map(e => ({ ...e, src: 'official' }));
  // earnings for what you own and watch, in the same list
  const syms = [...new Set([...(await rows('select distinct sym from pos')).map(r => r.sym), ...(await rows('select sym from watch')).map(r => r.sym)])].filter(s => !isCrypto(s));
  const cal = await earningsCal().catch(() => ({}));
  for (const s of syms) { const e = cal[s]; if (e && e.date >= today && e.date <= until) events.push({ date: e.date, time: e.hour === 'bmo' ? 'Before the open' : e.hour === 'amc' ? 'After the close' : '', k: 'earn', sym: s, name: `${s} earnings`, why: e.eps != null ? `Analysts expect ${money(e.eps)} a share.` : 'Quarterly results.' }); }
  events.sort((a, b) => a.date.localeCompare(b.date) || (a.k === 'earn') - (b.k === 'earn'));
  return { events, last: ECON.reduce((t, e) => e.date > t ? e.date : t, ''), sources: ['Federal Reserve', 'Bureau of Labor Statistics', 'Bureau of Economic Analysis'] };
});

// ---------- one stock's research, one section at a time (the screen loads them in parallel) ----------
const R_PARTS = { insiders, congress, earnings: earningsHistory, short: shortData, dividend: dividendSafety, funds: async s => fundsHolding(s), etf: s => etfHoldings(s).then(d => ({ ...d, count: d.holdings.length, holdings: d.holdings.slice(0, 15), top10: d.holdings.slice(0, 10).reduce((t, x) => t + x.pct, 0) })) };
GET('/api/research/s/:sym/:part', auth, async req => {
  const sym = cleanSym(req.params.sym), fn = R_PARTS[req.params.part]; if (!sym || !fn) fail('Unknown research', 404);
  if (isCrypto(sym)) fail('Research is for stocks and funds'); learnSeen('research');
  try { return { ok: true, ...(await fn(sym)) }; }
  catch (e) { return { ok: false, error: e.status === 503 && /FINNHUB/.test(e.message) ? 'Add FINNHUB_KEY in Railway to turn this on.' : e.message }; }
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
  every(fast ? 5e3 : 60e3, () => { if (S.f_bots !== false && !standbyPaused()) return botScheduler(); }, 'bots');
  every(fast ? 5e3 : 10 * 60e3, standbyPing, 'standby keep-awake'); setTimeout(() => standbyPing().catch(() => {}), 20e3);
  every(fast ? 5e3 : 10 * 60e3, backupJob, 'backups');
  every(fast ? 5e3 : 30 * 60e3, settleCouncil, 'council');
  // smarter bots: weekly bench check, one re-tune an hour (each bot about monthly), tournaments scored every 6 hours
  every(fast ? 30e3 : 6 * 3600e3, () => { if (!standbyPaused()) return benchCheck(); }, 'bench');
  every(fast ? 600e3 : 3600e3, () => { if (!standbyPaused()) return tuneJob(); }, 'tune');
  every(fast ? 20e3 : 6 * 3600e3, () => { if (!standbyPaused()) return tournamentJob(); }, 'tournaments');
  // more AI: the morning brief, the Friday letter from the chair, post-mortems on losing trades
  every(fast ? 5e3 : 60e3, () => { if (!standbyPaused()) return briefJob(); }, 'brief');
  every(fast ? 5e3 : 5 * 60e3, () => { if (!standbyPaused()) return letterJob(); }, 'letter');
  every(fast ? 10e3 : 10 * 60e3, () => { if (!standbyPaused()) return postmortemJob(); }, 'post-mortems');
  // practice tools: chart-signal alerts, options expiry, the monthly review
  every(fast ? 5e3 : 5 * 60e3, () => indAlertJob(), 'chart alerts');
  every(fast ? 10e3 : 60e3, () => optJob(), 'options');
  every(6 * 3600e3, () => { if (!standbyPaused()) return warmFunds(); }, 'research');
  every(fast ? 60e3 : 6 * 3600e3, () => { if (!standbyPaused()) return decayJob(); }, 'decay');
  every(fast ? 10e3 : 60e3, () => dcallJob(), 'daily call'); // each bot checked about weekly setTimeout(() => { if (!standbyPaused()) warmFunds(); }, fast ? 15e3 : 5 * 60e3); // famous investors' 13F filings
  every(fast ? 10e3 : 30 * 60e3, () => reviewJob(), 'monthly review');
  // safety: offsite backup (nightly), uptime watch between main and standby
  every(fast ? 10e3 : 10 * 60e3, () => offsiteJob(), 'offsite backup');
  every(fast ? 5e3 : 3 * 60e3, () => uptimeJob(), 'uptime');
  setTimeout(() => upgradeAllBots().catch(e => warn('upgrade', e.message)), 8e3); // bring every bot up to the latest version of its strategy
  every(3600e3, () => Q("delete from bot_log where ts < now() - interval '2 years'"), 'bot log cleanup');
  every(3600e3, () => Q("delete from settle where settles < now() - interval '7 days'"), 'settlement cleanup');
  if (BROKER && STANDBY) warn('Standby copy has Alpaca keys: orders are blocked while standby is off, but keep real-money keys on one server only.');
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
const APP_VERSION = '4.3.0'; // stamped by build.sh from package.json
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
  await loadSettings(); await refreshPasskeysOn(); await loadAssets(); await loadPriceCache(); await migrateOptSandbox().catch(e => warn('options sandbox', e.message)); await loadOrders(); await loadMstate(); await loadProfile(); await loadAlerts(); await initPush();
  await refreshUserSyms(); await walletsSummary();
  server.listen(PORT, () => log(`🥭 Mango is running on port ${PORT} · v${APP_VERSION} (build ${BUILD}) · ${FINNHUB_KEY ? 'stocks on' : 'stocks off (no FINNHUB_KEY)'}${BROKER ? ` · Alpaca ${BROKER.env}` : ''}`));
  cbConnect(); fhConnect(); if (BROKER) adConnect(); startJobs();
})().catch(e => { console.error('Mango could not start:', e); process.exit(1); });
