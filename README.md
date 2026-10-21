# Mango Paper Trading

A paper trading app for iPhone with real market prices. It runs from the Home Screen and is built on Node and Postgres.

Trade stocks, ETFs and crypto with practice money. You can also put strategies to work as bots, let Autopilot run a mix of them, and watch it all happen in **Mango HQ**: a small isometric world where your bots walk around and a six-member Council votes on every trade. If you connect an Alpaca account, the same app can trade it, on paper or with real money, on a separate side with its own safeguards.

---

## What it does

**Trading**
- Real-time prices for stocks and crypto, with charts, a watchlist, news, movers and a heat map.
- Market, limit, stop, stop-limit and trailing-stop orders, plus extended hours and orders queued for the open.
- Several wallets, each with its own rules: Robinhood-style, Alpaca-style or a simple practice wallet. Fees, settlement and margin are modeled.
- Price alerts, push notifications, stats, achievements and prediction games.

**Bots**
- Strategies include trend, momentum, RSI-2 dips, breakout, dual momentum, low volatility, news drift, turn of the month, snapback and hold.
- Each bot owns a wallet and decides once a day. Before buying it checks the news, StockTwits and upcoming earnings, keeps a stop loss on every position, and writes down why it did what it did.
- Limits are per bot: position size, trades per day, daily loss, a drawdown stop and a budget.
- **Autopilot** tests 21 strategy tryouts on pretend money and backs the ones that are working. It also handles volatility targeting, a safety switch and a brake after a drop.
- A strategy lab for backtesting up to 10 years with out-of-sample checks, and a leaderboard that compares every bot against simply holding the S&P 500.

**Mango HQ**
- Each bot is a character in one of four themed worlds: **Office**, **Starship**, **Village** or **Island**. Each world has its own rooms, furniture, people, sky and way of talking.
- Bots really do what they're shown doing: they read the news, take buys and sells to the Council, and trade at the desk. Speech bubbles carry real trades and journal entries.
- **The Council** is six rule-based voters (trend, momentum, news, risk, stats and contrarian) who vote on every buy and sell. Votes are checked a week later, and "Who called it?" shows who has been right. An optional council veto lets a majority NO block a buy.
- Day and night follow the market clock. You can drag, fling, pinch to zoom, replay the day, follow a bot, or turn on the auto camera.

**Alpaca (real brokerage)**
- A separate side of the app for an Alpaca account, paper or live.
- Safeguards on that side: a kill switch, a daily loss limit, a per-order cap, a real-money cap per bot, and Face ID confirmation for real-money actions.
- Bots earn their way over: you can move a copy of a paper bot to Alpaca with its own budget.

**Security and data**
- Password login, with Face ID login and confirmation through passkeys.
- Nightly backups (7 kept), plus download and restore.
- Standby mode for running a backup copy on a second host (see below).

---

## Running it

The repo holds the built app: `server.js` (the server) and `index.html` (the whole front end in one file). The host runs:

```
npm install
npm start
```

It needs **Node 20+** and a **Postgres** database. The database tables are created and upgraded automatically on startup.

### Environment variables

| Variable | Required | What it's for |
|---|---|---|
| `DATABASE_URL` | yes | Postgres connection string |
| `APP_USER` | yes | Login username |
| `APP_PASS` | yes | Login password |
| `SESSION_SECRET` | recommended | Signs login sessions. Use a long random value. If it's missing, the password is used instead. |
| `FINNHUB_KEY` | recommended | Free key from finnhub.io for live stock prices, news and earnings dates. Crypto works without it. |
| `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY` | optional | Connect an Alpaca account |
| `ALPACA_ENV` | optional | `paper` (default) or `live`. Real money only ever turns on here, never from inside the app. |
| `ALPACA_MAX_ORDER` | optional | Largest real-money order allowed, in dollars |
| `ALPACA_FEED` | optional | `iex` (default) or `sip` |
| `VAPID_EMAIL` | optional | Contact address for push notifications (`mailto:...`) |
| `MANGO_STANDBY` | optional | Set to `1` on a backup copy only (see below) |
| `PUBLIC_URL` | optional | The standby copy's own address, if the host doesn't provide one (Render does) |
| `PORT` | set by host | The port to listen on |

Keep secrets in your host's variables. Never put them in the code or commit them to the repo.

---

## Standby copy (backup server)

You can run a second copy on another host, for example Render's free plan with a free Supabase database, to switch to when your main server is down.

1. Deploy the same repo on the second host with its own database, and set `MANGO_STANDBY=1`.
2. Use its own `APP_PASS`, `SESSION_SECRET` and `FINNHUB_KEY`, and keep Alpaca keys on one server only.
3. **Off (default):** bots don't trade there and no Alpaca orders go out, so the two copies never trade twice. It's free to let it sleep.
4. **On:** go to Settings, tap **Standby server** and type ON. Bots trade there, and it pings its own address every 10 minutes so free hosts don't put it to sleep.

**Switching over:** download a backup from the main server (Settings → Face ID and backups), restore it on the standby, then switch the standby on. To switch back, back up the standby, restore that on the main server, and switch the standby off.

---

## Developing

The source lives in `src/` and is bundled by `build.sh`:

- `src/server/*.js` is concatenated in order into `dist/server.js`. It covers the core and router, database and migrations, prices and market data, trading rules, the broker, bots, Autopilot, backtests, the Council, security, standby and scheduled jobs.
- `src/client/js/*.js`, `src/client/css/*.css` and `shell.html` are bundled into `dist/index.html`. The `11f-hq-*` files are Mango HQ: the world and renderer, the themes, the people and the voices.
- `test/` holds a simulated market (`sim.js`) with fake price, news and Alpaca feeds, plus API and browser tests that take screenshots.
- `demo/make.js` builds an offline Mango HQ demo from the real `dist/index.html` using recorded data.

```
sh build.sh          # build dist/server.js and dist/index.html
sh test/restart.sh   # start a test server against the simulated market
```

---

## Notes

- Paper trading is a simulation. Fills, fees and rules are modeled and won't match a real brokerage exactly.
- Nothing in the app is financial advice. The bots and the Council follow fixed rules, and past results don't predict future ones.
