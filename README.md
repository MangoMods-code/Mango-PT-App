# Mango Paper Trading

A paper trading app for iPhone with real market prices. It runs from the Home Screen and is built on Node and Postgres.

**⬇️ [Download the latest version](https://github.com/MangoMods-code/Mango-PT-App/releases/latest)** · Current: [V5.0.0, Friends and the monthly league](https://github.com/MangoMods-code/Mango-PT-App/releases/tag/V5.0.0) · [All releases](https://github.com/MangoMods-code/Mango-PT-App/releases)

Trade stocks, ETFs and crypto with practice money. You can also put strategies to work as bots, let Autopilot run a mix of them, and watch it all happen in **Mango HQ**: a small isometric world where your bots walk around and a Council of up to ten members votes on every trade. If you connect an Alpaca account, the same app can trade it, on paper or with real money, on a separate side with its own safeguards.

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
- **Autopilot 2** tests 32 strategy tryouts on pretend money and backs the ones that are working. It favors what has worked in the current kind of market, splits money by score per unit of risk, re-plans monthly (and weekly when things shift a lot), breeds variations of its best tryouts that are judged only on what happens after they're born, parks idle money in T-bills, scales down for small accounts, and has the Analyst review each plan. It also keeps volatility targeting, the safety switch and the brake after a drop.
- **Strategy versions**: every strategy shows its version (v1, v2…) with what changed, on the New bot screen, the bot page and its Change sheet. v2 of Trend follower, Dip buyer (RSI-2), Breakout and Buy and hold (3.3.0) each pick which shared safety layers they use, so a dip buyer isn't blocked by bad-mood news and buy and hold isn't stopped out; idle money waits in SGOV. v2 of Dual momentum, Low volatility, News jump drift, Turn of the month and Snap-back followed in 3.4.0.
- **Taxes**: estimates with your bracket and state (New York built in), the long-term countdown, loss harvesting with wash-sale checks, the $3,000 limit and carryover, qualified vs ordinary dividends, a printable Form 8949 and Schedule D, and tax-aware bots.
- **Account types**: real cash-account rules (good faith violations and the 90-day restriction), IRA and Roth IRA wallets with the yearly limit, withdrawal penalties and required withdrawals, and age rules from your date of birth.
- **Market rules**: market-wide circuit breakers (7%, 13%, 20%), single-stock price bands with 5-minute pauses, fractional-share order rules, price improvement on market orders, "are you sure?" order checks, and a market calendar with holidays and early closes. Each rule can be switched off in Settings.
- **Leaderboard**: fair time-weighted returns (deposits never count), a 1–99 score against the S&P 500 that new bots grow into, podium, periods, sorting, rank arrows, streaks, badges, sparklines, a full stats breakdown per bot, and a by-strategy view.
- **Trading checkup**: each bot page explains, from its last 30 decisions, whether it is trading normally, waiting for its setup, or being held back, and which setting to change.
- **Compare every strategy**: the lab tests all 11 strategies on your money and weekly deposits, ranked, with a one-tap Use button.
- **Fits any wallet**: every strategy holds fewer positions in a small wallet so each is at least $25 (or twice the smallest trade); in a tiny wallet the per-position limit stretches to that minimum. Index funds can go past the per-position limit in Dual momentum and Turn of the month.
- **Self-upgrading bots**: when a strategy gets a new version, every bot using it upgrades on its next run (or at server start). Settings you changed yourself are kept, and the journal notes what changed.
- **What's new**: the changelog shows once after each update, and any time in Settings.
- A strategy lab for backtesting up to 10 years with out-of-sample checks, and a leaderboard that compares every bot against simply holding the S&P 500.
- **Small accounts**: every bot can add money every week (paper wallets get the deposit automatically; Alpaca bots get a bigger budget, and you make the deposit at Alpaca). The strategy lab and the new-bot test can add weekly deposits too: you see what you put in, what it grew to, and the same deposits into the benchmark, with returns kept time-weighted.
- **Momentum rotation, small-account version**: it holds fewer positions when there's less money (each at least a set size), keeps a winner until it falls past a wider rank so it trades less, puts new money into the most underweight holding without selling anything, and parks idle money in a T-bill ETF (SGOV or BIL) instead of cash. A "Small account preset" on the New bot screen sets all of this up. Older momentum bots keep working exactly as before.
- **Report card** on every bot: average win and loss, payoff, and how it did against the S&P 500 over the last 60 days. A bot trailing by more than 3% is **benched** (you're notified; with Auto-bench on it's paused).
- **Smarter exits** per bot: sell half once a position is up a set amount (default 15%), sell a trade that went nowhere after a set number of days (default 30), and tighten the stop as a trade wins (break-even at +6%, 7% under the high at +12%, 5% under at +20%).
- **Portfolio guards** across all bots: a market mood switch (new buys at 70% size in a choppy market, 40% in a falling one), a correlation guard (no new position that moves almost exactly like one already held), and shared caps on one symbol, one sector and crypto.
- **Monthly re-tune**: each bot's settings are re-tested on 3 years, scored only on the part they weren't tuned on. Better settings are suggested, never applied without you.
- **Tournaments**: up to 20 variations of one strategy compete on real prices from the day they join. Each month the bottom is replaced by mutations of the top 3. Adopt any entrant as a bot.
- **Earnings drift** strategy: buys after a real earnings beat that the market liked, holds about five weeks. Needs FINNHUB_KEY for the beat check live; the lab tests the price pattern.

**Mango HQ**
- Each bot is a character in one of eight themed worlds: **Office**, **Starship**, **Village**, **Island**, **Underwater**, **Castle**, **Cyberpunk** or **Mars**. The four newer worlds reuse the original furniture, recolored, with their own skies, grounds, scenery, life (fish and bubbles, torches and towers, neon signs and flying cars, dust devils and domes), costumes, speech and weather. Each world has its own rooms, furniture, people, sky and way of talking.
- Bots really do what they're shown doing: they read the news, take buys and sells to the Council, and trade at the desk. Speech bubbles carry real trades and journal entries.
- **The Council** is six rule-based voters (trend, momentum, news, risk, stats and contrarian) who vote on every buy and sell. Votes are checked a week later, and "Who called it?" shows who has been right. The council veto (on for every bot by default) lets a majority NO block a buy.
- **AI members** (Claude Haiku, optional): three specialists who each see only one slice of the evidence (the **News Hawk** sees headlines and earnings, the **Risk Officer** sees position size, swings and drawdowns, and the **Chartist** sees the price chart and volume), plus the **Analyst**, who speaks last. In a **debate**, the Analyst reads every vote, calls out the argument it disagrees with most, and then votes. Their votes count and they have real say: a confident NO from the Analyst vetoes a buy, it sets the size (smaller for risk, up to 1.5x when it's very sure, never past the bot's max position), and a **daily position check** lets it trim or sell a holding that's turning. They're graded like everyone else, and every one of these powers has its own switch. Each member can be switched off in Settings, and they share one daily call cap.
- **Weighted votes** (on by default): a member's vote counts for more when it has been right more often (after 20 graded votes), when the situation is its area (news on a big news day, risk when the market is wild, trend in a clean trend) and when it's sure. Each vote counts between ½ and 2, so no one decides alone. The weighted vote decides the council veto and the verdict, and "Who called it?" compares weighted calls with simply counting heads.
- **Night shift** (on by default): from 9 PM to 7 AM ET the Council and the chair get up, walk to their bedrooms in the new **quarters** wing and go to bed (and walk back to the table in the morning), while a two-member night crew takes over: the **Night Watch** (rules: trend, risk and the overnight move) and the **Night Owl** (one AI call instead of four). They check crypto bots 1–3 times a night (11 PM and 3 AM by default). Only crypto is touched, only for strategies that decide every day, and a night check never replaces the bot's daily decision.
- **Market events**: a crash alarm when the S&P 500 is down 2% or more (or your bots are down 3% on the day), with a red alert, a turning beacon over Control and everyone heading there. A win party in the lounge with confetti when the market is up 1.5% or more. Earnings day sends holders to the newsroom.
- **Weather and seasons**: rain, snow or a storm with lightning when the market falls, sun when it rises, plus falling leaves in fall, petals in spring and flurries in winter. The Starship gets meteor showers instead.
- **Levels and gear**: bots earn XP from trades, winning sales, days on the job and profit. Each level shows on the name tag and unlocks gear (a cap, gold trim, a top hat, a gold hat, a crown).
- **Hire and farewell ceremonies**: a new bot walks in and the chair welcomes it with confetti. A deleted bot comes back to say goodbye and walks out.
- **Visitors** drop by now and then: the mail carrier in the morning, the auditor on Fridays, a reporter on earnings days, a risk inspector when a bot hits its limits, the new intern and the pizza delivery.
- **Photo mode** hides the interface, adds filters (Warm, Noir, Film, Pop) and saves or shares a picture.
- **Timelapse**: Replay plays back the last day with a clock, a progress bar, pause and 1×, 2× and 4× speed.
- Market events, weather and visitors can each be switched off in the Look sheet.
- You can rename every Council member and the chair, with the same names in every world or different names for each one.
- Day and night follow the market clock. You can drag, fling, pinch to zoom, replay the day, follow a bot, or turn on the auto camera.

**AI extras** (all share the daily AI cap; most have a plain version without a key)
- **Morning brief** every trading day (7:30, 8:45 or 9:15 AM ET): the market mood, earnings for what you hold, the biggest moves and your bots, as a push notification.
- **Ask the Council** on any stock or coin page: every member votes on a small buy today, with reasons and weights. Nothing is traded.
- **Friday letter from the chair**: how the week went, who earned praise and who got scolded, signed by Big Mango (or whatever you named the chair).
- **Chat with a bot** from its page: it answers from its own holdings, decisions, journal and rules.
- **Lessons from losing trades**: after a bot sells at a loss, a short post-mortem lands in its journal (what went wrong, which Council members called it).
- **Explain the news** on any stock or coin page: what the headlines say and whether they explain the move.

**Practice tools**
- **Chart-signal alerts**: RSI levels, crossing the 50- or 200-day average, golden and death crosses, new 52-week highs and lows, and volume spikes. Checked every 5 minutes with the live price.
- **Monthly review** of your trades and journal: return against the S&P 500, win rate, average hold, best and worst trades, results by tag and weekday, and coach's notes (AI, or a plain summary). A notification on the 1st of each month.
- **What if?**: what a past buy would be worth today (adjusted for splits and dividends) against the S&P 500, with the worst drop along the way, plus what your own sells would be worth if you'd held.
- **Options**: calls, puts, vertical spreads and SPX in any wallet on Robinhood or Alpaca rules, behind broker-style approval levels (1 covered calls and cash-secured puts, 2 buying, 3 spreads, 4 naked with $25,000+) with a short quiz for each. Shares and cash are held as collateral, positions count toward the wallet, and expiration follows OCC rules: automatic exercise a cent in the money, assignment, early assignment before dividends, cash settlement for SPX, pin-risk warnings. Prices are Black-Scholes estimates from recent volatility with a modeled bid and ask (clearly labeled, not real quotes). A **Covered calls** bot sells monthly calls on 100-share lots.
- **Research**: on every stock page, earnings history (beats and how the stock moved), insider buys and sales from Form 4 filings, which famous investors own it (their latest 13F, from the SEC), Congress trades (with Finnhub's paid data), FINRA daily short volume, and a dividend safety score. Fund pages show their holdings (SEC N-PORT reports). The Research screen has the economic calendar (Fed decisions, CPI, jobs, GDP, PCE, plus your stocks' earnings), each famous investor's portfolio and changes, and a fund overlap tool.
- **Bot lab 2**: walk-forward tests (tune on a year, trade the next 3 unseen months, repeat), Monte Carlo (1,000 reshuffles of the backtest's days), crossbreeding two bots into tested children, weekly strategy-decay checks that alert and pause, an Economic cycle sectors bot, a Filings follower bot that acts only on Form 4 and 13F filings, and Bot vs you (your wallet against a strategy given the same money and deposits).
- **Learning and fun**: an Academy of 9 chapters you finish by doing the real thing, a Time machine that replays the 2020 crash, 2008, the dot-com bust and 2022 day by day with the news of the time, compound interest and DCA calculators on real prices, a risk profile meter, a daily S&P 500 up-or-down call against three bots, shareable trade and wallet cards (with or without dollar amounts), more achievements, and Mango HQ events on real days.
- **Safety and quality of life**: a cool-off timer that pauses your own new buys for 30 minutes after 3 losing sales in a row or a 5% down day (type "I'm sure" to skip), alerts written in plain sentences, alerts you can type in plain English, and exports of everything as spreadsheets, a TurboTax-ready .txf file or one zip.
- **Dividend calendar**: upcoming ex-dividend dates for what you hold and watch, estimated from each company's pattern, with the expected payment.
- **Home Screen widget** via the Shortcuts app: a secret read-only link (text or JSON) with your balance, today's change and top movers. You can make a new link at any time.
- New weekly challenges for these tools in Games.

**Friends and the league**
- Friends can make their own free account from the login screen. Each account has its own wallets, bots and history, runs as its own process and keeps its data in its own database schema.
- Friends never see your keys. Prices and AI go through your server; Alpaca stays yours (invite-only for friends until you open it).
- A monthly league: everyone starts with $10,000 on the same day, no deposits, bots allowed, live standings and final results. Each person picks what others see.
- Owner controls for accounts, invites, sign-ups and per-person AI limits.

**Alpaca (real brokerage)**
- A separate side of the app for an Alpaca account, paper or live.
- Safeguards on that side: a kill switch, a daily loss limit, a per-order cap, a real-money cap per bot, and Face ID confirmation for real-money actions.
- Bots earn their way over: you can move a copy of a paper bot to Alpaca with its own budget.

**Security and data**
- Password login, with Face ID login and confirmation through passkeys.
- Nightly backups (7 kept), plus download and restore.
- Standby mode for running a backup copy on a second host (see below).

---

## Releases

Every version is on the repo's **[Releases](https://github.com/MangoMods-code/Mango-PT-App/releases)** page with what changed and a ready-to-run zip (`server.js`, `index.html`, `package.json`, `README.md` and an `.env.example` listing the settings). Download it, put it on any Node 20+ host with Postgres, fill in the variables and run `npm install` then `npm start`.

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
| `ANTHROPIC_API_KEY` | optional | Turns on the **AI Council members** (Analyst, News Hawk, Risk Officer, Chartist), powered by Claude Haiku. It costs well under $1 a month, with a shared daily cap you set in Settings. |
| `SEC_CONTACT` | optional | Your name and email, sent with requests for SEC filings (famous investors' 13F holdings, fund holdings). The SEC asks apps to identify themselves; without it those may not load. |
| `VAPID_EMAIL` | optional | Contact address for push notifications (`mailto:...`) |
| `MANGO_STANDBY` | optional | Set to `1` on a backup copy only (see below) |
| `PUBLIC_URL` | optional | The standby copy's own address, if the host doesn't provide one (Render does) |
| `GITHUB_BACKUP_TOKEN`, `GITHUB_BACKUP_REPO` | optional | Nightly offsite backup to a **private** GitHub repo (`owner/name`). Use a fine-grained token with Contents read and write on that one repo only. |
| `STANDBY_URL` | optional | On the main server: the standby's address, so the main keeps it awake |
| `MAIN_URL` | optional | On the standby: the main server's address, so the standby watches it and notifies you if it goes down |
| `PORT` | set by host | The port to listen on |

Friend accounts need no extra variables. Each awake account uses about 60 to 120 MB of memory, and accounts sleep after an hour without use unless a bot is running, so watch your plan's memory if many people join (Settings → Accounts and invites shows it). `HUB_WORKER_LOG=1` adds friends' account logs to the server log.

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
