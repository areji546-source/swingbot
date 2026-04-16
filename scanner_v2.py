"""
SwingBot v2 - Nifty 50 Swing Scanner
- On-demand scan via Telegram /scan command
- Tracks Batch 1 stocks separately from new scans
- Shows 6-month low, IPO price, entry, targets
- Clean formatted Telegram messages
"""

import os, csv, time, json, pyotp, requests, pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

try:
    from SmartApi import SmartConnect
except ImportError:
    os.system("pip install smartapi-python pyotp")
    from SmartApi import SmartConnect

# ── PATHS ─────────────────────────────────────────────────────────────────────
BASE         = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE  = os.path.join(BASE, "config.txt")
TRADES_FILE  = os.path.join(BASE, "trades.csv")
BATCH_FILE   = os.path.join(BASE, "batch1.json")
LOG_FILE     = os.path.join(BASE, "bot.log")
IST          = pytz.timezone("Asia/Kolkata")

# ── NIFTY 50 STOCKS WITH ANGEL ONE TOKENS ────────────────────────────────────
NIFTY50 = {
    "RELIANCE":   "2885", "TCS":        "11536", "HDFCBANK":   "1333",
    "BHARTIARTL": "10604","ICICIBANK":  "4963",  "INFY":       "1594",
    "SBIN":       "3045", "HINDUNILVR": "1394",  "ITC":        "1660",
    "LT":         "11483","KOTAKBANK":  "1922",  "AXISBANK":   "5900",
    "BAJFINANCE": "317",  "ASIANPAINT": "236",   "MARUTI":     "10999",
    "TITAN":      "3506", "SUNPHARMA":  "3351",  "WIPRO":      "3787",
    "HCLTECH":    "7229", "ULTRACEMCO": "11532", "NTPC":       "11630",
    "POWERGRID":  "14977","TATAMOTORS": "3456",  "M&M":        "2031",
    "ADANIENT":   "25",   "ONGC":       "2475",  "NESTLEIND":  "17963",
    "TATASTEEL":  "3499", "JSWSTEEL":   "11723", "COALINDIA":  "20374",
    "BAJAJFINSV": "16675","TECHM":      "13538", "DRREDDY":    "881",
    "CIPLA":      "694",  "DIVISLAB":   "10940", "EICHERMOT":  "910",
    "HEROMOTOCO": "1348", "APOLLOHOSP": "157",   "BRITANNIA":  "547",
    "GRASIM":     "1232", "HINDALCO":   "1363",  "INDUSINDBK": "5258",
    "TATACONSUM": "3432", "BPCL":       "526",   "ADANIPORTS": "15083",
    "BAJAJ-AUTO": "16669","SBILIFE":    "21808", "HDFCLIFE":   "467",
    "LTI":        "17818","SHREECEM":   "3103",
}

# Approximate IPO / listing prices (for reference)
IPO_PRICES = {
    "RELIANCE":2330,"TCS":850,"HDFCBANK":40,"BHARTIARTL":45,"ICICIBANK":90,
    "INFY":145,"SBIN":10,"HINDUNILVR":120,"ITC":8,"LT":18,"KOTAKBANK":54,
    "AXISBANK":35,"BAJFINANCE":50,"ASIANPAINT":100,"MARUTI":125,"TITAN":65,
    "SUNPHARMA":50,"WIPRO":10,"HCLTECH":30,"ULTRACEMCO":200,"NTPC":57,
    "POWERGRID":90,"TATAMOTORS":20,"M&M":50,"ADANIENT":1430,"ONGC":115,
    "NESTLEIND":2600,"TATASTEEL":10,"JSWSTEEL":100,"COALINDIA":245,
    "BAJAJFINSV":45,"TECHM":170,"DRREDDY":200,"CIPLA":40,"DIVISLAB":1950,
    "EICHERMOT":50,"HEROMOTOCO":70,"APOLLOHOSP":290,"BRITANNIA":100,
    "GRASIM":35,"HINDALCO":30,"INDUSINDBK":145,"TATACONSUM":150,"BPCL":60,
    "ADANIPORTS":440,"BAJAJ-AUTO":1900,"SBILIFE":700,"HDFCLIFE":290,
    "LTI":550,"SHREECEM":5000,
}

# ── HELPERS ───────────────────────────────────────────────────────────────────
def load_config():
    cfg = {}
    # First read from config.txt if it exists (local mode)
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = v.strip()
    # Then override with environment variables (cloud/Railway mode)
    env_keys = [
        "BOT_TOKEN","CHAT_ID",
        "ANGEL_API_KEY","ANGEL_CLIENT_ID","ANGEL_PIN","ANGEL_TOTP_SECRET",
        "CAPITAL","RISK_PCT","WEEKLY_TARGET"
    ]
    for key in env_keys:
        val = os.environ.get(key)
        if val:
            cfg[key] = val
    return cfg

def log(msg):
    ts   = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f: f.write(line + "\n")

def send_telegram(message, config, parse_mode="HTML"):
    token   = config.get("BOT_TOKEN", "")
    chat_id = config.get("CHAT_ID", "")
    if not token or not chat_id: return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": parse_mode},
            timeout=15)
        return r.status_code == 200
    except Exception as e:
        log(f"Telegram error: {e}")
        return False

def market_is_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    return time.strptime("09:15", "%H:%M") <= time.strptime(now.strftime("%H:%M"), "%H:%M") <= time.strptime("15:30", "%H:%M")

# ── ANGEL ONE ─────────────────────────────────────────────────────────────────
_session = None

def get_session():
    global _session
    if _session: return _session
    cfg = load_config()
    try:
        totp  = pyotp.TOTP(cfg.get("ANGEL_TOTP_SECRET","")).now()
        smart = SmartConnect(api_key=cfg.get("ANGEL_API_KEY",""))
        smart.timeout = 30
        data  = smart.generateSession(cfg.get("ANGEL_CLIENT_ID",""),
                                       cfg.get("ANGEL_PIN",""), totp)
        if data and (data.get("status") or (data.get("data") or {}).get("jwtToken")):
            log("Angel One login OK")
            _session = smart
            return smart
    except Exception as e:
        if "timed out" in str(e).lower() and _session:
            return _session
        log(f"Login error: {e}")
    return None

def get_candles(symbol, days=180):
    smart = get_session()
    if not smart: return None
    token = NIFTY50.get(symbol)
    if not token: return None
    end   = datetime.now(IST)
    start = end - timedelta(days=days)
    try:
        data = smart.getCandleData({
            "exchange": "NSE", "symboltoken": token,
            "interval": "ONE_DAY",
            "fromdate": start.strftime("%Y-%m-%d %H:%M"),
            "todate":   end.strftime("%Y-%m-%d %H:%M"),
        })
        if not data or not data.get("data"): return None
        df = pd.DataFrame(data["data"],
                          columns=["Date","Open","High","Low","Close","Volume"])
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df.astype(float)
    except Exception as e:
        log(f"Candle error {symbol}: {e}")
        return None

def get_ltp(symbol):
    smart = get_session()
    if not smart: return None
    try:
        d = smart.ltpData("NSE", symbol, NIFTY50.get(symbol,""))
        if d and d.get("data"): return float(d["data"]["ltp"])
    except: pass
    return None

# ── INDICATORS ────────────────────────────────────────────────────────────────
def add_indicators(df):
    df = df.copy()
    df["ema9"]    = df["Close"].ewm(span=9,   adjust=False).mean()
    df["ema21"]   = df["Close"].ewm(span=21,  adjust=False).mean()
    df["ema50"]   = df["Close"].ewm(span=50,  adjust=False).mean()
    df["ema200"]  = df["Close"].ewm(span=200, adjust=False).mean()
    delta = df["Close"].diff()
    gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df["rsi"]     = 100 - (100 / (1 + gain / loss))
    ema12         = df["Close"].ewm(span=12, adjust=False).mean()
    ema26         = df["Close"].ewm(span=26, adjust=False).mean()
    df["macd_h"]  = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
    df["vol_r"]   = df["Volume"] / df["Volume"].rolling(10).mean()
    return df

def score_stock(df):
    if len(df) < 30: return 0, []
    c, p   = df.iloc[-1], df.iloc[-2]
    score, sigs = 0, []
    if p["ema9"] <= p["ema21"] and c["ema9"] > c["ema21"]:
        if 45 <= c["rsi"] <= 68: score += 3; sigs.append("EMA crossover ↑")
    if abs(c["Close"] - c["ema50"]) / c["ema50"] < 0.015 and c["Close"] > c["Open"] and 40 <= c["rsi"] <= 58:
        score += 2; sigs.append("Pullback to EMA50")
    if p["macd_h"] < 0 < c["macd_h"] and c["Close"] > c["ema200"]:
        score += 2; sigs.append("MACD reversal")
    if c["vol_r"] >= 1.3: score += 1
    if c["Close"] > c["ema200"]: score += 1
    return score, sigs

# ── BATCH TRACKING ────────────────────────────────────────────────────────────
def load_batch1():
    if os.path.exists(BATCH_FILE):
        with open(BATCH_FILE) as f: return json.load(f)
    return []

def save_batch1(picks):
    with open(BATCH_FILE, "w") as f: json.dump(picks, f, indent=2)

# ── CORE SCAN ─────────────────────────────────────────────────────────────────
def run_scan(is_new_batch=True):
    config  = load_config()
    capital = float(config.get("CAPITAL", 75000))
    status  = "🟢 LIVE" if market_is_open() else "🔴 CLOSED"
    now_str = datetime.now(IST).strftime("%d %b %Y %I:%M %p")

    send_telegram(
        f"🔍 <b>SwingBot Scanning Nifty 50...</b>\n"
        f"Market: {status}\n"
        f"Time: {now_str} IST\n"
        f"Analysing 50 stocks — please wait ~2 min ⏳", config)

    picks = []
    for symbol in NIFTY50:
        df = get_candles(symbol, days=200)
        if df is None or len(df) < 40:
            time.sleep(0.5); continue

        df    = add_indicators(df)
        score, sigs = score_stock(df)
        if score < 3:
            time.sleep(0.3); continue

        c           = df.iloc[-1]
        low_6m      = round(df["Low"].tail(126).min(), 2)   # ~6 months trading days
        ipo_price   = IPO_PRICES.get(symbol, 0)
        ltp         = get_ltp(symbol) or round(c["Close"], 2)
        entry       = round(ltp * 1.005, 2)
        sl          = round(entry * 0.98, 2)
        t1          = round(entry * 1.03, 2)
        t2          = round(entry * 1.05, 2)
        t3          = round(entry * 1.08, 2)
        risk        = entry - sl
        qty         = max(1, int((capital * 0.02) / risk)) if risk > 0 else 1
        invest      = round(entry * qty, 0)
        profit_est  = round((t2 - entry) * qty, 0)

        picks.append({
            "symbol": symbol, "score": score,
            "signal": sigs[0] if sigs else "Multi-signal",
            "ltp": ltp, "entry": entry, "sl": sl,
            "t1": t1, "t2": t2, "t3": t3,
            "low_6m": low_6m, "ipo": ipo_price,
            "rsi": round(c["rsi"], 1),
            "vol": round(c["vol_r"], 2),
            "qty": qty, "invest": invest, "profit_est": profit_est,
            "added": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
        })
        log(f"PICK: {symbol} score={score}")
        time.sleep(0.4)

    picks.sort(key=lambda x: x["score"], reverse=True)
    top = picks[:6]

    if not top:
        send_telegram(
            "📋 <b>Scan Complete — No Strong Setups</b>\n\n"
            "Market conditions are weak this week.\n"
            "Best to stay in cash and wait.\n\n"
            "⚠️ Not SEBI-registered advice.", config)
        return []

    # Send each stock as a separate clean card
    send_telegram(
        f"📋 <b>Scan Complete — {len(top)} Picks Found</b>\n"
        f"{'Saving as Batch 1 — will monitor these.' if is_new_batch else 'New picks (Batch 2 scan).'}\n"
        f"Sending individual cards now... 👇", config)

    time.sleep(1)

    for i, p in enumerate(top, 1):
        ipo_line = f"₹{p['ipo']:,}" if p['ipo'] > 0 else "N/A"
        pct_from_low  = round((p['ltp'] - p['low_6m']) / p['low_6m'] * 100, 1)
        low_marker    = "🟢 Near low" if pct_from_low < 15 else ("🟡 Mid range" if pct_from_low < 40 else "🔴 Extended")

        msg = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{'🥇' if i==1 else '🥈' if i==2 else '🥉' if i==3 else '📌'} "
            f"<b>#{i} {p['symbol']}</b>  |  Score: {'⭐'*min(p['score'],5)}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"📊 <b>Price Levels</b>\n"
            f"  Current LTP  : ₹{p['ltp']:,}\n"
            f"  6-Month Low  : ₹{p['low_6m']:,}  {low_marker}\n"
            f"  IPO Price    : {ipo_line}\n"
            f"  From 6M Low  : +{pct_from_low}%\n"
            f"\n"
            f"🎯 <b>Trade Setup</b>\n"
            f"  Entry        : ₹{p['entry']:,}\n"
            f"  Stop Loss    : ₹{p['sl']:,}  (−2%)\n"
            f"  Target 1     : ₹{p['t1']:,}  (+3%) → sell 40%\n"
            f"  Target 2     : ₹{p['t2']:,}  (+5%) → sell 40%\n"
            f"  Target 3     : ₹{p['t3']:,}  (+8%) → sell 20%\n"
            f"\n"
            f"📐 <b>Position Size</b>\n"
            f"  Qty          : {p['qty']} shares\n"
            f"  Investment   : ₹{int(p['invest']):,}\n"
            f"  Est. profit  : ₹{int(p['profit_est']):,} at T2\n"
            f"\n"
            f"📈 <b>Indicators</b>\n"
            f"  Signal       : {p['signal']}\n"
            f"  RSI          : {p['rsi']}\n"
            f"  Volume       : {p['vol']}× avg\n"
            f"\n"
            f"⚠️ Not SEBI advice. Verify before trading."
        )
        send_telegram(msg, config)
        time.sleep(1.5)

    if is_new_batch:
        save_batch1(top)
        log(f"Batch 1 saved with {len(top)} stocks.")

    return top

# ── BATCH 1 MONITOR ───────────────────────────────────────────────────────────
def monitor_batch1():
    config = load_config()
    batch  = load_batch1()
    if not batch:
        send_telegram("📭 <b>No Batch 1 stocks found.</b>\nRun /scan first to create your watchlist.", config)
        return

    now_str = datetime.now(IST).strftime("%d %b %Y %I:%M %p")
    header  = f"👁 <b>Batch 1 Monitor — {now_str} IST</b>\n{'─'*28}\n"
    lines   = []
    alerts  = []

    for p in batch:
        symbol = p["symbol"]
        ltp    = get_ltp(symbol)
        if not ltp:
            lines.append(f"⚪ <b>{symbol}</b> — price unavailable")
            time.sleep(0.3)
            continue

        entry  = p["entry"]
        sl     = p["sl"]
        t1     = p["t1"]
        t2     = p["t2"]
        t3     = p["t3"]
        chg    = round((ltp - entry) / entry * 100, 2)
        arrow  = "📈" if chg >= 0 else "📉"

        if ltp >= t3:
            status = f"🎯🎯 T3 HIT +{chg}%"
            alerts.append(f"🎯 <b>{symbol} — TARGET 3 HIT!</b>\nEntry: ₹{entry} → Now: ₹{ltp}\nProfit: +{chg}% 🎉\n➡️ Book full profit!")
        elif ltp >= t2:
            status = f"🎯 T2 HIT +{chg}%"
            alerts.append(f"🎯 <b>{symbol} — TARGET 2 HIT!</b>\nEntry: ₹{entry} → Now: ₹{ltp}\nProfit: +{chg}%\n➡️ Sell remaining 40%")
        elif ltp >= t1:
            status = f"✅ T1 HIT +{chg}%"
            alerts.append(f"✅ <b>{symbol} — TARGET 1 HIT!</b>\nEntry: ₹{entry} → Now: ₹{ltp}\nProfit: +{chg}%\n➡️ Sell 40% now, trail rest")
        elif ltp <= sl:
            status = f"🔴 SL HIT {chg}%"
            alerts.append(f"🔴 <b>{symbol} — STOP LOSS HIT!</b>\nEntry: ₹{entry} → Now: ₹{ltp}\nLoss: {chg}%\n➡️ Exit immediately!")
        elif chg >= 1.5:
            status = f"🟢 +{chg}% ↑ running"
        elif chg <= -1:
            status = f"🟡 {chg}% ↓ watch"
        else:
            status = f"⚪ {chg}% sideways"

        lines.append(
            f"{arrow} <b>{symbol}</b>  ₹{ltp:,}  {status}\n"
            f"   Entry ₹{entry} | SL ₹{sl} | T1 ₹{t1} | T2 ₹{t2}"
        )
        time.sleep(0.3)

    summary = header + "\n\n".join(lines)
    send_telegram(summary, config)

    # Send individual alerts for target/SL hits
    time.sleep(1)
    for alert in alerts:
        send_telegram(alert, config)
        time.sleep(1)

    log(f"Batch 1 monitor done. {len(alerts)} alerts sent.")

# ── TELEGRAM COMMAND LISTENER ─────────────────────────────────────────────────
def listen_telegram():
    """Poll Telegram for /scan and /monitor commands."""
    offset = 0
    log("Telegram listener starting...")

    # Wait for valid token with retries
    for attempt in range(10):
        config = load_config()
        token  = config.get("BOT_TOKEN","").strip()
        if token and token != "your_telegram_bot_token":
            break
        log(f"Waiting for BOT_TOKEN... attempt {attempt+1}")
        time.sleep(5)

    if not token or token == "your_telegram_bot_token":
        log("ERROR: BOT_TOKEN not set in environment variables!")
        return

    url_base = f"https://api.telegram.org/bot{token}"
    log(f"Telegram listener active. Token ends: ...{token[-6:]}")

    # Test connection first
    try:
        test = requests.get(f"{url_base}/getMe", timeout=10)
        if test.status_code == 200:
            bot_name = test.json().get("result",{}).get("username","unknown")
            log(f"Telegram connected — @{bot_name}")
        else:
            log(f"Telegram getMe failed: {test.status_code} {test.text}")
    except Exception as e:
        log(f"Telegram connection test error: {e}")

    while True:
        try:
            config = load_config()  # reload config each loop
            r = requests.get(
                f"{url_base}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=35)
            if r.status_code == 401:
                log("ERROR: BOT_TOKEN is invalid! Check Railway variables.")
                time.sleep(30)
                continue
            if r.status_code != 200:
                log(f"getUpdates error: {r.status_code}")
                time.sleep(5)
                continue
            updates = r.json().get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                msg    = upd.get("message", {})
                text   = msg.get("text", "").strip().lower()
                if not text:
                    continue
                log(f"Command: {text}")
                if text in ["/scan", "/scan@swingbot"]:
                    batch  = load_batch1()
                    is_new = len(batch) == 0
                    run_scan(is_new_batch=is_new)
                elif text == "/newscan":
                    if os.path.exists(BATCH_FILE): os.remove(BATCH_FILE)
                    run_scan(is_new_batch=True)
                elif text in ["/monitor", "/check"]:
                    monitor_batch1()
                elif text == "/status":
                    task_heartbeat()
                elif text == "/help":
                    send_telegram(
                        "🤖 <b>SwingBot Commands</b>\n\n"
                        "/scan — Scan Nifty 50 for swing picks\n"
                        "/newscan — Force fresh Batch 1 scan\n"
                        "/monitor — Check Batch 1 prices\n"
                        "/status — Bot health check\n"
                        "/help — This menu\n\n"
                        "📅 Auto: Mon 8AM scan, Daily 3PM monitor", config)
        except Exception as e:
            log(f"Listener error: {e}")
            time.sleep(5)

# ── SCHEDULED TASKS ───────────────────────────────────────────────────────────
def task_heartbeat():
    config = load_config()
    batch  = load_batch1()
    status = "🟢 LIVE" if market_is_open() else "🔴 CLOSED"
    names  = ", ".join(b["symbol"] for b in batch) if batch else "None"
    send_telegram(
        f"✅ <b>SwingBot Running</b>\n\n"
        f"Time    : {datetime.now(IST).strftime('%d %b %I:%M %p')} IST\n"
        f"Market  : {status}\n"
        f"Batch 1 : {names if names else 'Empty — run /scan'}\n\n"
        f"Commands: /scan /monitor /status /help", config)

def task_auto_monitor():
    log("Auto monitor triggered")
    monitor_batch1()

def task_friday_exit():
    config = load_config()
    batch  = load_batch1()
    if not batch: return
    names = ", ".join(b["symbol"] for b in batch)
    send_telegram(
        f"⏰ <b>Friday Exit Reminder</b>\n\n"
        f"Batch 1: <b>{names}</b>\n\n"
        f"Market closes in 45 min.\n"
        f"Consider exiting to avoid weekend gap risk.\n"
        f"Use /monitor to check current levels.", config)

def task_weekly_pnl():
    config = load_config()
    send_telegram(
        f"📊 <b>Weekly Summary — {datetime.now(IST).strftime('%d %b %Y')}</b>\n\n"
        f"Use /monitor to see current Batch 1 status.\n"
        f"Update trades.csv with your actual P&L.\n\n"
        f"New week starts Monday — run /newscan for fresh picks.", config)

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, threading

    cmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if cmd == "test":
        config = load_config()
        send_telegram("🤖 <b>SwingBot v2 connected!</b>\n\nSend /help to see all commands.\nSend /scan to start your first scan.", config)
        print("Telegram test sent. Check your phone!")

    elif cmd == "scan":
        run_scan(is_new_batch=True)

    elif cmd == "monitor":
        monitor_batch1()

    elif cmd == "status":
        task_heartbeat()

    else:
        # Start scheduler in background thread
        sched = BlockingScheduler(timezone=IST, daemon=True)
        sched.add_job(task_heartbeat,    CronTrigger(day_of_week="mon-fri", hour=8,  minute=55, timezone=IST))
        sched.add_job(run_scan,          CronTrigger(day_of_week="mon",     hour=8,  minute=0,  timezone=IST),
                      kwargs={"is_new_batch": True})
        sched.add_job(task_auto_monitor, CronTrigger(day_of_week="mon-fri", hour=15, minute=0,  timezone=IST))
        sched.add_job(task_friday_exit,  CronTrigger(day_of_week="fri",     hour=14, minute=45, timezone=IST))
        sched.add_job(task_weekly_pnl,   CronTrigger(day_of_week="fri",     hour=16, minute=0,  timezone=IST))

        t = threading.Thread(target=sched.start, daemon=True)
        t.start()
        log("Scheduler started in background.")

        # Listen for Telegram commands (blocking)
        log("SwingBot v2 running. Send /help on Telegram.")
        listen_telegram()
