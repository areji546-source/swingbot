"""
SwingBot v2 - Webhook version for Railway deployment
Uses Flask webhook instead of polling - works on all cloud servers
"""
import os, csv, json, time, pyotp, requests, pandas as pd, threading
from datetime import datetime, timedelta
from flask import Flask, request as flask_request
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

try:
    from SmartApi import SmartConnect
except ImportError:
    os.system("pip install smartapi-python pyotp flask")
    from SmartApi import SmartConnect

BASE         = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE  = os.path.join(BASE, "config.txt")
BATCH_FILE   = os.path.join(BASE, "batch1.json")
LOG_FILE     = os.path.join(BASE, "bot.log")
IST          = pytz.timezone("Asia/Kolkata")
app          = Flask(__name__)

NIFTY50 = {
    "RELIANCE":"2885","TCS":"11536","HDFCBANK":"1333","BHARTIARTL":"10604",
    "ICICIBANK":"4963","INFY":"1594","SBIN":"3045","HINDUNILVR":"1394",
    "ITC":"1660","LT":"11483","KOTAKBANK":"1922","AXISBANK":"5900",
    "BAJFINANCE":"317","ASIANPAINT":"236","MARUTI":"10999","TITAN":"3506",
    "SUNPHARMA":"3351","WIPRO":"3787","HCLTECH":"7229","ULTRACEMCO":"11532",
    "NTPC":"11630","POWERGRID":"14977","TATAMOTORS":"3456","M&M":"2031",
    "ADANIENT":"25","ONGC":"2475","NESTLEIND":"17963","TATASTEEL":"3499",
    "JSWSTEEL":"11723","COALINDIA":"20374","BAJAJFINSV":"16675","TECHM":"13538",
    "DRREDDY":"881","CIPLA":"694","DIVISLAB":"10940","EICHERMOT":"910",
    "HEROMOTOCO":"1348","APOLLOHOSP":"157","BRITANNIA":"547","GRASIM":"1232",
    "HINDALCO":"1363","INDUSINDBK":"5258","TATACONSUM":"3432","BPCL":"526",
    "ADANIPORTS":"15083","BAJAJ-AUTO":"16669","SBILIFE":"21808",
    "HDFCLIFE":"467","LTI":"17818","SHREECEM":"3103",
}

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
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    cfg[k.strip()] = v.strip()
    for key in ["BOT_TOKEN","CHAT_ID","ANGEL_API_KEY","ANGEL_CLIENT_ID",
                "ANGEL_PIN","ANGEL_TOTP_SECRET","CAPITAL","RISK_PCT","WEEKLY_TARGET"]:
        val = os.environ.get(key)
        if val: cfg[key] = val
    return cfg

def log(msg):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f: f.write(line + "\n")
    except: pass

def send_telegram(message, config=None):
    if config is None: config = load_config()
    token   = config.get("BOT_TOKEN","").strip()
    chat_id = config.get("CHAT_ID","").strip()
    if not token or not chat_id: return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=15)
        return r.status_code == 200
    except Exception as e:
        log(f"Telegram error: {e}")
        return False

def market_is_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    t = now.strftime("%H:%M")
    return "09:15" <= t <= "15:30"

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
        data  = smart.generateSession(
            cfg.get("ANGEL_CLIENT_ID",""), cfg.get("ANGEL_PIN",""), totp)
        if data and (data.get("status") or
                     (data.get("data") or {}).get("jwtToken")):
            log("Angel One login OK")
            _session = smart
            return smart
    except Exception as e:
        if "timed out" in str(e).lower() and _session:
            return _session
        log(f"Login error: {e}")
    return None

def get_candles(symbol, days=200):
    smart = get_session()
    if not smart: return None
    token = NIFTY50.get(symbol)
    if not token: return None
    end, start = datetime.now(IST), datetime.now(IST) - timedelta(days=days)
    try:
        data = smart.getCandleData({
            "exchange":"NSE","symboltoken":token,"interval":"ONE_DAY",
            "fromdate":start.strftime("%Y-%m-%d %H:%M"),
            "todate":end.strftime("%Y-%m-%d %H:%M"),
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

def add_indicators(df):
    df = df.copy()
    df["ema9"]  = df["Close"].ewm(span=9,  adjust=False).mean()
    df["ema21"] = df["Close"].ewm(span=21, adjust=False).mean()
    df["ema50"] = df["Close"].ewm(span=50, adjust=False).mean()
    df["ema200"]= df["Close"].ewm(span=200,adjust=False).mean()
    delta = df["Close"].diff()
    gain  = delta.clip(lower=0).ewm(com=13,adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13,adjust=False).mean()
    df["rsi"]   = 100 - (100/(1+gain/loss))
    ema12       = df["Close"].ewm(span=12,adjust=False).mean()
    ema26       = df["Close"].ewm(span=26,adjust=False).mean()
    df["macd_h"]= (ema12-ema26)-(ema12-ema26).ewm(span=9,adjust=False).mean()
    df["vol_r"] = df["Volume"]/df["Volume"].rolling(10).mean()
    return df

def score_stock(df):
    if len(df) < 30: return 0, []
    c, p = df.iloc[-1], df.iloc[-2]
    score, sigs = 0, []
    if p["ema9"]<=p["ema21"] and c["ema9"]>c["ema21"] and 45<=c["rsi"]<=68:
        score+=3; sigs.append("EMA crossover ↑")
    if abs(c["Close"]-c["ema50"])/c["ema50"]<0.015 and c["Close"]>c["Open"] and 40<=c["rsi"]<=58:
        score+=2; sigs.append("Pullback to EMA50")
    if p["macd_h"]<0<c["macd_h"] and c["Close"]>c["ema200"]:
        score+=2; sigs.append("MACD reversal")
    if c["vol_r"]>=1.3: score+=1
    if c["Close"]>c["ema200"]: score+=1
    return score, sigs

def load_batch1():
    if os.path.exists(BATCH_FILE):
        with open(BATCH_FILE) as f: return json.load(f)
    return []

def save_batch1(picks):
    with open(BATCH_FILE,"w") as f: json.dump(picks, f, indent=2)

# ── SCAN ──────────────────────────────────────────────────────────────────────
def run_scan(is_new_batch=True):
    config  = load_config()
    capital = float(config.get("CAPITAL",75000))
    status  = "🟢 LIVE" if market_is_open() else "🔴 CLOSED"
    send_telegram(
        f"🔍 <b>Scanning Nifty 50...</b>\n"
        f"Market: {status} | "
        f"{datetime.now(IST).strftime('%d %b %Y %I:%M %p')} IST\n"
        f"Analysing 50 stocks — ~2 min ⏳", config)

    picks = []
    for symbol in NIFTY50:
        df = get_candles(symbol)
        if df is None or len(df)<40: time.sleep(0.5); continue
        df = add_indicators(df)
        score, sigs = score_stock(df)
        if score < 3: time.sleep(0.3); continue
        c       = df.iloc[-1]
        low_6m  = round(df["Low"].tail(126).min(), 2)
        ipo     = IPO_PRICES.get(symbol, 0)
        ltp     = get_ltp(symbol) or round(c["Close"],2)
        entry   = round(ltp*1.005, 2)
        sl      = round(entry*0.98, 2)
        t1      = round(entry*1.03, 2)
        t2      = round(entry*1.05, 2)
        t3      = round(entry*1.08, 2)
        risk    = entry - sl
        qty     = max(1, int((capital*0.02)/risk)) if risk>0 else 1
        picks.append({
            "symbol":symbol,"score":score,
            "signal":sigs[0] if sigs else "Multi-signal",
            "ltp":ltp,"entry":entry,"sl":sl,
            "t1":t1,"t2":t2,"t3":t3,
            "low_6m":low_6m,"ipo":ipo,
            "rsi":round(c["rsi"],1),"vol":round(c["vol_r"],2),
            "qty":qty,"invest":round(entry*qty,0),
            "profit_est":round((t2-entry)*qty,0),
            "added":datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
        })
        time.sleep(0.4)

    picks.sort(key=lambda x: x["score"], reverse=True)
    top = picks[:6]

    if not top:
        send_telegram("📋 <b>No strong setups found.</b>\nStay in cash this week.", config)
        return []

    send_telegram(
        f"📋 <b>Scan Complete — {len(top)} Picks</b>\n"
        f"{'Saved as Batch 1 ✅' if is_new_batch else 'New scan results 👇'}", config)
    time.sleep(1)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]
    for i, p in enumerate(top):
        pct_low = round((p["ltp"]-p["low_6m"])/p["low_6m"]*100, 1)
        low_tag = "🟢 Near 6M low" if pct_low<15 else ("🟡 Mid range" if pct_low<40 else "🔴 Extended")
        ipo_str = f"₹{p['ipo']:,}" if p["ipo"]>0 else "N/A"
        send_telegram(
            f"{'━'*22}\n"
            f"{medals[i]} <b>#{i+1} {p['symbol']}</b>  {'⭐'*min(p['score'],5)}\n"
            f"{'━'*22}\n\n"
            f"📊 <b>Price Info</b>\n"
            f"  LTP        : ₹{p['ltp']:,}\n"
            f"  6-Month Low: ₹{p['low_6m']:,}  {low_tag}\n"
            f"  IPO Price  : {ipo_str}\n"
            f"  Above 6M Low: +{pct_low}%\n\n"
            f"🎯 <b>Trade Setup</b>\n"
            f"  Entry   : ₹{p['entry']:,}\n"
            f"  Stop Loss: ₹{p['sl']:,}  (−2%)\n"
            f"  Target 1: ₹{p['t1']:,}  (+3%) → sell 40%\n"
            f"  Target 2: ₹{p['t2']:,}  (+5%) → sell 40%\n"
            f"  Target 3: ₹{p['t3']:,}  (+8%) → sell 20%\n\n"
            f"📐 <b>Position</b>\n"
            f"  Qty      : {p['qty']} shares\n"
            f"  Invest   : ₹{int(p['invest']):,}\n"
            f"  Est.Profit: ₹{int(p['profit_est']):,} at T2\n\n"
            f"📈 Signal: {p['signal']} | RSI: {p['rsi']} | Vol: {p['vol']}×\n\n"
            f"⚠️ Not SEBI advice.", config)
        time.sleep(1.5)

    if is_new_batch: save_batch1(top)
    return top

# ── MONITOR ───────────────────────────────────────────────────────────────────
def monitor_batch1():
    config = load_config()
    batch  = load_batch1()
    if not batch:
        send_telegram("📭 No Batch 1 stocks.\nSend /scan first.", config)
        return
    now_str = datetime.now(IST).strftime("%d %b %Y %I:%M %p")
    lines   = [f"👁 <b>Batch 1 Monitor — {now_str}</b>\n{'─'*26}\n"]
    alerts  = []
    for p in batch:
        symbol = p["symbol"]
        ltp    = get_ltp(symbol)
        if not ltp:
            lines.append(f"⚪ <b>{symbol}</b> — unavailable")
            time.sleep(0.3); continue
        entry = p["entry"]; sl=p["sl"]; t1=p["t1"]; t2=p["t2"]; t3=p["t3"]
        chg   = round((ltp-entry)/entry*100, 2)
        arrow = "📈" if chg>=0 else "📉"
        if ltp>=t3:
            st="🎯🎯 T3 HIT"
            alerts.append(f"🎯 <b>{symbol} TARGET 3!</b>\n₹{entry}→₹{ltp} (+{chg}%)\n➡️ Book full profit!")
        elif ltp>=t2:
            st="🎯 T2 HIT"
            alerts.append(f"🎯 <b>{symbol} TARGET 2!</b>\n₹{entry}→₹{ltp} (+{chg}%)\n➡️ Sell remaining 40%")
        elif ltp>=t1:
            st="✅ T1 HIT"
            alerts.append(f"✅ <b>{symbol} TARGET 1!</b>\n₹{entry}→₹{ltp} (+{chg}%)\n➡️ Sell 40%, trail rest")
        elif ltp<=sl:
            st="🔴 SL HIT"
            alerts.append(f"🔴 <b>{symbol} STOP LOSS!</b>\n₹{entry}→₹{ltp} ({chg}%)\n➡️ Exit now!")
        elif chg>=1.5: st=f"🟢 +{chg}% running"
        elif chg<=-1:  st=f"🟡 {chg}% watch"
        else:          st=f"⚪ {chg}% sideways"
        lines.append(f"{arrow} <b>{symbol}</b> ₹{ltp:,}  {st}\n   Entry ₹{entry} | SL ₹{sl} | T1 ₹{t1} | T2 ₹{t2}")
        time.sleep(0.3)
    send_telegram("\n\n".join(lines), config)
    time.sleep(1)
    for a in alerts: send_telegram(a, config); time.sleep(1)

def task_heartbeat():
    config = load_config()
    batch  = load_batch1()
    status = "🟢 LIVE" if market_is_open() else "🔴 CLOSED"
    names  = ", ".join(b["symbol"] for b in batch) if batch else "None — send /scan"
    send_telegram(
        f"✅ <b>SwingBot Running</b>\n\n"
        f"Time   : {datetime.now(IST).strftime('%d %b %I:%M %p')} IST\n"
        f"Market : {status}\n"
        f"Batch 1: {names}\n\n"
        f"Commands: /scan /monitor /status /help", config)

HELP_TEXT = (
    "🤖 <b>SwingBot Commands</b>\n\n"
    "/scan — Scan Nifty 50, save as Batch 1\n"
    "/newscan — Fresh scan, replace Batch 1\n"
    "/monitor — Check Batch 1 prices\n"
    "/status — Bot health check\n"
    "/help — This menu\n\n"
    "📅 <b>Auto schedule (IST):</b>\n"
    "Mon 8:00 AM — Weekly scan\n"
    "Mon–Fri 3:00 PM — Auto monitor\n"
    "Fri 2:45 PM — Exit reminder\n"
    "Mon–Fri 8:55 AM — Daily heartbeat"
)

# ── FLASK WEBHOOK ─────────────────────────────────────────────────────────────
@app.route("/")
def home():
    return "SwingBot is running ✅", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data   = flask_request.get_json(force=True)
        config = load_config()
        msg    = data.get("message", {})
        text   = msg.get("text","").strip().lower()
        log(f"Webhook command: {text}")
        if text in ["/scan","/scan@swingbot"]:
            threading.Thread(target=run_scan,
                             kwargs={"is_new_batch": len(load_batch1())==0},
                             daemon=True).start()
        elif text == "/newscan":
            if os.path.exists(BATCH_FILE): os.remove(BATCH_FILE)
            threading.Thread(target=run_scan,
                             kwargs={"is_new_batch":True}, daemon=True).start()
        elif text in ["/monitor","/check"]:
            threading.Thread(target=monitor_batch1, daemon=True).start()
        elif text == "/status":
            threading.Thread(target=task_heartbeat, daemon=True).start()
        elif text == "/help":
            send_telegram(HELP_TEXT, config)
    except Exception as e:
        log(f"Webhook error: {e}")
    return "ok", 200

# ── WEBHOOK SETUP ─────────────────────────────────────────────────────────────
def setup_webhook():
    """Register webhook URL with Telegram."""
    config = load_config()
    token  = config.get("BOT_TOKEN","").strip()
    url    = os.environ.get("RAILWAY_STATIC_URL") or \
             os.environ.get("RAILWAY_PUBLIC_DOMAIN") or ""
    if url and not url.startswith("http"):
        url = "https://" + url
    if not url:
        log("No RAILWAY_PUBLIC_DOMAIN found — trying to get it...")
        return False
    webhook_url = f"{url}/webhook"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json={"url": webhook_url}, timeout=10)
        if r.status_code == 200 and r.json().get("ok"):
            log(f"Webhook set: {webhook_url}")
            return True
        else:
            log(f"Webhook set failed: {r.text}")
            return False
    except Exception as e:
        log(f"Webhook setup error: {e}")
        return False

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
def start_scheduler():
    sched = BlockingScheduler(timezone=IST)
    sched.add_job(task_heartbeat,  CronTrigger(day_of_week="mon-fri",hour=8, minute=55,timezone=IST))
    sched.add_job(lambda: run_scan(is_new_batch=True),
                                   CronTrigger(day_of_week="mon",    hour=8, minute=0, timezone=IST))
    sched.add_job(monitor_batch1,  CronTrigger(day_of_week="mon-fri",hour=15,minute=0, timezone=IST))
    sched.add_job(lambda: send_telegram(
        f"⏰ <b>Friday Exit Reminder</b>\n"
        f"Batch 1: {', '.join(b['symbol'] for b in load_batch1()) or 'None'}\n"
        f"Exit before 3:15 PM to avoid weekend gap!", load_config()),
                                   CronTrigger(day_of_week="fri",    hour=14,minute=45,timezone=IST))
    log("Scheduler started.")
    sched.start()

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv)>1 else ""

    if cmd == "test":
        config = load_config()
        ok = send_telegram("🤖 <b>SwingBot v2 connected!</b>\nSend /help for commands.", config)
        print("Telegram OK ✅" if ok else "Telegram FAILED ❌")

    elif cmd == "scan":
        run_scan(is_new_batch=True)

    elif cmd == "monitor":
        monitor_batch1()

    else:
        # Start scheduler in background
        t = threading.Thread(target=start_scheduler, daemon=True)
        t.start()

        # Setup webhook
        time.sleep(2)
        if not setup_webhook():
            log("Webhook setup failed — will retry in 30s")
            time.sleep(30)
            setup_webhook()

        # Start Flask (Railway needs a web server)
        port = int(os.environ.get("PORT", 8080))
        log(f"Starting Flask on port {port}")
        app.run(host="0.0.0.0", port=port)
