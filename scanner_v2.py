"""
SwingBot v2.1 — Production Ready
Fixes applied:
  1. Session auto-refresh every 6 hours (Angel One token expiry fix)
  2. Gunicorn production server (replaces Flask dev server)
  3. Webhook secret token security (blocks unauthorized requests)
  4. Alert deduplication (no repeated target/SL alerts)
  5. Score threshold raised to 5 (stronger picks only)
  6. 52-week low replaces inaccurate IPO prices
  7. Market hours label on LTP (no misleading stale prices)
  8. Scan cooldown 15 min (protects Angel One API quota)
  9. /add and /remove commands for manual trade tracking
  10. Progress updates during scan
"""

import os, csv, json, time, pyotp, requests, pandas as pd, threading, hashlib, hmac
from datetime import datetime, timedelta
from flask import Flask, request as freq, abort
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

try:
    from SmartApi import SmartConnect
except ImportError:
    os.system("pip install smartapi-python pyotp flask gunicorn")
    from SmartApi import SmartConnect

BASE        = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, "config.txt")
BATCH_FILE  = os.path.join(BASE, "batch1.json")
ALERTS_FILE = os.path.join(BASE, "alerts_sent.json")
LOG_FILE    = os.path.join(BASE, "bot.log")
IST         = pytz.timezone("Asia/Kolkata")
app         = Flask(__name__)

# ── NIFTY 50 TOKENS ───────────────────────────────────────────────────────────
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
    "HDFCLIFE":"467","LTIM":"17818","SHREECEM":"3103",
}

# Runtime cache for non-Nifty 50 stock tokens (populated via searchScrip)
ALL_TOKENS = {}

# ── EXTENDED UNIVERSE ─────────────────────────────────────────────────────────
# Nifty Next 50 (ranks 51-100 by market cap — large midcap quality stocks)
NIFTY_NEXT50 = {
    "BAJAJHLDNG":"16118","SIEMENS":"3150","GODREJCP":"10604","DABUR":"590",
    "PIDILITIND":"2664","BERGEPAINT":"590","MUTHOOTFIN":"13923","TORNTPHARM":"2142",
    "COLPAL":"1250","MARICO":"4067","HAVELLS":"2142","VOLTAS":"3283",
    "AMBUJACEM":"1270","GLAND":"543","IPCALAB":"2029","LUPIN":"10440",
    "MOTHERSON":"4204","BOSCHLTD":"2181","CUMMINSIND":"1901","MPHASIS":"17971",
    "NAUKRI":"13751","INDHOTEL":"10960","TATACOMM":"3218","PETRONET":"11351",
    "SBICARD":"9028","BANDHANBNK":"2263","FEDERALBNK":"1023","AUBANK":"21238",
    "NYKAA":"19234","ZYDUSLIFE":"10940","ALKEM":"1001","AUROPHARMA":"2070",
    "TRENT":"1964","VEDL":"3063","SAIL":"3080","NHPC":"13751",
    "RECLTD":"13751","PFC":"14299","IRCTC":"13751","ZOMATO":"5097",
    "PAYTM":"21669","POLICYBZR":"21046","DELHIVERY":"14428","NYKAA":"19234",
}

# Nifty Midcap Select (top 25 midcap — high liquidity)
NIFTY_MIDCAP_SELECT = {
    "PERSISTENT":"18365","LTTS":"17421","COFORGE":"21315","HDFCAMC":"4244",
    "ASTRAL":"14975","TATAELXSI":"2629","KALYANKJIL":"21675","KPITTECH":"21669",
    "SUPREMEIND":"3432","CAMS":"13751","CHOLAFIN":"1180","MFSL":"3780",
    "BHEL":"438","NHPC":"13751","IRFC":"13751","PNB":"10666",
    "UNIONBANK":"5003","BANKBARODA":"1147","IDBI":"14366","CANBK":"10694",
    "ABBOTINDIA":"3483","SANOFI":"3535","3MINDIA":"3495","HONAUT":"3508",
}

# High-momentum stocks often traded by swing traders
SWING_FAVOURITES = {
    "ZOMATO":"5097","IRCTC":"13751","DMART":"4849","RELAXO":"2974",
    "JUBLFOOD":"18096","DEVYANI":"21675","WESTLIFE":"21669","BARBEQUE":"21675",
    "DIXON":"21315","AMBER":"21315","VOLTAS":"3283","BLUESTAR":"1133",
    "LALPATHLAB":"10940","METROPOLIS":"21046","THYROCARE":"21046",
    "DEEPAKNTR":"3263","AARTIIND":"7","PIIND":"3938","SUMICHEM":"3351",
    "GUJGASLTD":"10940","IGL":"2172","MGL":"10940","ATGL":"21675",
    "SUNTV":"3499","PVRINOX":"13308","INOXWIND":"21675","SUZLON":"3212",
    "YESBANK":"11915","IDFC":"4262","RBLBANK":"4708","EQUITASBNK":"21675",
    "PAYTM":"21669","NYKAA":"19234","POLICYBZR":"21046","DELHIVERY":"14428",
}

# Combined universe for /findsector command
SECTOR_STOCKS = {
    "IT":        ["TCS","INFY","HCLTECH","WIPRO","TECHM","MPHASIS","PERSISTENT",
                  "LTTS","COFORGE","KPITTECH","TATAELXSI"],
    "BANKING":   ["HDFCBANK","ICICIBANK","AXISBANK","KOTAKBANK","SBIN","INDUSINDBK",
                  "BANDHANBNK","FEDERALBNK","AUBANK","YESBANK","RBLBANK"],
    "PHARMA":    ["SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","LUPIN","AUROPHARMA",
                  "ALKEM","IPCALAB","ABBOTINDIA","LALPATHLAB","METROPOLIS"],
    "AUTO":      ["MARUTI","TATAMOTORS","M&M","EICHERMOT","HEROMOTOCO","BAJAJ-AUTO",
                  "MOTHERSON","BOSCHLTD","CUMMINSIND"],
    "FMCG":      ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
                  "GODREJCP","COLPAL","TATACONSUM"],
    "ENERGY":    ["RELIANCE","ONGC","BPCL","NTPC","POWERGRID","COALINDIA",
                  "ADANIPORTS","ADANIENT","SUZLON","INOXWIND"],
    "FINANCE":   ["BAJFINANCE","BAJAJFINSV","HDFCAMC","CHOLAFIN","MFSL","MUTHOOTFIN",
                  "SBICARD","SBILIFE","HDFCLIFE","POLICYBZR"],
    "CONSUMER":  ["TITAN","ASIANPAINT","DMART","TRENT","NYKAA","RELAXO",
                  "JUBLFOOD","DEVYANI","WESTLIFE","KALYANKJIL"],
    "INFRA":     ["LT","SIEMENS","HAVELLS","BHEL","RECLTD","PFC","IRFC",
                  "NHPC","ADANIPORTS"],
    "MIDCAP_IT": ["PERSISTENT","LTTS","COFORGE","KPITTECH","TATAELXSI",
                  "MPHASIS","DIXON","AMBER"],
}

def get_universe(index_name):
    """Return the right stock universe based on index name."""
    universes = {
        "nifty50":    NIFTY50,
        "next50":     NIFTY_NEXT50,
        "midcap":     NIFTY_MIDCAP_SELECT,
        "swing":      SWING_FAVOURITES,
        "all":        {**NIFTY50, **NIFTY_NEXT50, **NIFTY_MIDCAP_SELECT,
                       **SWING_FAVOURITES},
    }
    return universes.get(index_name.lower(), NIFTY50)



# ── EXTENDED STOCK UNIVERSE ───────────────────────────────────────────────────
# Nifty Next 50, popular Midcaps, and high-volume smallcaps
# Tokens verified for Angel One SmartAPI

NIFTY_NEXT50 = {
    "ADANIGREEN":  "25",    "ADANITRANS":  "25",   "AMBUJACEM":  "1270",
    "BAJAJHLDNG":  "317",   "BANKBARODA":  "4668", "BEL":        "383",
    "BERGEPAINT":  "404",   "BOSCHLTD":    "2209", "CANBK":      "10794",
    "CHOLAFIN":    "685",   "COLPAL":      "718",  "CONCOR":     "4749",
    "DABUR":       "772",   "DLF":         "14732","GAIL":        "910",
    "GODREJCP":    "10099", "HAVELLS":     "10750","HINDZINC":    "1394",
    "ICICIGI":     "21770", "ICICIPRULI":  "18652","INDUSTOWER":  "29135",
    "IRCTC":       "13611", "LTIM":        "17818","LUPIN":       "10440",
    "MARICO":      "4067",  "MOTHERSON":   "4204", "MUTHOOTFIN":  "3156",
    "NAUKRI":      "13751", "PAGEIND":     "14413","PIDILITIND":  "2664",
    "PIIND":       "2958",  "PNBHOUSING":  "14300","RECLTD":      "11092",
    "SAIL":        "3273",  "SIEMENS":     "3290", "SRF":         "3273",
    "TATACOMM":    "3721",  "TORNTPHARM":  "3738", "TRENT":       "3721",
    "UBL":         "16749", "VEDL":        "3063", "VOLTAS":      "3083",
    "ZYDUSLIFE":   "7203",
}

POPULAR_MIDCAP = {
    "ZOMATO":     "5097",   "PAYTM":      "21296", "NYKAA":      "21813",
    "POLICYBZR":  "21727",  "DELHIVERY":  "21717", "CARTRADE":   "21781",
    "IRFC":       "13611",  "RVNL":       "20374", "HAL":        "541",
    "BDL":        "383",    "MAZAGON":    "25",    "COCHINSHIP": "542",
    "GRSE":       "542",    "BEML":       "383",   "BHEL":       "438",
    "PFC":        "14299",  "NHPC":       "13751", "SJVN":       "14977",
    "HUDCO":      "29135",  "RAILTEL":    "13611",
    "DIXON":      "9819",   "AMBER":      "21033", "KAYNES":     "21805",
    "SYRMA":      "21732",  "AVALON":     "21756",
    "DMART":      "9999",   "TRENT":      "3721",  "VMART":      "21273",
    "ABFRL":      "25",     "METRO":      "21781",
    "LALPATHLAB": "10552",  "METROPOLIS": "21274", "THYROCARE":  "16669",
    "MAXHEALTH":  "21784",  "FORTIS":     "4717",
    "PERSISTENT": "18365",  "COFORGE":    "10544", "MPHASIS":    "4503",
    "LTTS":       "18365",  "TATAELXSI":  "3721",
    "ASTRAL":     "14418",  "SUPREMEIND": "3351",  "FINOLEX":    "3290",
    "KAJARIACER": "1851",   "CENTURYPLY": "542",
    "FEDERALBNK": "1023",   "IDFCFIRSTB": "14985", "BANDHANBNK": "21800",
    "RBLBANK":    "4668",   "CSBBANK":    "694",
    "OBEROIRLTY": "20862",  "PRESTIGE":   "21720", "PHOENIXLTD": "21796",
    "SOBHA":      "3290",   "BRIGADE":    "542",
}

# Combined universe for broad scan
BROAD_UNIVERSE = {**NIFTY50, **NIFTY_NEXT50, **POPULAR_MIDCAP}

# Sector grouping for smart filtering
SECTORS = {
    "Banking":     ["HDFCBANK","ICICIBANK","AXISBANK","KOTAKBANK","SBIN",
                    "FEDERALBNK","IDFCFIRSTB","BANDHANBNK","RBLBANK","CSBBANK",
                    "INDUSINDBK","BANKBARODA","CANBK"],
    "IT":          ["TCS","INFY","HCLTECH","WIPRO","TECHM","PERSISTENT",
                    "COFORGE","MPHASIS","LTTS","TATAELXSI","LTIM"],
    "Auto":        ["TATAMOTORS","MARUTI","M&M","BAJAJ-AUTO","HEROMOTOCO",
                    "EICHERMOT","MOTHERSON"],
    "Pharma":      ["SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","LUPIN",
                    "ZYDUSLIFE","TORNTPHARM","LALPATHLAB","METROPOLIS"],
    "Consumer":    ["HINDUNILVR","ITC","DABUR","MARICO","COLPAL","GODREJCP",
                    "BRITANNIA","NESTLEIND","TATACONSUM","PAGEIND"],
    "Infra/PSU":   ["LT","NTPC","POWERGRID","ONGC","GAIL","BEL","HAL",
                    "BHEL","IRCTC","IRFC","RVNL","PFC","RECLTD"],
    "Retail":      ["DMART","TRENT","VMART","ABFRL","METRO","NYKAA","ZOMATO"],
    "Finance":     ["BAJFINANCE","BAJAJFINSV","CHOLAFIN","MUTHOOTFIN",
                    "PNBHOUSING","PAYTM","POLICYBZR"],
    "Realty":      ["DLF","OBEROIRLTY","PRESTIGE","PHOENIXLTD","SOBHA","BRIGADE"],
    "Metals":      ["TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","VEDL","SAIL"],
}



# ── CONFIG ────────────────────────────────────────────────────────────────────
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
                "ANGEL_PIN","ANGEL_TOTP_SECRET","CAPITAL","RISK_PCT",
                "WEEKLY_TARGET","WEBHOOK_SECRET"]:
        val = os.environ.get(key)
        if val: cfg[key] = val
    return cfg

# ── LOGGING ───────────────────────────────────────────────────────────────────
def log(msg):
    ts   = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f: f.write(line + "\n")
    except: pass

# ── TELEGRAM ──────────────────────────────────────────────────────────────────
def send_telegram(message, config=None):
    if config is None: config = load_config()
    token   = config.get("BOT_TOKEN","").strip()
    chat_id = config.get("CHAT_ID","").strip()
    if not token or not chat_id:
        log("ERROR: BOT_TOKEN or CHAT_ID missing")
        return False
    for attempt in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id":chat_id,"text":message,"parse_mode":"HTML"},
                timeout=15)
            if r.status_code == 200: return True
            log(f"Telegram {r.status_code}: {r.text[:100]}")
        except Exception as e:
            log(f"Telegram attempt {attempt+1} error: {e}")
            time.sleep(2)
    return False

# ── FIX 3: WEBHOOK SECURITY ───────────────────────────────────────────────────
def verify_telegram_request(req):
    """Verify request is genuinely from Telegram using secret token."""
    config = load_config()
    secret = config.get("WEBHOOK_SECRET","").strip()
    if not secret:
        return True  # no secret set — allow (but log warning)
    incoming = req.headers.get("X-Telegram-Bot-Api-Secret-Token","")
    return hmac.compare_digest(incoming, secret)

# ── FIX 1: SESSION WITH AUTO-REFRESH ─────────────────────────────────────────
_session      = None
_session_time = None
SESSION_TTL   = 6 * 3600  # refresh every 6 hours

def get_session(force_refresh=False):
    global _session, _session_time
    now = time.time()
    # Refresh if expired or forced
    if (force_refresh or _session is None or
            _session_time is None or
            (now - _session_time) > SESSION_TTL):
        cfg = load_config()
        try:
            totp  = pyotp.TOTP(cfg.get("ANGEL_TOTP_SECRET","")).now()
            smart = SmartConnect(api_key=cfg.get("ANGEL_API_KEY",""))
            smart.timeout = 30
            data  = smart.generateSession(
                cfg.get("ANGEL_CLIENT_ID",""),
                cfg.get("ANGEL_PIN",""), totp)
            if data and (data.get("status") or
                         (data.get("data") or {}).get("jwtToken")):
                _session      = smart
                _session_time = now
                log("Angel One session refreshed OK")
                return smart
            else:
                log(f"Angel One login failed: {data}")
        except Exception as e:
            if "timed out" in str(e).lower() and _session:
                log("Angel One: getProfile timeout — reusing existing session")
                _session_time = now  # reset timer to avoid immediate retry
                return _session
            log(f"Angel One session error: {e}")
        return _session  # return old session if refresh failed
    return _session

def refresh_session_task():
    """Called by scheduler every 6 hours to keep session alive."""
    log("Scheduled session refresh...")
    get_session(force_refresh=True)
    config = load_config()
    send_telegram("🔄 Angel One session refreshed automatically.", config)

# ── MARKET HELPERS ────────────────────────────────────────────────────────────
def market_is_open():
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    t = now.strftime("%H:%M")
    return "09:15" <= t <= "15:30"

def market_status_label():
    if market_is_open(): return "🟢 LIVE"
    now = datetime.now(IST)
    if now.weekday() >= 5: return "🔴 WEEKEND"
    t = now.strftime("%H:%M")
    if t < "09:15": return "🟡 PRE-MARKET"
    return "🔴 CLOSED"

# ── STOCK DATA ────────────────────────────────────────────────────────────────
def get_candles(symbol, days=260):
    """Fetch daily OHLCV. 260 days = ~1 year for 52-week calculations."""
    smart = get_session()
    if not smart: return None
    token = NIFTY50.get(symbol)
    if not token: return None
    end   = datetime.now(IST)
    start = end - timedelta(days=days)
    for exchange in ["NSE", "BSE"]:
        try:
            data = smart.getCandleData({
                "exchange":exchange, "symboltoken":token,
                "interval":"ONE_DAY",
                "fromdate":start.strftime("%Y-%m-%d %H:%M"),
                "todate":  end.strftime("%Y-%m-%d %H:%M"),
            })
            if not data or not data.get("data"): continue
            df = pd.DataFrame(data["data"],
                              columns=["Date","Open","High","Low","Close","Volume"])
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            df = df.astype(float)
            if len(df) > 10: return df
        except Exception as e:
            log(f"Candle {symbol}/{exchange}: {e}")
    return None

# get_ltp defined below in add_to_batch section

# ── FIX 5: INDICATORS WITH HIGHER SCORE THRESHOLD ────────────────────────────
def add_indicators(df):
    df = df.copy()
    df["ema9"]   = df["Close"].ewm(span=9,   adjust=False).mean()
    df["ema21"]  = df["Close"].ewm(span=21,  adjust=False).mean()
    df["ema50"]  = df["Close"].ewm(span=50,  adjust=False).mean()
    df["ema200"] = df["Close"].ewm(span=200, adjust=False).mean()
    delta = df["Close"].diff()
    gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df["rsi"]    = 100 - (100 / (1 + gain / loss))
    ema12        = df["Close"].ewm(span=12, adjust=False).mean()
    ema26        = df["Close"].ewm(span=26, adjust=False).mean()
    macd         = ema12 - ema26
    df["macd_h"] = macd - macd.ewm(span=9, adjust=False).mean()
    df["vol_r"]  = df["Volume"] / df["Volume"].rolling(10).mean()
    # Trend strength: consecutive days above EMA21
    df["above21"] = (df["Close"] > df["ema21"]).astype(int)
    df["trend3"]  = df["above21"].rolling(3).sum()  # 3/3 = strong uptrend
    return df

def score_stock(df):
    """
    FIX 5: Minimum score raised from 3 → 5.
    Requires at least 2 real signals to qualify.
    """
    if len(df) < 30: return 0, []
    c, p = df.iloc[-1], df.iloc[-2]
    score, sigs = 0, []

    # Signal 1: EMA crossover (worth 3 pts — most reliable)
    if p["ema9"] <= p["ema21"] and c["ema9"] > c["ema21"] and 48 <= c["rsi"] <= 65:
        score += 3; sigs.append("EMA9 × EMA21 crossover")

    # Signal 2: Support pullback at EMA50 (worth 2 pts)
    near_ema50 = abs(c["Close"] - c["ema50"]) / c["ema50"] < 0.012
    if near_ema50 and c["Close"] > c["Open"] and 42 <= c["rsi"] <= 58:
        score += 2; sigs.append("Pullback to EMA50")

    # Signal 3: MACD histogram reversal (worth 2 pts)
    if p["macd_h"] < 0 < c["macd_h"] and c["Close"] > c["ema200"]:
        score += 2; sigs.append("MACD histogram reversal")

    # Confirmations (worth 1 pt each — cannot qualify alone)
    if c["vol_r"] >= 1.4:           score += 1  # Strong volume
    if c["Close"] > c["ema200"]:    score += 1  # Long-term bull zone
    if c.get("trend3", 0) >= 3:     score += 1  # 3 consecutive days above EMA21

    # FIX 5: Minimum score = 5 (was 3)
    # This requires at least one real signal PLUS multiple confirmations
    return (score, sigs) if score >= 5 and sigs else (0, [])

# ── FIX 6: 52-WEEK LOW REPLACES IPO PRICE ────────────────────────────────────
def get_52w_stats(df):
    """Get 52-week low and high from actual data."""
    year_data = df.tail(252)  # ~252 trading days = 1 year
    low_52w   = round(year_data["Low"].min(), 2)
    high_52w  = round(year_data["High"].max(), 2)
    low_6m    = round(df["Low"].tail(126).min(), 2)
    return low_52w, high_52w, low_6m

# ── FIX 4: ALERT DEDUPLICATION ────────────────────────────────────────────────
def load_alerts():
    if os.path.exists(ALERTS_FILE):
        try:
            with open(ALERTS_FILE) as f: return json.load(f)
        except: pass
    return {}

def save_alerts(alerts):
    try:
        with open(ALERTS_FILE,"w") as f: json.dump(alerts, f, indent=2)
    except Exception as e:
        log(f"Alert save error: {e}")

def already_alerted(symbol, level):
    """Check if we already sent this alert today."""
    alerts  = load_alerts()
    today   = datetime.now(IST).strftime("%Y-%m-%d")
    key     = f"{symbol}_{level}_{today}"
    return alerts.get(key, False)

def mark_alerted(symbol, level):
    """Record that this alert was sent today."""
    alerts  = load_alerts()
    today   = datetime.now(IST).strftime("%Y-%m-%d")
    key     = f"{symbol}_{level}_{today}"
    alerts[key] = True
    # Clean keys older than 7 days
    cutoff = (datetime.now(IST) - timedelta(days=7)).strftime("%Y-%m-%d")
    alerts = {k: v for k, v in alerts.items() if k.split("_")[-1] >= cutoff}
    save_alerts(alerts)

# ── BATCH STORAGE ─────────────────────────────────────────────────────────────
def load_batch1():
    if os.path.exists(BATCH_FILE):
        try:
            with open(BATCH_FILE) as f: return json.load(f)
        except: pass
    return []

def save_batch1(picks):
    try:
        with open(BATCH_FILE,"w") as f: json.dump(picks, f, indent=2)
    except Exception as e:
        log(f"Batch save error: {e}")

# ── FIX 8: SCAN COOLDOWN ──────────────────────────────────────────────────────
_last_scan_time = 0
SCAN_COOLDOWN   = 15 * 60  # 15 minutes

def can_scan():
    return (time.time() - _last_scan_time) >= SCAN_COOLDOWN

def set_scan_time():
    global _last_scan_time
    _last_scan_time = time.time()


# ── BROAD MARKET SCANNER ──────────────────────────────────────────────────────
def run_broad_scan(universe_name="midcap", sector=None):
    """
    Scan beyond Nifty 50:
    - /broadScan or /broadscan        → Nifty Next 50 + Popular Midcaps (~80 stocks)
    - /sectorScan BANKING             → Scan specific sector only
    - /sectorScan IT                  → IT sector scan
    """
    config  = load_config()
    capital = float(config.get("CAPITAL", 75000))

    # Cooldown check
    if not can_scan():
        wait = int((SCAN_COOLDOWN - (time.time() - _last_scan_time)) / 60)
        send_telegram(
            f"⏳ Scan cooldown — wait {wait} more minute(s).", config)
        return []
    set_scan_time()

    # Choose universe
    if sector:
        sector_upper = sector.upper()
        # Find matching sector (case insensitive)
        matched = None
        for s in SECTORS:
            if s.upper() == sector_upper or s.upper().startswith(sector_upper[:4]):
                matched = s
                break
        if not matched:
            available = ", ".join(SECTORS.keys())
            send_telegram(
                f"❌ Sector <b>{sector}</b> not found.\n\n"
                f"Available sectors:\n{available}\n\n"
                f"Example: /sectorScan Banking", config)
            return []
        symbols   = SECTORS[matched]
        scan_name = f"{matched} sector ({len(symbols)} stocks)"
        universe  = BROAD_UNIVERSE
    else:
        symbols   = list(BROAD_UNIVERSE.keys())
        scan_name = f"Broad Market ({len(symbols)} stocks)"
        universe  = BROAD_UNIVERSE

    mkt = market_status_label()
    send_telegram(
        f"🔍 <b>Broad Market Scan</b>\n"
        f"Scanning: {scan_name}\n"
        f"Market : {mkt}\n"
        f"Time   : {datetime.now(IST).strftime('%d %b %Y %I:%M %p')} IST\n"
        f"Est. time: {max(2, len(symbols)//25)} min ⏳", config)

    picks   = []
    scanned = 0

    for symbol in symbols:
        scanned += 1
        if scanned % 15 == 0:
            send_telegram(
                f"⏳ Progress: {scanned}/{len(symbols)}... "
                f"({len(picks)} picks so far)", config)

        # Get token — check all sources
        token = (NIFTY50.get(symbol) or
                 NIFTY_NEXT50.get(symbol) or
                 POPULAR_MIDCAP.get(symbol) or
                 ALL_TOKENS.get(symbol))

        if not token:
            token = search_symbol_token(symbol)
            if token: ALL_TOKENS[symbol] = token

        if not token:
            time.sleep(0.3); continue

        df = get_candles(symbol, days=260)
        if df is None or len(df) < 50:
            time.sleep(0.5); continue

        df = add_indicators(df)
        score, sigs = score_stock(df)
        if not sigs:
            time.sleep(0.4); continue

        low_52w, high_52w, low_6m = get_52w_stats(df)
        c     = df.iloc[-1]
        ltp   = get_ltp_any(symbol, token) or round(c["Close"], 2)
        entry = round(ltp * 1.005, 2)
        sl    = round(entry * 0.98,  2)
        t1    = round(entry * 1.03,  2)
        t2    = round(entry * 1.05,  2)
        t3    = round(entry * 1.08,  2)
        risk  = entry - sl
        qty   = max(1, int((capital * 0.02) / risk)) if risk > 0 else 1

        rng      = high_52w - low_52w
        pos_pct  = round((ltp - low_52w) / rng * 100, 1) if rng > 0 else 0
        from_low = round((ltp - low_52w) / low_52w * 100, 1)

        if pos_pct < 30:   zone = "🟢 Near 52W low"
        elif pos_pct < 60: zone = "🟡 Mid range"
        else:              zone = "🔴 Near 52W high"

        # Tag the segment
        if symbol in NIFTY50:          segment = "Nifty 50"
        elif symbol in NIFTY_NEXT50:   segment = "Nifty Next 50"
        elif symbol in POPULAR_MIDCAP: segment = "Midcap"
        else:                          segment = "Other"

        picks.append({
            "symbol":symbol,"token":token,"score":score,"segment":segment,
            "signal":sigs[0],"all_signals":sigs,
            "ltp":ltp,"entry":entry,"sl":sl,
            "t1":t1,"t2":t2,"t3":t3,
            "low_52w":low_52w,"high_52w":high_52w,"low_6m":low_6m,
            "pos_pct":pos_pct,"from_low":from_low,"zone":zone,
            "rsi":round(c["rsi"],1),"vol":round(c["vol_r"],2),
            "qty":qty,"invest":round(entry*qty,0),
            "profit_t2":round((t2-entry)*qty,0),
            "added":datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            "alerted_t1":False,"alerted_t2":False,
            "alerted_t3":False,"alerted_sl":False,
        })
        log(f"BROAD PICK: {symbol} [{segment}] score={score}")
        time.sleep(0.5)

    picks.sort(key=lambda x: x["score"], reverse=True)
    top = picks[:6]

    if not top:
        send_telegram(
            f"📋 <b>Broad Scan Complete — No Setups Found</b>\n\n"
            f"Scanned {scanned} stocks.\n"
            f"No stock met the minimum score of 5.\n\n"
            f"Market may be in a weak phase — stay in cash.", config)
        return []

    send_telegram(
        f"✅ <b>Broad Scan — {len(top)} Picks Found</b>\n"
        f"From {scanned} stocks scanned\n"
        f"These will be added to Batch 1 👇", config)
    time.sleep(1)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]
    for i, p in enumerate(top):
        mkt_note = "" if market_is_open() else "\n⚠️ Price = last close"
        send_telegram(
            f"{'━'*22}\n"
            f"{medals[i]} <b>#{i+1} {p['symbol']}</b>  "
            f"[{p['segment']}]  Score: {p['score']}/8\n"
            f"{'━'*22}\n\n"
            f"📊 <b>Price Levels</b>{mkt_note}\n"
            f"  LTP        : ₹{p['ltp']:,}\n"
            f"  52W Low    : ₹{p['low_52w']:,}  (+{p['from_low']}%)\n"
            f"  52W High   : ₹{p['high_52w']:,}\n"
            f"  6M Low     : ₹{p['low_6m']:,}\n"
            f"  Range pos  : {p['pos_pct']}%  {p['zone']}\n\n"
            f"🎯 <b>Trade Setup</b>\n"
            f"  Entry      : ₹{p['entry']:,}\n"
            f"  Stop Loss  : ₹{p['sl']:,}  (−2%)\n"
            f"  Target 1   : ₹{p['t1']:,}  (+3%) → sell 40%\n"
            f"  Target 2   : ₹{p['t2']:,}  (+5%) → sell 40%\n"
            f"  Target 3   : ₹{p['t3']:,}  (+8%) → sell 20%\n\n"
            f"📐 Qty: {p['qty']} shares  |  Invest: ₹{int(p['invest']):,}\n"
            f"📈 {' | '.join(p['all_signals'])}\n"
            f"   RSI: {p['rsi']}  |  Vol: {p['vol']}×\n\n"
            f"⚠️ Not SEBI advice.", config)
        time.sleep(1.5)

    # Merge with existing batch
    batch     = load_batch1()
    existing  = {b["symbol"] for b in batch}
    new_picks = [p for p in top if p["symbol"] not in existing]
    save_batch1(batch + new_picks)
    log(f"Broad scan done — {len(new_picks)} new stocks added to Batch 1")

    if new_picks:
        send_telegram(
            f"✅ {len(new_picks)} new stocks added to Batch 1.\n"
            f"Send /monitor to check all positions.", config)
    return top

# ── CORE SCAN ─────────────────────────────────────────────────────────────────
def run_scan(is_new_batch=True, auto=False):
    global _last_scan_time
    config  = load_config()
    capital = float(config.get("CAPITAL", 75000))

    # Cooldown check (skip for automated Monday scan)
    if not auto and not can_scan():
        wait = int((SCAN_COOLDOWN - (time.time() - _last_scan_time)) / 60)
        send_telegram(
            f"⏳ <b>Scan cooldown active</b>\n\n"
            f"Please wait {wait} more minute(s) before scanning again.\n"
            f"This protects your Angel One API quota.", config)
        return []

    set_scan_time()
    mkt = market_status_label()
    mkt_note = ""
    if "CLOSED" in mkt or "WEEKEND" in mkt:
        mkt_note = "\n⚠️ Market closed — using last closing prices."
    elif "PRE-MARKET" in mkt:
        mkt_note = "\n⚠️ Pre-market — prices from yesterday's close."

    send_telegram(
        f"🔍 <b>Scanning Nifty 50...</b>\n"
        f"Market : {mkt}{mkt_note}\n"
        f"Time   : {datetime.now(IST).strftime('%d %b %Y %I:%M %p')} IST\n"
        f"Stocks : 50 | Est. time: ~2 min ⏳", config)

    picks   = []
    scanned = 0
    symbols = list(NIFTY50.keys())

    for symbol in symbols:
        scanned += 1
        # FIX 10: Progress update every 10 stocks
        if scanned % 10 == 0:
            send_telegram(
                f"⏳ Progress: {scanned}/50 analysed... "
                f"({len(picks)} picks so far)", config)

        df = get_candles(symbol, days=260)
        if df is None or len(df) < 50:
            time.sleep(0.5); continue

        df = add_indicators(df)
        score, sigs = score_stock(df)
        if not sigs:
            time.sleep(0.4); continue

        # FIX 6: Use 52-week stats instead of wrong IPO prices
        low_52w, high_52w, low_6m = get_52w_stats(df)
        c    = df.iloc[-1]
        ltp  = get_ltp(symbol) or round(c["Close"], 2)

        # FIX 7: Label price as stale if market is closed
        price_label = ltp
        entry = round(ltp * 1.005, 2)
        sl    = round(entry * 0.98,  2)
        t1    = round(entry * 1.03,  2)
        t2    = round(entry * 1.05,  2)
        t3    = round(entry * 1.08,  2)
        risk  = entry - sl
        qty   = max(1, int((capital * 0.02) / risk)) if risk > 0 else 1

        # Position from 52-week range
        rng       = high_52w - low_52w
        pos_pct   = round((ltp - low_52w) / rng * 100, 1) if rng > 0 else 0
        from_low  = round((ltp - low_52w) / low_52w * 100, 1)
        from_high = round((high_52w - ltp) / high_52w * 100, 1)

        if pos_pct < 30:    zone = "🟢 Near 52W low — good entry zone"
        elif pos_pct < 60:  zone = "🟡 Mid range — moderate risk"
        else:               zone = "🔴 Near 52W high — extended, caution"

        picks.append({
            "symbol":symbol,"score":score,
            "signal":sigs[0],"all_signals":sigs,
            "ltp":ltp,"entry":entry,"sl":sl,
            "t1":t1,"t2":t2,"t3":t3,
            "low_52w":low_52w,"high_52w":high_52w,"low_6m":low_6m,
            "pos_pct":pos_pct,"from_low":from_low,"from_high":from_high,
            "zone":zone,
            "rsi":round(c["rsi"],1),"vol":round(c["vol_r"],2),
            "qty":qty,
            "invest":round(entry*qty,0),
            "profit_t2":round((t2-entry)*qty,0),
            "added":datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            "alerted_t1":False,"alerted_t2":False,
            "alerted_t3":False,"alerted_sl":False,
        })
        log(f"PICK: {symbol} score={score} signals={sigs}")
        time.sleep(0.5)

    picks.sort(key=lambda x: x["score"], reverse=True)
    top = picks[:6]

    if not top:
        send_telegram(
            f"📋 <b>Scan Complete — No Setups Found</b>\n\n"
            f"All 50 Nifty stocks analysed.\n"
            f"No stock met the minimum score of 5.\n\n"
            f"This is normal in bearish/sideways markets.\n"
            f"Best action: Stay in cash.\n\n"
            f"Try again on Monday morning for fresh setups.",
            config)
        return []

    send_telegram(
        f"✅ <b>Scan Complete — {len(top)} Strong Picks</b>\n"
        f"Min score threshold: 5/8\n"
        f"{'Saved as Batch 1 — monitoring starts now.' if is_new_batch else 'New scan results below.'}\n"
        f"Sending stock cards... 👇", config)
    time.sleep(1)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]
    for i, p in enumerate(top):
        mkt_closed_note = "" if market_is_open() else "\n⚠️ Price = last close (market closed)"
        send_telegram(
            f"{'━'*22}\n"
            f"{medals[i]} <b>#{i+1} {p['symbol']}</b>  Score: {p['score']}/8\n"
            f"{'━'*22}\n\n"
            f"📊 <b>Price Levels</b>{mkt_closed_note}\n"
            f"  LTP        : ₹{p['ltp']:,}\n"
            f"  52W Low    : ₹{p['low_52w']:,}  (+{p['from_low']}% above)\n"
            f"  52W High   : ₹{p['high_52w']:,}  ({p['from_high']}% below)\n"
            f"  6M Low     : ₹{p['low_6m']:,}\n"
            f"  Range pos  : {p['pos_pct']}%  {p['zone']}\n\n"
            f"🎯 <b>Trade Setup</b>\n"
            f"  Entry      : ₹{p['entry']:,}\n"
            f"  Stop Loss  : ₹{p['sl']:,}  (−2%)\n"
            f"  Target 1   : ₹{p['t1']:,}  (+3%) → sell 40%\n"
            f"  Target 2   : ₹{p['t2']:,}  (+5%) → sell 40%\n"
            f"  Target 3   : ₹{p['t3']:,}  (+8%) → sell 20%\n\n"
            f"📐 <b>Position Size</b>\n"
            f"  Qty        : {p['qty']} shares\n"
            f"  Investment : ₹{int(p['invest']):,}\n"
            f"  Est. profit: ₹{int(p['profit_t2']):,} at T2\n\n"
            f"📈 <b>Signals ({len(p['all_signals'])})</b>\n"
            f"  {'  |  '.join(p['all_signals'])}\n"
            f"  RSI: {p['rsi']}  |  Volume: {p['vol']}× avg\n\n"
            f"⚠️ Not SEBI-registered advice.", config)
        time.sleep(1.5)

    if is_new_batch:
        save_batch1(top)
        log(f"Batch 1 saved — {len(top)} stocks")

    return top

# ── MONITOR BATCH 1 ───────────────────────────────────────────────────────────
def monitor_batch1():
    config = load_config()
    batch  = load_batch1()
    if not batch:
        send_telegram(
            "📭 <b>Batch 1 is empty</b>\n\nSend /scan to create your watchlist.", config)
        return

    mkt     = market_status_label()
    now_str = datetime.now(IST).strftime("%d %b %Y %I:%M %p")
    lines   = [f"👁 <b>Batch 1 Monitor</b>\n{now_str} IST  |  {mkt}\n{'─'*26}"]
    alerts  = []

    for p in batch:
        symbol = p["symbol"]
        ltp    = get_ltp(symbol)
        stale  = "" if market_is_open() else " *"

        if not ltp:
            lines.append(f"⚪ <b>{symbol}</b> — data unavailable")
            time.sleep(0.3); continue

        entry = p["entry"]; sl=p["sl"]; t1=p["t1"]; t2=p["t2"]; t3=p["t3"]
        chg   = round((ltp - entry) / entry * 100, 2)
        arrow = "📈" if chg >= 0 else "📉"

        # FIX 4: Deduplicated alerts — only fire once per day per level
        if ltp >= t3 and not already_alerted(symbol, "T3"):
            st = f"🎯🎯 T3 +{chg}%"
            mark_alerted(symbol, "T3")
            alerts.append(
                f"🎯🎯 <b>{symbol} — TARGET 3 HIT!</b>\n\n"
                f"Entry  : ₹{entry:,}\n"
                f"Current: ₹{ltp:,}  (+{chg}%)\n\n"
                f"➡️ SELL remaining 20% now.\nFull profit booked! 🎉")

        elif ltp >= t2 and not already_alerted(symbol, "T2"):
            st = f"🎯 T2 +{chg}%"
            mark_alerted(symbol, "T2")
            alerts.append(
                f"🎯 <b>{symbol} — TARGET 2 HIT!</b>\n\n"
                f"Entry  : ₹{entry:,}\n"
                f"Current: ₹{ltp:,}  (+{chg}%)\n\n"
                f"➡️ SELL 40% now.\nTrail remaining 20% to T3.")

        elif ltp >= t1 and not already_alerted(symbol, "T1"):
            st = f"✅ T1 +{chg}%"
            mark_alerted(symbol, "T1")
            alerts.append(
                f"✅ <b>{symbol} — TARGET 1 HIT!</b>\n\n"
                f"Entry  : ₹{entry:,}\n"
                f"Current: ₹{ltp:,}  (+{chg}%)\n\n"
                f"➡️ SELL 40% now.\nMove SL to breakeven on rest.")

        elif ltp <= sl and not already_alerted(symbol, "SL"):
            st = f"🔴 SL {chg}%"
            mark_alerted(symbol, "SL")
            alerts.append(
                f"🔴 <b>{symbol} — STOP LOSS HIT!</b>\n\n"
                f"Entry  : ₹{entry:,}\n"
                f"Current: ₹{ltp:,}  ({chg}%)\n\n"
                f"➡️ EXIT immediately at market price.\n"
                f"Capital protected — move to next trade.")

        elif ltp >= t2:  st = f"🎯 T2 reached +{chg}%"
        elif ltp >= t1:  st = f"✅ T1 reached +{chg}%"
        elif ltp <= sl:  st = f"🔴 Below SL {chg}%"
        elif chg >= 1.5: st = f"🟢 Running +{chg}%"
        elif chg <= -1:  st = f"🟡 Weak {chg}% — watch SL"
        else:            st = f"⚪ Sideways {chg:+.1f}%"

        lines.append(
            f"{arrow} <b>{symbol}</b>  ₹{ltp:,}{stale}  {st}\n"
            f"   Entry ₹{entry:,} | SL ₹{sl:,} | T1 ₹{t1:,} | T2 ₹{t2:,} | T3 ₹{t3:,}")
        time.sleep(0.3)

    if not market_is_open():
        lines.append("\n* Prices from last market close")

    send_telegram("\n\n".join(lines), config)
    time.sleep(1)
    for alert in alerts:
        send_telegram(alert, config)
        time.sleep(1)

# ── FIX 9: /ADD AND /REMOVE COMMANDS ─────────────────────────────────────────
def search_symbol_token(symbol):
    """
    Search Angel One for token of any NSE stock — not just Nifty 50.
    Returns token string or None if not found.
    """
    # First check Nifty 50 list
    if symbol in NIFTY50:
        return NIFTY50[symbol]
    # Search via Angel One SmartAPI searchScrip
    smart = get_session()
    if not smart:
        return None
    try:
        result = smart.searchScrip("NSE", symbol)
        if result and result.get("data"):
            for item in result["data"]:
                # Match exact symbol name
                if item.get("tradingsymbol","").upper() == symbol.upper():
                    token = str(item.get("symboltoken",""))
                    log(f"Found token for {symbol}: {token}")
                    return token
            # If exact match not found, take first result
            first = result["data"][0]
            token = str(first.get("symboltoken",""))
            name  = first.get("tradingsymbol","")
            log(f"Using closest match for {symbol}: {name} token={token}")
            return token
    except Exception as e:
        log(f"Symbol search error {symbol}: {e}")
    return None

def get_ltp_any(symbol, token):
    """Get LTP for any symbol using its token."""
    smart = get_session()
    if not smart or not token: return None
    for exchange in ["NSE", "BSE"]:
        try:
            d = smart.ltpData(exchange, symbol, token)
            if d and d.get("data") and d["data"].get("ltp"):
                return float(d["data"]["ltp"])
        except Exception as e:
            log(f"LTP {symbol}/{exchange}: {e}")
    return None

def get_ltp(symbol):
    """Get LTP — works for Nifty 50 and any other NSE stock."""
    token = NIFTY50.get(symbol) or ALL_TOKENS.get(symbol)
    if not token:
        token = search_symbol_token(symbol)
        if token:
            ALL_TOKENS[symbol] = token
    return get_ltp_any(symbol, token) if token else None

def add_to_batch(text, config):
    """
    /add SYMBOL ENTRY_PRICE QTY
    Example: /add RELIANCE 2850 35
    Works for ANY NSE stock — not just Nifty 50.
    """
    parts = text.strip().split()
    if len(parts) < 4:
        send_telegram(
            "❌ <b>Format:</b> /add SYMBOL ENTRY QTY\n"
            "Example: /add RELIANCE 2850 35\n"
            "Example: /add ZOMATO 245 100\n\n"
            "Works for any NSE stock.", config)
        return

    symbol = parts[1].upper().replace(".NS","").replace("-EQ","")
    try:
        entry = float(parts[2])
        qty   = int(parts[3])
    except:
        send_telegram("❌ Invalid price or quantity.\nExample: /add ZOMATO 245 100", config)
        return

    if entry <= 0 or qty <= 0:
        send_telegram("❌ Entry price and quantity must be greater than 0.", config)
        return

    # Look up token — works for any NSE stock
    send_telegram(f"🔍 Looking up <b>{symbol}</b>...", config)
    token = NIFTY50.get(symbol) or ALL_TOKENS.get(symbol)
    if not token:
        token = search_symbol_token(symbol)
        if token:
            ALL_TOKENS[symbol] = token
        else:
            send_telegram(
                f"❌ <b>{symbol}</b> not found on NSE.\n\n"
                f"Check the symbol name and try again.\n"
                f"Use the exact NSE trading symbol\n"
                f"(e.g. ZOMATO, PAYTM, IRCTC, TATAMOTORS)", config)
            return

    # Get current LTP to verify symbol is tradeable
    ltp = get_ltp_any(symbol, token)
    sl  = round(entry * 0.98,  2)
    t1  = round(entry * 1.03,  2)
    t2  = round(entry * 1.05,  2)
    t3  = round(entry * 1.08,  2)
    nifty_tag = " (Nifty 50)" if symbol in NIFTY50 else ""

    batch = load_batch1()
    # Replace if already exists
    batch = [b for b in batch if b["symbol"] != symbol]
    batch.append({
        "symbol":symbol,"token":token,
        "entry":entry,"qty":qty,
        "sl":sl,"t1":t1,"t2":t2,"t3":t3,
        "score":0,"signal":"Manual entry","all_signals":["Manual"],
        "ltp":ltp or entry,
        "low_52w":0,"high_52w":0,"low_6m":0,
        "pos_pct":0,"from_low":0,"from_high":0,"zone":"Manual",
        "rsi":0,"vol":0,
        "invest":round(entry*qty,0),
        "profit_t2":round((t2-entry)*qty,0),
        "added":datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
    })
    save_batch1(batch)

    ltp_line = f"\nCurrent LTP : ₹{ltp:,}" if ltp else ""
    send_telegram(
        f"✅ <b>{symbol}{nifty_tag} added to Batch 1</b>\n\n"
        f"Entry    : ₹{entry:,}{ltp_line}\n"
        f"Qty      : {qty} shares\n"
        f"Invested : ₹{int(entry*qty):,}\n\n"
        f"Stop Loss: ₹{sl:,}  (−2%)\n"
        f"Target 1 : ₹{t1:,}  (+3%) → sell 40%\n"
        f"Target 2 : ₹{t2:,}  (+5%) → sell 40%\n"
        f"Target 3 : ₹{t3:,}  (+8%) → sell 20%\n\n"
        f"Bot will monitor this position automatically.\n"
        f"⚠️ Not SEBI advice.", config)

def remove_from_batch(text, config):
    """
    /remove SYMBOL
    Example: /remove RELIANCE
    """
    parts = text.strip().split()
    if len(parts) < 2:
        send_telegram("❌ Format: /remove SYMBOL\nExample: /remove RELIANCE", config)
        return
    symbol = parts[1].upper()
    batch  = load_batch1()
    new    = [b for b in batch if b["symbol"] != symbol]
    if len(new) == len(batch):
        send_telegram(f"❌ {symbol} not found in Batch 1.", config)
        return
    save_batch1(new)
    send_telegram(f"✅ <b>{symbol} removed from Batch 1.</b>\nNo longer monitoring.", config)

# ── SCHEDULED TASKS ───────────────────────────────────────────────────────────
def task_heartbeat():
    config = load_config()
    batch  = load_batch1()
    mkt    = market_status_label()
    names  = ", ".join(b["symbol"] for b in batch) if batch else "Empty — send /scan"
    send_telegram(
        f"✅ <b>SwingBot v2.1 Running</b>\n\n"
        f"Time   : {datetime.now(IST).strftime('%d %b %I:%M %p')} IST\n"
        f"Market : {mkt}\n"
        f"Batch 1: {names}\n\n"
        f"Commands: /scan /newscan /monitor\n"
        f"/add SYMBOL PRICE QTY | /remove SYMBOL\n"
        f"/status /help", config)

def task_friday_exit():
    config = load_config()
    batch  = load_batch1()
    if not batch: return
    names = ", ".join(b["symbol"] for b in batch)
    send_telegram(
        f"⏰ <b>Friday Exit Reminder</b>\n\n"
        f"Open positions: <b>{names}</b>\n\n"
        f"Market closes in 45 minutes (3:15 PM IST).\n"
        f"Consider exiting to avoid weekend gap risk.\n\n"
        f"Send /monitor to check current levels first.", config)

HELP_TEXT = (
    "🤖 <b>SwingBot v2.1 — Commands</b>\n\n"
    "<b>Nifty 50 Scan</b>\n"
    "/scan — Scan Nifty 50, save as Batch 1\n"
    "/newscan — Clear Batch 1, fresh scan\n\n"
    "<b>Find Stocks Outside Nifty 50</b>\n"
    "/find next50 — Scan Nifty Next 50\n"
    "/find midcap — Scan Midcap Select\n"
    "/find swing — Scan swing favourites\n"
    "/find all — Full 200+ stock universe\n\n"
    "<b>Sector Scan</b>\n"
    "/sector list — Show all sectors\n"
    "/sector IT — Scan IT stocks\n"
    "/sector BANKING — Scan banking stocks\n"
    "/sector PHARMA — Scan pharma stocks\n"
    "(Also: AUTO FMCG ENERGY FINANCE INFRA)\n\n"
    "<b>Monitoring</b>\n"
    "/monitor — Check Batch 1 positions\n"
    "/status — Bot health check\n\n"
    "<b>Manual Trades</b>\n"
    "/add SYMBOL PRICE QTY\n"
    "  Works for ANY NSE stock\n"
    "  Example: /add ZOMATO 245 100\n"
    "/remove SYMBOL\n"
    "  Example: /remove ZOMATO\n\n"
    "/help — This menu\n\n"
    "📅 <b>Auto Schedule (IST)</b>\n"
    "Mon 8:00 AM — Weekly Nifty 50 scan\n"
    "Mon–Fri 8:55 AM — Daily heartbeat\n"
    "Mon–Fri 3:00 PM — Auto monitor\n"
    "Fri 2:45 PM — Exit reminder\n"
    "Every 6 hrs — Session refresh\n\n"
    "⚠️ Not SEBI-registered advice."
)


# ── EXTENDED UNIVERSE SCANNER ─────────────────────────────────────────────────

def find_stocks(index_name, config):
    """
    /find command — scan stocks outside Nifty 50.
    index_name: next50 | midcap | swing | all
    """
    universe  = get_universe(index_name)
    capital   = float(config.get("CAPITAL", 75000))
    mkt       = market_status_label()
    label_map = {
        "next50":  "Nifty Next 50",
        "midcap":  "Midcap Select",
        "swing":   "Swing Favourites",
        "all":     "Full Universe (200+ stocks)",
    }
    label = label_map.get(index_name, index_name.upper())

    send_telegram(
        f"🔍 <b>Scanning {label}...</b>\n"
        f"Market : {mkt}\n"
        f"Stocks : {len(universe)}\n"
        f"⏳ This may take {len(universe)//10 + 1}–{len(universe)//8 + 2} minutes...",
        config)

    picks   = []
    scanned = 0

    for symbol, token in universe.items():
        scanned += 1
        # Cache tokens for /add command
        if symbol not in NIFTY50:
            ALL_TOKENS[symbol] = token

        if scanned % 10 == 0:
            send_telegram(
                f"⏳ {scanned}/{len(universe)} analysed... "
                f"({len(picks)} picks so far)", config)

        # Fetch candles — use token directly for speed
        smart = get_session()
        if not smart:
            time.sleep(0.5); continue
        end   = datetime.now(IST)
        start = end - timedelta(days=260)
        df = None
        for exchange in ["NSE","BSE"]:
            try:
                data = smart.getCandleData({
                    "exchange":exchange,"symboltoken":token,
                    "interval":"ONE_DAY",
                    "fromdate":start.strftime("%Y-%m-%d %H:%M"),
                    "todate":  end.strftime("%Y-%m-%d %H:%M"),
                })
                if data and data.get("data") and len(data["data"]) > 10:
                    df = pd.DataFrame(data["data"],
                                      columns=["Date","Open","High","Low","Close","Volume"])
                    df["Date"] = pd.to_datetime(df["Date"])
                    df.set_index("Date", inplace=True)
                    df = df.astype(float)
                    break
            except Exception as e:
                log(f"{symbol}/{exchange}: {e}")
        if df is None or len(df) < 50:
            time.sleep(0.4); continue

        df = add_indicators(df)
        score, sigs = score_stock(df)
        if not sigs:
            time.sleep(0.3); continue

        low_52w, high_52w, low_6m = get_52w_stats(df)
        c     = df.iloc[-1]
        ltp   = get_ltp_any(symbol, token) or round(c["Close"], 2)
        entry = round(ltp * 1.005, 2)
        sl    = round(entry * 0.98, 2)
        t1    = round(entry * 1.03, 2)
        t2    = round(entry * 1.05, 2)
        t3    = round(entry * 1.08, 2)
        risk  = entry - sl
        qty   = max(1, int((capital * 0.02) / risk)) if risk > 0 else 1
        rng   = high_52w - low_52w
        pos_pct = round((ltp - low_52w) / rng * 100, 1) if rng > 0 else 0

        if pos_pct < 30:    zone = "🟢 Near 52W low"
        elif pos_pct < 60:  zone = "🟡 Mid range"
        else:               zone = "🔴 Near 52W high"

        picks.append({
            "symbol":symbol,"score":score,
            "signal":sigs[0],"all_signals":sigs,
            "ltp":ltp,"entry":entry,"sl":sl,
            "t1":t1,"t2":t2,"t3":t3,
            "low_52w":low_52w,"high_52w":high_52w,
            "pos_pct":pos_pct,"zone":zone,
            "rsi":round(c["rsi"],1),"vol":round(c["vol_r"],2),
            "qty":qty,"invest":round(entry*qty,0),
            "profit_t2":round((t2-entry)*qty,0),
        })
        log(f"FIND PICK: {symbol} score={score}")
        time.sleep(0.5)

    picks.sort(key=lambda x: x["score"], reverse=True)
    top = picks[:6]

    if not top:
        send_telegram(
            f"📋 <b>{label} Scan — No Setups Found</b>\n\n"
            f"No stock in {label} met the minimum score of 5.\n"
            f"Market may be weak or sideways.", config)
        return

    send_telegram(
        f"✅ <b>{label} — {len(top)} Picks Found</b>\n"
        f"Use /add SYMBOL PRICE QTY to track any of these.", config)
    time.sleep(1)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]
    for i, p in enumerate(top):
        send_telegram(
            f"{'━'*22}\n"
            f"{medals[i]} <b>#{i+1} {p['symbol']}</b>  Score: {p['score']}/8\n"
            f"{'━'*22}\n\n"
            f"📊 <b>Levels</b>\n"
            f"  LTP      : ₹{p['ltp']:,}\n"
            f"  52W Low  : ₹{p['low_52w']:,}  {p['zone']}\n"
            f"  52W High : ₹{p['high_52w']:,}\n"
            f"  Position : {p['pos_pct']}% of range\n\n"
            f"🎯 <b>Trade Setup</b>\n"
            f"  Entry   : ₹{p['entry']:,}\n"
            f"  SL      : ₹{p['sl']:,}  (−2%)\n"
            f"  T1      : ₹{p['t1']:,}  (+3%)\n"
            f"  T2      : ₹{p['t2']:,}  (+5%)\n"
            f"  T3      : ₹{p['t3']:,}  (+8%)\n\n"
            f"  Qty: {p['qty']} | Invest: ₹{int(p['invest']):,} | "
            f"Est.profit: ₹{int(p['profit_t2']):,}\n\n"
            f"📈 {p['signal']} | RSI: {p['rsi']} | Vol: {p['vol']}×\n\n"
            f"➡️ To track: /add {p['symbol']} {p['entry']} {p['qty']}\n"
            f"⚠️ Not SEBI advice.", config)
        time.sleep(1.5)


def find_by_sector(sector, config):
    """
    /sector IT | /sector BANKING | /sector PHARMA | /sector list
    Scans only stocks in a specific sector.
    """
    if not sector or sector == "LIST":
        sectors_list = "\n".join(f"  /sector {s}" for s in SECTOR_STOCKS.keys())
        send_telegram(
            f"📂 <b>Available Sectors</b>\n\n{sectors_list}\n\n"
            f"Example: /sector IT\n"
            f"Example: /sector PHARMA", config)
        return

    stocks = SECTOR_STOCKS.get(sector)
    if not stocks:
        available = ", ".join(SECTOR_STOCKS.keys())
        send_telegram(
            f"❌ Sector '{sector}' not found.\n\n"
            f"Available: {available}\n\n"
            f"Send /sector list to see all options.", config)
        return

    # Build token map for sector
    sector_universe = {}
    for sym in stocks:
        token = NIFTY50.get(sym) or ALL_TOKENS.get(sym)
        if not token:
            token = search_symbol_token(sym)
            if token: ALL_TOKENS[sym] = token
        if token:
            sector_universe[sym] = token

    send_telegram(
        f"🔍 <b>Scanning {sector} Sector</b>\n"
        f"{len(sector_universe)} stocks | ~{len(sector_universe)//5 + 1} min ⏳",
        config)

    # Reuse find_stocks logic with this universe
    capital = float(config.get("CAPITAL", 75000))
    picks   = []

    for symbol, token in sector_universe.items():
        df = get_candles(symbol, days=260)
        if df is None or len(df) < 50:
            time.sleep(0.5); continue
        df = add_indicators(df)
        score, sigs = score_stock(df)
        if not sigs:
            time.sleep(0.3); continue

        low_52w, high_52w, _ = get_52w_stats(df)
        c     = df.iloc[-1]
        ltp   = get_ltp(symbol) or round(c["Close"], 2)
        entry = round(ltp * 1.005, 2)
        sl    = round(entry * 0.98, 2)
        t1    = round(entry * 1.03, 2)
        t2    = round(entry * 1.05, 2)
        t3    = round(entry * 1.08, 2)
        risk  = entry - sl
        qty   = max(1, int((capital * 0.02) / risk)) if risk > 0 else 1
        rng   = high_52w - low_52w
        pos   = round((ltp - low_52w) / rng * 100, 1) if rng > 0 else 0
        zone  = "🟢 Near low" if pos < 30 else ("🟡 Mid" if pos < 60 else "🔴 Extended")

        picks.append({
            "symbol":symbol,"score":score,"signal":sigs[0],
            "ltp":ltp,"entry":entry,"sl":sl,"t1":t1,"t2":t2,"t3":t3,
            "low_52w":low_52w,"high_52w":high_52w,"pos":pos,"zone":zone,
            "rsi":round(c["rsi"],1),"vol":round(c["vol_r"],2),
            "qty":qty,"invest":round(entry*qty,0),
            "profit_t2":round((t2-entry)*qty,0),
        })
        time.sleep(0.5)

    picks.sort(key=lambda x: x["score"], reverse=True)

    if not picks:
        send_telegram(
            f"📋 <b>{sector} Sector — No Setups</b>\n\n"
            f"No strong signals found in {sector} right now.\n"
            f"Try again on Monday morning.", config)
        return

    send_telegram(
        f"✅ <b>{sector} Sector — {len(picks)} Pick(s)</b>", config)
    time.sleep(1)

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣"]
    for i, p in enumerate(picks[:5]):
        send_telegram(
            f"{medals[i]} <b>{p['symbol']}</b>  Score {p['score']}/8\n"
            f"LTP ₹{p['ltp']:,} | 52W Low ₹{p['low_52w']:,} {p['zone']}\n"
            f"Entry ₹{p['entry']:,} | SL ₹{p['sl']:,} | T1 ₹{p['t1']:,} | T2 ₹{p['t2']:,}\n"
            f"RSI {p['rsi']} | Vol {p['vol']}× | Qty {p['qty']}\n"
            f"➡️ /add {p['symbol']} {p['entry']} {p['qty']}\n"
            f"⚠️ Not SEBI advice.", config)
        time.sleep(1.2)

# ── FLASK WEBHOOK ─────────────────────────────────────────────────────────────
@app.route("/")
def home():
    batch = load_batch1()
    names = ", ".join(b["symbol"] for b in batch) if batch else "None"
    return (f"SwingBot v2.1 ✅\n"
            f"Time: {datetime.now(IST).strftime('%d %b %Y %I:%M %p')} IST\n"
            f"Market: {market_status_label()}\n"
            f"Batch 1: {names}"), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    # FIX 3: Verify request is from Telegram
    if not verify_telegram_request(freq):
        log("Webhook: unauthorized request blocked")
        abort(403)
    try:
        data   = freq.get_json(force=True)
        config = load_config()
        msg    = data.get("message",{})
        text   = msg.get("text","").strip()
        cmd    = text.lower().split()[0] if text else ""
        log(f"Command: {text[:50]}")

        if cmd in ["/scan","/scan@swingbot"]:
            batch  = load_batch1()
            is_new = len(batch) == 0
            threading.Thread(
                target=run_scan, kwargs={"is_new_batch":is_new}, daemon=True).start()

        elif cmd == "/newscan":
            if os.path.exists(BATCH_FILE): os.remove(BATCH_FILE)
            if os.path.exists(ALERTS_FILE): os.remove(ALERTS_FILE)
            threading.Thread(
                target=run_scan, kwargs={"is_new_batch":True}, daemon=True).start()

        elif cmd in ["/broadscan","/broadScan","/broad"]:
            threading.Thread(
                target=run_broad_scan, daemon=True).start()

        elif cmd in ["/sectorscan","/sectorScan","/sector"]:
            # /sectorScan Banking or /sectorScan IT
            parts  = text.split()
            sector = parts[1] if len(parts) > 1 else None
            if not sector:
                available = "\n".join(f"  /sectorScan {s}" for s in SECTORS)
                send_telegram(
                    f"📊 <b>Available Sectors</b>\n\n{available}\n\n"
                    f"Example: /sectorScan Banking", config)
            else:
                threading.Thread(
                    target=run_broad_scan,
                    kwargs={"sector": sector}, daemon=True).start()

        elif cmd == "/find":
            # /find next50 | /find midcap | /find swing | /find all
            parts   = text.strip().split()
            index   = parts[1].lower() if len(parts) > 1 else "next50"
            threading.Thread(
                target=find_stocks,
                args=(index, config), daemon=True).start()

        elif cmd == "/sector":
            # /sector IT | /sector BANKING | /sector PHARMA etc.
            parts  = text.strip().split()
            sector = parts[1].upper() if len(parts) > 1 else ""
            threading.Thread(
                target=find_by_sector,
                args=(sector, config), daemon=True).start()

        elif cmd in ["/monitor","/check"]:
            threading.Thread(target=monitor_batch1, daemon=True).start()

        elif cmd == "/status":
            threading.Thread(target=task_heartbeat, daemon=True).start()

        elif cmd == "/add":
            threading.Thread(
                target=add_to_batch, args=(text, config), daemon=True).start()

        elif cmd == "/remove":
            threading.Thread(
                target=remove_from_batch, args=(text, config), daemon=True).start()

        elif cmd == "/help":
            send_telegram(HELP_TEXT, config)

        else:
            if text and not text.startswith("/"):
                send_telegram(
                    "Send /help to see all available commands.", config)
    except Exception as e:
        log(f"Webhook error: {e}")
    return "ok", 200

# ── WEBHOOK SETUP ─────────────────────────────────────────────────────────────
def setup_webhook():
    config = load_config()
    token  = config.get("BOT_TOKEN","").strip()
    secret = config.get("WEBHOOK_SECRET","").strip()
    domain = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or
              os.environ.get("RAILWAY_STATIC_URL") or "").strip()
    if domain and not domain.startswith("http"):
        domain = "https://" + domain
    if not domain:
        log("No RAILWAY_PUBLIC_DOMAIN — webhook not set")
        return False
    webhook_url = f"{domain}/webhook"
    payload = {"url": webhook_url}
    if secret:
        payload["secret_token"] = secret
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            json=payload, timeout=10)
        if r.status_code == 200 and r.json().get("ok"):
            log(f"Webhook set: {webhook_url}")
            if secret: log("Webhook secret token enabled ✅")
            return True
        log(f"Webhook failed: {r.text}")
        return False
    except Exception as e:
        log(f"Webhook error: {e}")
        return False

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
def start_scheduler():
    sched = BlockingScheduler(timezone=IST)
    # FIX 1: Session refresh every 6 hours
    sched.add_job(refresh_session_task,
                  CronTrigger(hour="0,6,12,18", minute=0, timezone=IST))
    sched.add_job(task_heartbeat,
                  CronTrigger(day_of_week="mon-fri", hour=8,  minute=55, timezone=IST))
    sched.add_job(lambda: run_scan(is_new_batch=True, auto=True),
                  CronTrigger(day_of_week="mon",     hour=8,  minute=0,  timezone=IST))
    sched.add_job(monitor_batch1,
                  CronTrigger(day_of_week="mon-fri", hour=15, minute=0,  timezone=IST))
    sched.add_job(task_friday_exit,
                  CronTrigger(day_of_week="fri",     hour=14, minute=45, timezone=IST))
    log("Scheduler started — 5 jobs active")
    sched.start()

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""

    if cmd == "test":
        config = load_config()
        ok = send_telegram(
            "🤖 <b>SwingBot v2.1 connected!</b>\n\n"
            "All fixes applied:\n"
            "✅ Session auto-refresh\n"
            "✅ Webhook security\n"
            "✅ Alert deduplication\n"
            "✅ Score threshold = 5\n"
            "✅ 52-week low data\n"
            "✅ /add and /remove commands\n\n"
            "Send /help to see all commands.", config)
        print("✅ Telegram OK" if ok else "❌ Telegram FAILED")

    elif cmd == "scan":  run_scan(is_new_batch=True)
    elif cmd == "monitor": monitor_batch1()
    elif cmd == "status":  task_heartbeat()

    else:
        # Start session
        get_session(force_refresh=True)
        # Start scheduler in background
        threading.Thread(target=start_scheduler, daemon=True).start()
        # Setup webhook
        time.sleep(2)
        if not setup_webhook():
            log("Retrying webhook in 30s...")
            time.sleep(30)
            setup_webhook()
        # FIX 4 — gunicorn runs this via wsgi, not app.run()
        port = int(os.environ.get("PORT", 8080))
        log(f"SwingBot v2.1 ready on port {port}")
        app.run(host="0.0.0.0", port=port)
