import os
import json
import sqlite3
import logging
import logging.handlers
import hashlib
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, jsonify, render_template_string, request
from apscheduler.schedulers.background import BackgroundScheduler
from openai import OpenAI
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────
load_dotenv()

# ── Logging setup: console + rotating file ──
LOG_FILE    = "app.log"
LOG_MAX_MB  = 5
LOG_BACKUPS = 3

_fmt     = logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s", "%Y-%m-%d %H:%M:%S")
_console = logging.StreamHandler()
_console.setFormatter(_fmt)
_file    = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_MB * 1024 * 1024, backupCount=LOG_BACKUPS, encoding="utf-8"
)
_file.setFormatter(_fmt)

logging.basicConfig(level=logging.INFO, handlers=[_console, _file])
log = logging.getLogger(__name__)
log.info("══ NEXUS starting up ══")

app = Flask(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    log.warning("OPENAI_API_KEY not set")

openai_client = OpenAI(api_key=OPENAI_API_KEY)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")
DAILY_LIMIT      = 10   # Only 10 BEST signals per day — quality over quantity
FETCH_INTERVAL   = 30
BATCH_SIZE       = 12
REQUEST_TIMEOUT  = 12
DEDUP_WINDOW_H   = 24
MAX_PROMPT_CHARS = 400

# API Keys
NEWSAPI_KEY    = os.environ.get("NEWSAPI_KEY", "")
NEWSAPI_URL    = "https://newsapi.org/v2/everything"
REDDIT_CLIENT       = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_SECRET       = os.environ.get("REDDIT_SECRET", "")
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT       = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_NEWS_CHAT  = os.environ.get("TELEGRAM_NEWS_CHAT_ID", "@cySignals_Official")
# News channel bot (separate from signal bot)
TELEGRAM_NEWS_TOKEN = os.environ.get("TELEGRAM_NEWS_TOKEN", "8759257766:AAFBVQyEj7PPIFG3N4BO89xq9YgLejxGpsE")
CRYPTOPANIC_API_KEY = os.environ.get("CRYPTOPANIC_API_KEY", "")

# RSS Sources — fastest crypto news feeds
RSS_SOURCES = [
    {"name": "Cointelegraph",  "url": "https://cointelegraph.com/rss",                         "limit": BATCH_SIZE},
    {"name": "Bitcoin.com",    "url": "https://news.bitcoin.com/feed/",                        "limit": BATCH_SIZE},
    {"name": "The Block",      "url": "https://www.theblock.co/rss.xml",                       "limit": BATCH_SIZE},
    {"name": "Blockworks",     "url": "https://blockworks.co/feed",                            "limit": BATCH_SIZE},
    {"name": "Decrypt",        "url": "https://decrypt.co/feed",                               "limit": BATCH_SIZE},
    {"name": "CoinDesk",       "url": "https://feeds.feedburner.com/CoinDesk",                 "limit": BATCH_SIZE},
    {"name": "BeInCrypto",     "url": "https://beincrypto.com/feed/",                         "limit": BATCH_SIZE},
    {"name": "CryptoSlate",    "url": "https://cryptoslate.com/feed/",                        "limit": BATCH_SIZE},
    {"name": "U.Today",        "url": "https://u.today/rss",                                  "limit": BATCH_SIZE},
    {"name": "CryptoBriefing", "url": "https://cryptobriefing.com/feed/",                     "limit": BATCH_SIZE},
]

NOISE_KEYWORDS = [
    "sponsored", "advertisement", "press release", "pr:",
    "giveaway", "airdrop scam", "quiz", "survey",
    "follow us", "join our", "subscribe", "learn more",
    "weekly recap", "morning brief", "daily roundup",
]

def is_noise(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in NOISE_KEYWORDS)

# ─────────────────────────────────────────────
# Fingerprint
# ─────────────────────────────────────────────
def title_fingerprint(title: str) -> str:
    import re
    STOPWORDS = {
        "the","a","an","in","on","of","to","for","is","are","was","with","by",
        "at","from","as","it","its","be","has","have","that","this","will",
        "how","why","what","bitcoin","crypto","cryptocurrency",
    }
    cleaned = re.sub(r"[^a-z0-9 ]", "", title.lower())
    words   = [w for w in cleaned.split() if w not in STOPWORDS][:8]
    return hashlib.sha256(" ".join(words).encode()).hexdigest()[:12]

_seen_this_sweep: set[str] = set()

def _sweep_seen(fp: str) -> bool:
    if fp in _seen_this_sweep:
        return True
    _seen_this_sweep.add(fp)
    return False

# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol       TEXT NOT NULL,
                news_title   TEXT NOT NULL,
                sentiment    TEXT NOT NULL,
                signal       TEXT NOT NULL,
                direction    TEXT NOT NULL DEFAULT 'המתן',
                reason       TEXT NOT NULL,
                entry        TEXT NOT NULL DEFAULT 'NA',
                stop_loss    TEXT NOT NULL DEFAULT 'NA',
                target1      TEXT NOT NULL DEFAULT 'NA',
                target2      TEXT NOT NULL DEFAULT 'NA',
                target3      TEXT NOT NULL DEFAULT 'NA',
                leverage     TEXT NOT NULL DEFAULT 'x10',
                target_price TEXT NOT NULL DEFAULT 'NA',
                source       TEXT NOT NULL DEFAULT 'Unknown',
                fingerprint  TEXT,
                timestamp    TEXT NOT NULL,
                result       TEXT DEFAULT 'OPEN',
                result_price TEXT DEFAULT 'NA',
                result_pnl   TEXT DEFAULT 'NA',
                result_time  TEXT DEFAULT 'NA'
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON signals (timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fp ON signals (fingerprint)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_title ON signals (news_title)")
        conn.commit()

    for col, defn in [
        ("source","TEXT NOT NULL DEFAULT 'Unknown'"),
        ("fingerprint","TEXT"),
        ("direction","TEXT NOT NULL DEFAULT 'המתן'"),
        ("entry","TEXT NOT NULL DEFAULT 'NA'"),
        ("stop_loss","TEXT NOT NULL DEFAULT 'NA'"),
        ("target1","TEXT NOT NULL DEFAULT 'NA'"),
        ("target2","TEXT NOT NULL DEFAULT 'NA'"),
        ("target3","TEXT NOT NULL DEFAULT 'NA'"),
        ("leverage","TEXT NOT NULL DEFAULT 'x10'"),
        ("result","TEXT DEFAULT 'OPEN'"),
        ("result_price","TEXT DEFAULT 'NA'"),
        ("result_pnl","TEXT DEFAULT 'NA'"),
        ("result_time","TEXT DEFAULT 'NA'"),
        ("news_title_he","TEXT DEFAULT ''"),
        ("reason_he","TEXT DEFAULT ''"),
        ("score","INTEGER DEFAULT 50"),
        ("position_size","TEXT DEFAULT '3'"),
        ("rr_ratio","TEXT DEFAULT '0'"),
        ("liq_price","TEXT DEFAULT 'NA'"),
        ("risk_warning","TEXT DEFAULT ''"),
        ("confluence_count","INTEGER DEFAULT 0"),
    ]:
        try:
            with get_db() as conn:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {defn}")
                conn.commit()
                log.info("DB migration: added column '%s'", col)
        except sqlite3.OperationalError:
            pass

    log.info("DB ready → %s", DB_PATH)

def count_today_signals() -> int:
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM signals WHERE timestamp LIKE ?",
            (f"{today}%",)
        ).fetchone()
    return row["cnt"] if row else 0

def fingerprint_seen_in_db(fp: str) -> bool:
    cutoff = (datetime.now() - timedelta(hours=DEDUP_WINDOW_H)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM signals WHERE fingerprint = ? AND timestamp >= ? LIMIT 1",
            (fp, cutoff),
        ).fetchone()
    return row is not None

def get_cached_signal(fp: str) -> dict | None:
    """
    Cache lookup: if we already analysed a near-identical headline
    in the last 24 h, return the stored result instead of calling OpenAI again.
    Saves tokens and keeps latency low.
    """
    cutoff = (datetime.now() - timedelta(hours=DEDUP_WINDOW_H)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        row = conn.execute(
            """SELECT sentiment, signal, reason, target_price
               FROM signals
               WHERE fingerprint = ? AND timestamp >= ?
               LIMIT 1""",
            (fp, cutoff),
        ).fetchone()
    return dict(row) if row else None

def save_signal(symbol, news_title, sentiment, signal, direction, reason,
               entry, stop_loss, target1, target2, target3, leverage,
               target_price, source, fingerprint,
               news_title_he="", reason_he="", score=50,
               position_size="3", rr_ratio="0", liq_price="NA",
               risk_warning="", confluence_count=0) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO signals
                   (symbol,news_title,sentiment,signal,direction,reason,
                    entry,stop_loss,target1,target2,target3,leverage,
                    target_price,source,fingerprint,timestamp,
                    news_title_he,reason_he,score,
                    position_size,rr_ratio,liq_price,risk_warning,confluence_count)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, news_title, sentiment, signal, direction, reason,
                 entry, stop_loss, target1, target2, target3, leverage,
                 target_price, source, fingerprint, ts,
                 news_title_he, reason_he, score,
                 position_size, rr_ratio, liq_price, risk_warning, confluence_count),
            )
            conn.commit()
    except sqlite3.Error as e:
        log.error("DB save failed: %s", e)

def fetch_recent_signals(limit: int = 200) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]

# ─────────────────────────────────────────────
# Source: CryptoPanic
# ─────────────────────────────────────────────
def fetch_cryptopanic(max_items: int = BATCH_SIZE) -> list[dict]:
    log.info("[CryptoPanic] Fetching up to %d items…", max_items)
    try:
        resp = requests.get(
            CRYPTOPANIC_URL,
            params={"auth_token": CRYPTOPANIC_API_KEY, "filter": "hot", "public": "true"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])[:max_items]
        items = []
        for post in results:
            title      = (post.get("title") or "").strip()
            summary    = (post.get("metadata", {}) or {}).get("description", "") or ""
            currencies = post.get("currencies") or []
            symbols    = [c.get("code", "UNKNOWN") for c in currencies] or ["CRYPTO"]
            if title:
                items.append({"title": title, "summary": summary[:200], "symbols": symbols, "source": "CryptoPanic"})
        log.info("[CryptoPanic] ✓ %d items received", len(items))
        return items
    except requests.Timeout:
        log.warning("[CryptoPanic] Timeout — will retry next sweep")
    except requests.HTTPError as e:
        log.warning("[CryptoPanic] HTTP %s — will retry next sweep", e.response.status_code)
    except Exception as e:
        log.error("[CryptoPanic] Unexpected error: %s", e)
    return []

# ─────────────────────────────────────────────
# Source: RSS Feeds
# ─────────────────────────────────────────────
_RSS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/rss+xml,application/xml,text/xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache",
}

# Requests session with connection pooling for faster RSS
_http_session = requests.Session()
_http_session.headers.update(_RSS_HEADERS)

def _parse_rss(xml_text: str, source_name: str, limit: int) -> list[dict]:
    import re
    items: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
        tag  = root.tag.lower()
        if "feed" in tag:
            ns = "http://www.w3.org/2005/Atom"
            for entry in root.findall(f"{{{ns}}}entry")[:limit]:
                t_el = entry.find(f"{{{ns}}}title")
                s_el = entry.find(f"{{{ns}}}summary") or entry.find(f"{{{ns}}}content")
                title   = (t_el.text or "").strip() if t_el else ""
                summary = (s_el.text or "").strip() if s_el else ""
                if title:
                    items.append({"title": title, "summary": summary[:200], "symbols": ["CRYPTO"], "source": source_name})
        else:
            for item in root.iter("item"):
                t_el = item.find("title")
                d_el = item.find("description")
                title   = (t_el.text or "").strip() if t_el else ""
                summary = re.sub(r"<[^>]+>", "", (d_el.text or "") if d_el else "")[:200]
                if title:
                    items.append({"title": title, "summary": summary, "symbols": ["CRYPTO"], "source": source_name})
                if len(items) >= limit:
                    break
    except ET.ParseError as e:
        log.error("[RSS:%s] XML parse error: %s", source_name, e)
    return items

def fetch_rss(source: dict) -> list[dict]:
    name  = source["name"]
    url   = source["url"]
    limit = source.get("limit", BATCH_SIZE)
    log.info("[RSS:%s] Fetching up to %d items…", name, limit)
    try:
        resp = requests.get(url, headers=_RSS_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        items = _parse_rss(resp.text, name, limit)
        log.info("[RSS:%s] ✓ %d items received", name, len(items))
        return items
    except requests.Timeout:
        log.warning("[RSS:%s] Timeout — will retry next sweep", name)
    except requests.HTTPError as e:
        log.warning("[RSS:%s] HTTP %s — will retry next sweep", name, e.response.status_code)
    except Exception as e:
        log.error("[RSS:%s] Unexpected error: %s", name, e)
    return []

# ─────────────────────────────────────────────
# Aggregator
# ─────────────────────────────────────────────
def fetch_reddit_crypto(max_items: int = BATCH_SIZE) -> list[dict]:
    """
    Fetch hot posts from crypto subreddits using Reddit JSON API.
    No API key needed — uses public JSON endpoint.
    Reddit often has news 10-20 min before mainstream sites.
    """
    subreddits = ["CryptoCurrency", "Bitcoin", "ethereum", "CryptoMarkets", "altcoin"]
    items = []
    headers = {"User-Agent": "CryptoSignalBot/2.0"}
    for sub in subreddits[:3]:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/hot.json",
                params={"limit": 10},
                headers=headers,
                timeout=8,
            )
            if r.status_code == 200:
                posts = r.json().get("data",{}).get("children",[])
                for post in posts:
                    d = post.get("data",{})
                    title = (d.get("title") or "").strip()
                    score = d.get("score", 0)
                    # Only high-score posts (trending)
                    if title and score > 100 and not d.get("is_self", True):
                        items.append({
                            "title":   title,
                            "summary": (d.get("selftext","") or "")[:200],
                            "symbols": ["CRYPTO"],
                            "source":  f"Reddit/r/{sub}",
                        })
                        if len(items) >= max_items:
                            break
        except Exception as e:
            log.debug("Reddit %s error: %s", sub, e)
    log.info("[Reddit] %d posts fetched", len(items))
    return items[:max_items]


def fetch_cryptopanic_free(max_items: int = BATCH_SIZE) -> list[dict]:
    """
    CryptoPanic public feed — no API key needed for public posts.
    Extremely fast crypto-specific news aggregator.
    """
    try:
        # Use CryptoPanic's public RSS which doesn't need auth
        r = requests.get(
            "https://cryptopanic.com/news/rss/",
            headers={"User-Agent": "Mozilla/5.0 (compatible; CryptoBot/2.0)"},
            timeout=10,
        )
        if r.status_code == 200:
            items = _parse_rss(r.text, "CryptoPanic", max_items)
            log.info("[CryptoPanic-RSS] %d items", len(items))
            return items
        # Fallback to API if key exists
        if CRYPTOPANIC_API_KEY:
            r2 = requests.get(
                "https://cryptopanic.com/api/v1/posts/",
                params={"auth_token": CRYPTOPANIC_API_KEY, "filter": "hot", "public": "true"},
                timeout=10,
            )
            if r2.status_code == 200:
                results = r2.json().get("results", [])[:max_items]
                items = []
                for post in results:
                    title = (post.get("title") or "").strip()
                    if title:
                        items.append({"title": title, "summary": "", "symbols": ["CRYPTO"], "source": "CryptoPanic"})
                return items
    except Exception as e:
        log.warning("[CryptoPanic] error: %s", e)
    return []


def fetch_github_crypto_trending() -> list[dict]:
    """
    GitHub trending crypto repos — early signal for new projects
    before they hit mainstream news.
    """
    try:
        r = requests.get(
            "https://api.github.com/search/repositories",
            params={
                "q": "crypto OR bitcoin OR ethereum OR defi OR web3 created:>2024-01-01",
                "sort": "stars", "order": "desc", "per_page": 10,
            },
            headers={"User-Agent": "CryptoSignalBot/2.0"},
            timeout=8,
        )
        if r.status_code == 200:
            repos = r.json().get("items", [])
            items = []
            for repo in repos[:5]:
                name = repo.get("full_name","")
                desc = repo.get("description","") or ""
                stars = repo.get("stargazers_count", 0)
                if stars > 1000:
                    items.append({
                        "title":   f"Trending crypto project: {name} ({stars:,} stars)",
                        "summary": desc[:200],
                        "symbols": ["CRYPTO"],
                        "source":  "GitHub Trending",
                    })
            return items
    except Exception as e:
        log.debug("GitHub trending error: %s", e)
    return []


def fetch_coingecko_trending() -> list[dict]:
    """
    CoinGecko trending coins — what people are searching for right now.
    Free API, no key needed. Very early signal.
    """
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=8,
        )
        if r.status_code == 200:
            coins = r.json().get("coins", [])
            items = []
            for coin in coins[:7]:
                item = coin.get("item", {})
                name   = item.get("name","")
                symbol = item.get("symbol","").upper()
                rank   = item.get("market_cap_rank","?")
                score  = item.get("score", 0)
                items.append({
                    "title":   f"{name} ({symbol}) trending on CoinGecko — rank #{rank} in searches",
                    "summary": f"Score: {score}. High search interest indicates upcoming price movement.",
                    "symbols": [symbol] if symbol else ["CRYPTO"],
                    "source":  "CoinGecko Trending",
                })
            log.info("[CoinGecko Trending] %d coins", len(items))
            return items
    except Exception as e:
        log.debug("CoinGecko trending error: %s", e)
    return []


def fetch_binance_announcements() -> list[dict]:
    """
    Binance official announcements RSS — listings cause 10-50% pumps.
    This is the MOST important signal source.
    """
    try:
        r = requests.get(
            "https://www.binance.com/en/support/announcement/rss",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r.status_code != 200:
            # Try alternate URL
            r = requests.get(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageNo=1&pageSize=10",
                timeout=8,
            )
            if r.status_code == 200:
                articles = r.json().get("data",{}).get("articles",[])
                items = []
                for a in articles[:5]:
                    title = a.get("title","").strip()
                    if title and any(kw in title.lower() for kw in
                                    ["listing","will list","adds","new token","launchpool","launchpad"]):
                        items.append({
                            "title":   f"BINANCE ANNOUNCEMENT: {title}",
                            "summary": "Official Binance listing — typically causes 20-100% price pump",
                            "symbols": ["CRYPTO"],
                            "source":  "Binance Official",
                        })
                if items:
                    log.info("[Binance Announcements] %d listing announcements", len(items))
                    return items
        else:
            parsed = _parse_rss(r.text, "Binance Official", 10)
            # Filter only listing announcements
            filtered = [p for p in parsed if any(kw in p["title"].lower()
                       for kw in ["listing","will list","adds","new token","launchpool"])]
            return filtered
    except Exception as e:
        log.debug("Binance announcements error: %s", e)
    return []


def fetch_whale_alert_rss() -> list[dict]:
    """
    Whale Alert public RSS — large on-chain transactions.
    Free, no API key needed. Very high signal value.
    """
    try:
        r = requests.get(
            "https://whale-alert.io/feed",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r.status_code == 200:
            items = _parse_rss(r.text, "Whale Alert", BATCH_SIZE)
            # Filter only large transactions
            filtered = [i for i in items if any(
                kw in i["title"].lower() for kw in
                ["million","billion","transferred","moved","whale","exchange"]
            )]
            log.info("[WhaleAlert] %d whale transactions", len(filtered))
            return filtered[:5]
    except Exception as e:
        log.debug("Whale Alert error: %s", e)
    # Fallback: use CryptoPanic whale filter
    try:
        r2 = requests.get(
            "https://cryptopanic.com/news/rss/?filter=hot",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r2.status_code == 200:
            return _parse_rss(r2.text, "CryptoPanic Hot", BATCH_SIZE)
    except:
        pass
    return []


def fetch_glassnode_free() -> dict:
    """
    Glassnode free on-chain metrics via their public API.
    No API key needed for basic metrics.
    """
    try:
        # BTC exchange net flow (free endpoint)
        r = requests.get(
            "https://api.glassnode.com/v1/metrics/transactions/count",
            params={"a": "BTC", "i": "24h"},
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            if data:
                latest = data[-1]
                count = latest.get("v", 0)
                return {"btc_tx_count_24h": f"{count:,.0f}", "source": "Glassnode"}
    except Exception as e:
        log.debug("Glassnode error: %s", e)
    return {}


def fetch_google_trends_crypto() -> dict:
    """
    Simulated Google Trends via SerpAPI free tier or public RSS.
    Checks if BTC/ETH search interest is rising.
    """
    try:
        # Use public trends RSS as proxy
        r = requests.get(
            "https://trends.google.com/trends/trendingsearches/daily/rss?geo=US",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r.status_code == 200:
            content = r.text.lower()
            crypto_terms = ["bitcoin","ethereum","crypto","btc","eth","solana","ripple"]
            trending = [t for t in crypto_terms if t in content]
            if trending:
                return {"trending": trending, "signal": "SOCIAL_SPIKE"}
    except Exception as e:
        log.debug("Google Trends error: %s", e)
    return {"trending": [], "signal": "NEUTRAL"}


def fetch_coinbase_blog() -> list[dict]:
    """Coinbase listing announcements — also cause significant pumps"""
    try:
        r = requests.get(
            "https://blog.coinbase.com/feed",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8,
        )
        if r.status_code == 200:
            items = _parse_rss(r.text, "Coinbase Blog", BATCH_SIZE)
            listing = [i for i in items if any(kw in i["title"].lower()
                      for kw in ["listing","adding","support","asset"])]
            return listing[:5]
    except Exception as e:
        log.debug("Coinbase blog error: %s", e)
    return []


def fetch_newsapi(max_items: int = BATCH_SIZE) -> list[dict]:
    """
    Fetch FRESH crypto news from NewsAPI.
    Uses 'from' parameter to get only last 30 minutes of news.
    Falls back to last 2 hours if nothing found.
    """
    if not NEWSAPI_KEY:
        log.warning("[NewsAPI] NEWSAPI_KEY not set")
        return []
    
    from datetime import timezone
    
    for minutes_back in [30, 120, 360]:
        try:
            from_time = (datetime.now(timezone.utc) - timedelta(minutes=minutes_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
            resp = requests.get(
                NEWSAPI_URL,
                params={
                    "q":        "bitcoin OR ethereum OR crypto OR altcoin OR defi OR blockchain OR binance OR coinbase",
                    "language": "en",
                    "sortBy":   "publishedAt",
                    "from":     from_time,
                    "pageSize": max_items * 2,
                    "apiKey":   NEWSAPI_KEY,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            articles = resp.json().get("articles", [])
            items = []
            for a in articles:
                title    = (a.get("title") or "").strip()
                desc     = (a.get("description") or "").strip()
                pub_at   = a.get("publishedAt","")
                src_name = (a.get("source", {}) or {}).get("name", "NewsAPI")
                if title and "[Removed]" not in title and "removed" not in title.lower():
                    items.append({
                        "title":      title,
                        "summary":    desc[:200],
                        "symbols":    ["CRYPTO"],
                        "source":     src_name,
                        "published":  pub_at,
                    })
            if items:
                log.info("[NewsAPI] %d fresh items (last %dm)", len(items), minutes_back)
                return items[:max_items]
        except Exception as e:
            log.error("[NewsAPI] Error: %s", e)
    return []

def fetch_all_sources(remaining_quota: int) -> list[dict]:
    n = len(RSS_SOURCES)
    per = max(4, min(BATCH_SIZE, remaining_quota // n))
    log.info("Aggregator: fetching %d sources × %d items each (quota left: %d)",
             n, per, remaining_quota)

    futures_map = {}
    with ThreadPoolExecutor(max_workers=20) as pool:
        # Tier 1: Real-time sources (fastest signals)
        futures_map[pool.submit(fetch_binance_announcements)]        = "Binance Official"
        futures_map[pool.submit(fetch_cryptopanic_free, per)]        = "CryptoPanic"
        futures_map[pool.submit(fetch_coingecko_trending)]           = "CoinGecko Trending"
        # Tier 2: News APIs
        futures_map[pool.submit(fetch_newsapi, per*2)]               = "NewsAPI"
        # Tier 3: RSS feeds
        for source in RSS_SOURCES:
            futures_map[pool.submit(fetch_rss, {**source, "limit": per})] = source["name"]
        # Tier 4: Social signals
        futures_map[pool.submit(fetch_reddit_crypto, per)]           = "Reddit"
        futures_map[pool.submit(fetch_coinbase_blog)]                = "Coinbase Blog"
        futures_map[pool.submit(fetch_whale_alert_rss)]              = "Whale Alert"

    per_source: dict[str, list] = {}
    for future, name in futures_map.items():
        try:
            per_source[name] = future.result()
        except Exception as e:
            log.error("[Aggregator:%s] Future raised: %s", name, e)
            per_source[name] = []

    from itertools import zip_longest
    interleaved = [item for group in zip_longest(*per_source.values())
                   for item in group if item is not None]

    active = sum(1 for v in per_source.values() if v)
    log.info("Aggregator: %d raw items from %d/%d sources", len(interleaved), active, n)
    return interleaved

# ─────────────────────────────────────────────
# OpenAI
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a senior crypto futures trader at a top hedge fund with 15 years experience.

You receive:
1. A news headline + context
2. Live Binance price
3. Fear & Greed Index (0=extreme fear, 100=extreme greed)
4. Funding Rate (positive=longs pay=market bullish, negative=shorts pay=market bearish)
5. RSI 14h (>70 overbought, <30 oversold)
6. 24h volume and price change
7. Open Interest

YOUR JOB: Combine ALL signals to make a high-conviction trading decision.

SIGNAL LOGIC:
BUY  = positive news + RSI<70 + funding not extremely positive + fear<greed
SELL = negative news + RSI>30 + funding not extremely negative + greed>fear
WAIT = conflicting signals or unclear direction
SKIP = no news alpha (opinions, promos, vague content)

CONTRARIAN SIGNALS:
- Extreme Fear (0-20) + positive news = STRONG BUY
- Extreme Greed (80-100) + negative news = STRONG SELL
- RSI>80 = caution on BUY signals
- RSI<20 = caution on SELL signals
- High positive funding = market overextended long = consider SELL
- High negative funding = market overextended short = consider BUY

OUTPUT: Valid JSON only, no markdown.

Actionable:
{"symbol":"BTC/USDT","signal":"BUY","direction":"ארוך","sentiment":"חיובי","entry":"107000","stop_loss":"100290","target1":"110210","target2":"113420","target3":"117700","leverage":"x10","reason":"סיבה בעברית — קצר וחד, כולל אינדיקטורים","target_price":"117700","conviction":"HIGH|MEDIUM|LOW"}

Noise: {"signal":"SKIP"}

PRICES: Use live price as entry base. Stop=6.25% against. T1=+3%, T2=+6%, T3=+10%.
CONVICTION: HIGH=strong news+indicators aligned, MEDIUM=mixed, LOW=weak signal."""

# ─────────────────────────────────────────────
# Market Intelligence Data Fetchers
# ─────────────────────────────────────────────

def fetch_fear_greed() -> dict:
    """Fear & Greed Index from alternative.me"""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
        if r.status_code == 200:
            d = r.json()["data"][0]
            return {"value": d["value"], "label": d["value_classification"]}
    except Exception as e:
        log.debug("Fear&Greed error: %s", e)
    return {"value": "NA", "label": "NA"}


def fetch_funding_rate(symbol: str) -> dict:
    """Binance perpetual funding rate"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": ticker}, timeout=5,
        )
        if r.status_code == 200:
            d = r.json()
            rate = float(d.get("lastFundingRate", 0)) * 100
            return {"rate": round(rate, 4), "mark_price": d.get("markPrice","NA")}
    except Exception as e:
        log.debug("Funding rate error %s: %s", symbol, e)
    return {"rate": "NA", "mark_price": "NA"}


def fetch_open_interest(symbol: str) -> str:
    """Binance open interest"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/openInterest",
            params={"symbol": ticker}, timeout=5,
        )
        if r.status_code == 200:
            oi = float(r.json().get("openInterest", 0))
            return f"{oi:,.0f}"
    except Exception as e:
        log.debug("OI error %s: %s", symbol, e)
    return "NA"


def fetch_volume_spike(symbol: str) -> dict:
    """Check if volume is spiking vs 24h average"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": ticker}, timeout=5,
        )
        if r.status_code == 200:
            d = r.json()
            vol   = float(d.get("volume", 0))
            price = float(d.get("lastPrice", 0))
            chg   = float(d.get("priceChangePercent", 0))
            return {
                "volume_usd": f"${vol * price:,.0f}",
                "change_24h": f"{chg:+.2f}%",
                "high_24h":   d.get("highPrice","NA"),
                "low_24h":    d.get("lowPrice","NA"),
            }
    except Exception as e:
        log.debug("Volume error %s: %s", symbol, e)
    return {"volume_usd": "NA", "change_24h": "NA", "high_24h": "NA", "low_24h": "NA"}


def fetch_rsi(symbol: str, period: int = 14) -> str:
    """Calculate RSI from Binance 1h candles"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": period + 1},
            timeout=5,
        )
        if r.status_code == 200:
            closes = [float(k[4]) for k in r.json()]
            gains, losses = [], []
            for i in range(1, len(closes)):
                diff = closes[i] - closes[i-1]
                gains.append(max(diff, 0))
                losses.append(max(-diff, 0))
            avg_gain = sum(gains) / period
            avg_loss = sum(losses) / period
            if avg_loss == 0:
                return "100"
            rs  = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            return str(round(rsi, 1))
    except Exception as e:
        log.debug("RSI error %s: %s", symbol, e)
    return "NA"


def fetch_macd(symbol: str) -> dict:
    """Calculate MACD (12,26,9) from 1h candles"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 50},
            timeout=5,
        )
        if r.status_code == 200:
            closes = [float(k[4]) for k in r.json()]
            def ema(data, period):
                k = 2/(period+1)
                e = [data[0]]
                for p in data[1:]:
                    e.append(p*k + e[-1]*(1-k))
                return e
            ema12 = ema(closes, 12)
            ema26 = ema(closes, 26)
            macd_line = [ema12[i]-ema26[i] for i in range(len(ema26))]
            signal = ema(macd_line, 9)
            hist = macd_line[-1] - signal[-1]
            trend = "BULLISH" if hist > 0 else "BEARISH"
            return {"macd": round(macd_line[-1],4), "signal": round(signal[-1],4),
                    "histogram": round(hist,4), "trend": trend}
    except Exception as e:
        log.debug("MACD error %s: %s", symbol, e)
    return {"macd":"NA","signal":"NA","histogram":"NA","trend":"NA"}


def fetch_bollinger_bands(symbol: str) -> dict:
    """Bollinger Bands (20,2) from 1h candles"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 20},
            timeout=5,
        )
        if r.status_code == 200:
            closes = [float(k[4]) for k in r.json()]
            sma = sum(closes) / 20
            std = (sum((c-sma)**2 for c in closes)/20)**0.5
            upper = round(sma + 2*std, 2)
            lower = round(sma - 2*std, 2)
            current = closes[-1]
            if current >= upper:
                position = "ABOVE_UPPER — overbought, expect reversal"
            elif current <= lower:
                position = "BELOW_LOWER — oversold, expect bounce"
            else:
                pct = (current-lower)/(upper-lower)*100
                position = f"MIDDLE ({pct:.0f}% of band)"
            return {"upper": upper, "lower": lower, "sma": round(sma,2), "position": position}
    except Exception as e:
        log.debug("BB error %s: %s", symbol, e)
    return {"upper":"NA","lower":"NA","sma":"NA","position":"NA"}


def fetch_ema_trend(symbol: str) -> dict:
    """EMA 50/200 trend direction"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "4h", "limit": 200},
            timeout=5,
        )
        if r.status_code == 200:
            closes = [float(k[4]) for k in r.json()]
            def ema(data, p):
                k=2/(p+1); e=[data[0]]
                for c in data[1:]: e.append(c*k+e[-1]*(1-k))
                return e[-1]
            e50  = round(ema(closes[-50:],  50),  2)
            e200 = round(ema(closes, 200), 2)
            price = closes[-1]
            trend = "STRONG UPTREND" if price>e50>e200 else                     "STRONG DOWNTREND" if price<e50<e200 else                     "MIXED"
            return {"ema50": e50, "ema200": e200, "trend": trend}
    except Exception as e:
        log.debug("EMA error %s: %s", symbol, e)
    return {"ema50":"NA","ema200":"NA","trend":"NA"}


def fetch_liquidations(symbol: str) -> dict:
    """Recent liquidation data from Binance"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/allForceOrders",
            params={"symbol": ticker, "limit": 50},
            timeout=5,
        )
        if r.status_code == 200:
            orders = r.json()
            long_liq  = sum(float(o["origQty"])*float(o["price"])
                           for o in orders if o.get("side")=="BUY")
            short_liq = sum(float(o["origQty"])*float(o["price"])
                           for o in orders if o.get("side")=="SELL")
            dominant = "LONG SQUEEZE" if long_liq > short_liq else "SHORT SQUEEZE"
            return {
                "long_liq":  f"${long_liq:,.0f}",
                "short_liq": f"${short_liq:,.0f}",
                "dominant":  dominant,
            }
    except Exception as e:
        log.debug("Liquidations error %s: %s", symbol, e)
    return {"long_liq":"NA","short_liq":"NA","dominant":"NA"}


def fetch_orderbook_imbalance(symbol: str) -> dict:
    """Order book bid/ask imbalance — shows buying vs selling pressure"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": ticker, "limit": 20},
            timeout=5,
        )
        if r.status_code == 200:
            d = r.json()
            bids = sum(float(b[1]) for b in d["bids"])
            asks = sum(float(a[1]) for a in d["asks"])
            ratio = bids / (bids + asks) * 100 if (bids+asks) > 0 else 50
            pressure = "STRONG BUY PRESSURE" if ratio > 65 else                        "STRONG SELL PRESSURE" if ratio < 35 else                        "BALANCED"
            return {"bid_ratio": f"{ratio:.1f}%", "pressure": pressure}
    except Exception as e:
        log.debug("Orderbook error %s: %s", symbol, e)
    return {"bid_ratio":"NA","pressure":"NA"}


def fetch_whale_trades(symbol: str) -> dict:
    """Recent large trades (whales) from Binance"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/trades",
            params={"symbol": ticker, "limit": 500},
            timeout=5,
        )
        if r.status_code == 200:
            trades = r.json()
            price = float(trades[-1]["price"]) if trades else 1
            threshold = price * 10  # trades worth 10+ BTC equiv
            big_buys  = sum(1 for t in trades if not t["isBuyerMaker"] and float(t["qty"]) > threshold/price)
            big_sells = sum(1 for t in trades if t["isBuyerMaker"]  and float(t["qty"]) > threshold/price)
            signal = "WHALE BUYING" if big_buys > big_sells*1.5 else                      "WHALE SELLING" if big_sells > big_buys*1.5 else "NEUTRAL"
            return {"big_buys": big_buys, "big_sells": big_sells, "signal": signal}
    except Exception as e:
        log.debug("Whale trades error %s: %s", symbol, e)
    return {"big_buys":"NA","big_sells":"NA","signal":"NA"}


def fetch_social_dominance(symbol: str) -> str:
    """LunarCrush-style social score via CoinGecko"""
    try:
        coin_map = {"BTC":"bitcoin","ETH":"ethereum","SOL":"solana",
                    "XRP":"ripple","BNB":"binancecoin","ADA":"cardano"}
        ticker = symbol.replace("/USDT","").replace("/","")
        cg_id  = coin_map.get(ticker)
        if not cg_id:
            return "NA"
        r = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cg_id}",
            params={"localization":"false","tickers":"false",
                    "market_data":"false","community_data":"true"},
            timeout=6,
        )
        if r.status_code == 200:
            d = r.json().get("community_data",{})
            twitter = d.get("twitter_followers",0)
            reddit  = d.get("reddit_subscribers",0)
            return f"Twitter: {twitter:,} | Reddit: {reddit:,}"
    except Exception as e:
        log.debug("Social error %s: %s", symbol, e)
    return "NA"


def fetch_market_dominance() -> dict:
    """BTC dominance from CoinGecko"""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/global", timeout=6,
        )
        if r.status_code == 200:
            d = r.json()["data"]
            btc_dom = d.get("market_cap_percentage",{}).get("btc", 0)
            total   = d.get("total_market_cap",{}).get("usd", 0)
            return {
                "btc_dominance": f"{btc_dom:.1f}%",
                "total_mcap":    f"${total/1e12:.2f}T",
            }
    except Exception as e:
        log.debug("Dominance error: %s", e)
    return {"btc_dominance": "NA", "total_mcap": "NA"}


def fetch_long_short_ratio(symbol: str) -> dict:
    """Long/Short ratio from Binance — who is winning bulls or bears"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            params={"symbol": ticker, "period": "1h", "limit": 1},
            timeout=5,
        )
        if r.status_code == 200:
            d = r.json()[0]
            ls = float(d.get("longShortRatio", 1))
            bias = "HEAVILY LONG" if ls > 2 else "SLIGHTLY LONG" if ls > 1.2 else                    "HEAVILY SHORT" if ls < 0.5 else "SLIGHTLY SHORT" if ls < 0.8 else "BALANCED"
            return {"ratio": round(ls, 3), "bias": bias,
                    "long_pct": d.get("longAccount",""), "short_pct": d.get("shortAccount","")}
    except Exception as e:
        log.debug("L/S ratio error %s: %s", symbol, e)
    return {"ratio":"NA","bias":"NA","long_pct":"NA","short_pct":"NA"}


def fetch_atr(symbol: str, period: int = 14) -> dict:
    """ATR — true volatility for realistic stop loss sizing"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": period + 1},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json()
            trs = []
            for i in range(1, len(candles)):
                high  = float(candles[i][2])
                low   = float(candles[i][3])
                prev_close = float(candles[i-1][4])
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                trs.append(tr)
            atr   = sum(trs) / len(trs)
            price = float(candles[-1][4])
            atr_pct = atr / price * 100
            volatility = "HIGH" if atr_pct > 3 else "MEDIUM" if atr_pct > 1 else "LOW"
            return {"atr": round(atr, 4), "atr_pct": round(atr_pct, 2),
                    "volatility": volatility,
                    "suggested_stop_pct": round(atr_pct * 1.5, 2)}
    except Exception as e:
        log.debug("ATR error %s: %s", symbol, e)
    return {"atr":"NA","atr_pct":"NA","volatility":"NA","suggested_stop_pct":"NA"}


def fetch_vwap(symbol: str) -> dict:
    """VWAP — key intraday support/resistance level"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 24},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json()
            total_vol   = sum(float(c[5]) for c in candles)
            total_pvol  = sum(((float(c[2])+float(c[3])+float(c[4]))/3) * float(c[5]) for c in candles)
            vwap  = total_pvol / total_vol if total_vol > 0 else 0
            price = float(candles[-1][4])
            pos   = "ABOVE VWAP — bullish bias" if price > vwap else "BELOW VWAP — bearish bias"
            dev   = abs(price - vwap) / vwap * 100
            return {"vwap": round(vwap, 4), "price_position": pos,
                    "deviation_pct": round(dev, 2)}
    except Exception as e:
        log.debug("VWAP error %s: %s", symbol, e)
    return {"vwap":"NA","price_position":"NA","deviation_pct":"NA"}


def fetch_support_resistance(symbol: str) -> dict:
    """Key S/R levels from 4h candles — swing highs/lows"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "4h", "limit": 50},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json()
            highs  = [float(c[2]) for c in candles]
            lows   = [float(c[3]) for c in candles]
            closes = [float(c[4]) for c in candles]
            price  = closes[-1]
            # Find swing highs and lows
            resistance_levels = sorted(
                [h for i,h in enumerate(highs[1:-1],1) if h>highs[i-1] and h>highs[i+1]],
                reverse=True
            )
            support_levels = sorted(
                [l for i,l in enumerate(lows[1:-1],1) if l<lows[i-1] and l<lows[i+1]]
            )
            nearest_resistance = next((r for r in resistance_levels if r > price), "NA")
            nearest_support    = next((s for s in reversed(support_levels) if s < price), "NA")
            if nearest_resistance != "NA" and nearest_support != "NA":
                risk_reward = round((float(nearest_resistance)-price)/(price-float(nearest_support)),2)
            else:
                risk_reward = "NA"
            return {
                "resistance": round(nearest_resistance,2) if nearest_resistance!="NA" else "NA",
                "support":    round(nearest_support,2) if nearest_support!="NA" else "NA",
                "risk_reward": risk_reward,
            }
    except Exception as e:
        log.debug("S/R error %s: %s", symbol, e)
    return {"resistance":"NA","support":"NA","risk_reward":"NA"}


def fetch_multi_timeframe_rsi(symbol: str) -> dict:
    """RSI on 15m, 1h, 4h — all must agree for HIGH conviction"""
    results = {}
    for tf, label in [("15m","rsi_15m"),("1h","rsi_1h"),("4h","rsi_4h")]:
        try:
            ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
            r = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": ticker, "interval": tf, "limit": 15},
                timeout=5,
            )
            if r.status_code == 200:
                closes = [float(k[4]) for k in r.json()]
                gains  = [max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
                losses = [max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
                ag, al = sum(gains)/14, sum(losses)/14
                rsi = 100-(100/(1+ag/al)) if al>0 else 100
                results[label] = round(rsi,1)
        except:
            results[label] = "NA"
    # Check alignment
    vals = [v for v in results.values() if v != "NA"]
    if len(vals) == 3:
        all_over  = all(v > 60 for v in vals)
        all_under = all(v < 40 for v in vals)
        results["alignment"] = "ALL BULLISH — strong signal" if all_over else                                "ALL BEARISH — strong signal" if all_under else                                "MIXED — reduce conviction"
    return results


def fetch_stoch_rsi(symbol: str) -> dict:
    """Stochastic RSI — more sensitive than regular RSI"""
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 28},
            timeout=5,
        )
        if r.status_code == 200:
            closes = [float(k[4]) for k in r.json()]
            # Calculate RSI series
            rsi_series = []
            for i in range(14, len(closes)):
                segment = closes[i-14:i]
                gains  = [max(segment[j]-segment[j-1],0) for j in range(1,14)]
                losses = [max(segment[j-1]-segment[j],0) for j in range(1,14)]
                ag,al  = sum(gains)/14, sum(losses)/14
                rsi_series.append(100-(100/(1+ag/al)) if al>0 else 100)
            if len(rsi_series) >= 14:
                rsi_window = rsi_series[-14:]
                rsi_min, rsi_max = min(rsi_window), max(rsi_window)
                stoch = (rsi_series[-1]-rsi_min)/(rsi_max-rsi_min)*100 if rsi_max!=rsi_min else 50
                label = "OVERSOLD — BUY signal" if stoch < 20 else                         "OVERBOUGHT — SELL signal" if stoch > 80 else "NEUTRAL"
                return {"stoch_rsi": round(stoch,1), "signal": label}
    except Exception as e:
        log.debug("StochRSI error %s: %s", symbol, e)
    return {"stoch_rsi":"NA","signal":"NA"}


def fetch_btc_correlation() -> dict:
    """Check BTC trend - most alts follow BTC direction."""
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1h", "limit": 4},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json()
            open_price  = float(candles[0][1])
            close_price = float(candles[-1][4])
            change_pct  = (close_price - open_price) / open_price * 100
            if change_pct > 2:
                trend = "STRONG UP — good for BUY signals"
            elif change_pct > 0.5:
                trend = "SLIGHTLY UP — neutral for signals"
            elif change_pct < -2:
                trend = "STRONG DOWN — avoid BUY on alts"
            elif change_pct < -0.5:
                trend = "SLIGHTLY DOWN — caution on BUY"
            else:
                trend = "SIDEWAYS — neutral"
            return {"change_1h": round(change_pct, 2), "trend": trend, "price": str(round(close_price, 2))}
    except Exception as e:
        log.debug("BTC correlation error: %s", e)
    return {"change_1h": "NA", "trend": "NA", "price": "NA"}


def score_signal_confidence(intel: dict, signal_type: str) -> dict:
    """
    Score signal confidence 0-100 based on all indicators alignment.
    Returns score and recommendation.
    """
    score = 50  # base score
    reasons = []

    # Fear & Greed
    fg_val = intel.get("fear_greed", {}).get("value", "NA")
    if fg_val != "NA":
        fg = int(fg_val)
        if signal_type == "BUY":
            if fg < 25:
                score += 15
                reasons.append("Extreme Fear = BUY opportunity")
            elif fg < 45:
                score += 8
                reasons.append("Fear = good for BUY")
            elif fg > 75:
                score -= 15
                reasons.append("Extreme Greed = risky BUY")
        elif signal_type == "SELL":
            if fg > 75:
                score += 15
                reasons.append("Extreme Greed = SELL opportunity")
            elif fg > 55:
                score += 8
                reasons.append("Greed = good for SELL")
            elif fg < 25:
                score -= 15
                reasons.append("Extreme Fear = risky SELL")

    # RSI
    rsi = intel.get("rsi", "NA")
    if rsi != "NA":
        rsi_f = float(rsi)
        if signal_type == "BUY":
            if rsi_f < 30:
                score += 15
                reasons.append("RSI oversold = strong BUY")
            elif rsi_f < 50:
                score += 8
                reasons.append("RSI below 50 = favorable")
            elif rsi_f > 70:
                score -= 15
                reasons.append("RSI overbought = risky BUY")
        elif signal_type == "SELL":
            if rsi_f > 70:
                score += 15
                reasons.append("RSI overbought = strong SELL")
            elif rsi_f > 50:
                score += 8
                reasons.append("RSI above 50 = favorable")
            elif rsi_f < 30:
                score -= 15
                reasons.append("RSI oversold = risky SELL")

    # MACD
    macd = intel.get("macd", {})
    if macd.get("trend") != "NA":
        if signal_type == "BUY" and macd.get("trend") == "BULLISH":
            score += 10
            reasons.append("MACD bullish")
        elif signal_type == "SELL" and macd.get("trend") == "BEARISH":
            score += 10
            reasons.append("MACD bearish")
        elif signal_type == "BUY" and macd.get("trend") == "BEARISH":
            score -= 10
            reasons.append("MACD bearish - caution")
        elif signal_type == "SELL" and macd.get("trend") == "BULLISH":
            score -= 10
            reasons.append("MACD bullish - caution")

    # Multi-timeframe RSI alignment
    mtf = intel.get("multi_tf_rsi", {})
    alignment = mtf.get("alignment", "")
    if "ALL BULLISH" in alignment and signal_type == "BUY":
        score += 15
        reasons.append("All timeframes bullish")
    elif "ALL BEARISH" in alignment and signal_type == "SELL":
        score += 15
        reasons.append("All timeframes bearish")
    elif "MIXED" in alignment:
        score -= 10
        reasons.append("Mixed timeframes - reduce size")

    # Funding rate
    fund = intel.get("funding", {})
    if fund.get("rate") != "NA":
        rate = float(fund["rate"])
        if signal_type == "BUY" and rate < -0.05:
            score += 10
            reasons.append("Negative funding = shorts squeezable")
        elif signal_type == "SELL" and rate > 0.05:
            score += 10
            reasons.append("Positive funding = longs squeezable")
        elif signal_type == "BUY" and rate > 0.1:
            score -= 10
            reasons.append("High positive funding = longs overextended")

    # BTC correlation
    btc = intel.get("btc_correlation", {})
    if btc.get("trend") != "NA":
        if signal_type == "BUY" and "STRONG DOWN" in btc.get("trend",""):
            score -= 20
            reasons.append("BTC falling - avoid alt BUY")
        elif signal_type == "BUY" and "STRONG UP" in btc.get("trend",""):
            score += 10
            reasons.append("BTC rising - good for BUY")

    # VWAP
    vwap = intel.get("vwap", {})
    pos = vwap.get("price_position", "")
    if signal_type == "BUY" and "ABOVE VWAP" in pos:
        score += 5
        reasons.append("Above VWAP = bullish")
    elif signal_type == "SELL" and "BELOW VWAP" in pos:
        score += 5
        reasons.append("Below VWAP = bearish")

    # Orderbook
    ob = intel.get("orderbook", {})
    pressure = ob.get("pressure", "")
    if signal_type == "BUY" and "STRONG BUY" in pressure:
        score += 8
        reasons.append("Strong buy pressure in orderbook")
    elif signal_type == "SELL" and "STRONG SELL" in pressure:
        score += 8
        reasons.append("Strong sell pressure in orderbook")

    # Google Trends boost
    gtrend = intel.get("google_trends", {})
    if gtrend.get("trending"):
        score += 8
        reasons.append("Social interest spike on Google")

    # News Volume boost
    nvol = intel.get("news_volume", {})
    mentions = nvol.get("mentions", 0)
    nscore = nvol.get("sentiment_score", 0)
    if mentions >= 10:
        score += 12
        reasons.append(f"High news buzz: {mentions} articles")
    elif mentions >= 5:
        score += 6
        reasons.append(f"Elevated news volume: {mentions} articles")
    if nscore > 30 and signal_type == "BUY":
        score += 8
        reasons.append("Strong positive news sentiment")
    elif nscore < -30 and signal_type == "SELL":
        score += 8
        reasons.append("Strong negative news sentiment")

    # Correlation Matrix
    corr = intel.get("correlation_matrix", {})
    if corr.get("market_mood") == "RISK_ON" and signal_type == "BUY":
        score += 8
        reasons.append("Risk-on market mood")
    elif corr.get("market_mood") == "RISK_OFF" and signal_type == "SELL":
        score += 8
        reasons.append("Risk-off market mood")
    elif corr.get("market_mood") == "RISK_OFF" and signal_type == "BUY":
        score -= 10
        reasons.append("Risk-off — caution on BUY")
    if corr.get("alt_season") and signal_type == "BUY":
        score += 5
        reasons.append("Altcoin season active")

    # Backtest insights — use historical win data
    bt = get_backtest_insights()
    if bt.get("status") == "analyzed":
        rec_threshold = bt.get("recommended_threshold", 65)
        best_sources  = bt.get("best_sources", [])
        bt_src_name   = intel.get("_source","") if intel else ""
        if bt_src_name in best_sources:
            score += 5
            reasons.append(f"High-accuracy source: {bt_src_name}")

    # Source credibility boost
    src_name_cred = intel.get("_source","") if intel else ""
    cred_score = get_source_credibility(src_name_cred)
    if cred_score >= 9:
        score += 8
        reasons.append(f"מקור Tier-1: {src_name_cred}")
    elif cred_score >= 7:
        score += 4
        reasons.append(f"מקור אמין: {src_name_cred}")
    elif cred_score <= 4:
        score -= 8
        reasons.append(f"מקור נמוך אמינות: {src_name_cred}")

    # Self-learning weights — learned from actual outcomes
    try:
        insights = get_learning_insights()
        src_name = intel.get("_source","") if intel else ""
        if src_name in insights.get("best_sources",[]):
            score += 7
            reasons.append("מקור מוכח היסטורית")
        elif src_name in insights.get("worst_sources",[]):
            score -= 8
            reasons.append("מקור עם דיוק נמוך — זהירות")
        # Adjust by signal direction historical performance
        if signal_type == "BUY" and insights.get("buy_win_rate",0) < 40:
            score -= 5
            reasons.append("BUY היסטורית חלש — מוריד ניקוד")
        elif signal_type == "SELL" and insights.get("sell_win_rate",0) < 40:
            score -= 5
            reasons.append("SELL היסטורית חלש — מוריד ניקוד")
    except Exception as e:
        log.debug("Learning score error: %s", e)

    # Cross-exchange confirmation
    cross = intel.get("cross_exchange", {})
    if cross.get("divergence") == "HIGH_DIVERGENCE":
        score += 8
        reasons.append("Price divergence across exchanges")
    if cross.get("exchanges_count",0) >= 3:
        score += 5
        reasons.append("Confirmed on 3 exchanges")

    # Liquidation heatmap
    hmap = intel.get("liq_heatmap", {})
    if hmap.get("liq_dominant") == "SHORT SQUEEZE" and signal_type == "BUY":
        score += 12
        reasons.append("Short squeeze zone — longs win")
    elif hmap.get("liq_dominant") == "LONG SQUEEZE" and signal_type == "SELL":
        score += 12
        reasons.append("Long squeeze zone — shorts win")
    if hmap.get("book_signal") == "BUY WALL" and signal_type == "BUY":
        score += 8
        reasons.append("Strong buy wall in order book")
    elif hmap.get("book_signal") == "SELL WALL" and signal_type == "SELL":
        score += 8
        reasons.append("Strong sell wall in order book")

    # Deribit Options P/C ratio
    derib = intel.get("deribit_options", {})
    if derib.get("score_boost"):
        boost = derib["score_boost"]
        if boost > 0 and signal_type == "BUY":
            score += boost
            reasons.append(f"Options fear={derib.get('put_call_ratio')} — contrarian BUY")
        elif boost < 0 and signal_type == "SELL":
            score += abs(boost)
            reasons.append(f"Options greed={derib.get('put_call_ratio')} — contrarian SELL")
    if derib.get("iv_signal") == "HIGH_IV — big move expected":
        score += 5
        reasons.append("High implied volatility — big move coming")

    # Smart Money flows
    smart = intel.get("smart_money", {})
    flow_score = smart.get("flow_score", 0)
    if flow_score > 0 and signal_type == "BUY":
        score += flow_score
        reasons.append("Smart money accumulating")
    elif flow_score < 0 and signal_type == "SELL":
        score += abs(flow_score)
        reasons.append("Smart money distributing")

    # Price Patterns
    patt = intel.get("price_patterns", {})
    patt_adj = patt.get("score_adj", 0)
    if patt_adj > 0 and signal_type == "BUY":
        score += min(patt_adj, 15)
        if patt.get("patterns"):
            reasons.append(patt["patterns"][0][:50])
    elif patt_adj < 0 and signal_type == "SELL":
        score += min(abs(patt_adj), 15)
        if patt.get("patterns"):
            reasons.append(patt["patterns"][0][:50])

    # Futures Basis
    basis = intel.get("futures_basis", {})
    basis_score = basis.get("score", 0)
    if basis_score < 0 and signal_type == "BUY":
        score += basis_score  # penalize BUY when overleveraged long
        reasons.append("Overleveraged futures — caution")
    elif basis_score < 0 and signal_type == "SELL":
        score += abs(basis_score)
        reasons.append("Futures premium — SELL signal")

    # Market Depth Score
    depth = intel.get("market_depth", {})
    dscore = depth.get("score", 0)
    if dscore > 0 and signal_type == "BUY":
        score += dscore
        reasons.append(depth.get("depth_signal","")[:40])
    elif dscore < 0 and signal_type == "SELL":
        score += abs(dscore)
        reasons.append(depth.get("depth_signal","")[:40])

    # CVIX — adjust based on volatility
    cvix_data = intel.get("cvix", {})
    if cvix_data.get("cvix", 0) > 100:
        score -= 5  # extreme volatility = reduce confidence
        reasons.append("Extreme volatility — reduce size")

    # Social Dominance
    soc = intel.get("social_dominance", {})
    if soc.get("social_signal") == "HIGH_ACTIVITY" and signal_type == "BUY":
        score += 6
        reasons.append("High social activity — rising interest")

    # OI Change — most important futures indicator
    oic = intel.get("oi_change", {})
    oi_sc = oic.get("oi_score", 0)
    if oi_sc > 0 and signal_type == "BUY":
        score += oi_sc
        reasons.append(oic.get("oi_signal","")[:45])
    elif oi_sc < 0 and signal_type == "SELL":
        score += abs(oi_sc)
        reasons.append(oic.get("oi_signal","")[:45])
    elif oi_sc < 0 and signal_type == "BUY":
        score += oi_sc  # penalize BUY when OI falling
        reasons.append("OI falling — weak BUY signal")

    # Funding Rate History
    fh = intel.get("funding_history", {})
    fs = fh.get("funding_score", 0)
    if fs > 0 and signal_type == "BUY":
        score += fs
        reasons.append(fh.get("funding_sentiment","")[:45])
    elif fs < 0 and signal_type == "SELL":
        score += abs(fs)
        reasons.append(fh.get("funding_sentiment","")[:45])
    elif fs < 0 and signal_type == "BUY":
        score += fs
        reasons.append("High funding — avoid long")

    # Long/Short Ratio (contrarian)
    ls = intel.get("ls_detailed", {})
    ls_sc = ls.get("top_score", 0)
    if ls_sc > 0 and signal_type == "BUY":
        score += ls_sc
        reasons.append("Top traders overshorted — contrarian BUY")
    elif ls_sc < 0 and signal_type == "SELL":
        score += abs(ls_sc)
        reasons.append("Top traders overlonged — contrarian SELL")

    # Volume Profile
    vp = intel.get("volume_profile", {})
    if vp.get("poc_vs_price") == "ABOVE POC" and signal_type == "BUY":
        score += 7
        reasons.append("Price above POC — institutional support")
    elif vp.get("poc_vs_price") == "BELOW POC" and signal_type == "SELL":
        score += 7
        reasons.append("Price below POC — institutional resistance")

    score = max(0, min(100, score))
    if score >= 75:
        conviction = "HIGH"
    elif score >= 55:
        conviction = "MEDIUM"
    else:
        conviction = "LOW"

    return {
        "score": score,
        "conviction": conviction,
        "reasons": reasons[:4],  # top 4 reasons
    }


# ═══════════════════════════════════════════
# BACKTEST ENGINE — Auto-tune parameters
# ═══════════════════════════════════════════

def run_backtest_analysis() -> dict:
    """
    Analyze closed signals to find what parameters lead to wins.
    Returns tuned thresholds based on historical performance.
    """
    try:
        with get_db() as conn:
            closed = conn.execute(
                """SELECT signal, conviction, score, result, result_pnl,
                   sentiment, direction, source
                   FROM signals
                   WHERE result IN ('TARGET_HIT','STOP_LOSS')
                   ORDER BY timestamp DESC LIMIT 200"""
            ).fetchall()

        if len(closed) < 10:
            return {"status": "insufficient_data", "min_needed": 10, "current": len(closed)}

        rows = [dict(r) for r in closed]
        total = len(rows)
        hits  = [r for r in rows if r["result"] == "TARGET_HIT"]
        sls   = [r for r in rows if r["result"] == "STOP_LOSS"]
        win_rate = len(hits) / total * 100

        # Score analysis — what score range wins most?
        score_buckets = {}
        for r in rows:
            score = int(r.get("score") or 50)
            bucket = (score // 10) * 10  # 60, 70, 80, 90
            if bucket not in score_buckets:
                score_buckets[bucket] = {"hits":0,"total":0}
            score_buckets[bucket]["total"] += 1
            if r["result"] == "TARGET_HIT":
                score_buckets[bucket]["hits"] += 1

        best_bucket = max(score_buckets.items(),
                         key=lambda x: x[1]["hits"]/x[1]["total"] if x[1]["total"]>=3 else 0,
                         default=(65,{}))

        # Source analysis — which source is most accurate?
        source_perf = {}
        for r in rows:
            src = r.get("source","Unknown")
            if src not in source_perf:
                source_perf[src] = {"hits":0,"total":0}
            source_perf[src]["total"] += 1
            if r["result"] == "TARGET_HIT":
                source_perf[src]["hits"] += 1

        best_sources = sorted(
            [(s,v) for s,v in source_perf.items() if v["total"]>=3],
            key=lambda x: x[1]["hits"]/x[1]["total"],
            reverse=True
        )[:3]

        # Conviction analysis
        conv_perf = {}
        for r in rows:
            conv = r.get("conviction","MEDIUM")
            if conv not in conv_perf:
                conv_perf[conv] = {"hits":0,"total":0}
            conv_perf[conv]["total"] += 1
            if r["result"] == "TARGET_HIT":
                conv_perf[conv]["hits"] += 1

        # Auto-tune: recommend new score threshold
        winning_scores = [int(r.get("score") or 50) for r in hits]
        recommended_threshold = int(sum(winning_scores)/len(winning_scores)) if winning_scores else 65

        result = {
            "status":               "analyzed",
            "total_closed":         total,
            "win_rate":             round(win_rate, 1),
            "recommended_threshold": max(55, min(85, recommended_threshold)),
            "best_score_bucket":    best_bucket[0],
            "best_sources":         [s[0] for s in best_sources],
            "conviction_perf":      {k: round(v["hits"]/v["total"]*100,1) for k,v in conv_perf.items() if v["total"]>=3},
            "avg_winning_score":    round(sum(winning_scores)/len(winning_scores),1) if winning_scores else 65,
        }
        log.info("[BACKTEST] Win rate: %.1f%% | Recommended threshold: %d | Best sources: %s",
                 win_rate, result["recommended_threshold"], result["best_sources"])
        return result
    except Exception as e:
        log.error("Backtest error: %s", e)
        return {"status": "error", "error": str(e)}


# Global backtest cache
_backtest_cache: dict = {}
_backtest_last_run: float = 0
BACKTEST_TTL = 3600  # re-run every hour

def get_backtest_insights() -> dict:
    """Get cached backtest results, refresh hourly."""
    import time
    global _backtest_cache, _backtest_last_run
    if not _backtest_cache or time.time() - _backtest_last_run > BACKTEST_TTL:
        _backtest_cache = run_backtest_analysis()
        _backtest_last_run = time.time()
    return _backtest_cache


# ═══════════════════════════════════════════
# CORRELATION MATRIX — Multi-asset context
# ═══════════════════════════════════════════

def fetch_correlation_matrix() -> dict:
    """
    Fetch multiple market indicators for full picture:
    - BTC dominance
    - ETH/BTC ratio
    - DeFi index (using top DeFi coins)
    - Total crypto market cap change
    - Altcoin season index
    """
    result = {}
    try:
        # Fetch multiple prices in parallel
        pairs = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","AVAXUSDT","LINKUSDT","UNIUSDT","AAVEUSDT"]
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbols": str(pairs).replace("'",'"')},
            timeout=8,
        )
        if r.status_code == 200:
            data = {item["symbol"]: item for item in r.json()}

            btc = data.get("BTCUSDT",{})
            eth = data.get("ETHUSDT",{})
            btc_chg = float(btc.get("priceChangePercent",0))
            eth_chg = float(eth.get("priceChangePercent",0))

            # ETH/BTC ratio trend
            btc_price = float(btc.get("lastPrice",1))
            eth_price = float(eth.get("lastPrice",1))
            eth_btc = round(eth_price/btc_price, 6) if btc_price > 0 else 0

            # DeFi performance (avg of DeFi tokens)
            defi_symbols = ["UNIUSDT","AAVEUSDT","LINKUSDT"]
            defi_changes = [float(data.get(s,{}).get("priceChangePercent",0)) for s in defi_symbols if s in data]
            defi_avg = round(sum(defi_changes)/len(defi_changes),2) if defi_changes else 0

            # Altcoin season: if most alts outperform BTC → alt season
            alt_symbols = ["ETHUSDT","SOLUSDT","BNBUSDT","AVAXUSDT"]
            alt_changes = [float(data.get(s,{}).get("priceChangePercent",0)) for s in alt_symbols if s in data]
            alts_beating_btc = sum(1 for c in alt_changes if c > btc_chg)
            alt_season = alts_beating_btc >= 3

            result = {
                "btc_24h":       round(btc_chg, 2),
                "eth_24h":       round(eth_chg, 2),
                "eth_btc_ratio": eth_btc,
                "defi_avg_24h":  defi_avg,
                "alt_season":    alt_season,
                "alts_vs_btc":   f"{alts_beating_btc}/{len(alt_changes)} alts beating BTC",
                "market_mood":   "RISK_ON" if btc_chg > 2 else "RISK_OFF" if btc_chg < -2 else "NEUTRAL",
            }
    except Exception as e:
        log.debug("Correlation matrix error: %s", e)
    return result


# ═══════════════════════════════════════════
# NEWS SENTIMENT VOLUME — Count-based signal
# ═══════════════════════════════════════════

_news_volume_cache: dict = {}
_news_volume_last: float = 0

def fetch_news_sentiment_volume(symbol: str) -> dict:
    """
    Count how many articles mention this coin in last hour.
    A spike in mention count = something is happening.
    Also analyzes sentiment polarity distribution.
    """
    import time
    global _news_volume_cache, _news_volume_last

    # Refresh cache every 10 minutes
    if not _news_volume_cache or time.time() - _news_volume_last > 600:
        try:
            if NEWSAPI_KEY:
                from datetime import timezone
                from_time = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
                r = requests.get(
                    NEWSAPI_URL,
                    params={
                        "q": "bitcoin OR ethereum OR crypto OR altcoin",
                        "language": "en",
                        "sortBy": "publishedAt",
                        "from": from_time,
                        "pageSize": 100,
                        "apiKey": NEWSAPI_KEY,
                    },
                    timeout=10,
                )
                if r.status_code == 200:
                    _news_volume_cache = r.json().get("articles", [])
                    _news_volume_last = time.time()
        except Exception as e:
            log.debug("News volume fetch error: %s", e)

    if not _news_volume_cache:
        return {"mentions": 0, "sentiment_score": 0, "signal": "NEUTRAL"}

    ticker = symbol.replace("/USDT","").replace("/","").lower()
    coin_names = {
        "btc": ["bitcoin","btc"],
        "eth": ["ethereum","eth","ether"],
        "sol": ["solana","sol"],
        "xrp": ["ripple","xrp"],
        "bnb": ["binance","bnb"],
    }
    search_terms = coin_names.get(ticker, [ticker])

    # Count mentions and sentiment
    mentions = 0
    positive_words = ["surge","rally","bullish","gain","rise","pump","adoption","partnership","listing","approval","breakthrough"]
    negative_words = ["crash","dump","bearish","fall","drop","hack","ban","lawsuit","sell","fear","risk","collapse"]

    pos_count = neg_count = 0
    for article in _news_volume_cache:
        title = (article.get("title","") or "").lower()
        desc  = (article.get("description","") or "").lower()
        content = title + " " + desc
        if any(term in content for term in search_terms):
            mentions += 1
            pos_count += sum(1 for w in positive_words if w in content)
            neg_count += sum(1 for w in negative_words if w in content)

    total_sentiment = pos_count + neg_count
    sentiment_score = round((pos_count - neg_count) / total_sentiment * 100) if total_sentiment > 0 else 0

    if mentions >= 10:
        volume_signal = "HIGH_BUZZ — major news event"
    elif mentions >= 5:
        volume_signal = "ELEVATED — increased attention"
    elif mentions >= 2:
        volume_signal = "NORMAL"
    else:
        volume_signal = "LOW — quiet period"

    return {
        "mentions":        mentions,
        "sentiment_score": sentiment_score,
        "positive_count":  pos_count,
        "negative_count":  neg_count,
        "volume_signal":   volume_signal,
    }


def gather_market_intel(symbol: str) -> dict:
    """
    Gather ALL market intelligence in parallel.
    Uses 3-min cache to avoid re-fetching for same symbol.
    """
    import time
    global _indicator_cache
    cache_key = symbol.upper()
    if cache_key in _indicator_cache:
        cached_data, cached_time = _indicator_cache[cache_key]
        if time.time() - cached_time < INDICATOR_TTL:
            log.debug("[CACHE HIT] Intel for %s (age: %.0fs)", symbol, time.time()-cached_time)
            return cached_data
    
    with ThreadPoolExecutor(max_workers=17) as pool:
        f_fg    = pool.submit(fetch_fear_greed)
        f_fund  = pool.submit(fetch_funding_rate,         symbol)
        f_oi    = pool.submit(fetch_open_interest,        symbol)
        f_vol   = pool.submit(fetch_volume_spike,         symbol)
        f_rsi   = pool.submit(fetch_rsi,                  symbol)
        f_macd  = pool.submit(fetch_macd,                 symbol)
        f_bb    = pool.submit(fetch_bollinger_bands,      symbol)
        f_ema   = pool.submit(fetch_ema_trend,            symbol)
        f_liq   = pool.submit(fetch_liquidations,         symbol)
        f_ob    = pool.submit(fetch_orderbook_imbalance,  symbol)
        f_ls    = pool.submit(fetch_long_short_ratio,     symbol)
        f_atr   = pool.submit(fetch_atr,                  symbol)
        f_vwap  = pool.submit(fetch_vwap,                 symbol)
        f_sr    = pool.submit(fetch_support_resistance,   symbol)
        f_mtf   = pool.submit(fetch_multi_timeframe_rsi,  symbol)
        f_srsi  = pool.submit(fetch_stoch_rsi,            symbol)
        f_btc    = pool.submit(fetch_btc_correlation)
        f_gtrend = pool.submit(fetch_google_trends_crypto)
        f_corr   = pool.submit(fetch_correlation_matrix)
        f_nsvol  = pool.submit(fetch_news_sentiment_volume, symbol)
        f_cross  = pool.submit(fetch_cross_exchange_analysis, symbol)
        f_hmap   = pool.submit(fetch_liquidation_heatmap, symbol)
        f_derib  = pool.submit(fetch_deribit_options, symbol)
        f_smart  = pool.submit(fetch_smart_money_flows)
        f_patt   = pool.submit(fetch_historical_pattern, symbol)
        f_basis  = pool.submit(fetch_futures_basis, symbol)
        f_depth  = pool.submit(fetch_market_depth_score, symbol)
        f_cvix   = pool.submit(fetch_crypto_volatility_index)
        f_social = pool.submit(fetch_social_dominance_score, symbol)
        f_oi_chg = pool.submit(fetch_open_interest_change, symbol)
        f_fund_h = pool.submit(fetch_funding_rate_history, symbol)
        f_ls_det = pool.submit(fetch_long_short_ratio_detailed, symbol)
        f_volp   = pool.submit(fetch_volume_profile, symbol)

    import time as _time
    intel = {
        "fear_greed":        f_fg.result(),
        "funding":           f_fund.result(),
        "open_interest":     f_oi.result(),
        "volume":            f_vol.result(),
        "rsi":               f_rsi.result(),
        "macd":              f_macd.result(),
        "bollinger":         f_bb.result(),
        "ema":               f_ema.result(),
        "liquidations":      f_liq.result(),
        "orderbook":         f_ob.result(),
        "long_short":        f_ls.result(),
        "atr":               f_atr.result(),
        "vwap":              f_vwap.result(),
        "support_resistance": f_sr.result(),
        "multi_tf_rsi":      f_mtf.result(),
        "stoch_rsi":         f_srsi.result(),
        "btc_correlation":    f_btc.result(),
        "google_trends":      f_gtrend.result(),
        "correlation_matrix": f_corr.result(),
        "news_volume":        f_nsvol.result(),
        "cross_exchange":     f_cross.result(),
        "liq_heatmap":        f_hmap.result(),
        "deribit_options":    f_derib.result(),
        "smart_money":        f_smart.result(),
        "price_patterns":     f_patt.result(),
        "futures_basis":      f_basis.result(),
        "market_depth":       f_depth.result(),
        "cvix":               f_cvix.result(),
        "social_dominance":   f_social.result(),
        "oi_change":          f_oi_chg.result(),
        "funding_history":    f_fund_h.result(),
        "ls_detailed":        f_ls_det.result(),
        "volume_profile":     f_volp.result(),
    }
    # Limit cache size to 50 symbols
    if len(_indicator_cache) > 50:
        oldest = min(_indicator_cache.keys(),
                    key=lambda k: _indicator_cache[k][1])
        del _indicator_cache[oldest]
    _indicator_cache[cache_key] = (intel, _time.time())
    return intel


def translate_to_hebrew(text: str) -> str:
    """Translate news headline to Hebrew using GPT-4o-mini."""
    if not text or len(text) < 5:
        return text
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Translate the following crypto news headline to Hebrew. Keep crypto terms in English (BTC, ETH, etc). Return ONLY the translation, nothing else."},
                {"role": "user", "content": text},
            ],
            temperature=0.1,
            max_tokens=100,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.debug("Translation error: %s", e)
        return text


def fetch_all_exchange_coins() -> list[str]:
    """
    Fetch ALL trading coins from multiple exchanges.
    Returns list of coin tickers (e.g. BTC, ETH, PEPE).
    Covers Binance + known coins from other exchanges.
    """
    coins = set()
    try:
        # Binance USDT pairs
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            timeout=10,
        )
        if r.status_code == 200:
            for item in r.json():
                sym = item["symbol"]
                if sym.endswith("USDT"):
                    coins.add(sym[:-4])
        log.info("[AllCoins] Binance: %d coins", len(coins))
    except Exception as e:
        log.error("[AllCoins] Binance error: %s", e)

    try:
        # CoinGecko top 500 coins
        r2 = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "order": "market_cap_desc",
                    "per_page": 250, "page": 1, "sparkline": False},
            timeout=10,
        )
        if r2.status_code == 200:
            for coin in r2.json():
                sym = coin.get("symbol","").upper()
                if sym:
                    coins.add(sym)
        log.info("[AllCoins] Total after CoinGecko: %d coins", len(coins))
    except Exception as e:
        log.debug("[AllCoins] CoinGecko error: %s", e)

    return list(coins)


def build_prompt(title: str, summary: str, live_price: str = "NA",
                 intel: dict | None = None) -> str:
    """Build comprehensive prompt with ALL market indicators."""
    lines = [f"HEADLINE: {title}"]
    if summary:
        lines.append(f"CONTEXT: {summary[:120]}")
    if live_price and live_price != "NA":
        lines.append(f"LIVE PRICE: ${live_price}")
    if intel:
        # Fear & Greed
        fg = intel.get("fear_greed",{})
        if fg.get("value") != "NA":
            lines.append(f"FEAR&GREED: {fg['value']}/100 ({fg.get('label','')})")
        # Funding Rate
        fund = intel.get("funding",{})
        if fund.get("rate") != "NA":
            rate = float(fund["rate"])
            bias = "LONG BIAS" if rate > 0.05 else "SHORT BIAS" if rate < -0.05 else "NEUTRAL"
            lines.append(f"FUNDING: {rate:+.4f}% — {bias}")
        # RSI
        rsi = intel.get("rsi","NA")
        if rsi != "NA":
            rsi_f = float(rsi)
            label = "OVERBOUGHT — avoid BUY" if rsi_f>70 else "OVERSOLD — avoid SELL" if rsi_f<30 else "NEUTRAL"
            lines.append(f"RSI(14h): {rsi} — {label}")
        # MACD
        macd = intel.get("macd",{})
        if macd.get("trend") != "NA":
            lines.append(f"MACD: {macd.get('trend')} (hist: {macd.get('histogram')})")
        # Bollinger Bands
        bb = intel.get("bollinger",{})
        if bb.get("position") != "NA":
            lines.append(f"BOLLINGER: {bb.get('position')}")
        # EMA Trend
        ema = intel.get("ema",{})
        if ema.get("trend") != "NA":
            lines.append(f"EMA 50/200: {ema.get('trend')} (EMA50={ema.get('ema50')} EMA200={ema.get('ema200')})")
        # Volume
        vol = intel.get("volume",{})
        if vol.get("change_24h") != "NA":
            lines.append(f"24H: {vol['change_24h']} change | Vol: {vol.get('volume_usd','NA')}")
        # Open Interest
        oi = intel.get("open_interest","NA")
        if oi != "NA":
            lines.append(f"OPEN INTEREST: {oi} contracts")
        # Liquidations
        liq = intel.get("liquidations",{})
        if liq.get("dominant") != "NA":
            lines.append(f"LIQUIDATIONS: {liq.get('dominant')} (longs: {liq.get('long_liq')} shorts: {liq.get('short_liq')})")
        # Order book
        ob = intel.get("orderbook",{})
        if ob.get("pressure") != "NA":
            lines.append(f"ORDER BOOK: {ob.get('pressure')} (bid ratio: {ob.get('bid_ratio')})")
        # Long/Short Ratio
        ls = intel.get("long_short",{})
        if ls.get("bias") != "NA":
            lines.append(f"LONG/SHORT: ratio={ls.get('ratio')} — {ls.get('bias')}")
        # ATR Volatility
        atr = intel.get("atr",{})
        if atr.get("volatility") != "NA":
            lines.append(f"ATR: {atr.get('atr_pct')}% volatility ({atr.get('volatility')}) — use {atr.get('suggested_stop_pct')}% stop")
        # VWAP
        vwap = intel.get("vwap",{})
        if vwap.get("price_position") != "NA":
            lines.append(f"VWAP: ${vwap.get('vwap')} — {vwap.get('price_position')} (dev: {vwap.get('deviation_pct')}%)")
        # Support/Resistance
        sr = intel.get("support_resistance",{})
        if sr.get("resistance") != "NA":
            lines.append(f"RESISTANCE: ${sr.get('resistance')} | SUPPORT: ${sr.get('support')} | R/R: {sr.get('risk_reward')}")
        # Multi-timeframe RSI
        mtf = intel.get("multi_tf_rsi",{})
        if mtf.get("alignment"):
            lines.append(f"MTF RSI: 15m={mtf.get('rsi_15m')} 1h={mtf.get('rsi_1h')} 4h={mtf.get('rsi_4h')} — {mtf.get('alignment')}")
        # Stochastic RSI
        srsi = intel.get("stoch_rsi",{})
        if srsi.get("stoch_rsi") != "NA":
            lines.append(f"STOCH RSI: {srsi.get('stoch_rsi')} — {srsi.get('signal')}")
        # Google Trends
        gtrend = intel.get("google_trends",{})
        if gtrend.get("trending"):
            lines.append(f"GOOGLE TRENDING: {','.join(gtrend['trending'])}")
        # Correlation Matrix
        corr = intel.get("correlation_matrix",{})
        if corr.get("market_mood"):
            lines.append(f"MARKET: BTC {corr.get('btc_24h')}% | ETH {corr.get('eth_24h')}% | DeFi {corr.get('defi_avg_24h')}% | {corr.get('market_mood')} | Alt season: {corr.get('alt_season')}")
        # News Volume/Sentiment
        nvol = intel.get("news_volume",{})
        if nvol.get("mentions",0) > 0:
            lines.append(f"NEWS VOLUME: {nvol.get('mentions')} articles | Sentiment: {nvol.get('sentiment_score')}% | {nvol.get('volume_signal')}")
        # Cross-Exchange Analysis
        cross = intel.get("cross_exchange",{})
        if cross.get("spread_pct"):
            lines.append(f"CROSS-EXCHANGE: spread={cross.get('spread_pct')}% | {cross.get('divergence')} | Bybit funding={cross.get('bybit_funding')}")
        # Liquidation Heatmap
        hmap = intel.get("liq_heatmap",{})
        if hmap.get("liq_dominant"):
            lines.append(f"LIQ HEATMAP: {hmap.get('liq_dominant')} | upper zone=${hmap.get('liq_upper_zone')} | lower=${hmap.get('liq_lower_zone')} | book={hmap.get('book_signal')}")
            if hmap.get("support_wall"):
                lines.append(f"WALLS: support=${hmap.get('support_wall')} | resistance=${hmap.get('resistance_wall')}")
        # Deribit Options
        derib = intel.get("deribit_options",{})
        if derib.get("put_call_ratio"):
            lines.append(f"OPTIONS: P/C ratio={derib.get('put_call_ratio')} | {derib.get('sentiment')} | IV={derib.get('avg_iv')}%")
        # Smart Money
        smart = intel.get("smart_money",{})
        if smart.get("flow_signal"):
            lines.append(f"SMART MONEY: {smart.get('flow_signal')}")
        # Price Patterns
        patt = intel.get("price_patterns",{})
        if patt.get("patterns"):
            lines.append(f"PATTERNS: {' | '.join(patt.get('patterns',[])[:2])} | Trend={patt.get('trend')}")
        # Futures Basis
        basis = intel.get("futures_basis",{})
        if basis.get("basis_pct") is not None:
            lines.append(f"BASIS: {basis.get('basis_pct')}% | {basis.get('signal')}")
        # Market Depth
        depth = intel.get("market_depth",{})
        if depth.get("depth_signal"):
            lines.append(f"DEPTH: {depth.get('depth_signal')} | ratio={depth.get('ratio_1pct')} | iceberg={depth.get('iceberg_detected')}")
        # CVIX
        cvix = intel.get("cvix",{})
        if cvix.get("cvix"):
            lines.append(f"CVIX: {cvix.get('cvix')} | {cvix.get('vol_regime')}")
        # Social
        soc = intel.get("social_dominance",{})
        if soc.get("social_signal"):
            lines.append(f"SOCIAL: {soc.get('social_signal')} | activity={soc.get('activity_score')} | 7d={soc.get('price_7d')}%")
        # OI Change (key futures indicator)
        oic = intel.get("oi_change",{})
        if oic.get("oi_signal"):
            lines.append(f"OI CHANGE: {oic.get('oi_change_pct')}% | price {oic.get('price_change_pct')}% | {oic.get('oi_signal')}")
        # Funding History
        fh = intel.get("funding_history",{})
        if fh.get("funding_sentiment"):
            lines.append(f"FUNDING TREND: avg={fh.get('avg_funding_8h')}% | {fh.get('funding_trend')} | {fh.get('funding_sentiment')} | annual={fh.get('annualized_rate')}%")
        # Long/Short Detailed
        ls = intel.get("ls_detailed",{})
        if ls.get("top_traders_long_pct"):
            lines.append(f"TOP TRADERS: {ls.get('top_traders_long_pct')}% long | {ls.get('top_signal')} | global: {ls.get('global_long_pct')}% long")
        # Volume Profile
        vp = intel.get("volume_profile",{})
        if vp.get("poc"):
            lines.append(f"VOLUME PROFILE: POC=${vp.get('poc')} | {vp.get('poc_vs_price')} | {vp.get('signal')}")
    raw = "\n".join(lines)
    return raw[:1800]

# Live price cache (5 min TTL)
_price_cache: dict[str, tuple[str, float]] = {}
PRICE_TTL = 300  # seconds

# Indicator cache — avoid re-fetching for same symbol in same sweep
_indicator_cache: dict[str, tuple[dict, float]] = {}
INDICATOR_TTL = 180  # 3 minutes

PAIR_MAP = {
    # ── Bitcoin ──
    "BTC":"BTCUSDT","BITCOIN":"BTCUSDT","XBT":"BTCUSDT",
    "WBTC":"WBTCUSDT","CBBTC":"CBBTCUSDT","LBTC":"LBTCUSDT",
    "ORDI":"ORDIUSDT","SATS":"SATSUSDT","CORE":"COREUSDT",
    "STX":"STXUSDT","STACKS":"STXUSDT",

    # ── Ethereum ──
    "ETH":"ETHUSDT","ETHEREUM":"ETHUSDT","ETHER":"ETHUSDT",
    "WETH":"WETHUSDT","STETH":"STETHUSDT","RETH":"RETHUSDT",
    "CBETH":"CBETHUSDT","ETHFI":"ETHFIUSDT","RENZO":"REZUSDT",
    "REZ":"REZUSDT","EIGEN":"EIGENUSDT","LDO":"LDOUSDT",
    "RPL":"RPLUSDT","SSV":"SSVUSDT","ANKR":"ANKRUSDT",
    "FXS":"FXSUSDT","ENS":"ENSUSDT","SAFE":"SAFEUSDT",

    # ── Layer 1 ──
    "BNB":"BNBUSDT","BINANCE":"BNBUSDT",
    "SOL":"SOLUSDT","SOLANA":"SOLUSDT",
    "XRP":"XRPUSDT","RIPPLE":"XRPUSDT",
    "ADA":"ADAUSDT","CARDANO":"ADAUSDT",
    "AVAX":"AVAXUSDT","AVALANCHE":"AVAXUSDT",
    "DOT":"DOTUSDT","POLKADOT":"DOTUSDT",
    "MATIC":"MATICUSDT","POLYGON":"MATICUSDT","POL":"POLUSDT",
    "ATOM":"ATOMUSDT","COSMOS":"ATOMUSDT",
    "NEAR":"NEARUSDT","FTM":"FTMUSDT","S":"SUSDT",
    "ALGO":"ALGOUSDT","HBAR":"HBARUSDT","ICP":"ICPUSDT",
    "VET":"VETUSDT","EGLD":"EGLDUSDT","FLOW":"FLOWUSDT",
    "MINA":"MINAUSDT","ROSE":"ROSEUSDT","KAVA":"KAVAUSDT",
    "CELO":"CELOUSDT","ONE":"ONEUSDT","ZIL":"ZILUSDT",
    "IOTA":"IOTAUSDT","XLM":"XLMUSDT","STELLAR":"XLMUSDT",
    "TRX":"TRXUSDT","TRON":"TRXUSDT","EOS":"EOSUSDT",
    "BCH":"BCHUSDT","LTC":"LTCUSDT","ETC":"ETCUSDT",
    "NEO":"NEOUSDT","WAVES":"WAVESUSDT","XTZ":"XTZUSDT",
    "THETA":"THETAUSDT","TFUEL":"TFUELUSDT","CHZ":"CHZUSDT",
    "SUI":"SUIUSDT","APT":"APTUSDT","APTOS":"APTUSDT",
    "SEI":"SEIUSDT","TIA":"TIAUSDT","CELESTIA":"TIAUSDT",
    "DYM":"DYMUSDT","INJ":"INJUSDT","INJECTIVE":"INJUSDT",
    "OSMO":"OSMOUSDT","AKT":"AKTUSDT","SCRT":"SCRTUSDT",
    "KDA":"KDAUSDT","KADENA":"KDAUSDT",
    "CSPR":"CSPRUSDT","CASPER":"CSPRUSDT",
    "CFX":"CFXUSDT","CONFLUX":"CFXUSDT",
    "OAS":"OASUSDT","OASYS":"OASUSDT",
    "VLX":"VLXUSDT","VELAS":"VLXUSDT",
    "TLOS":"TLOSUSDT","CANTO":"CANTOUSDT",
    "KLAY":"KLAYUSDT","KLAYTN":"KLAYUSDT",
    "AURORA":"AURORAUSDT","WAN":"WANUSDT",
    "AURORA2":"AURORAUSDT","NYM":"NYMUSDT",
    "EVMOS":"EVMOSUSDT","JUNO":"JUNOUSDT",
    "LUNA":"LUNAUSDT","LUNC":"LUNCUSDT",
    "XMR":"XMRUSDT","MONERO":"XMRUSDT",
    "ZEC":"ZECUSDT","ZCASH":"ZECUSDT",
    "DASH":"DASHUSDT","XEM":"XEMUSDT",
    "QTUM":"QTUMUSDT","ZEN":"ZENUSDT",
    "DGB":"DGBUSDT","RVN":"RVNUSDT",
    "SC":"SCUSDT","DCR":"DCRUSDT",
    "KMD":"KMDUSDT","LSK":"LSKUSDT",
    "ARDR":"ARDRUSDT","NANO":"NANOUSDT",
    "STEEM":"STEEMUSDT","XVG":"XVGUSDT",
    "BTT":"BTTCUSDT","WIN":"WINUSDT",
    "SXP":"SXPUSDT","OGN":"OGNUSDT",
    "DENT":"DENTUSDT","FUN":"FUNUSDT",
    "TNT":"TNTUSDT","MOD":"MODUSDT",
    "REQ":"REQUSDT","POWR":"POWRUSDT",
    "AMB":"AMBUSDT","MTL":"MTLUSDT",
    "WTC":"WTCUSDT","PHB":"PHBUSDT",

    # ── Layer 2 / Scaling ──
    "ARB":"ARBUSDT","ARBITRUM":"ARBUSDT",
    "OP":"OPUSDT","OPTIMISM":"OPUSDT",
    "STRK":"STRKUSDT","STARKNET":"STRKUSDT",
    "ZK":"ZKUSDT","ZKSYNC":"ZKUSDT",
    "BLAST":"BLASTUSDT","SCROLL":"SCROLLUSDT",
    "MANTA":"MANTAUSDT","METIS":"METISUSDT",
    "BOBA":"BOBAUSDT","IMX":"IMXUSDT",
    "LRC":"LRCUSDT","SKL":"SKLUSDT",
    "CELR":"CELRUSDT","SYN":"SYNUSDT",
    "MULTI":"MULTIUSDT","ZETA":"ZETAUSDT",
    "TAIKO":"TAIKOUSDT","GLMR":"GLMRUSDT",
    "MOVR":"MOVRUSDT","ACX":"ACXUSDT",

    # ── DeFi ──
    "UNI":"UNIUSDT","UNISWAP":"UNIUSDT",
    "AAVE":"AAVEUSDT","CRV":"CRVUSDT",
    "MKR":"MKRUSDT","SNX":"SNXUSDT",
    "COMP":"COMPUSDT","SUSHI":"SUSHIUSDT",
    "YFI":"YFIUSDT","BAL":"BALUSDT",
    "1INCH":"1INCHUSDT","DYDX":"DYDXUSDT",
    "GMX":"GMXUSDT","GNS":"GNSUSDT",
    "PENDLE":"PENDLEUSDT","RDNT":"RDNTUSDT",
    "VELO":"VELOUSDT","PERP":"PERPUSDT",
    "STG":"STGUSDT","WOO":"WOOUSDT",
    "RUNE":"RUNEUSDT","THORCHAIN":"RUNEUSDT",
    "COW":"COWUSDT","BLUR":"BLURUSDT",
    "LOOKS":"LOOKSUSDT","X2Y2":"X2Y2USDT",
    "BADGER":"BADGERUSDT","ALCX":"ALCXUSDT",
    "SPELL":"SPELLUSDT","OHM":"OHMUSDT",
    "TOKE":"TOKEUSDT","IDLE":"IDLEUSDT",
    "INDEX":"INDEXUSDT","FARM":"FARMUSDT",
    "TRIBE":"TRIBEUSDT","RBN":"RBNUSDT",
    "FRAX":"FRAXUSDT","FPI":"FPIUSDT",
    "ALPACA":"ALPACAUSDT","FOR":"FORUSDT",
    "DODO":"DODOUSDT","BURGER":"BURGERUSDT",
    "AUTO":"AUTOUSDT","BAKE":"BAKEUSDT",
    "CREAM":"CREAMUSDT","EPS":"EPSUSDT",
    "HARD":"HARDUSDT","NMX":"NMXUSDT",
    "BELT":"BELTUSDT","POLS":"POLSUSDT",
    "SWAP":"SWAPUSDT","DERI":"DERIUSDT",
    "GFI":"GFIUSDT","TRU":"TRUUSDT",
    "MPL":"MAPLUSDT","CFG":"CFGUSDT",
    "ONDO":"ONDOUSDT","PAXG":"PAXGUSDT",
    "BIFI":"BIFIUSDT","LINA":"LINAUSDT",
    "REEF":"REEFUSDT","CTXC":"CTXCUSDT",
    "COS":"COSUSDT","PERL":"PERLUSDT",
    "DREP":"DREPUSDT","LTO":"LTOUSDT",
    "BEL":"BELUSDT","WING":"WINGUSDT",
    "UNFI":"UNFIUSDT","CHR":"CHRUSDT",
    "XVS":"XVSUSDT","VENUS":"XVSUSDT",
    "FOR2":"FORUSDT","MDT":"MDTUSDT",
    "DEGO":"DEGOUSDT","PROM":"PROMUSDT",
    "BOND":"BONDUSDT","TORN":"TORNUSDT",
    "ALPHA":"ALPHAUSDT","FRONT":"FRONTUSDT",
    "HEGIC":"HEGICUSDT","COVER":"COVERUSDT",
    "MIRROR":"MIRUSDT","MIR":"MIRUSDT",
    "QUICK":"QUICKUSDT","DFYN":"DFYNUSDT",
    "GALA2":"GALAUSDT","REVV":"REVVUSDT",

    # ── Solana Ecosystem ──
    "RAY":"RAYUSDT",# "RAYDIUM":"RAYUSDT",  # alias handled by main key
    
    "ORCA":"ORCAUSDT","FIDA":"FIDAUSDT",
    "SAMO":"SAMOUSDT","NEON":"NEONUSDT",
    "DRIFT":"DRIFTUSDT","JTO":"JTOUSDT",
    "JUP":"JUPUSDT","JUPITER":"JUPUSDT",
    "PYTH":"PYTHUSDT","BONK":"BONKUSDT",
    "WIF":"WIFUSDT","BOME":"BOMEUSDT",
    "POPCAT":"POPCATUSDT","SLERF":"SLERFUSDT",
    "MYRO":"MYROUSDT","TNSR":"TNSRUSDT",
    "AEVO":"AEVOUSDT","KMNO":"KMNOUSDT",
    "MERL":"MERLUSDT","STEP":"STEPUSDT",
    "COPE":"COPEUSDT","MEDIA":"MEDIAUSDT",
    "SLND":"SLNDUSDT","PORT":"PORTUSDT",
    "MNGO":"MNGOUSDT","GRAPE":"GRAPEUSDT",

    # ── AI / Web3 ──
    "FET":"FETUSDT","FETCH":"FETUSDT",
    "AGIX":"AGIXUSDT","OCEAN":"OCEANUSDT",
    "NMR":"NMRUSDT","RLC":"RLCUSDT",
    "GRT":"GRTUSDT","RNDR":"RNDRUSDT",
    "WLD":"WLDUSDT","TAO":"TAOUSDT",
    "ALT":"ALTUSDT","AIOZ":"AIOZUSDT",
    "MASA":"MASAUSDT","COOKIE":"COOKIEUSDT",
    "VIRTUAL":"VIRTUALUSDT","AI16Z":"AI16ZUSDT",
    "AIXBT":"AIXBTUSDT","ARC":"ARCUSDT",
    "GRIFFAIN":"GRIFFAINUSDT","SKYAI":"SKYAIUSDT",
    "KAITO":"KAITOUSDT","BERA":"BERAUSDT",
    "LAYER":"LAYERUSDT","PARTI":"PARTIUSDT",
    "INIT":"INITUSDT","SIGN":"SIGNUSDT",
    "IP":"IPUSDT","ANIME":"ANIMEUSDT",
    "DEEP":"DEEPUSDT","SOLV":"SOLVUSDT",
    "WAL":"WALUSDT","BIO":"BIOUSDT",
    "VANA":"VANAUSDT","FORM":"FORMUSDT",
    "PLUME":"PLUMEUSDT","NIL":"NILUSDT",
    "HYPE":"HYPEUSDT","ME":"MEUSDT",
    "USUAL":"USUALUSDT","SPX":"SPXUSDT",

    # ── Gaming / Metaverse ──
    "AXS":"AXSUSDT",# "AXIE":"AXSUSDT",  # alias handled by main key
    
    "SAND":"SANDUSDT","MANA":"MANAUSDT",
    "GALA":"GALAUSDT","GMT":"GMTUSDT",
    "GODS":"GODSUSDT","YGG":"YGGUSDT",
    "BEAM":"BEAMUSDT","PRIME":"PRIMEUSDT",
    "RON":"RONUSDT","RONIN":"RONUSDT",
    "PIXEL":"PIXELUSDT","PORTAL":"PORTALUSDT",
    "SUPER":"SUPERUSDT","MAVIA":"MAVIAUSDT",
    "VANRY":"VANRYUSDT","ACE":"ACEUSDT",
    "ILV":"ILVUSDT","SLP":"SLPUSDT",
    "ALICE":"ALICEUSDT","TLM":"TLMUSDT",
    "LOKA":"LOKAUSDT","HERO":"HEROUSDT",
    "WILD":"WILDUSDT","GHST":"GHSTUSDT",
    "TOWER":"TOWERUSDT","PYR":"PYRUSDT",
    "FEVR":"FEVRUSDT","RACA":"RACAUSDT",
    "MBOX":"MBOXUSDT","NFTB":"NFTBUSDT",
    "CHESS":"CHESSUSDT","DAR":"DARUSDT",
    "PLA":"PLAUSDT","EPIK":"EPIKPUSDT",
    "MOBOX":"MBOXUSDT","FEAR":"FEARUSDT",
    "REVV":"REVVUSDT","SHROOM":"SHROOMUSDT",
    "ZOON":"ZOONUSDT","MIST":"MISTUSDT",
    "DPET":"DPETUSDT","SKILL":"SKILLUSDT",

    # ── Fan Tokens ──
    "BAR":"BARUSDT","JUV":"JUVUSDT",
    "PSG":"PSGUSDT","ACM":"ACMUSDT",
    "INTER":"INTERUSDT","LAZIO":"LAZIOUSDT",
    "ATM":"ATMUSDT","OG":"OGUSDT",
    "ALPINE":"ALPINEUSDT","SANTOS":"SANTOSUSDT",
    "CITY":"CITYUSDT","POR":"PORUSDT",
    "ASR":"ASRUSDT","AFC":"AFCUSDT",
    "NAP":"NAPUSDT","SG":"SGUSDT",
    "LEVANTE":"LEVANTEUSDT","GOS":"GOSUSDT",

    # ── Meme coins ──
    "DOGE":"DOGEUSDT","DOGECOIN":"DOGEUSDT",
    "SHIB":"SHIBUSDT","SHIBA":"SHIBUSDT",
    "PEPE":"PEPEUSDT","FLOKI":"FLOKIUSDT",
    "TURBO":"TURBOUSDT","BRETT":"BRETTUSDT",
    "NEIRO":"NEIROUSDT","MOG":"MOGUSDT",
    "TRUMP":"TRUMPUSDT","MELANIA":"MELANIAUSDT",
    "FARTCOIN":"FARTCOINUSDT","GOAT":"GOATUSDT",
    "PNUT":"PNUTUSDT","ACT":"ACTUSDT",
    "MOODENG":"MOODENGUSDT","CHILLGUY":"CHILLGUYUSDT",
    "BONK2":"BONKUSDT","WIF2":"WIFUSDT",
    "BABYDOGE":"BABYDOGEUSDT","ELON":"ELONUSDT",
    "SAMO2":"SAMOUSDT","CHEEMS":"CHEEMSUSDT",
    "WOJAK":"WOJAKUSDT","BONE":"BONEUSDT",
    "LEASH":"LEASHUSDT","SNEK":"SNEKUSDT",
    "MYRO2":"MYROUSDT","POPCAT2":"POPCATUSDT",
    "PEPE2":"PEPE2USDT","COQ":"COQUSDT",
    "MAGA":"TRUMPUSDT","BOME2":"BOMEUSDT",
    "GFOX":"GFOXUSDT","PORK":"PORKUSDT",
    "LANDWOLF":"LANDWOLFUSDT","MOZ":"MOZUSDT",
    "ANDY":"ANDYUSDT","TOSHI":"TOSHIUSDT",
    "DEGEN":"DEGENUSDT","HIGHER":"HIGHERUSDT",

    # ── Oracle / Data ──
    "LINK":"LINKUSDT","CHAINLINK":"LINKUSDT",
    "BAND":"BANDUSDT","API3":"API3USDT",
    "DIA":"DIAUSDT","TRB":"TRBUSDT",
    "UMA":"UMAUSDT","SUPRA":"SUPRAUSDT",
    "PYTH2":"PYTHUSDT","TELL":"TELLUSDT",
    "FLUX":"FLUXUSDT","COTI":"COTIUSDT",

    # ── Exchange Tokens ──
    "CRO":"CROUSDT","CRONOS":"CROUSDT",
    "OKB":"OKBUSDT","KCS":"KCSUSDT",
    "GT":"GTUSDT","HT":"HTUSDT",
    "MX":"MXUSDT","WRX":"WRXUSDT",
    "BNX":"BNXUSDT","TWT":"TWTUSDT",

    # ── Infrastructure ──
    "FIL":"FILUSDT","FILECOIN":"FILUSDT",
    "AR":"ARUSDT","ARWEAVE":"ARUSDT",
    "STORJ":"STORJUSDT","HNT":"HNTUSDT",
    "GLM":"GLMUSDT","NKN":"NKNUSDT",
    "POKT":"POKTUSDT","LPT":"LPTUSDT",
    "LIVEPEER":"LPTUSDT","MYST":"MYSTUSDT",
    "PHA":"PHAUSDT","FLUX2":"FLUXUSDT",
    "CLORE":"CLOREUSDT","CUDOS":"CUDOSUSDT",
    "REN":"RENUSDT","KEEP":"KEEPUSDT",
    "NU":"NUUSDT","CTSI":"CTSIUSDT",
    "ARPA":"ARPAUSDT","MITH":"MITHUSDT",
    "COCOS":"COCOSUSDT","VIDT":"VIDTUSDT",
    "TROY":"TROYUSDT","PROS":"PROSUSDT",
    "PUNDIX":"PUNDIXUSDT","DF":"DFUSDT",
    "AUCTION":"AUCTIONUSDT","BETA":"BETAUSDT",
    "TVK":"TVKUSDT","AKRO":"AKROUSDT",
    "HIVE":"HIVEUSDT","STPT":"STPTUSDT",
    "OM":"OMUSDT","IRIS":"IRISUSDT",
    "FOR3":"FORUSDT","BURGER2":"BURGERUSDT",

    # ── Social / Identity ──
    "MASK":"MASKUSDT","GTC":"GTCUSDT",
    "RAD":"RADUSDT","RSS3":"RSS3USDT",
    "CKB":"CKBUSDT","CYBER":"CYBERUSDT",
    "ID":"IDUSDT","DESO":"DESOUSDT",
    "SPACE":"SPACEUSDT","CLV":"CLVUSDT",
    "FORTH":"FORTHUSDT","REQ2":"REQUSDT",
    "IDEX":"IDEXUSDT","RARE":"RAREUSDT",
    "CONV":"CONVUSDT","DERC":"DERCUSDT",

    # ── Cross-chain ──
    "HOP":"HOPUSDT","MULTI2":"MULTIUSDT",
    "SYN2":"SYNUSDT","CELR2":"CELRUSDT",
    "ACX2":"ACXUSDT","STG2":"STGUSDT",
    "ACROSS":"ACXUSDT","POLS2":"POLSUSDT",

    # ── New 2024-2025 ──
    "ENA":"ENAUSDT","ETHENA":"ENAUSDT",
    "BB":"BBUSDT","OMNI":"OMNIUSDT",
    "SAGA":"SAGAUSDT","W":"WUSDT",
    "IO":"IOUSDT","LISTA":"LISTAUSDT",
    "BANANA":"BANANAUSDT","DOGS":"DOGSUSDT",
    "HMSTR":"HMSTRUSDT","MAJOR":"MAJORUSDT",
    "CATI":"CATIUSDT","NOT":"NOTUSDT",
    "NOTCOIN":"NOTUSDT","PANDORA":"PANDORAUSDT",
    "ZEREBRO":"ZEREBROUSDT","MOVE":"MOVEUSDT",
    "PENGU":"PENGUUSDT","TST":"TSTUSDT",
    "RED":"REDUSDT","SHELL":"SHELLUSDT",
    "SYRUP":"SYRUPUSDT","KITE":"KITEUSDT",
    "QUIZ":"QUIZUSDT","THE":"THEUSDT",
    "VINE":"VINEUSDT","GOLD":"GOLDUSDT",
    "B":"BUSDT","WAL2":"WALUSDT",

    # ── Privacy ──
    "XMR2":"XMRUSDT","ZEC2":"ZECUSDT",
    "DASH2":"DASHUSDT","FIRO":"FIROUSDT",
    "NAV":"NAVUSDT","DUSK":"DUSKUSDT",
    "BEAM2":"BEAMUSDT","SCRT2":"SCRTUSDT",

    # ── Misc popular ──
    "ENJ":"ENJUSDT","ENJIN":"ENJUSDT",
    "BAT":"BATUSDT","BRAVE":"BATUSDT",
    "ZRX":"ZRXUSDT","KNC":"KNCUSDT",
    "SNT":"SNTUSDT","STATUS":"SNTUSDT",
    "GVT":"GVTUSDT","CHAT":"CHATUSDT",
    "BCPT":"BCPTUSDT","VIBE":"VIBEUSDT",
    "OST":"OSTUSDT","TNB":"TNBUSDT",
    "BRD":"BRDUSDT","GXS":"GXSUSDT",
    "NCASH":"NCASHUSDT","APPC":"APPCUSDT",
    "SALT":"SALTUSDT","RCN":"RCNUSDT",
    "QSP":"QSPUSDT","WABI":"WABIUSDT",
    "GAS":"GASUSDT","ARK":"ARKUSDT",
    "XZC":"XZCUSDT","PIVX":"PIVXUSDT",
    "MTH":"MTHUSDT","SUB":"SUBUSDT",
    "OAX":"OAXUSDT","DNT":"DNTUSDT",
    "BNT":"BNTUSDT","AST":"ASTUSDT",
    "SNM":"SNMUSDT","EVX":"EVXUSDT",
    "FUEL":"FUELUSDT","MDA":"MDAUSDT",
    "CND":"CNDUSDT","XVR":"XVRUSDT",
    "POE":"POEUSDT","QLC":"QLCUSDT",
    "SYS":"SYSUSDT","GTO":"GTOUSDT",
    "ELF":"ELFUSDT","IOST":"IOSTUSDT",
    "AION":"AIONUSDT","WINGS":"WINGSUSDT",
    "BTS":"BTSUSDT","PIVX2":"PIVXUSDT",
    "BCOIN":"BCOINUSDT","MBL":"MBLUSDT",
    "TFUEL2":"TFUELUSDT","BCDN":"BCDNUSDT",
    "ATA":"ATAUSDT","FARM2":"FARMUSDT",
    "ORN":"ORNUSDT","UTK":"UTKUSDT",
    "WTC":"WTCUSDT","LOOM":"LOOMUSDT",
    "BLZ":"BLZUSDT","STORM":"STORMUSDT",
    "GNT":"GNTUSDT","SKY":"SKYUSDT",
    "RPX":"RPXUSDT","YOYOW":"YOYOWUSDT",
    "ETN":"ETNUSDT","DOCK":"DOCKUSDT",
    "POLY":"POLYUSDT","GBX":"GBXUSDT",
    "CLOAKC":"CLOAKCUSDT","MOD2":"MODUSDT",
    "ENJ2":"ENJUSDT","STORJ2":"STORJUSDT",
    "DATA":"DATAUSDT","MANA2":"MANAUSDT",
    "THETA2":"THETAUSDT","TFUEL3":"TFUELUSDT",
    "QKC":"QKCUSDT","AION2":"AIONUSDT",
    "GO":"GOUSDT","PAX":"PAXUSDT",
    "NPXS":"NPXSUSDT","COFI":"COFIUSDT",
    "ELEC":"ELECUSDT","CHAT2":"CHATUSDT",
    "IOTA2":"IOTAUSDT","BCD":"BCDUSDT",
    "BTG":"BTGUSDT","HSR":"HSRUSDT",
    "OAK":"OAKUSDT","VIA":"VIAUSDT",
    "EDO":"EDOUSDT","INS":"INSUSDT",
    "PPT":"PPTUSDT","REX":"REXUSDT",
    "CDT":"CDTUSDT","GXC":"GXCUSDT",
    "LEND":"LENDUSDT","TNB2":"TNBUSDT",
    "NULS":"NULSUSDT","VIB":"VIBUSDT",
    "MANA3":"MANAUSDT","STRAT":"STRATUSDT",
    "SNGLS":"SNGLSUSDT","BQX":"BQXUSDT",
    "KNC2":"KNCUSDT","FUN2":"FUNUSDT",
    "SNM2":"SNMUSDT","NEO2":"NEOUSDT",
    "IOTX":"IOTXUSDT","ENG":"ENGUSDT",
    "ZIL2":"ZILUSDT","NCASH2":"NCASHUSDT",
    "POA":"POAUSDT","ZEN2":"ZENUSDT",
    "SKY2":"SKYUSDT","MITH2":"MITHUSDT",
    "SC2":"SCUSDT","NANO2":"NANOUSDT",
    "XEM2":"XEMUSDT","DENT2":"DENTUSDT",
    "IOS":"IOST2USDT","ARDR2":"ARDRUSDT",
}

def get_live_price(ticker: str) -> str:
    """Fetch live Binance price with 5-min cache."""
    import time
    ticker = ticker.upper().replace("/USDT","").replace("/USD","")
    pair   = PAIR_MAP.get(ticker, ticker + "USDT")

    # Check cache
    if pair in _price_cache:
        cached_price, cached_time = _price_cache[pair]
        if time.time() - cached_time < PRICE_TTL:
            return cached_price

    try:
        resp = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": pair},
            timeout=5,
        )
        if resp.status_code == 200:
            price = str(round(float(resp.json()["price"]), 4))
            _price_cache[pair] = (price, time.time())
            log.debug("Binance price %s = $%s", pair, price)
            return price
        # Try CoinGecko as fallback
        cg_ids = {"BTCUSDT":"bitcoin","ETHUSDT":"ethereum","SOLUSDT":"solana",
                  "XRPUSDT":"ripple","BNBUSDT":"binancecoin"}
        cg_id = cg_ids.get(pair)
        if cg_id:
            r2 = requests.get(
                f"https://api.coingecko.com/api/v3/simple/price",
                params={"ids":cg_id,"vs_currencies":"usd"},
                timeout=5,
            )
            if r2.status_code == 200:
                price = str(r2.json()[cg_id]["usd"])
                _price_cache[pair] = (price, time.time())
                return price
    except Exception as e:
        log.debug("Price fetch error %s: %s", ticker, e)
    return "NA"

# Cache all Binance tickers (refreshed every hour)
_binance_tickers: dict[str, str] = {}
_tickers_last_fetch: float = 0
TICKERS_TTL = 3600  # 1 hour

def get_all_binance_tickers() -> dict[str, str]:
    """
    Fetch ALL USDT pairs from Binance.
    Returns {symbol: price} e.g. {"BTC": "107000", "ETH": "3500"}
    Cached for 1 hour.
    """
    import time
    global _binance_tickers, _tickers_last_fetch
    if _binance_tickers and time.time() - _tickers_last_fetch < TICKERS_TTL:
        return _binance_tickers
    try:
        resp = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            tickers = {}
            for item in data:
                sym = item["symbol"]
                if sym.endswith("USDT"):
                    coin = sym[:-4]  # remove USDT
                    tickers[coin] = str(round(float(item["price"]), 6))
            _binance_tickers = tickers
            _tickers_last_fetch = time.time()
            log.info("Binance: loaded %d USDT pairs", len(tickers))
            return tickers
        else:
            log.warning("Binance ticker fetch failed: %s", resp.status_code)
    except Exception as e:
        log.error("Binance all tickers error: %s", e)
    return _binance_tickers  # return cached even if stale


def extract_coin_from_title(title: str) -> tuple[str, str]:
    """
    Dynamically extract coin from headline using ALL Binance tickers.
    1. Check static PAIR_MAP for common names/aliases
    2. Search ALL Binance tickers for any ticker mentioned in title
    Returns (symbol/USDT, live_price)
    """
    title_upper = title.upper()

    # 1. Static map first (handles names like "Bitcoin", "Ethereum" etc)
    for name, pair in PAIR_MAP.items():
        if name in title_upper:
            ticker = pair.replace("USDT","")
            price  = _binance_tickers.get(ticker) or get_live_price(ticker)
            if price and price != "NA":
                return ticker + "/USDT", price

    # 2. Dynamic search in all Binance tickers
    all_tickers = get_all_binance_tickers()
    # Sort by length descending so longer matches win (e.g. DOGE before D)
    sorted_tickers = sorted(all_tickers.keys(), key=len, reverse=True)
    for ticker in sorted_tickers:
        if len(ticker) < 2:  # skip single letter tickers
            continue
        # Look for ticker as whole word in title
        import re as _re
        if _re.search(r'\b' + _re.escape(ticker) + r'\b', title_upper):
            price = all_tickers[ticker]
            return ticker + "/USDT", price

    return "CRYPTO/USDT", "NA"


def analyse_item(title: str, summary: str, intel: dict | None = None) -> dict | None:
    # Extract coin + live price from Binance
    detected_symbol, live_price = extract_coin_from_title(title)
    if live_price != "NA":
        log.info("Live price for %s = $%s", detected_symbol, live_price)
    prompt = build_prompt(title, summary, live_price, intel)
    prompt_tokens_est = len(prompt) // 4
    log.debug("OpenAI call | ~%d input tokens | %.60s", prompt_tokens_est, title)
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.2,
            max_tokens=150,
            response_format={"type": "json_object"},
        )
        raw    = response.choices[0].message.content
        result = json.loads(raw)

        if result.get("signal") == "SKIP":
            return {"signal": "SKIP"}

        for key in ("sentiment", "signal", "reason", "target_price"):
            if key not in result:
                raise ValueError(f"Missing key: {key}")

        result["sentiment"]    = str(result["sentiment"]).strip().capitalize()
        result["signal"]       = str(result["signal"]).strip().upper()
        result["target_price"] = str(result.get("target_price", "NA")).strip()

        if result["signal"] not in ("BUY", "SELL", "WAIT"):
            return {"signal": "SKIP"}

        log.info("OpenAI → [%s] %s | target: %s",
                 result["signal"], result["sentiment"], result["target_price"])
        return result

    except json.JSONDecodeError as e:
        log.warning("OpenAI invalid JSON for '%.50s': %s", title, e)
        return None
    except Exception as e:
        log.error("OpenAI error for '%.50s': %s", title, e)
        return None

# ─────────────────────────────────────────────
# Background Job
# ─────────────────────────────────────────────
def send_telegram_signal(signal_data):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        sym    = str(signal_data.get("symbol", "CRYPTO/USDT"))
        sig    = str(signal_data.get("signal", "WAIT"))
        dirn   = str(signal_data.get("direction", "המתן"))
        conv   = str(signal_data.get("conviction", "MEDIUM"))
        entry  = str(signal_data.get("entry", "NA"))
        sl     = str(signal_data.get("stop_loss", "NA"))
        t1     = str(signal_data.get("target1", "NA"))
        t2     = str(signal_data.get("target2", "NA"))
        t3     = str(signal_data.get("target3", "NA"))
        lev    = str(signal_data.get("leverage", "x10"))
        reason = str(signal_data.get("reason", ""))
        title  = str(signal_data.get("news_title", ""))[:100]
        source = str(signal_data.get("source", ""))
        title_he  = str(signal_data.get("news_title_he", title)) or title
        reason_he = str(signal_data.get("reason_he", reason)) or reason
        score_val = str(signal_data.get("score", 50))
        pos_size = str(signal_data.get("position_size","3"))
        rr       = str(signal_data.get("rr_ratio","0"))
        warning  = str(signal_data.get("risk_warning",""))
        explain  = str(signal_data.get("simple_explanation") or signal_data.get("simple_explain") or "")

        liq_p  = str(signal_data.get("liq_price","NA"))

        msg = (
            "🔮 חוזה עתידי — " + sig + " | " + sym + "\n"
            "ניקוד: " + score_val + "/100 | " + conv + "\n\n"
            "📍 כניסה: $" + entry + "\n"
            "🛑 סטופ לוס: $" + sl + "\n"
            "💥 מחיר חיסול: $" + liq_p + "\n"
            "⚙️ מינוף: x" + lev + "\n\n"
            "🎯 מטרה 1 (50%): $" + t1 + "\n"
            "🎯 מטרה 2 (30%): $" + t2 + "\n"
            "🎯 מטרה 3 (20%): $" + t3 + "\n\n"
            "💼 גודל פוזיציה: " + pos_size + "% מהתיק\n"
            "⚖️ סיכון/תשואה: 1:" + rr + "\n"
            "🛡️ מקס׳ סיכון כולל: 25% מהתיק\n"
            + ("\n⚠️ " + warning if warning else "") + "\n\n"
            "💡 " + explain + "\n\n"
            "📰 " + title_he + "\n"
            "🔍 " + reason_he + "\n"
            "📡 מקור: " + source
        )
        # Signals are WEBSITE ONLY — Telegram is for news/learning only
        log.info("[Signal] Saved to website: %s %s", sig, sym)
    except Exception as e:
        log.error("[Telegram] Error: %s", e)


# Track which news we already sent to Telegram (avoid duplicates)
_sent_news_fingerprints: set = set()


def send_telegram_news(news_item: dict, chat_id: str = "") -> bool:
    """
    Send a beautiful educational news post to the Telegram learning channel.
    Format: Breaking header + Hebrew title + 5 learning layers + footer.
    Uses TELEGRAM_NEWS_TOKEN (NexusNewsChannel_bot).
    """
    if not TELEGRAM_NEWS_TOKEN:
        return False

    target_chat = chat_id or TELEGRAM_NEWS_CHAT or TELEGRAM_CHAT
    if not target_chat:
        return False

    # Dedup — never send same news twice
    fp = (news_item.get("title", "")[:60]).lower().strip()
    if not fp or fp in _sent_news_fingerprints:
        return False

    try:
        title_he  = str(news_item.get("title_he")  or "").strip()
        title_en  = str(news_item.get("title")      or "").strip()
        source    = str(news_item.get("source")     or "")
        heat      = int(news_item.get("heat")       or 0)
        cat       = str(news_item.get("category")   or "GENERAL")
        why_hot   = str(news_item.get("why_hot")    or "").strip()
        what_is   = str(news_item.get("what_is")    or "").strip()
        impact    = str(news_item.get("market_impact")   or "").strip()
        lesson    = str(news_item.get("trading_lesson")  or "").strip()
        action    = str(news_item.get("action")     or "").strip()
        coins     = str(news_item.get("affected_coins")  or "").strip()
        sentiment = str(news_item.get("sentiment")  or
                        news_item.get("sentiment_news") or "NEUTRAL").strip()
        age_min   = int(news_item.get("age_minutes") or 0)
        cred      = int(news_item.get("credibility") or 5)

        # Auto-translate title if no Hebrew version yet
        if not title_he and title_en:
            try:
                title_he = translate_to_hebrew(title_en)
            except Exception:
                title_he = title_en

        display_title = title_he or title_en

        # ── Emoji sets ──
        heat_fires = "🔥" * min(5, max(1, heat // 2))
        sent_label = {
            "BULLISH":  "📈 שורי  ✅",
            "BEARISH":  "📉 דובי  ⚠️",
            "NEUTRAL":  "➡️ נייטרלי",
        }.get(sentiment, "➡️ נייטרלי")

        cat_icon = {
            "BTC": "₿", "ETH": "Ξ", "ALTCOIN": "🔷",
            "DeFi": "⚡", "REGULATION": "⚖️", "WHALE": "🐋",
            "SECURITY": "🔐", "NFT/WEB3": "🎨",
            "EDUCATIONAL": "📚", "ANALYSIS": "📊", "GENERAL": "📰",
        }.get(cat, "📰")

        cat_he = {
            "BTC": "ביטקוין", "ETH": "אית׳ריום", "ALTCOIN": "אלטקוין",
            "DeFi": "DeFi — פיננסים מבוזרים", "REGULATION": "רגולציה",
            "WHALE": "לוויתן — כסף גדול זזה", "SECURITY": "אבטחה / פריצה",
            "NFT/WEB3": "NFT / Web3", "EDUCATIONAL": "חינוכי",
            "ANALYSIS": "ניתוח שוק", "GENERAL": "חדשות כלליות",
        }.get(cat, cat)

        cred_label = "⭐ מקור Tier-1" if cred >= 9 else "✓ מקור אמין" if cred >= 7 else ""
        age_label  = f"לפני {age_min} דק׳" if age_min else "עכשיו"

        # ════════════════════════════════
        # Build message — 5 learning layers
        # ════════════════════════════════
        lines = []

        # ── TOP HEADER ──
        if heat >= 8:
            lines.append("🔴🔴 *BREAKING NEWS* 🔴🔴")
        elif heat >= 6:
            lines.append("🟠 *חדשות חמות*")
        else:
            lines.append("📰 *חדשות קריפטו*")

        lines.append("╔══════════════════════════╗")
        lines.append(f"  {cat_icon}  *{display_title}*")
        lines.append("╚══════════════════════════╝")
        lines.append("")

        # ── META LINE ──
        lines.append(f"{heat_fires}  חום: {heat}/10  |  {sent_label}")
        lines.append(f"🏷  {cat_he}  |  🕐  {age_label}")
        if coins:
            lines.append(f"🪙  מטבעות: *{coins}*")
        lines.append("")
        lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")
        lines.append("")

        # ── LAYER 1: What is it? ──
        if what_is:
            lines.append("📖  *מה זה בכלל?*")
            lines.append(f"_{what_is}_")
            lines.append("")

        # ── LAYER 2: Why hot? ──
        if why_hot:
            lines.append("💡  *למה זה חשוב עכשיו?*")
            lines.append(why_hot)
            lines.append("")

        # ── LAYER 3: Market impact ──
        if impact:
            lines.append("📊  *איך זה ישפיע על השוק?*")
            lines.append(impact)
            lines.append("")

        # ── LAYER 4: Trading lesson ──
        if lesson:
            lines.append("🎓  *שיעור לטריידר*")
            lines.append(lesson)
            lines.append("")

        # ── LAYER 5: What to do ──
        if action:
            lines.append("🔮  *מה לעשות בפיוצ׳רס?*")
            lines.append(f"*{action}*")
            lines.append("")

        # ── FOOTER ──
        lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")
        src_line = f"📡  {source}"
        if cred_label:
            src_line += f"  {cred_label}"
        lines.append(src_line)
        lines.append("🤖  *NEXUS AI*  |  @cySignals\\_Official")
        lines.append("")
        lines.append("_הצטרפו לערוץ ללמוד קריפטו בעברית_ 🇮🇱")

        msg = "\n".join(lines)

        # Telegram has 4096 char limit — trim if needed
        if len(msg) > 4000:
            msg = msg[:3950] + "\n\n_...המשך בערוץ_"

        resp = requests.post(
            "https://api.telegram.org/bot" + TELEGRAM_NEWS_TOKEN + "/sendMessage",
            json={
                "chat_id":                target_chat,
                "text":                   msg,
                "parse_mode":             "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=12,
        )

        if resp.status_code == 200:
            _sent_news_fingerprints.add(fp)
            if len(_sent_news_fingerprints) > 500:
                for _ in range(100):
                    try:
                        _sent_news_fingerprints.pop()
                    except Exception:
                        break
            log.info("[Telegram-EDU] ✓ Sent to %s: %s", target_chat, display_title[:55])
            return True
        else:
            log.warning("[Telegram-EDU] HTTP %d — %s", resp.status_code, resp.text[:150])
            return False

    except Exception as e:
        log.error("[Telegram-EDU] Error: %s", e)
        return False


# ═══════════════════════════════════════════
# EDUCATIONAL KNOWLEDGE BASE — יומי לערוץ
# 20+ הודעות ידע קריפטו בעברית
# ═══════════════════════════════════════════

CRYPTO_KNOWLEDGE_BANK = [

    # ══ BEGINNER LEVEL ══

    {
        "title": "מה זה ביטקוין?",
        "icon": "₿", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "ביטקוין הוא מטבע דיגיטלי מבוזר — אין בנק מרכזי שמנהל אותו.\n\n"
            "📅 נוצר ב-2009 על ידי *ساتوشي ناكاموتو* (Satoshi Nakamoto) — זהותו עדיין לא ידועה.\n\n"
            "🔑 *3 עקרונות בסיסיים:*\n"
            "1. מכסה קשיח — רק 21 מיליון BTC אי פעם\n"
            "2. בלוקצ׳יין — כל עסקה מתועדת לנצח\n"
            "3. Peer-to-Peer — אפשר לשלוח ישירות בלי בנק\n\n"
            "💡 BTC נקרא גם *דיגיטל גולד* — חנות ערך לטווח ארוך."
        ),
        "tip": "ביטקוין הוא השכבה הראשונה. כל שאר הקריפטו בנוי על הרעיון שלו.",
    },

    {
        "title": "מה זה בלוקצ׳יין?",
        "icon": "⛓", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "בלוקצ׳יין הוא מסד נתונים מבוזר שרץ על אלפי מחשבים בו זמנית.\n\n"
            "🔷 *מה זה בלוק?*\n"
            "קבוצה של עסקות שנארזו יחד ונחתמו קריפטוגרפית.\n\n"
            "🔗 *מה זה שרשרת?*\n"
            "כל בלוק מכיל את ה-hash של הבלוק הקודם — כך לא אפשר לשנות היסטוריה.\n\n"
            "✅ *למה זה מהפכני?*\n"
            "• שקוף — כולם יכולים לראות\n"
            "• בלתי ניתן לזיוף\n"
            "• אין נקודת כשל מרכזית"
        ),
        "tip": "blockchain.com מאפשר לראות כל עסקת ביטקוין שנעשתה אי פעם.",
    },

    {
        "title": "מה זה ארנק קריפטו?",
        "icon": "👛", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "ארנק קריפטו הוא לא 'ארנק' אמיתי — הוא מפתח פרטי שמאפשר גישה למטבעות שלך בבלוקצ׳יין.\n\n"
            "🔑 *שני סוגים:*\n\n"
            "🏦 *Custodial* (בורסה) — Binance, Coinbase\n"
            "  → פשוט, אבל הבורסה שולטת בכסף שלך\n"
            "  → \"Not your keys, not your coins\"\n\n"
            "💾 *Non-Custodial* — MetaMask, Ledger\n"
            "  → אתה שולט, אבל אחראי לשמור את ה-Seed Phrase\n\n"
            "⚠️ *כלל אחד:* לעולם אל תשתף Seed Phrase עם אף אחד!"
        ),
        "tip": "Ledger / Trezor = ארנק חומרה. הדרך הבטוחה ביותר לאחסון לטווח ארוך.",
    },

    {
        "title": "מה זה מינוף (Leverage)?",
        "icon": "🎚", "category": "מתחילים", "level": "🟡 בינוני",
        "body": (
            "מינוף מאפשר לסחור בסכום גדול יותר ממה שיש לך.\n\n"
            "📊 *דוגמה עם x10 מינוף:*\n"
            "💵 יש לך: $1,000\n"
            "💪 כוח קנייה: $10,000\n"
            "📈 עלייה 5%: רווח $500 (50% על ההון!)\n"
            "📉 ירידה 5%: הפסד $500 (50% מההון)\n"
            "📉 ירידה 10%: חיסול מלא!\n\n"
            "🎚 *מינופים נפוצים:*\n"
            "x2-x5 = שמרני יחסית\n"
            "x10 = ממוצע לטריידרים\n"
            "x20-x100 = מסוכן מאוד"
        ),
        "tip": "מתחילים? תתחיל עם x2-x3 בלבד. רוב הטריידרים מפסידים עם מינוף גבוה.",
    },

    {
        "title": "מה זה Market Order vs Limit Order?",
        "icon": "📋", "category": "מסחר", "level": "🟢 בסיסי",
        "body": (
            "שתי הדרכים העיקריות לקנות/למכור:\n\n"
            "⚡ *Market Order — הוראת שוק*\n"
            "קונה/מוכר מיד במחיר הטוב ביותר הזמין.\n"
            "✅ מהיר | ❌ לא יודע בדיוק באיזה מחיר\n\n"
            "🎯 *Limit Order — הוראת מגבלה*\n"
            "קובע מחיר מדויק. הוצע מתמלא רק כשהמחיר מגיע.\n"
            "✅ שליטה על מחיר | ❌ אולי לא יתמלא\n\n"
            "💡 *מתי להשתמש?*\n"
            "Market = כשמהירות חשובה (FOMO, חדשות פתאומיות)\n"
            "Limit = כשרוצה מחיר מדויק ויש זמן"
        ),
        "tip": "Limit Orders חוסכים עמלות — Makers משלמים פחות מ-Takers.",
    },

    # ══ TECHNICAL ANALYSIS ══

    {
        "title": "מה זה MACD?",
        "icon": "📉", "category": "טכני", "level": "🟡 בינוני",
        "body": (
            "MACD = Moving Average Convergence Divergence\n"
            "אינדיקטור מומנטום שמשווה שתי ממוצעות נעות.\n\n"
            "🔵 *קו MACD* = EMA12 פחות EMA26\n"
            "🟡 *קו Signal* = EMA9 של קו MACD\n"
            "📊 *היסטוגרם* = הפרש בין השניים\n\n"
            "📈 *אות קנייה:* MACD חוצה Signal מלמטה למעלה\n"
            "📉 *אות מכירה:* MACD חוצה Signal מלמעלה למטה\n\n"
            "⚠️ MACD מאחר — זה לג׳ינד אינדיקטור. לא מושלם לכניסה מדויקת."
        ),
        "tip": "MACD עובד הכי טוב על טיים פריים גבוה (4H, 1D). על 1M — הרבה רעש.",
    },

    {
        "title": "מה זה Bollinger Bands?",
        "icon": "📊", "category": "טכני", "level": "🟡 בינוני",
        "body": (
            "רצועות בולינגר = 3 קווים שמייצגים תנודתיות.\n\n"
            "🔵 *קו אמצע* = ממוצע נע 20 יום (SMA20)\n"
            "🔴 *רצועה עליונה* = SMA20 + 2 סטיות תקן\n"
            "🟢 *רצועה תחתונה* = SMA20 - 2 סטיות תקן\n\n"
            "📖 *כיצד לקרוא:*\n"
            "• מחיר נוגע ברצועה עליונה = Overbought\n"
            "• מחיר נוגע ברצועה תחתונה = Oversold\n"
            "• רצועות צרות (Squeeze) = פיצוץ מחיר קרוב!\n"
            "• רצועות רחבות = תנודתיות גבוהה"
        ),
        "tip": "Bollinger Squeeze + עלייה בנפח = הכנה לתנועה גדולה. כיוון לא ידוע!",
    },

    {
        "title": "מה זה EMA vs SMA?",
        "icon": "📈", "category": "טכני", "level": "🟡 בינוני",
        "body": (
            "שתי ממוצעות נעות — הכלים הבסיסיים של ניתוח טכני.\n\n"
            "📊 *SMA — Simple Moving Average*\n"
            "ממוצע פשוט של X ימים אחרונים.\n"
            "מדויק לזיהוי מגמה, מאחר יותר.\n\n"
            "⚡ *EMA — Exponential Moving Average*\n"
            "נותן משקל גבוה יותר לימים האחרונים.\n"
            "מגיב מהר יותר לשינויים.\n\n"
            "🔑 *הצלבות חשובות:*\n"
            "📈 EMA50 חוצה EMA200 מלמטה = Golden Cross (שורי!)\n"
            "📉 EMA50 חוצה EMA200 מלמעלה = Death Cross (דובי!)"
        ),
        "tip": "טריידרים יומיים: EMA9, EMA21. שבועיים: EMA50, EMA200.",
    },

    {
        "title": "מה זה Candlestick Patterns?",
        "icon": "🕯", "category": "טכני", "level": "🟡 בינוני",
        "body": (
            "נרות יפניים מספרים את סיפור המאבק בין קונים למוכרים.\n\n"
            "🟢 *נר ירוק* = מחיר סגירה > פתיחה (קונים ניצחו)\n"
            "🔴 *נר אדום* = מחיר סגירה < פתיחה (מוכרים ניצחו)\n\n"
            "📖 *דפוסים חשובים:*\n"
            "🔨 *Hammer* = ירידה + היפוך — אות קנייה\n"
            "⭐ *Doji* = פתיחה = סגירה — חוסר החלטיות\n"
            "🌟 *Engulfing* = נר גדול בולע את הקודם — היפוך חזק\n"
            "🌙 *Shooting Star* = זנב עליון ארוך — אות מכירה"
        ),
        "tip": "דפוס אחד לא מספיק. תמיד חפש אישור ב-2+ נרות + נפח גבוה.",
    },

    {
        "title": "מה זה Volume (נפח)?",
        "icon": "📊", "category": "טכני", "level": "🟢 בסיסי",
        "body": (
            "Volume = כמות המטבעות שנסחרו בפרק זמן מסוים.\n\n"
            "🔑 *כלל ברזל:* מחיר + נפח יחד = אמת. מחיר לבד = שאלה.\n\n"
            "📈 *עלייה + נפח גבוה* = תנועה אמיתית, מגמה חזקה\n"
            "📈 *עלייה + נפח נמוך* = חשוד! עלול להיות מניפולציה\n"
            "📉 *ירידה + נפח גבוה* = מכירה אמיתית, זהירות\n"
            "📉 *ירידה + נפח נמוך* = תיקון רגיל, לא בהלה\n\n"
            "🔍 Volume Spike = פעילות חריגה — תמיד בדוק למה!"
        ),
        "tip": "לפני כל כניסה לעסקה — תבדוק שהנפח מאשר את הכיוון.",
    },

    # ══ FUTURES ADVANCED ══

    {
        "title": "מה זה Perpetual Contracts?",
        "icon": "♾️", "category": "פיוצ׳רס", "level": "🟡 בינוני",
        "body": (
            "חוזה נצחי (Perpetual) = פיוצ׳רס ללא תאריך פקיעה.\n\n"
            "זה המוצר הנסחר ביותר בקריפטו — מיליארדי דולרים ביום!\n\n"
            "🆚 *Perpetual vs Regular Futures:*\n"
            "Perpetual: אין פקיעה, Funding Rate כל 8 שעות\n"
            "Regular: פוקע בתאריך קבוע, Basis pricing\n\n"
            "🏦 *בורסות מובילות:*\n"
            "• Binance Futures\n"
            "• Bybit\n"
            "• OKX\n"
            "• dYdX (דצנטרלי!)\n\n"
            "📊 95%+ מנפח הפיוצ׳רס בקריפטו = Perpetuals."
        ),
        "tip": "Perpetuals = הכלי הכי גמיש — אפשר להחזיק פוזיציה ימים, שבועות, חודשים.",
    },

    {
        "title": "מה זה Short Squeeze?",
        "icon": "🚀", "category": "פיוצ׳רס", "level": "🟡 בינוני",
        "body": (
            "Short Squeeze = לחיצת שורטים = עלייה פתאומית ואלימה.\n\n"
            "📖 *איך זה קורה:*\n"
            "1. הרבה טריידרים פותחים שורטים\n"
            "2. מחיר עולה בכל זאת\n"
            "3. שורטים מתחילים להפסיד\n"
            "4. מחיר חיסול מתקרב → הם נאלצים לסגור\n"
            "5. כדי לסגור שורט = קונים = מחיר עולה עוד!\n"
            "6. ספירלה של עליות מהירות\n\n"
            "📈 *דוגמאות היסטוריות:*\n"
            "DOGE 2021: +800% בשבועות\n"
            "GME 2021: +1700% בשבוע"
        ),
        "tip": "Short Interest גבוה + חדשות טובות = מתכון לשורט סקוויז. עקוב אחרי Funding שלילי.",
    },

    {
        "title": "מה זה Basis בפיוצ׳רס?",
        "icon": "📐", "category": "פיוצ׳רס", "level": "🔴 מתקדם",
        "body": (
            "Basis = הפרש בין מחיר ספוט לפיוצ׳רס.\n\n"
            "📊 *Contango:* Futures > Spot\n"
            "→ השוק ציפייתי ואופטימי\n"
            "→ לונגים משלמים פרמיה\n\n"
            "📊 *Backwardation:* Futures < Spot\n"
            "→ השוק חוזה ירידה\n"
            "→ שורטים משלמים פרמיה\n\n"
            "💡 *אסטרטגיית Basis Trade:*\n"
            "קנה ספוט + שורט פיוצ׳רס = רווח מה-Basis ללא סיכון כיוון!\n\n"
            "זו האסטרטגיה שקרנות גידור משתמשות בה."
        ),
        "tip": "Basis גבוה מ-1% שנתי = כדאי לבחון Basis Trade במקום להחזיק ספוט.",
    },

    {
        "title": "מה זה Liquidation Heatmap?",
        "icon": "🌡", "category": "פיוצ׳רס", "level": "🔴 מתקדם",
        "body": (
            "Liquidation Heatmap = מפת חום של מחירי חיסול.\n\n"
            "מראה היכן צבורים מחירי חיסול של פוזיציות פתוחות.\n\n"
            "🎯 *למה חשוב?*\n"
            "המחיר \"נמשך\" לאזורי חיסול גדולים — Liquidity Hunting!\n\n"
            "🔴 *אזורים אדומים גדולים:*\n"
            "= הרבה לונגים יחוסלו שם\n"
            "= המחיר עשוי לרדת לשם לפני שיעלה\n\n"
            "🟢 *אזורים ירוקים גדולים:*\n"
            "= הרבה שורטים יחוסלו שם\n"
            "= המחיר עשוי לעלות לשם\n\n"
            "🛠 כלי: Coinglass.com → Liquidation Map"
        ),
        "tip": "לפני כניסה — בדוק Heatmap. אל תיכנס ישר מעל/מתחת לאזור חיסול גדול.",
    },

    # ══ ON-CHAIN & ADVANCED ══

    {
        "title": "מה זה On-Chain Analysis?",
        "icon": "🔍", "category": "מתקדם", "level": "🔴 מתקדם",
        "body": (
            "ניתוח On-Chain = ניתוח ישיר של הבלוקצ׳יין.\n\n"
            "בניגוד לניתוח טכני (גרפים), On-Chain מסתכל על *מה שקורה בפועל*:\n\n"
            "📊 *מדדים מרכזיים:*\n"
            "• UTXO Age — כמה זמן BTC לא זז\n"
            "• Exchange Flow — כניסה/יציאה מבורסות\n"
            "• NVT Ratio — שווי שוק חלקי נפח עסקות\n"
            "• SOPR — האם מוכרים ברווח או הפסד\n"
            "• MVRV — שווי שוק vs שווי ממומש\n\n"
            "🛠 *כלים חינמיים:*\n"
            "Glassnode, CryptoQuant, IntoTheBlock"
        ),
        "tip": "MVRV מעל 3.5 = שוק חם מאוד, שקול להקטין חשיפה. מתחת 1 = קנייה היסטורית.",
    },

    {
        "title": "מה זה Hash Rate?",
        "icon": "⛏", "category": "ביטקוין", "level": "🟡 בינוני",
        "body": (
            "Hash Rate = כוח המחשוב שמאבטח את רשת ביטקוין.\n\n"
            "📈 *Hash Rate עולה = טוב!*\n"
            "• יותר כורים = רשת חזקה יותר\n"
            "• קשה יותר לתקוף\n"
            "• בדרך כלל מלווה בעלייה במחיר\n\n"
            "📉 *Hash Rate יורד = זהירות!*\n"
            "• כורים מכבים מחשבים\n"
            "• לא כדאי להם לכרות = מחיר נמוך\n\n"
            "🔢 *ספרות:*\n"
            "2009: כמה MH/s\n"
            "2024: ~600 EH/s (600 מיליון טריליון hash לשנייה!)"
        ),
        "tip": "Hash Ribbon Indicator: כשקצב Hash מתאושש אחרי ירידה = אות קנייה חזק.",
    },

    {
        "title": "מה זה Stablecoin?",
        "icon": "💵", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "Stablecoin = מטבע קריפטו שמחיר שלו יציב (בדרך כלל $1).\n\n"
            "📦 *סוגים עיקריים:*\n\n"
            "🏦 *Fiat-Backed:*\n"
            "USDT (Tether), USDC (Circle)\n"
            "→ גיבוי בדולרים אמיתיים\n\n"
            "🔐 *Crypto-Backed:*\n"
            "DAI — גיבוי ב-ETH, מנוהל בדצנטרליזציה\n\n"
            "⚙️ *Algorithmic:*\n"
            "UST (לונה) — קרסה ב-2022, הפסד של $40 מיליארד!\n\n"
            "💡 *שימושים:*\n"
            "✅ חסינות מתנודתיות\n"
            "✅ Yield Farming\n"
            "✅ Collateral לפיוצ׳רס"
        ),
        "tip": "USDT גדול מ-USDC, אבל USDC שקוף יותר מבחינת רזרבות. USDC מועדף על מוסדיים.",
    },

    {
        "title": "מה זה Airdrop?",
        "icon": "🪂", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "Airdrop = חלוקת מטבעות חינם לבעלי ארנקים מסוימים.\n\n"
            "📖 *למה פרויקטים עושים זאת?*\n"
            "• ליצור קהילה\n"
            "• לתגמל משתמשים נאמנים\n"
            "• להפיץ את המטבע לרבים\n\n"
            "💰 *Airdrops מפורסמים:*\n"
            "• Uniswap (UNI) 2020: $1,200+ למשתמש\n"
            "• Arbitrum (ARB) 2023: $1,000-$10,000\n"
            "• dYdX 2021: עד $50,000!\n\n"
            "🎯 *איך לזכות?*\n"
            "השתמש בפרוטוקולים חדשים, ספק נזילות, Bridge."
        ),
        "tip": "שמור על פעילות קבועה בפרוטוקולים חדשים — Airdrops מגיעים למי שמשתמש.",
    },

    {
        "title": "מה זה Gas Fee?",
        "icon": "⛽", "category": "מתחילים", "level": "🟢 בסיסי",
        "body": (
            "Gas Fee = עמלת עסקה ברשת Ethereum.\n\n"
            "🔢 *מחושב ב-Gwei (מיליארדית ETH):*\n"
            "Gas Price × Gas Limit = עמלה סופית\n\n"
            "📈 *מתי Gas גבוה?*\n"
            "• עומס ברשת (NFT Drop, DeFi Farm חם)\n"
            "• בימי מסחר פעילים\n\n"
            "📉 *מתי Gas נמוך?*\n"
            "• סופי שבוע\n"
            "• לילה (UTC)\n\n"
            "💡 *הפתרון:* Layer 2!\n"
            "Arbitrum: ~$0.01 במקום $20 על ETH mainnet"
        ),
        "tip": "ethgasstation.info - בדוק Gas לפני כל עסקה ב-Ethereum.",
    },

    {
        "title": "מה זה NFT?",
        "icon": "🎨", "category": "NFT/Web3", "level": "🟢 בסיסי",
        "body": (
            "NFT = Non-Fungible Token = נכס דיגיטלי ייחודי.\n\n"
            "🔑 *מה ש-NFT לא יכול לשכפל:*\n"
            "• בעלות מוכחת בבלוקצ׳יין\n"
            "• היסטוריה מלאה של עסקות\n"
            "• Rarity מוכח\n\n"
            "🎭 *שימושים:\n"
            "🎨 אמנות דיגיטלית — Bored Ape ($400K+)\n"
            "🎮 גיימינג — נכסים אמיתיים במשחק\n"
            "🎫 כרטיסים — VIP, הטבות\n"
            "🏠 נדל\"ן וירטואלי — Decentraland\n\n"
            "📉 שוק NFT צנח ב-2022. האם יחזור?"
        ),
        "tip": "OpenSea, Blur = הפלטפורמות הגדולות. תמיד בדוק Volume ו-Floor Price.",
    },

    {
        "title": "מה זה DAO?",
        "icon": "🏛", "category": "Web3", "level": "🟡 בינוני",
        "body": (
            "DAO = Decentralized Autonomous Organization\n"
            "= ארגון מבוזר אוטונומי.\n\n"
            "🔄 *איך עובד?*\n"
            "• חברים מחזיקים Governance Tokens\n"
            "• מצביעים על החלטות\n"
            "• חוזים חכמים מממשים אוטומטית\n\n"
            "📊 *דוגמאות:*\n"
            "MakerDAO — שולט ב-DAI\n"
            "Uniswap DAO — שולט בפרוטוקול\n"
            "ENS DAO — שולט בשמות Ethereum\n\n"
            "✅ *יתרון:* שקיפות מלאה, אין CEO\n"
            "❌ *חיסרון:* החלטות איטיות, voter apathy"
        ),
        "tip": "DAO Tokens = הצבעה על עתיד הפרוטוקול. MKR, UNI, COMP = ה-Governance tokens הכי גדולים.",
    },

    {
        "title": "מה זה Staking?",
        "icon": "🌾", "category": "השקעה", "level": "🟡 בינוני",
        "body": (
            "Staking = נעילת מטבעות לאבטחת הרשת תמורת תגמול.\n\n"
            "🔄 *Proof of Stake (PoS):*\n"
            "במקום כורים (כמו בBTC), Validators נועלים מטבעות\n"
            "ומאמתים עסקות. תמורה = תשואה שנתית.\n\n"
            "📊 *תשואות נפוצות:*\n"
            "ETH Staking: ~4% שנתי\n"
            "SOL: ~7% שנתי\n"
            "ATOM: ~15% שנתי\n"
            "אלטים קטנים: 50-200% (בסיכון גבוה!)\n\n"
            "⚠️ *Liquid Staking:* stETH, rETH\n"
            "מאפשר Staking + שמירת נזילות."
        ),
        "tip": "APY גבוה מ-20%? בדוק מאיפה בא הרווח. Unsustainable = Ponzi.",
    },

    {
        "title": "מה זה Yield Farming?",
        "icon": "🚜", "category": "DeFi", "level": "🔴 מתקדם",
        "body": (
            "Yield Farming = אסטרטגיה מתקדמת להרוויח תשואה ב-DeFi.\n\n"
            "📖 *איך עובד?*\n"
            "1. מספק נזילות ל-Liquidity Pool\n"
            "2. מקבל LP Tokens\n"
            "3. מכניס LP Tokens ל-Farm\n"
            "4. מרוויח Token נוסף!\n\n"
            "💰 *בשיא DeFi Summer 2020:*\n"
            "APY של 1000%+ היה נפוץ\n\n"
            "⚠️ *הסיכונים:*\n"
            "• Impermanent Loss — הפסד מהחזקת Pool\n"
            "• Smart Contract Bug — פריצה\n"
            "• Rug Pull — הפרויקט בורח עם הכסף"
        ),
        "tip": "Impermanent Loss קורה כשהיחס בין שני המטבעות בPool משתנה. מחשבון: dailydefi.org",
    },

    {
        "title": "מה זה Crypto Cycle?",
        "icon": "🔄", "category": "מאקרו", "level": "🟡 בינוני",
        "body": (
            "קריפטו עובד במחזורים של ~4 שנים, מסונכרן עם Halving.\n\n"
            "📅 *מחזור קלאסי:*\n\n"
            "1️⃣ *Bear Market* — דיכאון, כולם יצאו\n"
            "   BTC יורד 70-90% מהשיא\n\n"
            "2️⃣ *Accumulation* — Smart Money קונה בשקט\n\n"
            "3️⃣ *Bull Market* — עלייה, Altseason\n"
            "   BTC שובר שיאים, אלטים ×10-×100\n\n"
            "4️⃣ *Distribution* — Smart Money מוכר\n"
            "   חמדנות קיצונית, FOMO של קמעונאים\n\n"
            "🔑 *הכלל:* קנה בדיכאון, מכור ב-FOMO."
        ),
        "tip": "כשה-Crypto Fear & Greed מגיע ל-10-15 = זמן היסטורי לקנות.",
    },

    {
        "title": "מה זה Protocol Revenue?",
        "icon": "💎", "category": "מתקדם", "level": "🔴 מתקדם",
        "body": (
            "Protocol Revenue = הכנסות שפרוטוקול DeFi מייצר.\n\n"
            "📊 *מדד P/F Ratio (Price to Fees):*\n"
            "כמו P/E בשוק המניות — כמה השוק משלם על כל $1 הכנסה.\n\n"
            "💰 *פרוטוקולים רווחיים:*\n"
            "Uniswap: מיליארדי דולר עמלות שנתיות\n"
            "Aave: מאות מיליונים\n"
            "GMX: ~$150M+ שנתי\n\n"
            "🔑 *למה חשוב?*\n"
            "Token עם הכנסות אמיתיות = Fundamental Value.\n"
            "Token ללא הכנסות = ספקולציה טהורה.\n\n"
            "🛠 מקור נתונים: Token Terminal"
        ),
        "tip": "לפני השקעה ב-DeFi Token — בדוק ב-Token Terminal אם יש הכנסות אמיתיות.",
    },

]

_knowledge_index: int = 0  # Track which tip to send next


def send_daily_knowledge(chat_id: str = "") -> bool:
    """
    Send the next knowledge tip from CRYPTO_KNOWLEDGE_BANK to the channel.
    Rotates through all 20+ tips in order.
    """
    global _knowledge_index
    import time as _t

    if not TELEGRAM_NEWS_TOKEN:
        return False

    target = chat_id or TELEGRAM_NEWS_CHAT or TELEGRAM_CHAT
    if not target:
        return False

    try:
        tip = CRYPTO_KNOWLEDGE_BANK[_knowledge_index % len(CRYPTO_KNOWLEDGE_BANK)]
    except (IndexError, ZeroDivisionError):
        tip = CRYPTO_KNOWLEDGE_BANK[0]
    _knowledge_index += 1

    lines = []
    lines.append("📚 *אקדמיית NEXUS* | שיעור יומי")
    lines.append("╔══════════════════════════╗")
    lines.append(f"  {tip['icon']}  *{tip['title']}*")
    lines.append("╚══════════════════════════╝")
    lines.append(f"🏷  קטגוריה: {tip['category']}")
    lines.append("")
    lines.append(tip["body"])
    lines.append("")
    lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")
    lines.append(f"💡 *טיפ:* _{tip['tip']}_")
    lines.append("")
    lines.append(f"📖 שיעור {(_knowledge_index) % len(CRYPTO_KNOWLEDGE_BANK) + 1}/{len(CRYPTO_KNOWLEDGE_BANK)}")
    lines.append("🤖 *NEXUS AI*  |  @cySignals\\_Official")
    lines.append("_הצטרפו ללמוד קריפטו בעברית_ 🇮🇱")

    msg = "\n".join(lines)

    try:
        resp = requests.post(
            "https://api.telegram.org/bot" + TELEGRAM_NEWS_TOKEN + "/sendMessage",
            json={
                "chat_id":    target,
                "text":       msg,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=12,
        )
        if resp.status_code == 200:
            log.info("[KNOWLEDGE] Sent tip #%d: %s", _knowledge_index, tip["title"])
            return True
        else:
            log.warning("[KNOWLEDGE] HTTP %d: %s", resp.status_code, resp.text[:100])
            return False
    except Exception as e:
        log.error("[KNOWLEDGE] Error: %s", e)
        return False


def broadcast_news_to_telegram(min_heat: int = 5, max_items: int = 5, chat_id: str = "") -> int:
    """
    Send the hottest educational news to the Telegram channel in Hebrew.
    chat_id overrides env variable TELEGRAM_NEWS_CHAT_ID.
    Returns count of items sent.
    """
    import time
    target = chat_id or TELEGRAM_NEWS_CHAT or TELEGRAM_CHAT
    if not TELEGRAM_NEWS_TOKEN or not target:
        return 0
    try:
        items = get_educational_news()
    except Exception as e:
        log.error("[Telegram] broadcast fetch error: %s", e)
        return 0

    hot = []
    for item in items:
        try:
            h = int(item.get("heat") or 0)
            if h >= min_heat:
                hot.append(item)
        except (TypeError, ValueError):
            pass
    hot.sort(key=lambda x: int(x.get("heat") or 0), reverse=True)

    sent = 0
    for item in hot:
        if sent >= max_items:
            break
        if send_telegram_news(item, chat_id=target):
            sent += 1
            time.sleep(2)   # Telegram rate limit: max 30 msg/sec, be safe

    if sent:
        log.info("[Telegram-EDU] Broadcast %d educational news items to %s", sent, target)
    return sent


def run_signal_job() -> None:
    global _seen_this_sweep
    _seen_this_sweep = set()

    if not OPENAI_API_KEY:
        log.warning("Job skipped — OPENAI_API_KEY not configured")
        return

    today_count = count_today_signals()
    remaining   = DAILY_LIMIT - today_count

    if remaining <= 0:
        log.info("Daily quota of %d reached — job idle", DAILY_LIMIT)
        return

    log.info("══ Sweep START | %d / %d quota used ══", today_count, DAILY_LIMIT)
    # Preload all Binance tickers for fast coin detection
    get_all_binance_tickers()
    sweep_start = datetime.now()

    all_items = fetch_all_sources(remaining)
    if not all_items:
        log.info("No items from any source — sweep aborted")
        return

    approved = cached = ai_skipped = noise_ct = dupes = errors = 0

    for item in all_items:
        if approved + cached >= remaining:
            log.info("Quota filled mid-sweep (%d approved + %d cached)", approved, cached)
            break

        title   = item["title"]
        summary = item.get("summary", "")
        source  = item["source"]
        symbols = item.get("symbols", ["CRYPTO"])

        # L1 — keyword noise
        if is_noise(title):
            log.debug("[NOISE][%s] %.60s", source, title)
            noise_ct += 1
            continue

        fp = title_fingerprint(title)

        # L2 — in-memory sweep dedup
        if _sweep_seen(fp):
            log.debug("[NEAR-DUPE:sweep][%s] %.60s", source, title)
            dupes += 1
            continue

        # L3 — Cache hit: same fingerprint already in DB within 24h
        cached_result = get_cached_signal(fp)
        if cached_result:
            log.info("[CACHE HIT][%s] Reusing %s signal — %.55s",
                     source, cached_result["signal"], title)
            symbol = symbols[0] if symbols else "CRYPTO"
            save_signal(
                symbol       = symbol,
                news_title   = title,
                sentiment    = cached_result["sentiment"],
                signal       = cached_result["signal"],
                reason       = cached_result["reason"],
                target_price = cached_result["target_price"],
                source       = source,
                fingerprint  = fp,
            )
            cached += 1
            continue

        # L4 — DB 24h fingerprint window (new headline but similar story)
        if fingerprint_seen_in_db(fp):
            log.debug("[NEAR-DUPE:db24h][%s] %.60s", source, title)
            dupes += 1
            continue

        # L5 — Gather market intelligence (parallel)
        ticker_clean = detected_symbol.replace("/USDT","") if detected_symbol != "CRYPTO/USDT" else "BTC"
        intel = gather_market_intel(ticker_clean)
        intel["_source"] = source
        # Inject pre-fetched global data (faster, no duplicate API calls)
        if global_intel_cache:
            for key, val in global_intel_cache.items():
                intel.setdefault(key, val)
        log.debug("Intel for %s: RSI=%s FG=%s Fund=%s",
                  ticker_clean, intel.get("rsi"), 
                  intel.get("fear_greed",{}).get("value"),
                  intel.get("funding",{}).get("rate"))

        # L6 — AI analysis with full context
        analysis = analyse_item(title, summary, intel)
        if analysis is None:
            log.warning("[ERROR] AI returned None for '%.55s'", title)
            errors += 1
            continue

        score_result = {}  # default empty
        if analysis["signal"] == "SKIP":
            log.info("[SKIP][%s] %.70s", source, title)
            ai_skipped += 1
            continue

        # Override conviction with our quantitative score
        if analysis["signal"] in ("BUY", "SELL"):
            score_result = score_signal_confidence(intel, analysis["signal"])
            analysis["conviction"] = score_result["conviction"]
            score = score_result["score"]

            # CONFLUENCE CHECK — must have 3+ confirming indicators
            if not intel:
                log.warning("[CONFLUENCE] No intel for %s, skipping confluence", detected_symbol)
                ai_skipped += 1
                continue
            confluence = detect_confluence(intel, analysis["signal"])
            analysis["confluence"] = confluence["summary"]
            analysis["confluence_count"] = confluence["count"]
            if not confluence["pass"]:
                log.info("[NO-CONFLUENCE] %s %s: only %d/10 indicators agree — skipping",
                         analysis["signal"], detected_symbol, confluence["count"])
                ai_skipped += 1
                continue
            log.info("[CONFLUENCE] %s %s: %d/10 indicators confirm (%s)",
                     analysis["signal"], detected_symbol,
                     confluence["count"], confluence["level"])
            log.info("[SCORE] %s %s: %d/100 (%s) — %s",
                     analysis["signal"], detected_symbol, score,
                     score_result["conviction"],
                     ", ".join(score_result["reasons"][:2]))
            # Skip LOW conviction signals entirely
            if score_result["conviction"] == "LOW":
                log.info("[LOW-CONV] Skipping low confidence signal for %s", detected_symbol)
                ai_skipped += 1
                continue

        # Prefer AI-extracted symbol over RSS metadata
        ai_symbol = analysis.get("symbol", "")
        symbol = ai_symbol if ai_symbol and ai_symbol != "CRYPTO" else (symbols[0] if symbols and symbols[0] != "UNKNOWN" else "CRYPTO")
        # Validate entry price with live Binance price
        ticker = symbol.replace("/USDT","").replace("/USD","")
        live_price = get_live_price_fast(ticker)
        if live_price != "NA":
            try:
                live_f = float(live_price)
                entry_f = float(analysis.get("entry","0") or "0")
                # If AI price is off by more than 50%, use live price
                if entry_f > 0 and abs(live_f - entry_f) / live_f > 0.5:
                    log.info("Price corrected: AI=%s Live=%s for %s", entry_f, live_f, symbol)
                    pct_sl = 0.06
                    analysis["entry"]     = str(round(live_f, 4))
                    analysis["stop_loss"] = str(round(live_f * (1 - pct_sl if analysis.get("signal")=="BUY" else 1 + pct_sl), 4))
                    analysis["target1"]   = str(round(live_f * (1.03 if analysis.get("signal")=="BUY" else 0.97), 4))
                    analysis["target2"]   = str(round(live_f * (1.06 if analysis.get("signal")=="BUY" else 0.94), 4))
                    analysis["target3"]   = str(round(live_f * (1.10 if analysis.get("signal")=="BUY" else 0.90), 4))
            except (ValueError, ZeroDivisionError):
                pass
        # Risk management
        risk_mgmt = calculate_risk_management(
            {**analysis, "symbol": symbol, "score": score_result.get("score",50)},
            intel
        )
        analysis["position_size"]  = str(risk_mgmt["position_size_pct"])
        analysis["rr_ratio"]       = str(risk_mgmt["rr_ratio"])
        analysis["risk_warning"]   = risk_mgmt["warning"]
        analysis["simple_explanation"] = risk_mgmt.get("simple_explanation", risk_mgmt.get("simple_explain",""))

        # Translate to Hebrew
        title_he  = translate_to_hebrew(title)
        reason_he = translate_to_hebrew(analysis["reason"])

        save_signal(
            symbol        = symbol,
            news_title    = title,
            sentiment     = analysis["sentiment"],
            signal        = analysis["signal"],
            direction     = analysis.get("direction", "המתן"),
            reason        = analysis["reason"],
            entry         = analysis.get("entry", "NA"),
            stop_loss     = analysis.get("stop_loss", "NA"),
            target1       = analysis.get("target1", "NA"),
            target2       = analysis.get("target2", "NA"),
            target3       = analysis.get("target3", "NA"),
            leverage      = analysis.get("leverage", "x10"),
            target_price  = analysis["target_price"],
            source        = source,
            fingerprint   = fp,
            news_title_he = title_he,
            reason_he     = reason_he,
            score         = score_result.get("score", 50) if score_result else 50,
            position_size = analysis.get("position_size", "3"),
            rr_ratio      = analysis.get("rr_ratio", "0"),
            liq_price     = analysis.get("liq_price", "NA"),
            risk_warning  = analysis.get("risk_warning", ""),
            confluence_count = analysis.get("confluence_count", 0),
        )
        approved += 1
        log.info("[%s][%s] %s | %s → %.55s",
                 analysis["signal"], source, symbol, analysis["sentiment"], title)

    elapsed = (datetime.now() - sweep_start).seconds
    log.info(
        "══ Sweep END (%ds) | ✓ %d new | 💾 %d cached | "
        "✗ %d AI-skip | ⊘ %d noise | ≈ %d dupes | ⚠ %d errors ══",
        elapsed, approved, cached, ai_skipped, noise_ct, dupes, errors,
    )

# ─────────────────────────────────────────────
# Flask Routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    try:
        with open(path, encoding="utf-8") as f:
            return render_template_string(f.read())
    except FileNotFoundError:
        return "<h1>index.html not found</h1>", 500

@app.route("/api/signals")
def api_signals():
    try:
        return jsonify(fetch_recent_signals(200))
    except Exception as e:
        log.error("api/signals: %s", e)
        return jsonify({"error": "DB unavailable"}), 500

@app.route("/api/stats")
def api_stats():
    try:
        today = count_today_signals()
        return jsonify({"today": today, "daily_limit": DAILY_LIMIT,
                        "remaining": max(0, DAILY_LIMIT - today)})
    except Exception as e:
        log.error("api/stats: %s", e)
        return jsonify({"error": "DB unavailable"}), 500

@app.route("/api/sources")
def api_sources():
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT source, signal, COUNT(*) as cnt FROM signals "
                "GROUP BY source, signal ORDER BY source, signal"
            ).fetchall()
        bd: dict[str, dict] = {}
        for row in rows:
            src = row["source"]
            bd.setdefault(src, {"BUY": 0, "SELL": 0, "WAIT": 0, "total": 0})
            if row["signal"] in ("BUY", "SELL", "WAIT"):
                bd[src][row["signal"]] += row["cnt"]
                bd[src]["total"]       += row["cnt"]
        return jsonify(bd)
    except Exception as e:
        log.error("api/sources: %s", e)
        return jsonify({}), 500

@app.route("/api/trigger", methods=["POST"])
def api_trigger():
    try:
        run_signal_job()
        return jsonify({"status": "ok"})
    except Exception as e:
        log.error("api/trigger: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/news")
def api_news():
    """Return latest raw headlines from all RSS sources for the news page."""
    try:
        items = fetch_all_sources(50)
        news = []
        seen = set()
        for item in items:
            title = item.get("title","").strip()
            if title and title not in seen:
                seen.add(title)
                news.append({
                    "title":  title,
                    "source": item.get("source","Unknown"),
                })
            if len(news) >= 50:
                break
        return jsonify(news)
    except Exception as e:
        log.error("api/news: %s", e)
        return jsonify([]), 500

@app.route("/api/performance")
def api_performance():
    """Return signal performance stats."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT signal, result, COUNT(*) as cnt,
                   AVG(CAST(result_pnl AS FLOAT)) as avg_pnl
                   FROM signals
                   WHERE result != 'OPEN'
                   GROUP BY signal, result"""
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
            open_ct = conn.execute("SELECT COUNT(*) as c FROM signals WHERE result='OPEN'").fetchone()["c"]
        stats = {"total": total, "open": open_ct, "breakdown": [dict(r) for r in rows]}
        return jsonify(stats)
    except Exception as e:
        log.error("api/performance: %s", e)
        return jsonify({}), 500


@app.route("/api/price/<symbol>")
def api_price(symbol):
    """Return live Binance price for a symbol."""
    try:
        ticker = symbol.upper().replace("USDT","").replace("/","")
        all_t  = get_all_binance_tickers()
        price  = all_t.get(ticker) or get_live_price(ticker)
        return jsonify({"symbol": symbol, "price": price})
    except Exception as e:
        return jsonify({"symbol": symbol, "price": "NA"}), 500


@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    """Return 24h kline data from Binance for mini chart."""
    try:
        ticker = symbol.upper().replace("/USDT","").replace("/","") + "USDT"
        resp   = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 24},
            timeout=8,
        )
        if resp.status_code == 200:
            klines = resp.json()
            data   = [{"t": k[0], "o": k[1], "h": k[2], "l": k[3], "c": k[4]} for k in klines]
            return jsonify({"symbol": symbol, "data": data})
        return jsonify({"symbol": symbol, "data": []}), 404
    except Exception as e:
        log.error("api/chart/%s: %s", symbol, e)
        return jsonify({"symbol": symbol, "data": []}), 500


# ═══════════════════════════════════════
# REAL-TIME PRICE MONITOR via Binance WS
# ═══════════════════════════════════════

import threading
import json as _json

_ws_prices: dict[str, float] = {}
_ws_thread = None
_ws_running = False

def start_price_websocket():
    """
    Connect to Binance WebSocket for real-time prices.
    Updates _ws_prices dict continuously.
    Monitors BTC, ETH, SOL, XRP, BNB — top coins.
    """
    global _ws_running
    try:
        import websocket as ws_lib
        symbols = [
            "btcusdt","ethusdt","solusdt","xrpusdt","bnbusdt",
            "adausdt","dogeusdt","avaxusdt","linkusdt","dotusdt",
            "maticusdt","nearusdt","ftmusdt","arbusdt","opusdt",
            "aptusdt","suiusdt","injusdt","tiausdt","seiusdt",
            "wldusdt","taousdt","rndrusdt","fetusdt","jupusdt",
            "enausdt","pendleusdt","wifusdt","bonkusdt","pepeusdt",
            "stxusdt","ordiusdt","ldousdt","rpllusdt","gmxusdt",
            "dydxusdt","grtusdt","snxusdt","aaveusdt","crvusdt",
            "uniusdt","mkrusdt","compusdt","sushiusdt","1inchusdt",
            "atomusdt","osmousdt","axsusdt","sandusdt","manausdt",
            "galausdt","imxusdt","ronusdt","beamusdt","primeusdt",
            "xlmusdt","trxusdt","etcusdt","bchusdt","ltcusdt",
        ]
        # Split into chunks of 50 (Binance limit per connection)
        chunk = symbols[:50]
        stream = "/".join([f"{s}@miniTicker" for s in chunk])
        url = f"wss://stream.binance.com:9443/stream?streams={stream}"

        def on_message(ws, message):
            try:
                data = _json.loads(message)
                ticker = data.get("data",{})
                sym = ticker.get("s","")
                price = float(ticker.get("c",0))
                if sym and price:
                    _ws_prices[sym] = price
                    # Update price cache too
                    coin = sym.replace("USDT","")
                    _price_cache[sym] = (str(round(price,4)), __import__('time').time())
            except:
                pass

        def on_error(ws, error):
            log.warning("[WS] Error: %s", error)

        def on_close(ws, *args):
            global _ws_running
            _ws_running = False
            log.info("[WS] Connection closed, will reconnect...")

        def on_open(ws):
            log.info("[WS] Connected to Binance real-time feed")
            _ws_running = True

        wsapp = ws_lib.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        wsapp.run_forever(ping_interval=30, ping_timeout=10)
    except ImportError:
        log.warning("[WS] websocket-client not installed, using REST polling")
    except Exception as e:
        log.error("[WS] Fatal error: %s", e)


def ensure_websocket():
    """Start WebSocket thread if not running."""
    global _ws_thread, _ws_running
    if not _ws_running or _ws_thread is None or not _ws_thread.is_alive():
        _ws_thread = threading.Thread(target=start_price_websocket, daemon=True)
        _ws_thread.start()
        log.info("[WS] Price WebSocket thread started")


def get_live_price_fast(ticker: str) -> str:
    """Get price from WebSocket cache first, then REST fallback."""
    sym = ticker.upper().replace("/USDT","").replace("/","") + "USDT"
    # Try WebSocket cache first (instant)
    if sym in _ws_prices:
        return str(round(_ws_prices[sym], 6))
    # Fallback to REST
    return get_live_price(ticker)


# ═══════════════════════════════════════════
# MULTI-EXCHANGE DATA — Bybit + OKX + Binance
# ═══════════════════════════════════════════

def fetch_bybit_data(symbol: str) -> dict:
    """Bybit open interest + funding rate for cross-exchange validation."""
    try:
        ticker = symbol.replace("/USDT","").replace("/","")
        r = requests.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category":"linear","symbol": ticker+"USDT"},
            timeout=5,
        )
        if r.status_code == 200:
            items = r.json().get("result",{}).get("list",[])
            if items:
                d = items[0]
                return {
                    "bybit_price":      d.get("lastPrice","NA"),
                    "bybit_funding":    d.get("fundingRate","NA"),
                    "bybit_oi":         d.get("openInterest","NA"),
                    "bybit_volume_24h": d.get("volume24h","NA"),
                    "bybit_bid1":       d.get("bid1Price","NA"),
                    "bybit_ask1":       d.get("ask1Price","NA"),
                }
    except Exception as e:
        log.debug("Bybit error %s: %s", symbol, e)
    return {}


def fetch_okx_data(symbol: str) -> dict:
    """OKX ticker + open interest."""
    try:
        ticker = symbol.replace("/USDT","").replace("/","")
        inst_id = ticker + "-USDT-SWAP"
        r = requests.get(
            "https://www.okx.com/api/v5/market/ticker",
            params={"instId": inst_id},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json().get("data",[])
            if data:
                d = data[0]
                return {
                    "okx_price":   d.get("last","NA"),
                    "okx_volume":  d.get("vol24h","NA"),
                    "okx_bid":     d.get("bidPx","NA"),
                    "okx_ask":     d.get("askPx","NA"),
                }
    except Exception as e:
        log.debug("OKX error %s: %s", symbol, e)
    return {}


def fetch_cross_exchange_analysis(symbol: str) -> dict:
    """
    Compare prices across Binance, Bybit, OKX.
    Price divergence = arbitrage opportunity = signal confirmation.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","")
        with ThreadPoolExecutor(max_workers=3) as pool:
            f_bb  = pool.submit(fetch_bybit_data, symbol)
            f_okx = pool.submit(fetch_okx_data, symbol)
            f_bin = pool.submit(get_live_price, ticker)

        bybit = f_bb.result()
        okx   = f_okx.result()
        binance_price = f_bin.result()

        prices = {}
        if binance_price and binance_price != "NA":
            prices["Binance"] = float(binance_price)
        if bybit.get("bybit_price") and bybit["bybit_price"] != "NA":
            prices["Bybit"] = float(bybit["bybit_price"])
        if okx.get("okx_price") and okx["okx_price"] != "NA":
            prices["OKX"] = float(okx["okx_price"])

        if len(prices) >= 2:
            max_p = max(prices.values())
            min_p = min(prices.values())
            spread = (max_p - min_p) / min_p * 100 if min_p > 0 else 0

            # High spread = unusual = potential big move coming
            signal = "HIGH_DIVERGENCE" if spread > 0.3 else "NORMAL"

            # Compare funding rates — if both exchanges show extreme funding
            bin_funding = float(bybit.get("bybit_funding","0") or "0")
            cross_signal = "ALIGNED" if abs(bin_funding) > 0.05 else "NORMAL"

            return {
                "prices":         prices,
                "spread_pct":     round(spread, 4),
                "divergence":     signal,
                "cross_funding":  cross_signal,
                "bybit_funding":  bybit.get("bybit_funding","NA"),
                "bybit_oi":       bybit.get("bybit_oi","NA"),
                "exchanges_count": len(prices),
            }
    except Exception as e:
        log.debug("Cross-exchange error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# LIQUIDATION HEATMAP — Where stops are clustered
# ═══════════════════════════════════════════

def fetch_liquidation_heatmap(symbol: str) -> dict:
    """
    Analyze where liquidations are likely clustered.
    Uses recent liquidation data + order book depth to find
    price levels where mass liquidations would occur.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_liq = pool.submit(requests.get,
                "https://fapi.binance.com/fapi/v1/allForceOrders",
                params={"symbol": ticker, "limit": 100},
                timeout=6,
            )
            f_book = pool.submit(requests.get,
                "https://api.binance.com/api/v3/depth",
                params={"symbol": ticker, "limit": 100},
                timeout=6,
            )

        liq_r  = f_liq.result()
        book_r = f_book.result()

        result = {}

        # Analyze recent liquidations
        if liq_r.status_code == 200:
            orders = liq_r.json()
            if orders:
                # Find price clusters where liquidations happened
                liq_prices = [float(o.get("price",0)) for o in orders if o.get("price")]
                if liq_prices:
                    avg_liq = sum(liq_prices) / len(liq_prices)
                    max_liq = max(liq_prices)
                    min_liq = min(liq_prices)
                    current_price_r = requests.get(
                        "https://api.binance.com/api/v3/ticker/price",
                        params={"symbol": ticker}, timeout=3,
                    )
                    current = float(current_price_r.json().get("price",0)) if current_price_r.status_code==200 else avg_liq

                    # Danger zones
                    upper_zone = round(current * 1.05, 2)  # +5% liquidation zone
                    lower_zone = round(current * 0.95, 2)  # -5% liquidation zone

                    long_liqs  = sum(1 for o in orders if o.get("side")=="BUY")
                    short_liqs = sum(1 for o in orders if o.get("side")=="SELL")

                    result["liq_upper_zone"]  = upper_zone
                    result["liq_lower_zone"]  = lower_zone
                    result["long_liqs_count"] = long_liqs
                    result["short_liqs_count"]= short_liqs
                    result["liq_dominant"]    = "LONG SQUEEZE" if long_liqs > short_liqs else "SHORT SQUEEZE"
                    result["liq_avg_price"]   = round(avg_liq, 2)
                    result["liq_range"]       = f"${round(min_liq,0)}-${round(max_liq,0)}"

        # Analyze order book for thin liquidity zones
        if book_r.status_code == 200:
            book = book_r.json()
            bids = [(float(p), float(q)) for p,q in book.get("bids",[])]
            asks = [(float(p), float(q)) for p,q in book.get("asks",[])]

            if bids and asks:
                # Find thinnest zones (where price could move fast)
                bid_total = sum(q for p,q in bids[:20])
                ask_total = sum(q for p,q in asks[:20])

                # Walls — large orders that might stop price
                big_bids = [(p,q) for p,q in bids if q > bid_total/20*3]
                big_asks = [(p,q) for p,q in asks if q > ask_total/20*3]

                if big_bids:
                    result["support_wall"] = round(big_bids[0][0], 2)
                if big_asks:
                    result["resistance_wall"] = round(big_asks[0][0], 2)

                imbalance = (bid_total - ask_total) / (bid_total + ask_total) * 100
                result["book_imbalance"] = round(imbalance, 1)
                result["book_signal"] = "BUY WALL" if imbalance > 20 else "SELL WALL" if imbalance < -20 else "BALANCED"

        return result
    except Exception as e:
        log.debug("Liquidation heatmap error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# DERIBIT OPTIONS — Put/Call Ratio + Fear
# ═══════════════════════════════════════════

def fetch_deribit_options(symbol: str = "BTC") -> dict:
    """
    Deribit options market — free public API.
    Put/Call ratio: >1 = fear (good for contrarian BUY)
                   <0.5 = greed (good for contrarian SELL)
    """
    try:
        coin = symbol.replace("/USDT","").replace("/","").upper()
        if coin not in ("BTC","ETH"):
            coin = "BTC"  # Deribit mainly has BTC/ETH options

        r = requests.get(
            "https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
            params={"currency": coin, "kind": "option"},
            timeout=8,
        )
        if r.status_code == 200:
            instruments = r.json().get("result", [])
            puts  = [i for i in instruments if i.get("instrument_name","").endswith("-P")]
            calls = [i for i in instruments if i.get("instrument_name","").endswith("-C")]

            put_vol  = sum(float(i.get("volume",0) or 0) for i in puts)
            call_vol = sum(float(i.get("volume",0) or 0) for i in calls)

            if call_vol > 0:
                pc_ratio = round(put_vol / call_vol, 3)
                if pc_ratio > 1.5:
                    sentiment = "EXTREME FEAR — strong contrarian BUY"
                    score_boost = 15
                elif pc_ratio > 1.0:
                    sentiment = "FEAR — slight contrarian BUY"
                    score_boost = 8
                elif pc_ratio < 0.4:
                    sentiment = "EXTREME GREED — strong contrarian SELL"
                    score_boost = -10
                elif pc_ratio < 0.7:
                    sentiment = "GREED — slight contrarian SELL"
                    score_boost = -5
                else:
                    sentiment = "NEUTRAL"
                    score_boost = 0

                # Also get implied volatility (IV) - high IV = big move expected
                ivs = [float(i.get("mark_iv",0) or 0) for i in instruments if i.get("mark_iv")]
                avg_iv = round(sum(ivs)/len(ivs), 1) if ivs else 0
                iv_signal = "HIGH_IV — big move expected" if avg_iv > 80 else "NORMAL_IV"

                return {
                    "put_call_ratio": pc_ratio,
                    "sentiment":      sentiment,
                    "score_boost":    score_boost,
                    "avg_iv":         avg_iv,
                    "iv_signal":      iv_signal,
                    "put_volume":     round(put_vol, 0),
                    "call_volume":    round(call_vol, 0),
                }
    except Exception as e:
        log.debug("Deribit error: %s", e)
    return {}


# ═══════════════════════════════════════════
# ETHERSCAN SMART MONEY — On-chain whale tracking
# ═══════════════════════════════════════════

def fetch_smart_money_flows() -> dict:
    """
    Track large ETH/ERC-20 movements via Etherscan free API.
    Large outflows from exchanges = accumulation = bullish.
    Large inflows to exchanges = distribution = bearish.
    """
    try:
        # Known exchange cold wallets
        exchange_wallets = {
            "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503": "Binance",
            "0x28c6c06298d514db089934071355e5743bf21d60": "Binance Hot",
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",
            "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance CEO",
        }

        ETHERSCAN_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
        if not ETHERSCAN_KEY:
            # Use free tier without key (rate limited but works)
            ETHERSCAN_KEY = "YourApiKeyToken"

        # Get large recent transactions (>100 ETH)
        r = requests.get(
            "https://api.etherscan.io/api",
            params={
                "module":     "account",
                "action":     "txlist",
                "address":    "0x28c6c06298d514db089934071355e5743bf21d60",
                "startblock": 0,
                "endblock":   99999999,
                "page":       1,
                "offset":     20,
                "sort":       "desc",
                "apikey":     ETHERSCAN_KEY,
            },
            timeout=8,
        )
        if r.status_code == 200:
            txs = r.json().get("result", [])
            if isinstance(txs, list):
                inflows  = sum(1 for tx in txs if tx.get("to","").lower() in exchange_wallets)
                outflows = sum(1 for tx in txs if tx.get("from","").lower() in exchange_wallets)

                if outflows > inflows * 1.5:
                    flow_signal = "ACCUMULATION — whales moving to cold wallets (BULLISH)"
                    flow_score  = 10
                elif inflows > outflows * 1.5:
                    flow_signal = "DISTRIBUTION — whales moving to exchanges (BEARISH)"
                    flow_score  = -8
                else:
                    flow_signal = "NEUTRAL — balanced flows"
                    flow_score  = 0

                return {
                    "exchange_inflows":  inflows,
                    "exchange_outflows": outflows,
                    "flow_signal":       flow_signal,
                    "flow_score":        flow_score,
                }
    except Exception as e:
        log.debug("Etherscan error: %s", e)
    return {}


# ═══════════════════════════════════════════
# PATTERN RECOGNITION — Historical patterns
# ═══════════════════════════════════════════

def fetch_historical_pattern(symbol: str) -> dict:
    """
    Recognize historical price patterns from Binance 4h candles.
    Checks if current conditions match past winning setups.
    Patterns: Golden Cross, Death Cross, Double Bottom, RSI Divergence
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "4h", "limit": 100},
            timeout=8,
        )
        if r.status_code != 200:
            return {}

        candles = r.json()
        closes  = [float(c[4]) for c in candles]
        highs   = [float(c[2]) for c in candles]
        lows    = [float(c[3]) for c in candles]
        volumes = [float(c[5]) for c in candles]

        patterns = []
        score_adj = 0

        # 1. Golden Cross (EMA20 crosses above EMA50)
        def ema(data, period):
            k = 2/(period+1)
            e = [data[0]]
            for p in data[1:]:
                e.append(p*k + e[-1]*(1-k))
            return e

        ema20 = ema(closes, 20)
        ema50 = ema(closes, 50)
        if ema20[-1] > ema50[-1] and ema20[-2] < ema50[-2]:
            patterns.append("GOLDEN CROSS — EMA20 crossed EMA50 (strong BUY)")
            score_adj += 15
        elif ema20[-1] < ema50[-1] and ema20[-2] > ema50[-2]:
            patterns.append("DEATH CROSS — EMA20 crossed below EMA50 (strong SELL)")
            score_adj -= 15

        # 2. Double Bottom pattern
        recent_lows = lows[-20:]
        min_low = min(recent_lows)
        low_count = sum(1 for l in recent_lows if abs(l-min_low)/min_low < 0.02)
        if low_count >= 2 and closes[-1] > min_low * 1.03:
            patterns.append("DOUBLE BOTTOM — strong reversal pattern (BUY)")
            score_adj += 12

        # 3. Volume Breakout
        avg_vol = sum(volumes[-20:-1]) / 19
        if volumes[-1] > avg_vol * 2.5 and closes[-1] > closes[-2]:
            patterns.append("VOLUME BREAKOUT — 2.5x average volume (BUY confirmation)")
            score_adj += 10
        elif volumes[-1] > avg_vol * 2.5 and closes[-1] < closes[-2]:
            patterns.append("VOLUME BREAKDOWN — 2.5x average volume (SELL confirmation)")
            score_adj -= 10

        # 4. Higher Highs / Lower Lows trend
        recent_highs = highs[-10:]
        if all(recent_highs[i] > recent_highs[i-1] for i in range(1,5)):
            patterns.append("HIGHER HIGHS — strong uptrend (BUY)")
            score_adj += 8
        elif all(recent_highs[i] < recent_highs[i-1] for i in range(1,5)):
            patterns.append("LOWER HIGHS — strong downtrend (SELL)")
            score_adj -= 8

        # 5. RSI Divergence
        closes_rsi = closes[-15:]
        gains  = [max(closes_rsi[i]-closes_rsi[i-1],0) for i in range(1,len(closes_rsi))]
        losses = [max(closes_rsi[i-1]-closes_rsi[i],0) for i in range(1,len(closes_rsi))]
        ag,al  = sum(gains)/14, sum(losses)/14
        rsi_current = 100-(100/(1+ag/al)) if al>0 else 100

        price_trend = closes[-1] > closes[-8]   # price going up
        rsi_trend   = rsi_current > 50           # RSI going up

        if price_trend and not rsi_trend:
            patterns.append("BEARISH DIVERGENCE — price up but RSI down (SELL warning)")
            score_adj -= 10
        elif not price_trend and rsi_trend:
            patterns.append("BULLISH DIVERGENCE — price down but RSI up (BUY opportunity)")
            score_adj += 10

        return {
            "patterns":     patterns[:3],
            "score_adj":    score_adj,
            "ema20":        round(ema20[-1], 4),
            "ema50":        round(ema50[-1], 4),
            "trend":        "UPTREND" if ema20[-1] > ema50[-1] else "DOWNTREND",
            "rsi_4h":       round(rsi_current, 1),
        }
    except Exception as e:
        log.debug("Pattern recognition error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# FUTURES BASIS — Spot vs Futures premium
# ═══════════════════════════════════════════

def fetch_futures_basis(symbol: str) -> dict:
    """
    Futures basis = (futures_price - spot_price) / spot_price * 100
    High positive = market overleveraged LONG (bearish signal)
    High negative = market overleveraged SHORT (bullish signal)
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        with ThreadPoolExecutor(max_workers=2) as pool:
            f_spot = pool.submit(requests.get,
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": ticker}, timeout=5)
            f_fut  = pool.submit(requests.get,
                "https://fapi.binance.com/fapi/v1/ticker/price",
                params={"symbol": ticker}, timeout=5)
        spot_r = f_spot.result()
        fut_r  = f_fut.result()
        if spot_r.status_code == 200 and fut_r.status_code == 200:
            spot = float(spot_r.json()["price"])
            fut  = float(fut_r.json()["price"])
            basis = (fut - spot) / spot * 100
            if basis > 1.0:
                signal = "OVERLEVERAGED LONG — bearish risk"
                score  = -8
            elif basis > 0.3:
                signal = "SLIGHT PREMIUM — normal bullish"
                score  = 3
            elif basis < -0.3:
                signal = "BACKWARDATION — bearish sentiment"
                score  = -5
            else:
                signal = "FAIR VALUE"
                score  = 0
            return {
                "basis_pct": round(basis, 4),
                "signal":    signal,
                "score":     score,
                "spot":      round(spot, 4),
                "futures":   round(fut, 4),
            }
    except Exception as e:
        log.debug("Futures basis error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# MARKET DEPTH SCORE — Institutional interest
# ═══════════════════════════════════════════

def fetch_market_depth_score(symbol: str) -> dict:
    """
    Analyzes order book depth to detect institutional orders.
    Large hidden orders = institutional interest = strong signal.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": ticker, "limit": 500},
            timeout=6,
        )
        if r.status_code == 200:
            book  = r.json()
            bids  = [(float(p), float(q)) for p,q in book["bids"]]
            asks  = [(float(p), float(q)) for p,q in book["asks"]]
            if not bids or not asks:
                return {}
            mid   = (bids[0][0] + asks[0][0]) / 2
            # Calculate depth within 1% and 2%
            bid_1pct = sum(q for p,q in bids if p >= mid * 0.99)
            ask_1pct = sum(q for p,q in asks if p <= mid * 1.01)
            bid_2pct = sum(q for p,q in bids if p >= mid * 0.98)
            ask_2pct = sum(q for p,q in asks if p <= mid * 1.02)
            # Detect iceberg orders (many small orders at same price)
            bid_prices = [p for p,q in bids[:50]]
            price_clusters = len(set(round(p,1) for p in bid_prices))
            iceberg = price_clusters < 20  # many orders at same levels
            ratio_1pct = bid_1pct / ask_1pct if ask_1pct > 0 else 1
            if ratio_1pct > 2.0:
                depth_signal = "STRONG BID WALL — institutional buying"
                dscore = 12
            elif ratio_1pct > 1.3:
                depth_signal = "BID PRESSURE — buyers in control"
                dscore = 6
            elif ratio_1pct < 0.5:
                depth_signal = "STRONG ASK WALL — institutional selling"
                dscore = -10
            else:
                depth_signal = "BALANCED DEPTH"
                dscore = 0
            return {
                "bid_depth_1pct":  round(bid_1pct, 2),
                "ask_depth_1pct":  round(ask_1pct, 2),
                "ratio_1pct":      round(ratio_1pct, 3),
                "depth_signal":    depth_signal,
                "iceberg_detected": iceberg,
                "score":           dscore,
            }
    except Exception as e:
        log.debug("Market depth error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# CVIX — Crypto Volatility Index
# ═══════════════════════════════════════════

def fetch_crypto_volatility_index() -> dict:
    """
    Calculate crypto VIX from BTC price movement.
    High volatility = big moves coming = trade with tighter stops.
    """
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1h", "limit": 24},
            timeout=5,
        )
        if r.status_code == 200:
            candles = r.json()
            returns = []
            for i in range(1, len(candles)):
                prev = float(candles[i-1][4])
                curr = float(candles[i][4])
                if prev > 0:
                    returns.append((curr - prev) / prev * 100)
            if returns:
                import math
                avg = sum(returns) / len(returns)
                variance = sum((r - avg)**2 for r in returns) / len(returns)
                std_dev = math.sqrt(variance)
                # Annualize (24h * 365)
                cvix = round(std_dev * math.sqrt(24 * 365), 2)
                if cvix > 100:
                    vol_regime = "EXTREME — use tight stops (2-3%)"
                elif cvix > 60:
                    vol_regime = "HIGH — reduce position size"
                elif cvix > 30:
                    vol_regime = "NORMAL — standard position"
                else:
                    vol_regime = "LOW — can increase position"
                return {
                    "cvix":       cvix,
                    "vol_regime": vol_regime,
                    "std_dev_1h": round(std_dev, 4),
                }
    except Exception as e:
        log.debug("CVIX error: %s", e)
    return {}


# ═══════════════════════════════════════════
# SOCIAL DOMINANCE TRACKER
# ═══════════════════════════════════════════

def fetch_social_dominance_score(symbol: str) -> dict:
    """
    Track social media dominance via CoinGecko community data.
    Rising community = rising interest = price follows.
    """
    try:
        coin_map = {
            "BTC":"bitcoin","ETH":"ethereum","SOL":"solana",
            "XRP":"ripple","BNB":"binancecoin","ADA":"cardano",
            "DOGE":"dogecoin","AVAX":"avalanche-2","LINK":"chainlink",
            "DOT":"polkadot","MATIC":"matic-network","NEAR":"near",
        }
        ticker = symbol.replace("/USDT","").replace("/","").upper()
        cg_id  = coin_map.get(ticker)
        if not cg_id:
            return {}
        r = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cg_id}",
            params={
                "localization": "false",
                "tickers":      "false",
                "market_data":  "true",
                "community_data": "true",
                "developer_data": "false",
            },
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            md   = data.get("market_data", {})
            cd   = data.get("community_data", {})
            # Market data
            price_chg_7d = md.get("price_change_percentage_7d", 0) or 0
            price_chg_30d = md.get("price_change_percentage_30d", 0) or 0
            mcap_rank    = data.get("market_cap_rank", 999)
            # Community
            twitter_f = cd.get("twitter_followers", 0) or 0
            reddit_s  = cd.get("reddit_subscribers", 0) or 0
            reddit_active = cd.get("reddit_accounts_active_48h", 0) or 0
            # Activity score
            activity_score = min(100, int(reddit_active / 100)) if reddit_active else 0
            social_signal = "HIGH_ACTIVITY" if activity_score > 70 else                            "MODERATE" if activity_score > 30 else "LOW"
            return {
                "mcap_rank":      mcap_rank,
                "price_7d":       round(price_chg_7d, 2),
                "price_30d":      round(price_chg_30d, 2),
                "twitter_followers": twitter_f,
                "reddit_active":  reddit_active,
                "activity_score": activity_score,
                "social_signal":  social_signal,
            }
    except Exception as e:
        log.debug("Social dominance error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# RISK MANAGEMENT — Position sizing & alerts
# ═══════════════════════════════════════════

def calculate_risk_management(signal: dict, intel: dict) -> dict:
    """
    Calculate safe position sizing based on:
    - Signal conviction/score
    - Market volatility (CVIX)
    - Current market session
    - Portfolio risk rules (max 25% total, 5% per trade)
    """
    score      = signal.get("score", 50)
    conviction = signal.get("conviction", "MEDIUM")
    sig_type   = signal.get("signal", "WAIT")

    # Base position size by conviction
    # Futures position sizing (smaller than spot due to leverage)
    if conviction == "HIGH" and score >= 80:
        base_pct = 4.0   # max 4% per futures trade (leveraged)
    elif conviction == "HIGH":
        base_pct = 3.0
    elif conviction == "MEDIUM" and score >= 70:
        base_pct = 2.5
    elif conviction == "MEDIUM":
        base_pct = 2.0
    else:
        base_pct = 1.0   # LOW conviction = tiny position

    # Futures-specific: reduce if funding rate is high (bad for longs)
    fund_rate = 0.0
    try:
        fund_data = intel.get("funding", {})
        fund_rate = float(fund_data.get("rate", 0) or 0)
    except:
        pass
    if signal.get("signal") == "BUY" and fund_rate > 0.1:
        base_pct *= 0.5
        log.debug("High funding rate %.3f%% — reducing long size by 50%%", fund_rate)

    # Adjust for volatility
    cvix_data = intel.get("cvix", {})
    cvix_val  = cvix_data.get("cvix", 50)
    if cvix_val > 100:
        base_pct *= 0.5   # extreme volatility = half size
        vol_note = "⚠️ תנודתיות קיצונית — גודל פוזיציה קוצץ ב-50%"
    elif cvix_val > 60:
        base_pct *= 0.75
        vol_note = "⚠️ תנודתיות גבוהה — גודל פוזיציה קוצץ ב-25%"
    else:
        vol_note = "✅ תנודתיות נורמלית"

    # Adjust for market session
    from zoneinfo import ZoneInfo
    now_il = datetime.now(ZoneInfo("Asia/Jerusalem"))
    hour   = now_il.hour
    if 14 <= hour < 22:
        session_note = "🟢 שעות US — נזילות מקסימלית"
        session_mult = 1.0
    elif 9 <= hour < 14:
        session_note = "🔵 שעות EU — נזילות טובה"
        session_mult = 0.9
    else:
        session_note = "🟡 שעות אסיה/לילה — נזילות נמוכה, הקטן פוזיציה"
        session_mult = 0.75

    final_pct = round(base_pct * session_mult, 1)

    # Risk/Reward calculation
    entry     = float(signal.get("entry", 0) or 0)
    stop      = float(signal.get("stop_loss", 0) or 0)
    target1   = float(signal.get("target1", 0) or 0)
    target3   = float(signal.get("target3", 0) or 0)

    risk_pct = reward_pct = rr_ratio = 0
    if entry > 0 and stop > 0:
        risk_pct   = abs(entry - stop) / entry * 100
        if target3 > 0:
            reward_pct = abs(target3 - entry) / entry * 100
            rr_ratio   = round(reward_pct / risk_pct, 1) if risk_pct > 0 else 0

    # Hebrew explanation
    direction = signal.get("direction", "המתן")
    symbol    = signal.get("symbol", "CRYPTO/USDT")

    if sig_type == "BUY":
        action_he = f"קנה {symbol}"
        color     = "🟢"
    elif sig_type == "SELL":
        action_he = f"מכור {symbol}"
        color     = "🔴"
    else:
        action_he = f"המתן — אל תיכנס ל-{symbol}"
        color     = "🟡"

    warning = ""
    if final_pct > 5:
        warning = "⚠️ אל תשקיע יותר מ-5% מהתיק בעסקה אחת"
    if rr_ratio < 2 and sig_type in ("BUY","SELL"):
        warning += " | ⚠️ יחס סיכון/תשואה נמוך מ-1:2"

    # Calculate liquidation price
    lev_str = signal.get("leverage","x10").replace("x","").replace("X","")
    try:
        lev = float(lev_str)
    except:
        lev = 10.0

    liq_price = "NA"
    if entry > 0 and lev > 0:
        if sig_type == "BUY":
            liq_price = round(entry * (1 - 0.9/lev), 4)
        else:
            liq_price = round(entry * (1 + 0.9/lev), 4)

    funding_warn = ""
    if fund_rate > 0.1 and sig_type == "BUY":
        funding_warn = "⚠️ פאנדינג גבוה — לונגים משלמים שורטים. שקול להקטין פוזיציה."

    return {
        "position_size_pct":  final_pct,
        "max_portfolio_risk":  25,
        "risk_pct":           round(risk_pct, 2),
        "reward_pct":         round(reward_pct, 2),
        "rr_ratio":           rr_ratio,
        "liq_price":          str(liq_price),
        "leverage":           lev_str,
        "vol_note":           vol_note,
        "session_note":       session_note,
        "warning":            (warning + " " + funding_warn).strip(),
        "action_hebrew":      color + " " + action_he,
        "simple_explanation": "חוזה עתידי — " + sig_type + " על " + symbol + ". "
                              + "ניקוד: " + str(score) + "/100. "
                              + "פוזיציה: " + str(final_pct) + "% מהתיק. "
                              + "מינוף: x" + str(lev_str) + ". "
                              + "מחיר חיסול: $" + str(liq_price) + ". "
                              + "יחס סיכון/תשואה: 1:" + str(rr_ratio) + ".",
    }


# ═══════════════════════════════════════════
# CONFLUENCE DETECTOR — Only signal when multiple
# indicators agree at same price level
# ═══════════════════════════════════════════

def detect_confluence(intel: dict, signal_type: str) -> dict:
    """
    Top sites use confluence — multiple indicators must agree.
    This function counts how many indicators confirm the signal.
    Returns confluence score and list of confirming indicators.
    If < 4 indicators confirm → reject signal entirely.
    """
    confirmations = []
    rejections    = []

    if not intel:
        return {"confirmations":[],"rejections":[],"count":0,"level":"NO_CONFLUENCE","pass":False,"summary":"0/10"}
    # 1. RSI
    try:
        rsi = float(str(intel.get("rsi","50") or "50"))
    except Exception:
        rsi = 50
    if signal_type == "BUY" and rsi < 35:
        confirmations.append(f"RSI oversold ({rsi})")
    elif signal_type == "SELL" and rsi > 65:
        confirmations.append(f"RSI overbought ({rsi})")
    elif signal_type == "BUY" and rsi > 65:
        rejections.append(f"RSI too high for BUY ({rsi})")
    elif signal_type == "SELL" and rsi < 35:
        rejections.append(f"RSI too low for SELL ({rsi})")

    # 2. MACD
    macd = intel.get("macd", {})
    if signal_type == "BUY" and macd.get("trend") == "BULLISH":
        confirmations.append("MACD bullish")
    elif signal_type == "SELL" and macd.get("trend") == "BEARISH":
        confirmations.append("MACD bearish")
    elif macd.get("trend") == "BEARISH" and signal_type == "BUY":
        rejections.append("MACD bearish vs BUY")

    # 3. Funding Rate
    fund = intel.get("funding", {})
    try:
        rate = float(fund.get("rate","0") or "0")
        if signal_type == "BUY" and rate < -0.03:
            confirmations.append(f"Funding negative ({rate}%) — shorts squeezable")
        elif signal_type == "SELL" and rate > 0.05:
            confirmations.append(f"Funding positive ({rate}%) — longs squeezable")
        elif signal_type == "BUY" and rate > 0.1:
            rejections.append(f"High positive funding for BUY ({rate}%)")
    except:
        pass

    # 4. Volume
    vol = intel.get("volume", {})
    if vol.get("spike") == "YES":
        confirmations.append(f"Volume spike {vol.get('ratio','?')}x")
    elif vol.get("spike") == "NO":
        rejections.append("Low volume — weak signal")

    # 5. BTC Correlation
    btc = intel.get("btc_correlation", {})
    trend = btc.get("trend","")
    if signal_type == "BUY" and "STRONG UP" in trend:
        confirmations.append("BTC rising — alts follow")
    elif signal_type == "BUY" and "STRONG DOWN" in trend:
        rejections.append("BTC falling — bad for BUY")
    elif signal_type == "SELL" and "STRONG DOWN" in trend:
        confirmations.append("BTC falling — SELL confirmed")

    # 6. Bollinger Bands
    bb = intel.get("bollinger", {})
    pos = bb.get("position","")
    if signal_type == "BUY" and "BELOW LOWER" in pos:
        confirmations.append("Price below Bollinger lower band")
    elif signal_type == "SELL" and "ABOVE UPPER" in pos:
        confirmations.append("Price above Bollinger upper band")

    # 7. EMA Trend
    ema = intel.get("ema", {})
    ema_trend = ema.get("trend","")
    if signal_type == "BUY" and "BULLISH" in ema_trend:
        confirmations.append("EMA trend bullish")
    elif signal_type == "SELL" and "BEARISH" in ema_trend:
        confirmations.append("EMA trend bearish")
    elif signal_type == "BUY" and "BEARISH" in ema_trend:
        rejections.append("EMA trend bearish vs BUY")

    # 8. Options P/C Ratio
    derib = intel.get("deribit_options", {})
    pc = derib.get("put_call_ratio", 1)
    if signal_type == "BUY" and pc and float(pc) > 1.2:
        confirmations.append(f"High Put/Call ratio — fear = buy opp")
    elif signal_type == "SELL" and pc and float(pc) < 0.6:
        confirmations.append(f"Low Put/Call ratio — greed = sell opp")

    # 9. Market Mood
    corr = intel.get("correlation_matrix", {})
    mood = corr.get("market_mood","")
    if signal_type == "BUY" and mood == "RISK_ON":
        confirmations.append("Market in risk-on mode")
    elif signal_type == "SELL" and mood == "RISK_OFF":
        confirmations.append("Market in risk-off mode")
    elif signal_type == "BUY" and mood == "RISK_OFF":
        rejections.append("Market risk-off — avoid BUY")

    # 10. Pattern Recognition
    patt = intel.get("price_patterns", {})
    patterns = patt.get("patterns", [])
    for p in patterns[:2]:
        if signal_type == "BUY" and any(w in p for w in ["BUY","BULLISH","BOTTOM","GOLDEN"]):
            confirmations.append(p[:40])
        elif signal_type == "SELL" and any(w in p for w in ["SELL","BEARISH","DEATH","BREAKDOWN"]):
            confirmations.append(p[:40])

    total = len(confirmations)
    rejected = len(rejections)

    # Confluence decision
    if total >= 7:
        level = "STRONG"
        pass_signal = True
    elif total >= 5:
        level = "GOOD"
        pass_signal = True
    elif total >= 3 and rejected == 0:
        level = "WEAK"
        pass_signal = True
    else:
        level = "NO_CONFLUENCE"
        pass_signal = False

    return {
        "confirmations":     confirmations,
        "rejections":        rejections,
        "count":             total,
        "level":             level,
        "pass":              pass_signal,
        "summary":           f"{total} מתוך 10 אינדיקטורים מסכימים",
    }


# ═══════════════════════════════════════════
# SIGNAL CANCELLATION — Cancel stale signals
# ═══════════════════════════════════════════

def should_cancel_signal(symbol: str, original_entry: str, signal_type: str) -> tuple[bool, str]:
    """
    Cancel signal if market conditions changed significantly since signal was created.
    Returns (should_cancel, reason)
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","")
        live   = get_live_price_fast(ticker)
        if not live or live == "NA":
            return False, ""
        live_f   = float(live)
        entry_f  = float(original_entry or "0")
        if entry_f <= 0:
            return False, ""
        move_pct = abs(live_f - entry_f) / entry_f * 100
        # Cancel if price moved more than 3% from entry
        if move_pct > 3:
            return True, f"מחיר זז {round(move_pct,1)}% מנקודת הכניסה"
        return False, ""
    except:
        return False, ""


# ═══════════════════════════════════════════
# ENHANCED NEWS SYSTEM — Hebrew + Impact + Category
# ═══════════════════════════════════════════

_news_analysis_cache: dict = {}

# Source credibility scores (1-10)
SOURCE_CREDIBILITY = {
    # Tier 1 — Most reliable
    "Binance Official":     10,
    "CoinDesk":             9,
    "The Block":            9,
    "Blockworks":           9,
    "Reuters Crypto":       10,
    "Bloomberg Crypto":     10,
    # Tier 2 — Reliable
    "Cointelegraph":        8,
    "Decrypt":              8,
    "CoinGecko Trending":   7,
    "Coinbase Blog":        8,
    "Whale Alert":          8,
    # Tier 3 — OK
    "CryptoSlate":          6,
    "CryptoNews":           6,
    "Bitcoinist":           5,
    "BeInCrypto":           5,
    "U.Today":              5,
    "CryptoPanic Hot":      5,
    # Tier 4 — Use with caution
    "Reddit":               4,
    "NewsAPI":              5,
    "CryptoBriefing":       6,
}

def get_source_credibility(source: str) -> int:
    """Return credibility score 1-10 for a source."""
    return SOURCE_CREDIBILITY.get(source, 5)


def filter_stale_news(items: list[dict], max_age_minutes: int = 60) -> list[dict]:
    """
    Filter out news older than max_age_minutes.
    Keeps only fresh news for signal generation.
    """
    from datetime import timezone
    now = datetime.now(timezone.utc)
    fresh = []
    stale_count = 0

    for item in items:
        pub = item.get("published","") or item.get("pub_date","")
        if not pub:
            fresh.append(item)  # no timestamp = assume fresh
            continue
        try:
            # Parse ISO format
            if "T" in pub:
                dt = datetime.fromisoformat(pub.replace("Z","+00:00"))
            else:
                dt = datetime.strptime(pub[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_minutes = (now - dt).total_seconds() / 60
            if age_minutes <= max_age_minutes:
                item["age_minutes"] = int(age_minutes)
                fresh.append(item)
            else:
                stale_count += 1
        except Exception:
            fresh.append(item)  # can't parse = assume fresh

    if stale_count > 0:
        log.debug("[FRESHNESS] Filtered %d stale items (>%dm)", stale_count, max_age_minutes)
    return fresh


def categorize_news(title: str, summary: str = "") -> str:
    """Categorize news into main crypto categories."""
    text = (title + " " + summary).lower()
    if any(w in text for w in ["bitcoin","btc","satoshi","lightning network"]):
        return "BTC"
    elif any(w in text for w in ["ethereum","eth","vitalik","erc-20","gas fee"]):
        return "ETH"
    elif any(w in text for w in ["solana","sol","avalanche","avax","bnb","binance"]):
        return "ALTCOIN"
    elif any(w in text for w in ["defi","uniswap","aave","compound","yield","liquidity","tvl"]):
        return "DeFi"
    elif any(w in text for w in ["sec","regulation","ban","law","government","congress","legal","etf"]):
        return "REGULATION"
    elif any(w in text for w in ["whale","million","billion","transfer","moved","exchange flow"]):
        return "WHALE"
    elif any(w in text for w in ["hack","exploit","scam","rug","stolen","breach","vulnerability"]):
        return "SECURITY"
    elif any(w in text for w in ["nft","metaverse","gaming","web3","dao"]):
        return "NFT/WEB3"
    else:
        return "GENERAL"


def calculate_news_heat(title: str, summary: str = "") -> int:
    """
    Calculate news heat score 1-10.
    Higher = more likely to move the market.
    """
    text = (title + " " + summary).lower()
    score = 3  # base

    # High impact keywords
    high_impact = [
        "sec approved","etf approved","federal reserve","interest rate",
        "hack","exploit","billion","major exchange","bankrupt","arrested",
        "banned","regulation","institutional","blackrock","fidelity","jp morgan",
        "all-time high","crash","liquidation","whale alert","emergency"
    ]
    medium_impact = [
        "partnership","listing","upgrade","fork","launch","integration",
        "adoption","million","staking","airdrop","merger","acquisition"
    ]

    for kw in high_impact:
        if kw in text:
            score += 2
    for kw in medium_impact:
        if kw in text:
            score += 1

    # Boost for numbers (specific data = more credible)
    import re
    if re.search(r"[$][0-9,]+", text):
        score += 1
    if re.search(r"[0-9]+[%]", text):
        score += 1

    return min(10, max(1, score))


def translate_news_batch(items: list[dict]) -> list[dict]:
    """
    Translate and analyze top news items in batch using GPT.
    Returns items with Hebrew titles, explanations, and impact.
    Only processes top 15 items to save API costs.
    """
    if not items:
        return items

    top_items = items[:15]

    # Build batch prompt
    news_list_parts = []
    for i, item in enumerate(top_items):
        news_list_parts.append(str(i+1) + ". " + item.get("title",""))
    news_list = "\n".join(news_list_parts)

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": """You are a crypto news analyst.
For each news item, provide JSON with:
- title_he: Hebrew translation of the title
- why_hot: 1 sentence in Hebrew explaining WHY this matters to crypto traders
- impact: which coins are affected (e.g. "BTC, ETH" or "כל השוק")
- sentiment: "BULLISH" or "BEARISH" or "NEUTRAL"
Return ONLY a JSON array, no markdown."""},
                {"role": "user", "content": "Analyze these crypto news:\n" + news_list}
            ],
            temperature=0.1,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "{}").strip()
        if raw.startswith("```"):
            raw = re.sub(r"```[a-z]*\n?","",raw).replace("```","").strip()
        import json as _json
        parsed = _json.loads(raw)
        if isinstance(parsed, dict):
            # Find the array in the dict
            arr = next((v for v in parsed.values() if isinstance(v, list)), [])
        else:
            arr = parsed

        for i, item in enumerate(top_items):
            if i < len(arr):
                analysis = arr[i]
                if isinstance(analysis, dict):
                    item["title_he"]  = analysis.get("title_he", item.get("title",""))
                    item["why_hot"]   = analysis.get("why_hot", "")
                    item["impact"]    = analysis.get("impact", "")
                    item["sentiment_news"] = analysis.get("sentiment", "NEUTRAL")

    except Exception as e:
        log.error("[News Translation] Batch error: %s", e)
        # Fallback: translate individually (slower)
        for item in top_items[:5]:
            if not item.get("title_he"):
                item["title_he"] = translate_to_hebrew(item.get("title",""))

    # Add categories and heat scores
    for item in items:
        if not item.get("category"):
            item["category"] = categorize_news(
                item.get("title",""), item.get("summary","")
            )
        if not item.get("heat"):
            item["heat"] = calculate_news_heat(
                item.get("title",""), item.get("summary","")
            )

    return items


# Cache for translated news
_translated_news_cache: list = []
_translated_news_time: float = 0
TRANSLATED_NEWS_TTL = 300  # 5 minutes


def get_translated_news() -> list:
    """Get news with translations, cached for 5 minutes."""
    import time
    global _translated_news_cache, _translated_news_time
    if _translated_news_cache and time.time() - _translated_news_time < TRANSLATED_NEWS_TTL:
        return _translated_news_cache
    # Fetch fresh news
    raw_news = fetch_all_sources(remaining_quota=50)
    # Sort by heat
    for item in raw_news:
        item["heat"] = calculate_news_heat(
            item.get("title",""), item.get("summary","")
        )
        item["category"] = categorize_news(
            item.get("title",""), item.get("summary","")
        )
    raw_news.sort(key=lambda x: x.get("heat",0), reverse=True)
    # Translate top items
    translated = translate_news_batch(raw_news)
    _translated_news_cache = translated
    _translated_news_time = time.time()
    return translated


# ═══════════════════════════════════════════
# FUTURES-SPECIFIC INTELLIGENCE
# ═══════════════════════════════════════════

def fetch_open_interest_change(symbol: str) -> dict:
    """
    Track OI change over time — rising OI + price up = strong trend.
    Falling OI + price up = weak move, likely reversal.
    This is one of the most reliable futures signals.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        # Get OI history (5m intervals, last 1 hour)
        r = requests.get(
            "https://fapi.binance.com/futures/data/openInterestHist",
            params={"symbol": ticker, "period": "5m", "limit": 12},
            timeout=6,
        )
        if r.status_code == 200:
            data = r.json()
            if len(data) >= 2:
                oi_old = float(data[0].get("sumOpenInterest", 0))
                oi_new = float(data[-1].get("sumOpenInterest", 0))
                oi_change = (oi_new - oi_old) / oi_old * 100 if oi_old > 0 else 0

                # Get price change in same period
                pr = requests.get(
                    "https://fapi.binance.com/fapi/v1/klines",
                    params={"symbol": ticker, "interval": "5m", "limit": 12},
                    timeout=5,
                )
                price_change = 0
                if pr.status_code == 200:
                    candles = pr.json()
                    if candles:
                        p_old = float(candles[0][1])
                        p_new = float(candles[-1][4])
                        price_change = (p_new - p_old) / p_old * 100 if p_old > 0 else 0

                # Interpret OI + Price combination
                if oi_change > 2 and price_change > 1:
                    signal = "STRONG TREND — OI rising + price rising = real buyers"
                    score = 15
                elif oi_change > 2 and price_change < -1:
                    signal = "STRONG DOWNTREND — OI rising + price falling = real sellers"
                    score = -12
                elif oi_change < -2 and price_change > 1:
                    signal = "SHORT SQUEEZE — OI falling + price rising = shorts closing"
                    score = 10
                elif oi_change < -2 and price_change < -1:
                    signal = "LONG LIQUIDATION — OI falling + price falling = longs forced out"
                    score = -10
                else:
                    signal = "NEUTRAL — no clear directional bias"
                    score = 0

                return {
                    "oi_change_pct":  round(oi_change, 3),
                    "price_change_pct": round(price_change, 3),
                    "oi_signal":      signal,
                    "oi_score":       score,
                    "oi_current":     round(oi_new, 0),
                }
    except Exception as e:
        log.debug("OI change error %s: %s", symbol, e)
    return {}


def fetch_funding_rate_history(symbol: str) -> dict:
    """
    Analyze funding rate trend over 8 hours.
    Consistently high funding = overheated longs = SELL signal.
    Consistently negative = overheated shorts = BUY signal.
    This is what top futures traders monitor constantly.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": ticker, "limit": 8},
            timeout=6,
        )
        if r.status_code == 200:
            rates = [float(item.get("fundingRate", 0)) * 100 for item in r.json()]
            if rates:
                avg_rate = sum(rates) / len(rates)
                latest   = rates[-1] if rates else 0
                trend    = "RISING" if len(rates) >= 2 and rates[-1] > rates[0] else "FALLING"

                if avg_rate > 0.08:
                    sentiment = "EXTREME LONG BIAS — longs overheated, SHORT opportunity"
                    score = -12  # bad for BUY
                elif avg_rate > 0.04:
                    sentiment = "HIGH LONG BIAS — reduce long size"
                    score = -6
                elif avg_rate < -0.04:
                    sentiment = "EXTREME SHORT BIAS — shorts overheated, LONG opportunity"
                    score = 12  # good for BUY
                elif avg_rate < -0.02:
                    sentiment = "HIGH SHORT BIAS — good for LONG"
                    score = 6
                else:
                    sentiment = "BALANCED — neutral funding"
                    score = 0

                return {
                    "avg_funding_8h":  round(avg_rate, 4),
                    "latest_funding":  round(latest, 4),
                    "funding_trend":   trend,
                    "funding_sentiment": sentiment,
                    "funding_score":   score,
                    "annualized_rate": round(avg_rate * 3 * 365, 1),  # 3x daily, 365 days
                }
    except Exception as e:
        log.debug("Funding history error %s: %s", symbol, e)
    return {}


def fetch_long_short_ratio_detailed(symbol: str) -> dict:
    """
    Detailed long/short analysis from multiple timeframes.
    Extreme long bias (>70%) = contrarian SHORT signal.
    Extreme short bias (<30% long) = contrarian LONG signal.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        results = {}

        # Top trader accounts ratio (most accurate)
        r1 = requests.get(
            "https://fapi.binance.com/futures/data/topLongShortAccountRatio",
            params={"symbol": ticker, "period": "1h", "limit": 1},
            timeout=5,
        )
        if r1.status_code == 200 and r1.json():
            d = r1.json()[0]
            long_pct = float(d.get("longAccount", 0.5)) * 100
            results["top_traders_long_pct"] = round(long_pct, 1)
            if long_pct > 70:
                results["top_signal"] = "TOP TRADERS EXTREME LONG — contrarian SELL"
                results["top_score"]  = -10
            elif long_pct < 30:
                results["top_signal"] = "TOP TRADERS EXTREME SHORT — contrarian BUY"
                results["top_score"]  = 10
            else:
                results["top_signal"] = "BALANCED"
                results["top_score"]  = 0

        # Global account ratio
        r2 = requests.get(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            params={"symbol": ticker, "period": "1h", "limit": 1},
            timeout=5,
        )
        if r2.status_code == 200 and r2.json():
            d2 = r2.json()[0]
            global_long = float(d2.get("longAccount", 0.5)) * 100
            results["global_long_pct"] = round(global_long, 1)

        return results
    except Exception as e:
        log.debug("L/S detailed error %s: %s", symbol, e)
    return {}


def fetch_volume_profile(symbol: str) -> dict:
    """
    Volume Profile — find Point of Control (POC) and High Volume Nodes.
    POC = price level with most trading = strongest support/resistance.
    Used by institutional traders to find entries.
    """
    try:
        ticker = symbol.replace("/USDT","").replace("/","") + "USDT"
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": ticker, "interval": "1h", "limit": 48},
            timeout=6,
        )
        if r.status_code == 200:
            candles = r.json()
            # Build volume profile
            price_vol = {}
            for c in candles:
                high  = float(c[2])
                low   = float(c[3])
                close = float(c[4])
                vol   = float(c[5])
                # Round to 0.1% buckets
                bucket = round(close, int(2 - len(str(int(close)))))
                price_vol[bucket] = price_vol.get(bucket, 0) + vol

            if price_vol:
                poc = max(price_vol.keys(), key=lambda k: price_vol[k])
                current = float(candles[-1][4])
                # HVN and LVN
                sorted_levels = sorted(price_vol.items(), key=lambda x: x[1], reverse=True)
                top_levels    = [str(round(p, 4)) for p,v in sorted_levels[:3]]

                return {
                    "poc":           round(poc, 4),
                    "current_price": round(current, 4),
                    "poc_vs_price":  "ABOVE POC" if current > poc else "BELOW POC",
                    "top_volume_levels": top_levels,
                    "signal": "BULLISH — price above POC" if current > poc else "BEARISH — price below POC",
                }
    except Exception as e:
        log.debug("Volume profile error %s: %s", symbol, e)
    return {}


# ═══════════════════════════════════════════
# EDUCATIONAL NEWS SYSTEM
# ═══════════════════════════════════════════

def fetch_crypto_edu_content() -> list[dict]:
    """
    Fetch educational crypto content from multiple sources.
    Combines breaking news + explanation + market impact.
    """
    edu_items = []

    # 1. CoinDesk Learn RSS
    try:
        r = _http_session.get(
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            timeout=7,
        )
        if r.status_code == 200:
            items = _parse_rss(r.text, "CoinDesk", 8)
            for item in items:
                item["edu_type"] = "NEWS"
            edu_items.extend(items)
    except Exception as e:
        log.debug("CoinDesk edu error: %s", e)

    # 2. Investopedia Crypto RSS (educational)
    try:
        r2 = _http_session.get(
            "https://www.investopedia.com/cryptocurrency-4427699",
            timeout=7,
        )
        if r2.status_code == 200:
            import re as _re
            titles = _re.findall(r'<h\d[^>]*class="[^"]*title[^"]*"[^>]*>([^<]+)<', r2.text)
            for t in titles[:5]:
                if len(t) > 20:
                    edu_items.append({
                        "title": t.strip(),
                        "summary": "",
                        "source": "Investopedia",
                        "edu_type": "EDUCATIONAL",
                    })
    except Exception as e:
        log.debug("Investopedia error: %s", e)

    # 3. CryptoSlate News
    try:
        r3 = _http_session.get(
            "https://cryptoslate.com/feed/",
            timeout=7,
        )
        if r3.status_code == 200:
            items3 = _parse_rss(r3.text, "CryptoSlate", 6)
            for item in items3:
                item["edu_type"] = "ANALYSIS"
            edu_items.extend(items3)
    except Exception as e:
        log.debug("CryptoSlate error: %s", e)

    return edu_items


def generate_news_education(item: dict) -> dict:
    """
    Use GPT to generate full educational breakdown of a news item.
    Returns:
    - title_he: Hebrew title
    - why_hot: Why this matters (Hebrew)
    - market_impact: How it affects prices (Hebrew)
    - what_is: Simple explanation of the concept (Hebrew)
    - trading_lesson: What traders can learn (Hebrew)
    - affected_coins: Which coins are affected
    - sentiment: BULLISH/BEARISH/NEUTRAL
    - heat: 1-10 importance score
    - key_terms: Important terms explained simply
    """
    try:
        title   = item.get("title","")
        summary = item.get("summary","")[:300]

        prompt = (
            "Crypto news educator. Respond ONLY with JSON. "
            "NEWS: " + title + " | DETAILS: " + summary + " | "
            "JSON keys needed: title_he (Hebrew title), "
            "why_hot (Hebrew: why matters now), "
            "market_impact (Hebrew: price effect), "
            "what_is (Hebrew: simple explanation for beginners), "
            "trading_lesson (Hebrew: what traders learn), "
            "affected_coins (e.g. BTC,ETH), "
            "sentiment (BULLISH/BEARISH/NEUTRAL), "
            "heat (integer 1-10), "
            "action (Hebrew: what futures trader should do)"
        )
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.15,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
        import json as _json
        _raw = (response.choices[0].message.content or "{}").strip()
        if _raw.startswith("```"):
            _raw = re.sub(r"```[a-z]*\n?","",_raw).replace("```","").strip()
        data = _json.loads(_raw)
        item.update(data)
    except Exception as e:
        log.debug("News education error: %s", e)
        item.setdefault("title_he", item.get("title",""))
        item.setdefault("why_hot", "")
        item.setdefault("market_impact", "")
        item.setdefault("what_is", "")
        item.setdefault("trading_lesson", "")
        item.setdefault("affected_coins", "")
        item.setdefault("sentiment", "NEUTRAL")
        item.setdefault("heat", 5)
        item.setdefault("action", "")
    return item


# Cache for educational news
_edu_news_cache: list = []
_edu_news_time: float = 0
EDU_NEWS_TTL = 600  # 10 minutes


def get_educational_news() -> list:
    """Get fully enriched educational news, cached 10 min."""
    import time
    global _edu_news_cache, _edu_news_time
    if _edu_news_cache and time.time() - _edu_news_time < EDU_NEWS_TTL:
        return _edu_news_cache

    # Get all news sources
    all_news = fetch_all_sources(remaining_quota=100)
    edu_extra = fetch_crypto_edu_content()
    all_news.extend(edu_extra)

    # Remove duplicates by title similarity
    seen_titles = set()
    unique_news = []
    for item in all_news:
        title_key = item.get("title","")[:40].lower().strip()
        if title_key not in seen_titles and len(title_key) > 10:
            seen_titles.add(title_key)
            unique_news.append(item)

    # Score heat for all
    for item in unique_news:
        if not item.get("heat"):
            item["heat"] = calculate_news_heat(
                item.get("title",""), item.get("summary","")
            )
        if not item.get("category"):
            item["category"] = categorize_news(
                item.get("title",""), item.get("summary","")
            )

    # Sort by heat
    unique_news.sort(key=lambda x: x.get("heat",0), reverse=True)

    # Educate top 8 items with full GPT analysis
    for item in unique_news[:8]:
        if not item.get("why_hot"):
            generate_news_education(item)

    # Batch translate rest (cheaper)
    rest = [i for i in unique_news[8:20] if not i.get("title_he")]
    if rest:
        translate_news_batch(rest)

    _edu_news_cache = unique_news[:60]
    _edu_news_time  = time.time()
    return _edu_news_cache


# ═══════════════════════════════════════════
# SELF-LEARNING AI ENGINE
# Learns from every signal outcome — win or loss
# Automatically improves parameters over time
# ═══════════════════════════════════════════

import json as _json_mod

LEARNING_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "learning_data.json")

def load_learning_data() -> dict:
    """Load accumulated learning data from file."""
    try:
        with open(LEARNING_DB_PATH, "r") as f:
            return _json_mod.load(f)
    except Exception:
        return {
            "version": 1,
            "total_analyzed": 0,
            "score_thresholds": {"optimal": 65, "high": 75},
            "source_weights": {},
            "indicator_weights": {
                "rsi": 1.0, "macd": 1.0, "funding": 1.0,
                "volume": 1.0, "btc_correlation": 1.0,
                "bollinger": 1.0, "ema": 1.0, "oi_change": 1.0,
                "funding_history": 1.0, "ls_detailed": 1.0,
                "deribit_options": 1.0, "smart_money": 1.0,
                "price_patterns": 1.0, "volume_profile": 1.0,
            },
            "market_session_accuracy": {
                "US": {"wins":0,"total":0},
                "EU": {"wins":0,"total":0},
                "ASIA": {"wins":0,"total":0},
                "QUIET": {"wins":0,"total":0},
            },
            "direction_accuracy": {
                "BUY": {"wins":0,"total":0},
                "SELL": {"wins":0,"total":0},
            },
            "score_range_accuracy": {
                "65-70": {"wins":0,"total":0},
                "71-75": {"wins":0,"total":0},
                "76-80": {"wins":0,"total":0},
                "81-85": {"wins":0,"total":0},
                "86-100": {"wins":0,"total":0},
            },
            "winning_patterns": [],
            "losing_patterns": [],
            "last_calibration": None,
        }


def save_learning_data(data: dict) -> None:
    """Save learning data to file."""
    try:
        with open(LEARNING_DB_PATH, "w") as f:
            _json_mod.dump(data, f, indent=2)
    except Exception as e:
        log.error("[LEARN] Save error: %s", e)


def learn_from_signal(signal_row: dict) -> None:
    """
    Learn from a closed signal outcome.
    Updates weights and thresholds based on result.
    Called every time a signal closes (win OR loss).
    """
    result = signal_row.get("result", "OPEN")
    if result == "OPEN":
        return

    data = load_learning_data()
    data["total_analyzed"] += 1
    won = result == "TARGET_HIT"

    # 1. Track score range accuracy
    score = int(signal_row.get("score") or 50)
    if score <= 70:
        bucket = "65-70"
    elif score <= 75:
        bucket = "71-75"
    elif score <= 80:
        bucket = "76-80"
    elif score <= 85:
        bucket = "81-85"
    else:
        bucket = "86-100"

    if bucket in data["score_range_accuracy"]:
        data["score_range_accuracy"][bucket]["total"] += 1
        if won:
            data["score_range_accuracy"][bucket]["wins"] += 1

    # 2. Track direction accuracy
    direction = signal_row.get("signal", "")
    if direction in data["direction_accuracy"]:
        data["direction_accuracy"][direction]["total"] += 1
        if won:
            data["direction_accuracy"][direction]["wins"] += 1

    # 3. Track source accuracy
    source = signal_row.get("source", "Unknown")
    if source not in data["source_weights"]:
        data["source_weights"][source] = {"wins":0,"total":0,"weight":1.0}
    data["source_weights"][source]["total"] += 1
    if won:
        data["source_weights"][source]["wins"] += 1

    # 4. Market session tracking
    ts = signal_row.get("timestamp", "")
    session = "QUIET"
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.strptime(ts[:16], "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("UTC"))
        il_h = dt.astimezone(ZoneInfo("Asia/Jerusalem")).hour
        if 14 <= il_h < 22:
            session = "US"
        elif 9 <= il_h < 14:
            session = "EU"
        elif 1 <= il_h < 9:
            session = "ASIA"
    except Exception:
        pass

    if session in data["market_session_accuracy"]:
        data["market_session_accuracy"][session]["total"] += 1
        if won:
            data["market_session_accuracy"][session]["wins"] += 1

    # 5. Store winning/losing patterns for GPT to learn from
    pattern = {
        "score": score,
        "signal": signal_row.get("signal"),
        "source": source,
        "session": session,
        "result": result,
        "pnl": signal_row.get("result_pnl", "0"),
    }
    if won:
        data["winning_patterns"].append(pattern)
        data["winning_patterns"] = data["winning_patterns"][-100:]  # keep last 100
    else:
        data["losing_patterns"].append(pattern)
        data["losing_patterns"] = data["losing_patterns"][-100:]

    # 6. Auto-calibrate weights every 20 signals
    if data["total_analyzed"] % 20 == 0:
        auto_calibrate(data)

    save_learning_data(data)
    log.info("[LEARN] Recorded %s for %s (score=%d, session=%s)",
             result, signal_row.get("symbol","?"), score, session)


def auto_calibrate(data: dict) -> None:
    """
    Automatically adjust parameters based on accumulated learning.
    Called every 20 signals.
    """
    log.info("[LEARN] Running auto-calibration on %d signals...", data["total_analyzed"])

    # 1. Find optimal score threshold
    best_bucket = None
    best_rate = 0
    for bucket, stats in data["score_range_accuracy"].items():
        if stats["total"] >= 5:
            rate = stats["wins"] / stats["total"]
            if rate > best_rate:
                best_rate = rate
                best_bucket = bucket

    if best_bucket:
        thresholds = {"65-70":65,"71-75":71,"76-80":76,"81-85":81,"86-100":86}
        new_threshold = thresholds.get(best_bucket, 65)
        data["score_thresholds"]["optimal"] = new_threshold
        log.info("[LEARN] New score threshold: %d (win rate %.1f%% in bucket %s)",
                 new_threshold, best_rate*100, best_bucket)

    # 2. Update source weights
    for source, stats in data["source_weights"].items():
        if stats["total"] >= 5:
            win_rate = stats["wins"] / stats["total"]
            # Weight: 0.5 to 2.0 based on performance vs 50% baseline
            new_weight = max(0.5, min(2.0, win_rate * 2))
            stats["weight"] = round(new_weight, 2)
            log.info("[LEARN] Source '%s': win_rate=%.1f%% weight=%.1f",
                     source, win_rate*100, new_weight)

    # 3. Find best market session
    best_session = max(
        data["market_session_accuracy"].items(),
        key=lambda x: x[1]["wins"]/x[1]["total"] if x[1]["total"]>=3 else 0,
        default=("US",{})
    )
    log.info("[LEARN] Best session: %s", best_session[0])

    data["last_calibration"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    log.info("[LEARN] Auto-calibration complete!")


def get_learning_insights() -> dict:
    """Get current learning insights for the AI prompt and scoring."""
    data = load_learning_data()
    insights = {
        "optimal_score_threshold": data["score_thresholds"].get("optimal", 65),
        "total_analyzed": data["total_analyzed"],
        "best_sources": [],
        "worst_sources": [],
        "best_session": None,
        "score_accuracy": {},
        "buy_win_rate": 0,
        "sell_win_rate": 0,
    }

    # Best/worst sources
    ranked = sorted(
        [(s, v) for s,v in data["source_weights"].items() if v["total"]>=3],
        key=lambda x: x[1]["wins"]/x[1]["total"],
        reverse=True
    )
    insights["best_sources"]  = [s for s,v in ranked[:3]]
    insights["worst_sources"] = [s for s,v in ranked[-2:]]

    # Best session
    sess_rates = {
        s: v["wins"]/v["total"]
        for s,v in data["market_session_accuracy"].items()
        if v["total"]>=3
    }
    if sess_rates:
        insights["best_session"] = max(sess_rates, key=sess_rates.get)

    # Score accuracy
    for bucket, stats in data["score_range_accuracy"].items():
        if stats["total"] >= 3:
            insights["score_accuracy"][bucket] = round(stats["wins"]/stats["total"]*100, 1)

    # Direction accuracy
    buy_stats = data["direction_accuracy"].get("BUY", {})
    sell_stats = data["direction_accuracy"].get("SELL", {})
    if buy_stats.get("total", 0) >= 3:
        insights["buy_win_rate"] = round(buy_stats["wins"]/buy_stats["total"]*100, 1)
    if sell_stats.get("total", 0) >= 3:
        insights["sell_win_rate"] = round(sell_stats["wins"]/sell_stats["total"]*100, 1)

    return insights


def build_learning_prompt(insights: dict) -> str:
    """Build learning context for GPT prompt."""
    if not insights or insights.get("total_analyzed", 0) < 5:
        return ""

    lines = [f"LEARNING CONTEXT ({insights['total_analyzed']} signals analyzed):"]

    if insights.get("best_sources"):
        lines.append(f"MOST ACCURATE SOURCES: {', '.join(insights['best_sources'])}")
    if insights.get("worst_sources"):
        lines.append(f"LEAST ACCURATE SOURCES: {', '.join(insights['worst_sources'])} — reduce confidence")
    if insights.get("best_session"):
        lines.append(f"BEST TRADING SESSION: {insights['best_session']}")
    if insights.get("buy_win_rate"):
        lines.append(f"HISTORICAL BUY WIN RATE: {insights['buy_win_rate']}%")
    if insights.get("sell_win_rate"):
        lines.append(f"HISTORICAL SELL WIN RATE: {insights['sell_win_rate']}%")
    if insights.get("score_accuracy"):
        for bucket, rate in sorted(insights["score_accuracy"].items()):
            lines.append(f"SCORE {bucket}: {rate}% historical win rate")

    return "\n".join(lines)


# ═══════════════════════════════════════════
# REAL-TIME NEWS ENGINE
# Multiple instant sources — no delay
# ═══════════════════════════════════════════

import threading as _threading
from collections import deque

# Real-time news buffer — stores last 200 items
_realtime_news_buffer: deque = deque(maxlen=200)
_realtime_news_lock = _threading.Lock()
_last_realtime_scan: float = 0


def add_to_realtime_buffer(items: list[dict]) -> int:
    """Add fresh news to buffer. Returns count of new items added."""
    added = 0
    with _realtime_news_lock:
        existing_fps = {item.get("fingerprint","") for item in _realtime_news_buffer}
        for item in items:
            fp = item.get("title","")[:60].lower().strip()
            item["fingerprint"] = fp
            item["received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if fp and fp not in existing_fps:
                _realtime_news_buffer.appendleft(item)
                existing_fps.add(fp)
                added += 1
    return added


def fetch_cryptopanic_realtime() -> list[dict]:
    """
    CryptoPanic public API — no key needed for basic tier.
    Returns breaking news with coins tagged.
    """
    try:
        r = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={
                "auth_token": os.environ.get("CRYPTOPANIC_API_KEY",""),
                "public": "true",
                "filter": "hot",
                "kind": "news",
            },
            timeout=6,
        )
        if r.status_code == 200:
            results = r.json().get("results", [])
            items = []
            for p in results[:15]:
                title = p.get("title","")
                url   = p.get("url","")
                pub   = p.get("published_at","")
                currencies = [c.get("code","") for c in (p.get("currencies") or [])]
                source_name = (p.get("source") or {}).get("title","CryptoPanic")
                items.append({
                    "title":     title,
                    "summary":   "",
                    "source":    source_name,
                    "url":       url,
                    "published": pub,
                    "symbols":   currencies or ["CRYPTO"],
                    "is_hot":    True,
                    "heat":      calculate_news_heat(title),
                    "category":  categorize_news(title),
                })
            log.info("[RT] CryptoPanic: %d items", len(items))
            return items
    except Exception as e:
        log.debug("[RT] CryptoPanic error: %s", e)
    return []


def fetch_binance_announcements_realtime() -> list[dict]:
    """
    Binance official announcements — instant listings, delistings, etc.
    These are THE most market-moving news.
    """
    try:
        r = requests.get(
            "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
            params={"type":1,"pageNo":1,"pageSize":10},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=6,
        )
        if r.status_code == 200:
            articles = r.json().get("data",{}).get("articles",[])
            items = []
            for a in articles:
                title = a.get("title","")
                if title and any(kw in title.lower() for kw in
                    ["list","delist","launch","futures","perpetual","margin","adds","will"]):
                    items.append({
                        "title":    title,
                        "summary":  title,
                        "source":   "Binance Official",
                        "published": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "symbols":  ["CRYPTO"],
                        "is_hot":   True,
                        "heat":     10,  # Binance official = always max heat
                        "category": categorize_news(title),
                    })
            log.info("[RT] Binance: %d announcements", len(items))
            return items
    except Exception as e:
        log.debug("[RT] Binance announcements error: %s", e)
    return []


def fetch_twitter_rss_realtime() -> list[dict]:
    """
    Follow major crypto Twitter accounts via Nitter RSS (free).
    Accounts: whale_alert, WuBlockchain, lookonchain, DegenSpartan
    """
    accounts = [
        ("WuBlockchain",    "https://nitter.net/WuBlockchain/rss"),
        ("lookonchain",     "https://nitter.net/lookonchain/rss"),
        ("whale_alert",     "https://nitter.net/whale_alert/rss"),
        ("AltcoinGordon",   "https://nitter.net/AltcoinGordon/rss"),
    ]
    items = []
    for account, url in accounts:
        try:
            r = _http_session.get(url, timeout=5)
            if r.status_code == 200:
                parsed = _parse_rss(r.text, f"Twitter/{account}", 5)
                for p in parsed:
                    p["heat"] = calculate_news_heat(p.get("title",""))
                    p["category"] = categorize_news(p.get("title",""))
                    p["is_realtime"] = True
                items.extend(parsed)
        except Exception as e:
            log.debug("[RT] Twitter/%s error: %s", account, e)
    log.info("[RT] Twitter RSS: %d items", len(items))
    return items


def fetch_rss_realtime_all() -> list[dict]:
    """
    Fetch all real-time RSS sources in parallel.
    Target: under 4 seconds total.
    """
    items = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(fetch_cryptopanic_realtime):       "CryptoPanic",
            pool.submit(fetch_binance_announcements_realtime): "Binance",
            pool.submit(fetch_twitter_rss_realtime):       "Twitter",
            pool.submit(fetch_whale_alert_rss):            "WhaleAlert",
        }
        for f, name in futures.items():
            try:
                result = f.result(timeout=7)
                items.extend(result)
            except Exception as e:
                log.debug("[RT] %s failed: %s", name, e)

    # Filter to last 30 minutes only
    items = filter_stale_news(items, max_age_minutes=30)
    # Sort by heat descending
    items.sort(key=lambda x: x.get("heat",0), reverse=True)
    return items


def realtime_news_scanner():
    """
    Background thread — scans for breaking news every 90 seconds.
    If HOT news (heat>=8) found → triggers immediate signal analysis.
    """
    global _last_realtime_scan
    import time

    log.info("[RT-SCANNER] Real-time news scanner started")
    import time as _rt_time
    while True:
        try:
            _rt_time.sleep(90)  # scan every 90 seconds
            fresh = fetch_rss_realtime_all()
            if not fresh:
                continue

            added = add_to_realtime_buffer(fresh)
            if added > 0:
                log.info("[RT-SCANNER] %d new breaking items", added)

            # Check for HOT news (heat>=8) → trigger immediate scan
            hot_items = [i for i in fresh if i.get("heat",0) >= 8]
            if hot_items:
                log.info("[RT-SCANNER] 🔥 HOT NEWS DETECTED: %s",
                         hot_items[0].get("title","")[:60])
                # Trigger signal job immediately if quota allows
                try:
                    from datetime import timezone
                    today = datetime.now(timezone.utc).date().isoformat()
                    with get_db() as conn:
                        count = conn.execute(
                            "SELECT COUNT(*) as c FROM signals WHERE DATE(timestamp)=?",
                            (today,)
                        ).fetchone()["c"]
                    if count < DAILY_LIMIT:
                        log.info("[RT-SCANNER] Triggering immediate scan for hot news!")
                        t = _threading.Thread(target=run_signal_job, daemon=True)
                        t.start()
                except Exception as e:
                    log.debug("[RT-SCANNER] Auto-trigger error: %s", e)

        except Exception as e:
            log.error("[RT-SCANNER] Error: %s", e)


def start_realtime_scanner():
    """Start the real-time news scanner background thread."""
    t = _threading.Thread(target=realtime_news_scanner, daemon=True, name="RT-Scanner")
    t.start()
    log.info("[RT-SCANNER] Started background real-time scanner")


def check_signal_results():
    """
    Background job: check if open signals hit target or stop loss.
    Runs every 5 minutes alongside the main sweep.
    """
    try:
        all_tickers = get_all_binance_tickers()
        with get_db() as conn:
            open_signals = conn.execute(
                """SELECT id, symbol, signal, entry, stop_loss, target1
                   FROM signals WHERE result = 'OPEN'
                   AND entry != 'NA' AND entry != ''
                   ORDER BY timestamp DESC LIMIT 50"""
            ).fetchall()

        for sig in open_signals:
            try:
                ticker  = str(sig["symbol"]).replace("/USDT","").replace("/","")
                current = float(all_tickers.get(ticker, 0) or 0)
                if current == 0:
                    continue
                entry   = float(sig["entry"] or 0)
                sl      = float(sig["stop_loss"] or 0)
                t1      = float(sig["target1"] or 0)
                if entry == 0:
                    continue

                pnl_pct = ((current - entry) / entry * 100) if sig["signal"] == "BUY"                            else ((entry - current) / entry * 100)
                result  = "OPEN"
                if sig["signal"] == "BUY":
                    if sl > 0 and current <= sl:
                        result = "STOP_LOSS"
                    elif t1 > 0 and current >= t1:
                        result = "TARGET_HIT"
                elif sig["signal"] == "SELL":
                    if sl > 0 and current >= sl:
                        result = "STOP_LOSS"
                    elif t1 > 0 and current <= t1:
                        result = "TARGET_HIT"

                if result != "OPEN":
                    with get_db() as conn:
                        conn.execute(
                            """UPDATE signals SET result=?, result_price=?, result_pnl=?, result_time=?
                               WHERE id=?""",
                            (result, str(current), str(round(pnl_pct,2)),
                             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), sig["id"])
                        )
                        conn.commit()
                    log.info("[RESULT] %s %s → %s | PnL: %.2f%%",
                             sig["symbol"], sig["signal"], result, pnl_pct)
            except Exception:
                continue
    except Exception as e:
        log.error("check_signal_results error: %s", e)


def get_market_session_multiplier() -> float:
    """
    Returns score multiplier based on market session.
    US session (14:00-22:00 Israel) = 1.0 (no penalty)
    Asia session (01:00-09:00 Israel) = 0.85 (require higher score)
    Dead hours = 0.75 (very strict)
    """
    try:
        now_israel = datetime.now(ZoneInfo("Asia/Jerusalem"))
        hour = now_israel.hour
        if 14 <= hour < 22:
            return 1.0
        elif 9 <= hour < 14:
            return 0.9
        elif 1 <= hour < 9:
            return 0.85
        else:
            return 0.75
    except Exception:
        return 1.0  # default to no penalty on error


@app.route("/api/winrate")
def api_winrate():
    """Real-time win rate calculation."""
    try:
        with get_db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) as c FROM signals WHERE result != 'OPEN'"
            ).fetchone()["c"]
            hits = conn.execute(
                "SELECT COUNT(*) as c FROM signals WHERE result = 'TARGET_HIT'"
            ).fetchone()["c"]
            sl = conn.execute(
                "SELECT COUNT(*) as c FROM signals WHERE result = 'STOP_LOSS'"
            ).fetchone()["c"]
            # By signal type
            by_type = conn.execute(
                """SELECT signal, result, COUNT(*) as cnt
                   FROM signals WHERE result != 'OPEN'
                   GROUP BY signal, result"""
            ).fetchall()
            # Average PnL
            avg_pnl = conn.execute(
                """SELECT AVG(CAST(result_pnl AS FLOAT)) as avg
                   FROM signals WHERE result != 'OPEN' AND result_pnl != 'NA'"""
            ).fetchone()["avg"]
            # Last 7 days
            week_hits = conn.execute(
                """SELECT COUNT(*) as c FROM signals
                   WHERE result='TARGET_HIT'
                   AND timestamp >= datetime('now','-7 days')"""
            ).fetchone()["c"]
            week_total = conn.execute(
                """SELECT COUNT(*) as c FROM signals
                   WHERE result != 'OPEN'
                   AND timestamp >= datetime('now','-7 days')"""
            ).fetchone()["c"]

        win_rate = round(hits / total * 100, 1) if total > 0 else 0
        week_rate = round(week_hits / week_total * 100, 1) if week_total > 0 else 0

        return jsonify({
            "total_closed":  total,
            "hits":          hits,
            "stop_losses":   sl,
            "win_rate_pct":  win_rate,
            "week_win_rate": week_rate,
            "avg_pnl_pct":   round(avg_pnl or 0, 2),
            "by_type":       [dict(r) for r in by_type],
        })
    except Exception as e:
        log.error("api/winrate: %s", e)
        return jsonify({}), 500


@app.route("/api/backtest")
def api_backtest():
    """Return backtest analysis results."""
    try:
        result = run_backtest_analysis()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/learning")
def api_learning():
    """Return self-learning data and insights."""
    try:
        data = load_learning_data()
        insights = get_learning_insights()
        return jsonify({
            "data": data,
            "insights": insights,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/learning/reset", methods=["POST"])
def api_learning_reset():
    """Reset learning data (use carefully)."""
    try:
        import os
        if os.path.exists(LEARNING_DB_PATH):
            os.remove(LEARNING_DB_PATH)
        return jsonify({"ok": True, "message": "Learning data reset"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/news/realtime")
def api_news_realtime():
    """Return only real-time breaking news (last 30 min)."""
    try:
        with _realtime_news_lock:
            items = list(_realtime_news_buffer)[:30]
        for item in items:
            item["credibility"] = get_source_credibility(item.get("source",""))
        return jsonify(items)
    except Exception as e:
        return jsonify([]), 500


@app.route("/api/news/knowledge", methods=["POST"])
def api_send_knowledge():
    """Send the next knowledge tip to the Telegram channel."""
    if not TELEGRAM_NEWS_TOKEN:
        return jsonify({"ok": False, "error": "News bot not configured"}), 400
    try:
        ok = send_daily_knowledge()
        tip_num = (_knowledge_index % len(CRYPTO_KNOWLEDGE_BANK)) + 1
        return jsonify({"ok": ok, "tip_number": tip_num, "total": len(CRYPTO_KNOWLEDGE_BANK)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/news/broadcast", methods=["POST"])
def api_news_broadcast():
    """Manually send the hottest educational news to the Telegram channel in Hebrew."""
    if not TELEGRAM_TOKEN:
        return jsonify({"ok": False, "error": "Telegram not configured"}), 400
    try:
        min_heat  = int(request.args.get("min_heat", 4))
        max_items = int(request.args.get("max_items", 8))
        chat_id   = request.args.get("chat_id", "")
        sent = broadcast_news_to_telegram(min_heat=min_heat, max_items=max_items, chat_id=chat_id)
        return jsonify({"ok": True, "sent": sent})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/prices")
def api_prices_bulk():
    """Return all live prices from WebSocket cache."""
    try:
        prices = {}
        for sym, price in _ws_prices.items():
            coin = sym.replace("USDT","")
            prices[coin] = price
        # Add REST prices for coins not in WS
        return jsonify({"prices": prices, "count": len(prices), "source": "websocket"})
    except Exception as e:
        return jsonify({"prices":{}, "error": str(e)}), 500


@app.route("/api/price/live/<symbol>")
def api_price_live(symbol: str):
    """Get single coin live price with entry/exit signals."""
    try:
        ticker = symbol.upper().replace("/USDT","").replace("/","")
        ws_sym = ticker + "USDT"

        # Get price from WebSocket cache (instant)
        price = _ws_prices.get(ws_sym)
        if not price:
            price_str = get_live_price(ticker)
            price = float(price_str) if price_str != "NA" else None

        if not price:
            return jsonify({"error": "price not available"}), 404

        # Get indicators for entry/exit signals
        intel = {}
        try:
            intel = gather_market_intel(ticker + "/USDT")
        except Exception:
            pass

        # Calculate entry/exit recommendation
        rsi = float(intel.get("rsi","50") or "50") if intel else 50
        vwap = intel.get("vwap",{}).get("vwap_price",0)
        try:
            vwap_f = float(vwap or 0)
        except Exception:
            vwap_f = 0

        # Entry signal
        if rsi < 30 and price < vwap_f * 0.99:
            entry_signal = "OVERSOLD — good entry for LONG"
            entry_color = "green"
        elif rsi > 70 and price > vwap_f * 1.01:
            entry_signal = "OVERBOUGHT — good entry for SHORT"
            entry_color = "red"
        else:
            entry_signal = "NEUTRAL — wait for better entry"
            entry_color = "amber"

        # ATR-based stop loss
        atr = intel.get("atr",{}) if intel else {}
        atr_pct = float(atr.get("atr_pct",2.0) or 2.0)
        stop_long  = round(price * (1 - atr_pct/100 * 1.5), 6)
        stop_short = round(price * (1 + atr_pct/100 * 1.5), 6)

        return jsonify({
            "symbol":       ticker + "/USDT",
            "price":        price,
            "rsi":          rsi,
            "vwap":         vwap_f,
            "entry_signal": entry_signal,
            "entry_color":  entry_color,
            "stop_long":    stop_long,
            "stop_short":   stop_short,
            "atr_pct":      atr_pct,
            "source":       "websocket" if ws_sym in _ws_prices else "rest",
        })
    except Exception as e:
        log.error("api/price/live: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def api_health():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1").fetchone()
        return jsonify({"status": "ok", "db": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "db": str(e)}), 500

@app.route("/api/logs")
def api_logs():
    """Return last 100 lines of app.log for the dashboard log viewer."""
    try:
        if not os.path.exists(LOG_FILE):
            return jsonify({"lines": []})
        with open(LOG_FILE, encoding="utf-8") as f:
            lines = f.readlines()
        return jsonify({"lines": [l.rstrip() for l in lines[-100:]]})
    except Exception as e:
        log.error("api/logs: %s", e)
        return jsonify({"lines": [], "error": str(e)}), 500

# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        func=run_signal_job, trigger="interval", minutes=FETCH_INTERVAL,
        id="signal_job", next_run_time=datetime.now(ZoneInfo("UTC")),
    )
    # Start real-time price WebSocket
    ensure_websocket()
    # Start real-time news scanner
    start_realtime_scanner()
    scheduler.add_job(
        func=ensure_websocket,
        trigger="interval",
        minutes=5,
        id="ws_health",
    )
    # Run learning calibration every 6 hours
    scheduler.add_job(
        func=lambda: auto_calibrate(load_learning_data()),
        trigger="interval",
        hours=6,
        id="learning_calibrate",
    )
    scheduler.add_job(
        func=check_signal_results, trigger="interval", minutes=5,
        id="results_job", next_run_time=datetime.now(ZoneInfo("UTC")),
    )
    # Broadcast hot news to Telegram channel every 15 minutes
    scheduler.add_job(
        func=lambda: broadcast_news_to_telegram(min_heat=3, max_items=4),
        trigger="interval",
        minutes=10,
        id="telegram_news",
    )
    # Send daily knowledge tip every 4 hours (6 tips/day)
    scheduler.add_job(
        func=send_daily_knowledge,
        trigger="interval",
        hours=3,
        id="telegram_knowledge",
    )
    scheduler.start()
    log.info("Scheduler running — sweep every %d min", FETCH_INTERVAL)

    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    finally:
        scheduler.shutdown()
        log.info("Scheduler stopped")
