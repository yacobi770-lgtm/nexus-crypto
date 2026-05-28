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

from flask import Flask, jsonify, render_template_string
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

DB_PATH          = "signals.db"
DAILY_LIMIT      = 50
FETCH_INTERVAL   = 30
BATCH_SIZE       = 12
REQUEST_TIMEOUT  = 12
DEDUP_WINDOW_H   = 24
MAX_PROMPT_CHARS = 400

# NewsAPI — works from server environments
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
NEWSAPI_URL = "https://newsapi.org/v2/everything"

# Backup RSS (some may work)
RSS_SOURCES = [
    {"name": "Cointelegraph", "url": "https://cointelegraph.com/rss",  "limit": BATCH_SIZE},
    {"name": "Bitcoin.com",   "url": "https://news.bitcoin.com/feed/", "limit": BATCH_SIZE},
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
                timestamp    TEXT NOT NULL
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
               target_price, source, fingerprint) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO signals
                   (symbol,news_title,sentiment,signal,direction,reason,
                    entry,stop_loss,target1,target2,target3,leverage,
                    target_price,source,fingerprint,timestamp)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, news_title, sentiment, signal, direction, reason,
                 entry, stop_loss, target1, target2, target3, leverage,
                 target_price, source, fingerprint, ts),
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
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
}

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
def fetch_newsapi(max_items: int = BATCH_SIZE) -> list[dict]:
    """Fetch crypto news from NewsAPI — works from server environments."""
    if not NEWSAPI_KEY:
        log.warning("[NewsAPI] NEWSAPI_KEY not set")
        return []
    try:
        resp = requests.get(
            NEWSAPI_URL,
            params={
                "q":        "bitcoin OR ethereum OR crypto OR cryptocurrency",
                "language": "en",
                "sortBy":   "publishedAt",
                "pageSize": max_items,
                "apiKey":   NEWSAPI_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        items = []
        for a in articles:
            title = (a.get("title") or "").strip()
            desc  = (a.get("description") or "").strip()
            src_name = (a.get("source", {}) or {}).get("name", "NewsAPI")
            if title and "[Removed]" not in title:
                items.append({
                    "title":   title,
                    "summary": desc[:200],
                    "symbols": ["CRYPTO"],
                    "source":  src_name,
                })
        log.info("[NewsAPI] %d items fetched", len(items))
        return items
    except Exception as e:
        log.error("[NewsAPI] Error: %s", e)
        return []

def fetch_all_sources(remaining_quota: int) -> list[dict]:
    n = len(RSS_SOURCES)
    per = max(4, min(BATCH_SIZE, remaining_quota // n))
    log.info("Aggregator: fetching %d sources × %d items each (quota left: %d)",
             n, per, remaining_quota)

    futures_map = {}
    with ThreadPoolExecutor(max_workers=n+1) as pool:
        # NewsAPI — primary source (works from servers)
        futures_map[pool.submit(fetch_newsapi, per*2)] = "NewsAPI"
        # RSS — backup
        for source in RSS_SOURCES:
            futures_map[pool.submit(fetch_rss, {**source, "limit": per})] = source["name"]

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
SYSTEM_PROMPT = """You are an expert crypto futures trading analyst. Analyze news headlines and produce actionable futures trading signals in Hebrew.

YOUR TASK
=========
Read the headline. Decide if it is market-moving. If yes, produce a full futures signal in Hebrew. If noise, return SKIP.

OUTPUT FORMAT
=============
Respond ONLY with a valid JSON object. No markdown, no extra text.

Actionable signal (all text values in Hebrew):
{
  "symbol": "<ticker/USDT, e.g. BTC/USDT, ETH/USDT, SOL/USDT>",
  "signal": "BUY|SELL|WAIT",
  "sentiment": "חיובי|שלילי|נייטרלי",
  "direction": "ארוך|קצר|המתן",
  "entry": "<entry price in USD, e.g. 67500>",
  "stop_loss": "<stop loss price in USD>",
  "target1": "<first target price>",
  "target2": "<second target price>",
  "target3": "<third target price>",
  "leverage": "<recommended leverage, e.g. x10, x20>",
  "reason": "<one sharp sentence in Hebrew explaining the signal>",
  "target_price": "<same as target3>"
}

Noise:
{"signal":"SKIP"}

RULES
=====
- BUY → go LONG. direction = ארוך
- SELL → go SHORT. direction = קצר
- WAIT → direction = המתן
- SKIP → not actionable news (opinion, promo, vague)

SYMBOL: always extract coin name from headline:
Bitcoin/BTC → BTC/USDT
Ethereum/ETH → ETH/USDT
Solana/SOL → SOL/USDT
Ripple/XRP → XRP/USDT
Unknown → CRYPTO/USDT

PRICES: use realistic current market prices. 
Leverage: 10x for major coins, 5x for altcoins, never more than 20x.
Stop loss: 5-8% from entry against position direction.
Targets: realistic 3 targets at +3%, +6%, +10% from entry.

Think like a professional futures trader. Be precise."""

def build_prompt(title: str, summary: str) -> str:
    summary = summary.strip()
    raw = f"Headline: {title}\nContext: {summary}" if summary else f"Headline: {title}"
    return raw[:MAX_PROMPT_CHARS]

def analyse_item(title: str, summary: str) -> dict | None:
    prompt = build_prompt(title, summary)
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

        # L5 — AI analysis
        analysis = analyse_item(title, summary)
        if analysis is None:
            log.warning("[ERROR] AI returned None for '%.55s'", title)
            errors += 1
            continue

        if analysis["signal"] == "SKIP":
            log.info("[SKIP][%s] %.70s", source, title)
            ai_skipped += 1
            continue

        # Prefer AI-extracted symbol over RSS metadata
        ai_symbol = analysis.get("symbol", "")
        symbol = ai_symbol if ai_symbol and ai_symbol != "CRYPTO" else (symbols[0] if symbols and symbols[0] != "UNKNOWN" else "CRYPTO")
        save_signal(
            symbol       = symbol,
            news_title   = title,
            sentiment    = analysis["sentiment"],
            signal       = analysis["signal"],
            direction    = analysis.get("direction", "המתן"),
            reason       = analysis["reason"],
            entry        = analysis.get("entry", "NA"),
            stop_loss    = analysis.get("stop_loss", "NA"),
            target1      = analysis.get("target1", "NA"),
            target2      = analysis.get("target2", "NA"),
            target3      = analysis.get("target3", "NA"),
            leverage     = analysis.get("leverage", "x10"),
            target_price = analysis["target_price"],
            source       = source,
            fingerprint  = fp,
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
    scheduler.start()
    log.info("Scheduler running — sweep every %d min", FETCH_INTERVAL)

    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    finally:
        scheduler.shutdown()
        log.info("Scheduler stopped")
