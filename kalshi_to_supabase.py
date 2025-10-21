#!/usr/bin/env python3
import os, sys, time, json, base64, math, re
import requests
from collections import Counter
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from supabase import create_client, Client

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import (
load_pem_private_key, load_ssh_private_key, load_der_private_key
)

# ===================== CONFIG FROM ENV =====================

API_HOST = “https://api.elections.kalshi.com/trade-api/v2”
API_KEY_ID = os.getenv(“KALSHI_API_KEY_ID”)
PRIVATE_KEY = os.getenv(“KALSHI_PRIVATE_KEY”, “”).encode()
PRIVATE_KEY_PASSPHRASE = os.getenv(“KALSHI_KEY_PASSPHRASE”)
if PRIVATE_KEY_PASSPHRASE:
PRIVATE_KEY_PASSPHRASE = PRIVATE_KEY_PASSPHRASE.encode()

SUPABASE_URL = os.getenv(“SUPABASE_URL”)
SUPABASE_SERVICE_KEY = os.getenv(“SUPABASE_SERVICE_KEY”)

PROXIES = None
proxy_http = os.getenv(“HTTP_PROXY”)
if proxy_http:
PROXIES = {“http”: proxy_http, “https”: proxy_http}

CORP_CA_PATH = os.getenv(“CA_BUNDLE_PATH”)
TIMEZONE = os.getenv(“TIMEZONE”, “America/New_York”)
LOOKBACK_DAYS = int(os.getenv(“LOOKBACK_DAYS”, “3”))

# Conservative batch sizes to prevent rate limiting

TICKER_BATCH = 3   # Very small batches to be safe
EVENT_BATCH = 3    # Very small batches to be safe

# Rate limiting delays (seconds)

REQUEST_DELAY = 2.0  # Wait 2 seconds between requests
RETRY_BASE_DELAY = 5.0  # Base delay for exponential backoff
MAX_RETRIES = 3  # Reduce retries to fail faster on persistent issues

# Maximum URL length before splitting batch

MAX_URL_LENGTH = 1500  # More conservative limit

# Regex patterns

SPORTS_REGEX = re.compile(
r”(nfl|mlb|nba|wnba|nhl|laliga|f1|pga|bundesliga|ucl|epl|mls|ligue1|seriea|fifa|ncaa|nascar|atp|wta|mensingles|womensingles|kxmarmad|kxwmarmad|ncaab|ncaaf|ufc|boxing)”,
re.IGNORECASE
)

NFL_REGEX = re.compile(r”nfl”, re.IGNORECASE)
MLB_REGEX = re.compile(r”mlb”, re.IGNORECASE)
WNBA_REGEX = re.compile(r”wnba”, re.IGNORECASE)
NBA_REGEX = re.compile(r”nba”, re.IGNORECASE)
NHL_REGEX = re.compile(r”nhl”, re.IGNORECASE)
SOCCER_REGEX = re.compile(r”(laliga|bundesliga|ucl|epl|mls|ligue1|seriea|fifa)”, re.IGNORECASE)
GOLF_REGEX = re.compile(r”pga”, re.IGNORECASE)
MOTORSPORT_REGEX = re.compile(r”(f1|nascar)”, re.IGNORECASE)
TENNIS_REGEX = re.compile(r”(atp|wta|mensingles|womensingles)”, re.IGNORECASE)
NCAAM_REGEX = re.compile(r”(kxmarmad|ncaam|ncaab)”, re.IGNORECASE)
NCAAW_REGEX = re.compile(r”(kxwmarmad|ncaaw)”, re.IGNORECASE)
NCAAF_REGEX = re.compile(r”ncaaf”, re.IGNORECASE)
COMBAT_REGEX = re.compile(r”(ufc|boxing)”, re.IGNORECASE)

SPORT_CATEGORIES = [“nfl”, “mlb”, “nba”, “wnba”, “nhl”, “soccer”, “golf”, “motorsport”, “tennis”, “ncaam”, “ncaaw”, “ncaaf”, “combat”]

# =================== END CONFIG ====================

def _log(msg: str) -> None:
print(msg, flush=True)

def _load_private_key():
“”“Try PEM -> OpenSSH -> DER(base64).”””
raw = PRIVATE_KEY.strip()
try:
return load_pem_private_key(raw, password=PRIVATE_KEY_PASSPHRASE)
except Exception:
pass
try:
return load_ssh_private_key(raw, password=PRIVATE_KEY_PASSPHRASE)
except Exception:
pass
try:
der = base64.b64decode(raw)
return load_der_private_key(der, password=PRIVATE_KEY_PASSPHRASE)
except Exception as e:
raise ValueError(“Could not parse PRIVATE_KEY as PEM/OpenSSH/DER.”) from e

def _kalshi_headers(method: str, path: str, key):
ts_ms = str(int(time.time() * 1000))
msg = (ts_ms + method.upper() + path).encode(“utf-8”)
sig = key.sign(
msg,
padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
hashes.SHA256(),
)
return {
“KALSHI-ACCESS-KEY”: API_KEY_ID,
“KALSHI-ACCESS-TIMESTAMP”: ts_ms,
“KALSHI-ACCESS-SIGNATURE”: base64.b64encode(sig).decode(“ascii”),
}

def _api_request_with_retry(session, method, url, headers, params=None, max_retries=MAX_RETRIES):
“”“Make API request with exponential backoff retry on rate limit.”””
for attempt in range(max_retries):
try:
r = session.request(method, url, headers=headers, params=params, timeout=60)

```
        if r.status_code == 429:  # Rate limited
            wait_time = RETRY_BASE_DELAY * (2 ** attempt)
            _log(f"  ⚠️  Rate limited (429). Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
            continue
        
        r.raise_for_status()
        return r
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429 and attempt < max_retries - 1:
            wait_time = RETRY_BASE_DELAY * (2 ** attempt)
            _log(f"  ⚠️  Rate limited (429). Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
        else:
            # Re-raise the exception on final attempt or non-429 errors
            raise

raise Exception("Max retries exceeded for API request")
```

def _daterange_inclusive(start_d: date, end_d: date):
cur = start_d
while cur <= end_d:
yield cur
cur += timedelta(days=1)

def _to_utc_bounds_for_local_day(d: date, tz: ZoneInfo):
local_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
local_end = local_start + timedelta(days=1)
return int(local_start.timestamp()), int(local_end.timestamp())

def _chunks(seq, n):
for i in range(0, len(seq), n):
yield seq[i:i+n]

def parse_ts(v) -> int:
if isinstance(v, (int, float)):
x = float(v)
return int(x / 1000.0) if x > 1e12 else int(x)
if isinstance(v, str):
s = v.strip()
if s.isdigit():
x = float(s)
return int(x / 1000.0) if x > 1e12 else int(x)
if s.endswith(“Z”):
s = s[:-1] + “+00:00”
try:
dt = datetime.fromisoformat(s)
if dt.tzinfo is None:
dt = dt.replace(tzinfo=timezone.utc)
return int(dt.timestamp())
except Exception:
pass
for fmt in (”%Y-%m-%dT%H:%M:%S.%f%z”, “%Y-%m-%dT%H:%M:%S%z”):
try:
dt = datetime.strptime(s, fmt)
return int(dt.timestamp())
except Exception:
continue
raise ValueError(f”Unrecognized timestamp format: {v!r}”)

def _get_all_trades(min_ts: int, max_ts: int, session: requests.Session, key):
path = “/markets/trades”
url = f”{API_HOST}{path}”
cursor = None
trades = []
page = 0
_log(f”Fetching trades in UTC span [{min_ts}, {max_ts}) …”)

```
while True:
    params = {"limit": 1000, "min_ts": min_ts, "max_ts": max_ts}
    if cursor:
        params["cursor"] = cursor
    
    headers = _kalshi_headers("GET", path, key)
    r = _api_request_with_retry(session, "GET", url, headers, params)
    
    data = r.json()
    batch = data.get("trades", [])
    trades.extend(batch)
    cursor = data.get("cursor")
    page += 1
    
    if page % 100 == 0:
        _log(f"  ▸ page {page}: +{len(batch)} trades (total: {len(trades)})")
    
    # Small delay to be nice to the API
    time.sleep(0.2)
    
    if not cursor:
        break

_log(f"Total trades fetched: {len(trades)}")
return trades
```

def _lookup_markets(tickers, session: requests.Session, key):
“””
Fetch market metadata for tickers one at a time to avoid URL length issues
and rate limiting. This is slower but more reliable.
“””
path = “/markets”
url = f”{API_HOST}{path}”
out = {}

```
total_tickers = len(tickers)
_log(f"Fetching market metadata for {total_tickers} tickers (one at a time)...")

for i, ticker in enumerate(list(tickers), start=1):
    if i % 50 == 0:
        _log(f"  ▸ Progress: {i}/{total_tickers} tickers processed...")
    
    headers = _kalshi_headers("GET", path, key)
    
    try:
        r = _api_request_with_retry(session, "GET", url, headers, params={"tickers": ticker})
        markets = r.json().get("markets", [])
        
        for m in markets:
            tkr = m.get("ticker")
            cat = (m.get("category") or "").strip()
            evt = (m.get("event_ticker") or m.get("eventTicker") or "").strip()
            if tkr:
                out[tkr] = {"category": cat, "event_ticker": evt}
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            _log(f"  ⚠️  Rate limited on ticker {ticker}, waiting longer...")
            time.sleep(10)  # Wait 10 seconds on rate limit
            # Try one more time for this ticker
            try:
                headers = _kalshi_headers("GET", path, key)
                r = _api_request_with_retry(session, "GET", url, headers, params={"tickers": ticker})
                markets = r.json().get("markets", [])
                
                for m in markets:
                    tkr = m.get("ticker")
                    cat = (m.get("category") or "").strip()
                    evt = (m.get("event_ticker") or m.get("eventTicker") or "").strip()
                    if tkr:
                        out[tkr] = {"category": cat, "event_ticker": evt}
            except Exception as retry_e:
                _log(f"  ✗ Failed to fetch ticker {ticker} after retry: {retry_e}")
        else:
            _log(f"  ✗ Error fetching ticker {ticker}: {e}")
    
    except Exception as e:
        _log(f"  ✗ Unexpected error fetching ticker {ticker}: {e}")
    
    # Rate limiting delay between each request
    time.sleep(REQUEST_DELAY)

_log(f"Successfully fetched metadata for {len(out)}/{total_tickers} tickers")
return out
```

def _lookup_event_categories(event_tickers, session: requests.Session, key):
“””
Fetch event categories one at a time to avoid rate limiting.
“””
path = “/events”
url = f”{API_HOST}{path}”
out = {}
if not event_tickers:
return out

```
total_events = len(event_tickers)
_log(f"Fetching event categories for {total_events} event_ticker(s) (one at a time)...")

for i, event_ticker in enumerate(list(event_tickers), start=1):
    if i % 20 == 0:
        _log(f"  ▸ Progress: {i}/{total_events} events processed...")
    
    headers = _kalshi_headers("GET", path, key)
    
    try:
        r = _api_request_with_retry(session, "GET", url, headers, params={"event_tickers": event_ticker})
        events = r.json().get("events", [])
        
        for e in events:
            evt = (e.get("ticker") or e.get("event_ticker") or "").strip()
            cat = (e.get("category") or "").strip()
            if evt:
                out[evt] = cat
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            _log(f"  ⚠️  Rate limited on event {event_ticker}, waiting longer...")
            time.sleep(10)
            # Try one more time
            try:
                headers = _kalshi_headers("GET", path, key)
                r = _api_request_with_retry(session, "GET", url, headers, params={"event_tickers": event_ticker})
                events = r.json().get("events", [])
                
                for e in events:
                    evt = (e.get("ticker") or e.get("event_ticker") or "").strip()
                    cat = (e.get("category") or "").strip()
                    if evt:
                        out[evt] = cat
            except Exception as retry_e:
                _log(f"  ✗ Failed to fetch event {event_ticker} after retry: {retry_e}")
        else:
            _log(f"  ✗ Error fetching event {event_ticker}: {e}")
    
    except Exception as e:
        _log(f"  ✗ Unexpected error fetching event {event_ticker}: {e}")
    
    # Rate limiting delay between each request
    time.sleep(REQUEST_DELAY)

_log(f"Successfully fetched categories for {len(out)}/{total_events} events")
return out
```

def classify_sport(ticker: str, category: str, event_ticker: str) -> str:
for field in (ticker, category, event_ticker):
if not field:
continue
if WNBA_REGEX.search(field):
return “wnba”
if NFL_REGEX.search(field):
return “nfl”
if MLB_REGEX.search(field):
return “mlb”
if NBA_REGEX.search(field):
return “nba”
if NHL_REGEX.search(field):
return “nhl”
if SOCCER_REGEX.search(field):
return “soccer”
if GOLF_REGEX.search(field):
return “golf”
if MOTORSPORT_REGEX.search(field):
return “motorsport”
if TENNIS_REGEX.search(field):
return “tennis”
if NCAAM_REGEX.search(field):
return “ncaam”
if NCAAW_REGEX.search(field):
return “ncaaw”
if NCAAF_REGEX.search(field):
return “ncaaf”
if COMBAT_REGEX.search(field):
return “combat”
return “”

def main():
if not all([API_KEY_ID, PRIVATE_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY]):
_log(“❌ Missing required environment variables”)
_log(“Required: KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY”)
sys.exit(1)

```
tz = ZoneInfo(TIMEZONE)

end_d = date.today() - timedelta(days=1)
start_d = end_d - timedelta(days=LOOKBACK_DAYS - 1)

_log(f"Processing dates: {start_d} to {end_d} (timezone: {TIMEZONE}, lookback: {LOOKBACK_DAYS} days)")

first_start_ts, _ = _to_utc_bounds_for_local_day(start_d, tz)
_, last_end_ts = _to_utc_bounds_for_local_day(end_d, tz)

key = _load_private_key()

session = requests.Session()
session.headers.update({"User-Agent": "KalshiDailyCron/1.0"})
if PROXIES:
    session.proxies.update(PROXIES)
    _log(f"Using proxies: {PROXIES}")
if CORP_CA_PATH:
    session.verify = CORP_CA_PATH

trades = _get_all_trades(first_start_ts, last_end_ts, session, key)

_log("Bucketing trades by local day...")
totals_by_day = {}
ticker_by_day = {}
unique_tickers = set()

for t in trades:
    ts = None
    for fld in ("created_time", "created_ts", "ts", "timestamp"):
        if fld in t and t[fld] is not None:
            try:
                ts = parse_ts(t[fld])
                break
            except Exception:
                continue
    if ts is None:
        continue

    local_dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)
    day_str = local_dt.date().isoformat()

    qty = int(t.get("count", 0) or 0)
    totals_by_day[day_str] = totals_by_day.get(day_str, 0) + qty

    tk = t.get("ticker")
    if tk:
        unique_tickers.add(tk)
        dmap = ticker_by_day.setdefault(day_str, {})
        dmap[tk] = dmap.get(tk, 0) + qty

for d in _daterange_inclusive(start_d, end_d):
    ds = d.isoformat()
    totals_by_day.setdefault(ds, 0)
    ticker_by_day.setdefault(ds, {})

_log(f"Unique tickers: {len(unique_tickers)}")

markets_map = _lookup_markets(unique_tickers, session, key)
blanks_evt = {info["event_ticker"] for info in markets_map.values() if not info["category"] and info["event_ticker"]}
event_cat_map = _lookup_event_categories(blanks_evt, session, key) if blanks_evt else {}

final_category = {}
for tk, info in markets_map.items():
    cat = (info.get("category") or "").strip()
    evt = info.get("event_ticker", "")
    if cat:
        final_category[tk] = (cat, "market", evt)
    else:
        ev_cat = (event_cat_map.get(evt, "") or "").strip() if evt else ""
        if ev_cat:
            final_category[tk] = (ev_cat, "event", evt)
        else:
            final_category[tk] = ("", "none", evt)

_log("Computing daily volumes...")
rows = []
for d in sorted(totals_by_day.keys()):
    total = totals_by_day[d]
    per_ticker = ticker_by_day[d]
    
    sport_volumes = {sport: 0 for sport in SPORT_CATEGORIES}
    sports_total = 0

    for tk, q in per_ticker.items():
        cat, _src, evt = final_category.get(tk, ("", "none", ""))
        sport = classify_sport(tk, cat, evt)
        if sport:
            sport_volumes[sport] += q
            sports_total += q

    pct = (sports_total / total * 100.0) if total else 0.0
    row = {
        "date": d,
        "total_volume": total,
        "sports_volume": sports_total,
        "sports_pct": round(pct, 4),
        **{f"{sport}_volume": sport_volumes[sport] for sport in SPORT_CATEGORIES},
    }
    rows.append(row)
    _log(f"  {d}: total={total:,} sports={sports_total:,} ({pct:.2f}%)")

_log(f"\nUploading {len(rows)} rows to Supabase...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

for row in rows:
    try:
        result = supabase.table("daily_volumes").upsert(row).execute()
        _log(f"  ✓ Upserted {row['date']}")
    except Exception as e:
        _log(f"  ✗ Error upserting {row['date']}: {e}")

_log("\n✅ Done!")
```

if **name** == “**main**”:
try:
main()
except Exception as e:
_log(f”❌ Error: {e}”)
import traceback
traceback.print_exc()
sys.exit(1)
