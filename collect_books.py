#!/usr/bin/env python3
"""
collect_books.py  --  forward collector of LIVE order books on Kalshi and Polymarket.

Why: neither venue serves past order books, so the only way to get real spreads
(and minute-level prices) is to record them as they happen.

Design: "collect wide, match later". The collector does NOT depend on the pair
matcher. It tracks a broad set of open markets on each venue that end within the
next few days, and saves a top-of-book + depth snapshot of each one every minute.
Once those markets settle, the normal pipeline matches them and the books are
joined by ticker / token id. That way the choice of matcher cannot bias which
books were recorded.

Usage (from the folder that contains this file):

    python collect_books.py smoke        # 30-second check of every endpoint, no data kept
    python collect_books.py run          # run until you stop it with Ctrl+C

Needs Python 3.9+. Uses the `requests` library if it is installed, otherwise the
standard library only. No API key is used anywhere.

Output (default folder: data/books):
    books_K_YYYY-MM-DD.jsonl.gz   one line per Kalshi market per snapshot
    books_P_YYYY-MM-DD.jsonl.gz   one line per Polymarket token per snapshot
    meta_K.jsonl.gz, meta_P.jsonl.gz   the full market description, written once per market
    collector.log, status.json    what happened, and when the last snapshot was taken

A line is written only when a market's book has changed since its last line (and at
least every 15 minutes). cycles_<date>.jsonl lists every snapshot taken, so a market
with no new line in a successful snapshot had an unchanged book: carry the last one forward.

Each snapshot line:
    ts        time the request was answered (UTC, ISO)
    venue     "K" or "P"
    id        Kalshi ticker, or Polymarket token id of the FIRST outcome (usually YES)
    best_bid, best_ask, bid_size, ask_size     top of book, in dollars / contracts
    bids, asks   the best `--depth` levels as [price, size], best first
For Kalshi the ask side is derived from the NO bids (a NO bid at p is a YES ask at 1-p).
"""
import argparse
import csv
import gzip
import json
import os
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
USER_AGENT = "pm-sprint1-book-collector/1.0 (university research project)"

try:  # optional, faster (keeps connections open)
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None
if os.environ.get("PM_NO_REQUESTS"):  # force the standard-library path (used in tests)
    requests = None


# --------------------------------------------------------------------------- helpers
def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (dt.microsecond // 1000)


def parse_time(s):
    """Parse the ISO timestamps both venues use. Returns aware datetime or None."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("Z", "+00:00")
    if " " in s and "T" not in s:
        s = s.replace(" ", "T", 1)
    if s.endswith("+00"):
        s += ":00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s[:19])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fnum(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def jloads_maybe(x):
    """Polymarket sends some lists as JSON-encoded strings."""
    if isinstance(x, str):
        try:
            return json.loads(x)
        except ValueError:
            return None
    return x


class Log:
    def __init__(self, path=None, quiet=False):
        self.path, self.quiet, self.lock = path, quiet, threading.Lock()

    def __call__(self, msg):
        line = "%s  %s" % (now_utc().strftime("%Y-%m-%d %H:%M:%S"), msg)
        with self.lock:
            if not self.quiet:
                print(line, flush=True)
            if self.path:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")


class Http:
    """Small HTTP client: shared rate limit, retries, JSON in and out.
    request() returns (status_code, parsed_json_or_None, short_error_text)."""

    def __init__(self, rate_per_s=8.0, timeout=20):
        self.gap = 1.0 / rate_per_s
        self.timeout = timeout
        self.lock = threading.Lock()
        self.next_t = 0.0
        self.local = threading.local()
        self.ctx = None
        if requests is None:
            import ssl
            try:
                import certifi  # type: ignore
                self.ctx = ssl.create_default_context(cafile=certifi.where())
            except Exception:
                self.ctx = ssl.create_default_context()

    def _wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next_t)
            self.next_t = t + self.gap
        if t > now:
            time.sleep(t - now)

    def _session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            self.local.s = s
        return s

    def _once(self, url, body):
        if requests is not None:
            s = self._session()
            r = s.post(url, json=body, timeout=self.timeout) if body is not None \
                else s.get(url, timeout=self.timeout)
            return r.status_code, r.text
        data = None
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8", "replace")

    def request(self, url, body=None, retries=3):
        err = ""
        for attempt in range(retries + 1):
            self._wait()
            try:
                status, text = self._once(url, body)
            except Exception as e:  # network trouble
                status, text, err = 0, "", "%s: %s" % (type(e).__name__, e)
            if status == 200:
                try:
                    return 200, json.loads(text), ""
                except ValueError:
                    return 200, None, "not JSON: " + text[:120]
            if status in (0, 429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(min(30, 1.5 * (2 ** attempt)) + random.random())
                continue
            return status, None, (err or text[:200])
        return 0, None, err


# --------------------------------------------------------------------------- Kalshi
def kalshi_list_markets(http, base, log, days_ahead, max_pages, stats=None, min_volume=0.0):
    """Open Kalshi markets that should finish within `days_ahead` days.
    Sports markets keep a late `close_time` and close early once the game is decided,
    so a second, later window is scanned too and the earlier of close / expected
    expiry is used. The list comes back latest-close first, so the near window is
    scanned completely before the late one."""
    now = now_utc()
    horizon = now + timedelta(days=days_ahead)
    windows = [(int(now.timestamp()), int(horizon.timestamp())),
               (int(horizon.timestamp()) + 1, int((horizon + timedelta(days=15)).timestamp()))]
    st = stats if stats is not None else {}
    st.update({"pages": 0, "scanned": 0, "kept": 0, "kept_late_close": 0, "complete": True,
               "statuses": {}, "fallback": False, "sample_keys": None, "active_ids": set()})
    out, seen = [], set()

    def handle(markets):
        for m in markets:
            st["scanned"] += 1
            if st["sample_keys"] is None:
                st["sample_keys"] = sorted(m.keys())
            status = str(m.get("status") or "").lower()
            st["statuses"][status] = st["statuses"].get(status, 0) + 1
            if status and status not in ("active", "open"):
                continue
            tk = m.get("ticker")
            if not tk:
                continue
            st["active_ids"].add(tk)                  # still trading: used to keep tracked markets
            close = parse_time(m.get("close_time"))
            exp = parse_time(m.get("expected_expiration_time"))
            ends = [t for t in (close, exp) if t is not None]
            end = min(ends) if ends else None         # the earlier of close and expected end
            if end is None or end <= now or end > horizon:
                continue
            if tk in seen:
                continue
            seen.add(tk)
            ev = m.get("event_ticker") or tk
            st["kept"] += 1
            if close and close > horizon:
                st["kept_late_close"] += 1
            if fnum(m.get("volume_fp"), fnum(m.get("volume"))) < min_volume:
                continue                              # never traded enough: not worth the memory
            out.append({
                "id": tk,
                "group": ev.split("-")[0],            # series prefix, e.g. KXNBAGAME
                "volume": fnum(m.get("volume_fp"), fnum(m.get("volume"))),
                # sports markets list a close_time days after the game, so the expected end
                # is used; a market still listed as trading after that is kept anyway
                "end": end,
                "raw": m,
            })

    def scan(params):
        cursor = ""
        while True:
            if st["pages"] >= max_pages:
                st["complete"] = False
                return True
            q = dict(params)
            if cursor:
                q["cursor"] = cursor
            status, js, err = http.request(base + "/markets?" + urllib.parse.urlencode(q))
            if status != 200 or not isinstance(js, dict):
                st["last_error"] = "HTTP %s %s" % (status, err[:120])
                return False
            st["pages"] += 1
            if st["pages"] % 25 == 0:
                log("Kalshi: reading the market list, page %d (%d markets seen, %d kept)"
                    % (st["pages"], st["scanned"], st["kept"]))
            handle(js.get("markets") or [])
            cursor = js.get("cursor") or ""
            if not cursor or not js.get("markets"):
                return True

    for i, (lo, hi) in enumerate(windows):
        ok = scan({"limit": 1000, "mve_filter": "exclude", "min_close_ts": lo, "max_close_ts": hi})
        if not ok and i == 0 and st["pages"] == 0:
            log("Kalshi: close-time filter refused (%s); using status=open" % st.get("last_error"))
            st["fallback"] = True
            scan({"limit": 1000, "mve_filter": "exclude", "status": "open"})
            break
        if not ok:
            log("Kalshi: market listing stopped at page %d (%s)" % (st["pages"], st.get("last_error")))
            st["complete"] = False
            break
    if not st["complete"]:
        log("Kalshi: market list not read to the end (%d pages); raise --kalshi-max-pages to see more" % st["pages"])
    return out


def parse_kalshi_book(ob, depth):
    """-> (bids, asks) as [[price, size], ...], best first, YES-side prices in dollars."""
    fp = ob.get("orderbook_fp") or ob.get("orderbook") or ob or {}
    yes, no, scale = fp.get("yes_dollars"), fp.get("no_dollars"), 1.0
    if yes is None and no is None:            # legacy format: integer cents
        yes, no, scale = fp.get("yes"), fp.get("no"), 0.01

    def levels(arr):
        lv = []
        for row in arr or []:
            try:
                lv.append((round(float(row[0]) * scale, 4), float(row[1])))
            except (TypeError, ValueError, IndexError):
                continue
        return sorted(lv, reverse=True)

    yb, nb = levels(yes), levels(no)
    bids = [[p, s] for p, s in yb[:depth]]
    asks = [[round(1.0 - p, 4), s] for p, s in nb[:depth]]
    return bids, asks


class KalshiBooks:
    """Reads Kalshi books. Tries the batch endpoint first; if it wants a key,
    falls back to one request per market (documented as public)."""

    def __init__(self, http, base, log, workers=6):
        self.http, self.base, self.log, self.workers = http, base, log, workers
        self.batch_ok = None     # None = not tested yet

    def _batch(self, tickers):
        q = urllib.parse.urlencode([("tickers", t) for t in tickers])
        status, js, err = self.http.request(self.base + "/markets/orderbooks?" + q, retries=1)
        if status != 200 or not isinstance(js, dict):
            return status, None, err
        return 200, {o.get("ticker"): o for o in js.get("orderbooks") or []}, ""

    def _single(self, ticker):
        url = self.base + "/markets/" + urllib.parse.quote(ticker) + "/orderbook"
        status, js, err = self.http.request(url, retries=1)
        return ticker, status, js, iso(now_utc())

    def fetch(self, tickers, depth):
        """-> (rows, n_failed)"""
        rows, failed = [], 0
        if self.batch_ok is not False:
            for i in range(0, len(tickers), 100):
                chunk = tickers[i:i + 100]
                status, books, err = self._batch(chunk)
                ts = iso(now_utc())
                if status == 200:
                    if self.batch_ok is None:
                        self.batch_ok = True
                        self.log("Kalshi: batch order-book endpoint works without a key")
                    for t in chunk:
                        ob = books.get(t)
                        if ob is None:
                            failed += 1
                            continue
                        rows.append(make_row("K", t, ts, *parse_kalshi_book(ob, depth)))
                elif self.batch_ok is None and status in (400, 401, 403, 404, 405):
                    self.batch_ok = False
                    self.log("Kalshi: batch order-book endpoint refused (HTTP %s); "
                             "using one request per market" % status)
                    return self._fetch_single(tickers, depth)
                else:
                    failed += len(chunk)
            return rows, failed
        return self._fetch_single(tickers, depth)

    def _fetch_single(self, tickers, depth):
        rows, failed = [], 0
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            for t, status, js, ts in ex.map(self._single, tickers):
                if status == 200 and isinstance(js, dict):
                    rows.append(make_row("K", t, ts, *parse_kalshi_book(js, depth)))
                else:
                    failed += 1
        return rows, failed


# --------------------------------------------------------------------------- Polymarket
def poly_list_markets(http, base, log, days_ahead, max_pages, min_volume):
    """Open Polymarket markets with an order book that end within `days_ahead` days.
    (For most sports `endDate` is the start of the game.)"""
    now = now_utc()
    params = {
        "limit": 100,
        "closed": "false",
        "end_date_min": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date_max": (now + timedelta(days=days_ahead)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "volume_num_min": min_volume,
    }
    out, cursor, pages = [], "", 0
    while pages < max_pages:
        q = dict(params)
        if cursor:
            q["after_cursor"] = cursor
        status, js, err = http.request(base + "/markets/keyset?" + urllib.parse.urlencode(q))
        if status != 200 or js is None:
            log("Polymarket: market listing stopped at page %d (HTTP %s %s)" % (pages, status, err[:80]))
            break
        pages += 1
        if isinstance(js, list):
            items, nxt = js, ""
        else:
            items = js.get("markets") or js.get("data") or []
            nxt = js.get("next_cursor") or js.get("nextCursor") or js.get("after_cursor") or ""
        for m in items:
            if m.get("closed") is True or m.get("enableOrderBook") is False:
                continue
            if m.get("acceptingOrders") is False:
                continue
            toks = jloads_maybe(m.get("clobTokenIds"))
            if not isinstance(toks, list) or len(toks) != 2:
                continue
            end = parse_time(m.get("endDate"))
            if end is None:
                continue
            evs = m.get("events") or []
            group = ""
            if evs and isinstance(evs[0], dict):
                group = str(evs[0].get("slug") or evs[0].get("id") or "")
            out.append({
                "id": str(toks[0]),
                "group": group or str(m.get("slug") or m.get("id")),
                "volume": fnum(m.get("volumeNum"), fnum(m.get("volume"))),
                "end": end,
                "raw": m,
            })
        if not nxt or not items or nxt == cursor:
            break
        cursor = nxt
    if pages >= max_pages:
        log("Polymarket: stopped at the page limit (%d); raise --poly-max-pages to see more" % max_pages)
    return out


def parse_poly_book(b, depth):
    def levels(arr, reverse):
        lv = []
        for row in arr or []:
            try:
                lv.append((float(row["price"]), float(row["size"])))
            except (TypeError, ValueError, KeyError):
                continue
        return [[p, s] for p, s in sorted(lv, reverse=reverse)[:depth]]
    # the docs disagree on the order of the arrays, so sort here instead of trusting it
    return levels(b.get("bids"), True), levels(b.get("asks"), False)


def poly_fetch_books(http, base, tokens, depth):
    """-> (rows, n_failed) using the batch endpoint POST /books."""
    rows, failed = [], 0
    for i in range(0, len(tokens), 100):
        chunk = tokens[i:i + 100]
        status, js, err = http.request(base + "/books", body=[{"token_id": t} for t in chunk], retries=1)
        ts = iso(now_utc())
        if status != 200 or not isinstance(js, list):
            failed += len(chunk)
            continue
        seen = set()
        for b in js:
            t = str(b.get("asset_id") or "")
            if not t or t in seen:
                continue
            seen.add(t)
            bids, asks = parse_poly_book(b, depth)
            row = make_row("P", t, ts, bids, asks)
            row["src_ts"] = b.get("timestamp")      # the venue's own time stamp of this book
            if b.get("last_trade_price") not in (None, ""):
                row["last_trade"] = fnum(b.get("last_trade_price"), None)
            rows.append(row)
        failed += len([t for t in chunk if t not in seen])
    return rows, failed


# --------------------------------------------------------------------------- shared
def make_row(venue, mid, ts, bids, asks):
    return {
        "ts": ts, "venue": venue, "id": mid,
        "best_bid": bids[0][0] if bids else None,
        "best_ask": asks[0][0] if asks else None,
        "bid_size": bids[0][1] if bids else None,
        "ask_size": asks[0][1] if asks else None,
        "bids": bids, "asks": asks,
    }


def choose(candidates, tracked, slots, per_group, tail_share, min_volume, rng):
    """Pick up to `slots` new markets: half by volume, the rest drawn at random from
    the lower-volume remainder, with at most `per_group` per series / event so that
    one sport or one crypto ladder cannot fill the whole sample."""
    if slots <= 0:
        return []
    group_count = {}
    for m in tracked.values():
        group_count[m["group"]] = group_count.get(m["group"], 0) + 1
    pool = [m for m in candidates if m["id"] not in tracked and m["volume"] >= min_volume]
    pool.sort(key=lambda m: -m["volume"])

    def take(seq, n):
        got = []
        for m in seq:
            if len(got) >= n:
                break
            if m["id"] in taken:
                continue
            if group_count.get(m["group"], 0) >= per_group:
                continue
            group_count[m["group"]] = group_count.get(m["group"], 0) + 1
            taken.add(m["id"])
            got.append(m)
        return got

    taken = set()
    n_top = slots - int(round(slots * tail_share))
    top = take(pool, n_top)
    rest = [m for m in pool if m["id"] not in taken]
    rng.shuffle(rest)
    tail = take(rest, slots - len(top))
    for m in top:
        m["stratum"] = "top_volume"
    for m in tail:
        m["stratum"] = "random_tail"
    return top + tail


class Universe:
    """The set of markets being tracked on one venue. Markets stay in ("sticky")
    from the moment they are picked until they end, so each one has a full series."""

    def __init__(self, venue, out_dir, max_markets, keep_after_h, log):
        self.venue, self.out_dir, self.max, self.log = venue, out_dir, max_markets, log
        self.keep_after = timedelta(hours=keep_after_h)
        self.lock = threading.Lock()
        self.tracked = {}                 # id -> {"group","end","volume"}
        self.meta_path = os.path.join(out_dir, "meta_%s.jsonl.gz" % venue)
        self.state_path = os.path.join(out_dir, "tracked_%s.json" % venue)
        self._load()

    def _load(self):
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, encoding="utf-8") as f:
                for k, v in json.load(f).items():
                    v["end"] = parse_time(v.get("end"))
                    self.tracked[k] = v
            self.log("%s: resumed with %d tracked markets" % (self.venue, len(self.tracked)))
        except Exception as e:
            self.log("%s: could not read saved state (%s); starting fresh" % (self.venue, e))

    def _save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({k: {"group": v["group"], "volume": v["volume"],
                           "end": iso(v["end"]) if v.get("end") else None,
                           "forced": v.get("forced", False)}
                       for k, v in self.tracked.items()}, f)
        os.replace(tmp, self.state_path)

    def ids(self):
        with self.lock:
            return list(self.tracked)

    def force(self, ids):
        with self.lock:
            for i in ids:
                if i and i not in self.tracked:
                    self.tracked[i] = {"group": "forced", "volume": 0.0, "end": None, "forced": True}
            self._save()

    def update(self, candidates, per_group, tail_share, min_volume, rng, alive=None):
        now = now_utc()
        by_id = {m["id"]: m for m in candidates}
        with self.lock:
            # refresh end times (games get rescheduled) and drop finished markets
            n_ended = 0
            for i, t in list(self.tracked.items()):
                if i in by_id and not t.get("forced"):
                    t["end"] = by_id[i]["end"]
                if t.get("end") and now > t["end"] + self.keep_after and not (alive and i in alive):
                    del self.tracked[i]
                    n_ended += 1
            n_before = len(self.tracked)
            n_forced = sum(1 for t in self.tracked.values() if t.get("forced"))
            slots = self.max - (n_before - n_forced)
            new = choose(candidates, self.tracked, slots, per_group, tail_share, min_volume, rng)
            if new:
                with gzip.open(self.meta_path, "at", encoding="utf-8") as f:
                    for m in new:
                        f.write(json.dumps({"added": iso(now), "venue": self.venue, "id": m["id"],
                                            "group": m["group"], "stratum": m.get("stratum"),
                                            "market": m["raw"]}, separators=(",", ":")) + "\n")
                for m in new:
                    self.tracked[m["id"]] = {"group": m["group"], "volume": m["volume"], "end": m["end"]}
            self._save()
            self.log("%s: %d candidates, %d tracked (+%d new, %d ended)" % (
                self.venue, len(candidates), len(self.tracked), len(new), n_ended))


def write_rows(out_dir, venue, rows):
    if not rows:
        return
    by_day = {}
    for r in rows:
        by_day.setdefault(r["ts"][:10], []).append(r)
    for day, rs in by_day.items():
        path = os.path.join(out_dir, "books_%s_%s.jsonl.gz" % (venue, day))
        with gzip.open(path, "at", encoding="utf-8") as f:   # one gzip block per cycle
            for r in rs:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")


def read_pairs(path):
    k, p = [], []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("kalshi_ticker"):
                k.append(row["kalshi_ticker"].strip())
            if row.get("poly_token_id"):
                p.append(row["poly_token_id"].strip())
    return k, p


def keep_awake(log):
    """On Windows, ask the system not to go to sleep while the collector runs.
    The request ends by itself when the program stops. Closing the lid still sleeps."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        log("Windows will not go to sleep by itself while this window is open (closing the lid still does)")
    except Exception as e:
        log("could not set keep-awake (%s); set Sleep to Never in the power settings" % e)


# --------------------------------------------------------------------------- commands
def cmd_smoke(a):
    log = Log(None)
    http_k, http_p = Http(a.kalshi_rate), Http(a.poly_rate)
    results, raw = [], {}

    def rec(name, ok, detail):
        results.append({"check": name, "ok": bool(ok), "detail": detail})
        print("  [%s] %-44s %s" % ("PASS" if ok else "FAIL", name, detail), flush=True)

    def save():
        with open("smoke_report.json", "w", encoding="utf-8") as f:
            json.dump({"time": iso(now_utc()), "python": sys.version, "requests": requests is not None,
                       "results": results, "raw": raw}, f, indent=1)

    print("\nTest of every endpoint the collector needs, with no API key. Takes 1 to 3 minutes.\n", flush=True)
    kst = {}
    km = kalshi_list_markets(http_k, a.kalshi_base, log, a.days_ahead, a.smoke_pages, kst, a.kalshi_min_volume)
    liquid = km
    series = {}
    for m in liquid:
        series[m["group"]] = series.get(m["group"], 0) + 1
    kst.pop("active_ids", None)
    raw["kalshi_list"] = dict(kst, liquid=len(liquid), n_series=len(series),
                              top_series=sorted(series.items(), key=lambda x: -x[1])[:40],
                              late_close_examples=[{k: m["raw"].get(k) for k in ("ticker", "close_time", "expected_expiration_time", "volume_fp")}
                                                   for m in sorted(liquid, key=lambda m: -m["volume"])
                                                   if (parse_time(m["raw"].get("close_time")) or now_utc()) > now_utc() + timedelta(days=a.days_ahead)][:8])
    rec("Kalshi: list open markets", len(km) > 0,
        "%d ending within %d days, %d with volume, %d series (%d pages%s)" % (
            kst.get("kept", 0), a.days_ahead, len(liquid), len(series), kst.get("pages", 0), "" if kst.get("complete") else ", list not read to the end"))
    save()
    km.sort(key=lambda m: -m["volume"])
    kt = [m["id"] for m in km[:5]]
    if kt:
        url = a.kalshi_base + "/markets/" + urllib.parse.quote(kt[0]) + "/orderbook"
        status, js, err = http_k.request(url, retries=1)
        raw["kalshi_single"] = {"url": url, "status": status, "error": err, "body": json.dumps(js)[:1500] if js is not None else None}
        bids, asks = parse_kalshi_book(js or {}, 3) if status == 200 else ([], [])
        rec("Kalshi: one order book, no key", status == 200 and bool(bids or asks),
            "HTTP %s  %s  bid %s  ask %s" % (status, kt[0], bids[0] if bids else None, asks[0] if asks else None))
        url = a.kalshi_base + "/markets/orderbooks?" + urllib.parse.urlencode([("tickers", t) for t in kt])
        status, js, err = http_k.request(url, retries=1)
        raw["kalshi_batch"] = {"url": url, "status": status, "error": err, "body": json.dumps(js)[:1500] if js is not None else None}
        n = len((js or {}).get("orderbooks") or []) if isinstance(js, dict) else 0
        rec("Kalshi: batch of order books, no key", status == 200 and n > 0,
            "HTTP %s  %s" % (status, ("%d books" % n) if n else err[:60]))
        save()
    now = now_utc()
    url = a.gamma_base + "/markets/keyset?" + urllib.parse.urlencode({
        "limit": 3, "closed": "false", "volume_num_min": a.poly_min_volume,
        "end_date_min": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date_max": (now + timedelta(days=a.days_ahead)).strftime("%Y-%m-%dT%H:%M:%SZ")})
    status, js, err = http_p.request(url, retries=1)
    first = None
    if isinstance(js, list) and js:
        first = js[0]
    elif isinstance(js, dict):
        items = js.get("markets") or js.get("data") or []
        first = items[0] if items else None
    raw["gamma_list"] = {"url": url, "status": status, "error": err,
                         "top_level": (sorted(js.keys()) if isinstance(js, dict) else type(js).__name__),
                         "first_market_keys": sorted(first.keys()) if isinstance(first, dict) else None,
                         "first_market": {k: first.get(k) for k in ("id", "question", "endDate", "closed", "enableOrderBook",
                                                                    "acceptingOrders", "outcomes", "clobTokenIds", "volumeNum", "volume", "conditionId")}
                         if isinstance(first, dict) else None,
                         "cursor_fields": {k: v for k, v in js.items() if k not in ("markets", "data")} if isinstance(js, dict) else None}
    save()
    pm = poly_list_markets(http_p, a.gamma_base, log, a.days_ahead, a.smoke_pages, a.poly_min_volume)
    events = len(set(m["group"] for m in pm))
    rec("Polymarket: list open markets", len(pm) > 0,
        "%d ending within %d days in %d events" % (len(pm), a.days_ahead, events))
    raw["gamma_list"]["n_markets"] = len(pm)
    pm.sort(key=lambda m: -m["volume"])
    pt = [m["id"] for m in pm[:5]]
    if pt:
        status, js, err = http_p.request(a.clob_base + "/books", body=[{"token_id": t} for t in pt], retries=1)
        raw["clob_books"] = {"status": status, "error": err, "body": json.dumps(js)[:1500] if js is not None else None}
        rows, failed = poly_fetch_books(http_p, a.clob_base, pt, 3)
        d = ""
        if rows:
            d = "  first: bid %s ask %s" % (rows[0]["best_bid"], rows[0]["best_ask"])
        rec("Polymarket: batch of order books", len(rows) > 0, "HTTP %s  %d of %d books%s" % (status, len(rows), len(pt), d))
    optional = "batch of order books, no key"
    ok = len(results) >= 4 and all(r["ok"] for r in results if optional not in r["check"])
    save()
    print("\n%s  (details saved to smoke_report.json)" % (
        "READY: the collector can run." if ok else "NOT READY: see the FAIL lines above."))
    if ok and any(optional in r["check"] and not r["ok"] for r in results):
        print("Note: the Kalshi batch endpoint wants a key; the collector will use one request per market instead.")
    return 0 if ok else 1


def cmd_run(a):
    os.makedirs(a.out, exist_ok=True)
    log = Log(os.path.join(a.out, "collector.log"))
    rng = random.Random(a.seed)
    http_k, http_p = Http(a.kalshi_rate), Http(a.poly_rate)
    uni_k = Universe("K", a.out, a.max_kalshi, a.keep_after_hours, log)
    uni_p = Universe("P", a.out, a.max_poly, a.keep_after_hours, log)
    if a.pairs:
        fk, fp = read_pairs(a.pairs)
        uni_k.force(fk)
        uni_p.force(fp)
        log("forced from %s: %d Kalshi tickers, %d Polymarket tokens" % (a.pairs, len(fk), len(fp)))
    kbooks = KalshiBooks(http_k, a.kalshi_base, log)
    stop = threading.Event()

    def on_signal(signum, frame):
        log("stop requested; finishing the current snapshot")
        stop.set()
    signal.signal(signal.SIGINT, on_signal)
    try:
        signal.signal(signal.SIGTERM, on_signal)
    except Exception:
        pass

    def refresh():
        try:
            kst = {}
            cands = kalshi_list_markets(http_k, a.kalshi_base, log, a.days_ahead, a.kalshi_max_pages, kst, a.kalshi_min_volume)
            uni_k.update(cands, a.per_series, a.tail_share, a.kalshi_min_volume, rng, kst.get("active_ids"))
        except Exception as e:
            log("Kalshi refresh failed: %s: %s" % (type(e).__name__, e))
        try:
            uni_p.update(poly_list_markets(http_p, a.gamma_base, log, a.days_ahead, a.poly_max_pages, a.poly_min_volume),
                         a.per_event, a.tail_share, a.poly_min_volume, rng)
        except Exception as e:
            log("Polymarket refresh failed: %s: %s" % (type(e).__name__, e))

    def refresher():
        while not stop.wait(a.refresh_min * 60):
            refresh()

    keep_awake(log)
    log("collector started: every %ds, up to %d Kalshi + %d Polymarket markets ending within %d days, depth %d"
        % (a.interval, a.max_kalshi, a.max_poly, a.days_ahead, a.depth))
    refresh()                                   # first universe before the first snapshot
    threading.Thread(target=refresher, daemon=True).start()

    cycles, total_rows = 0, 0
    last = {}            # (venue, id) -> (book signature, time of last write)
    heartbeat = a.heartbeat_min * 60.0

    def changed_only(venue, rows):
        """Keep a row only if the book differs from the last one written for that market,
        or if nothing was written for it for --heartbeat-min minutes. A market with no new
        row in a successful cycle (see cycles_<date>.jsonl) had an unchanged book."""
        if a.write_all:
            return rows
        keep, t = [], time.monotonic()
        for r in rows:
            sig = (tuple(map(tuple, r["bids"])), tuple(map(tuple, r["asks"])))
            prev = last.get((venue, r["id"]))
            if prev is None or prev[0] != sig or t - prev[1] >= heartbeat:
                last[(venue, r["id"])] = (sig, t)
                keep.append(r)
        return keep

    while not stop.is_set():
        t0 = time.monotonic()
        res = {}

        def do_k():
            ids = uni_k.ids()
            res["K"] = kbooks.fetch(ids, a.depth) if ids else ([], 0)

        def do_p():
            ids = uni_p.ids()
            res["P"] = poly_fetch_books(http_p, a.clob_base, ids, a.depth) if ids else ([], 0)

        threads = [threading.Thread(target=do_k), threading.Thread(target=do_p)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        (rk, fk), (rp, fp) = res.get("K", ([], 0)), res.get("P", ([], 0))
        wk, wp = changed_only("K", rk), changed_only("P", rp)
        took = time.monotonic() - t0
        try:
            write_rows(a.out, "K", wk)
            write_rows(a.out, "P", wp)
            ts = iso(now_utc())
            with open(os.path.join(a.out, "cycles_%s.jsonl" % ts[:10]), "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": ts, "K": {"ok": len(rk), "failed": fk, "written": len(wk)},
                                    "P": {"ok": len(rp), "failed": fp, "written": len(wp)},
                                    "seconds": round(took, 1)}, separators=(",", ":")) + "\n")
        except Exception as e:
            log("WRITE FAILED: %s: %s" % (type(e).__name__, e))
        cycles += 1
        total_rows += len(wk) + len(wp)
        quoted_k = sum(1 for r in rk if r["best_bid"] is not None and r["best_ask"] is not None)
        quoted_p = sum(1 for r in rp if r["best_bid"] is not None and r["best_ask"] is not None)
        status = {"last_snapshot": iso(now_utc()), "cycles": cycles, "rows_written_total": total_rows,
                  "rows_written_last": len(wk) + len(wp),
                  "kalshi_books": len(rk), "kalshi_two_sided": quoted_k, "kalshi_failed": fk,
                  "kalshi_mode": {True: "batch", False: "single", None: "untested"}[kbooks.batch_ok],
                  "poly_books": len(rp), "poly_two_sided": quoted_p, "poly_failed": fp,
                  "cycle_seconds": round(took, 1)}
        try:
            with open(os.path.join(a.out, "status.json"), "w", encoding="utf-8") as f:
                json.dump(status, f, indent=1)
        except Exception:
            pass
        many_failed = (fk + fp) > 0.2 * max(1, len(rk) + len(rp) + fk + fp)
        if cycles == 1 or cycles % a.log_every == 0 or many_failed or took > a.interval:
            log("snapshot %d: Kalshi %d books (%d failed, %d changed), Polymarket %d books (%d failed, %d changed), %.1fs%s"
                % (cycles, len(rk), fk, len(wk), len(rp), fp, len(wp), took,
                   "  SLOWER THAN THE INTERVAL" if took > a.interval else ""))
        if a.max_cycles and cycles >= a.max_cycles:
            break
        stop.wait(max(0.0, a.interval - (time.monotonic() - t0)))
    log("collector stopped after %d snapshots, %d rows written" % (cycles, total_rows))
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="Record live Kalshi and Polymarket order books.")
    p.add_argument("command", choices=["smoke", "run"])
    p.add_argument("--out", default=os.path.join("data", "books"), help="output folder")
    p.add_argument("--interval", type=int, default=60, help="seconds between snapshots")
    p.add_argument("--depth", type=int, default=5, help="price levels kept per side")
    p.add_argument("--days-ahead", type=int, default=7, help="track markets ending within this many days")
    p.add_argument("--max-kalshi", type=int, default=2000)
    p.add_argument("--max-poly", type=int, default=2000)
    p.add_argument("--per-series", type=int, default=60, help="max Kalshi markets per series")
    p.add_argument("--per-event", type=int, default=40, help="max Polymarket markets per event")
    p.add_argument("--heartbeat-min", type=float, default=15,
                   help="write an unchanged book again after this many minutes")
    p.add_argument("--write-all", action="store_true", help="write every book every cycle, changed or not")
    p.add_argument("--tail-share", type=float, default=0.5,
                   help="share of slots drawn at random from lower-volume markets")
    p.add_argument("--kalshi-min-volume", type=float, default=100, help="contracts")
    p.add_argument("--poly-min-volume", type=float, default=1000, help="dollars")
    p.add_argument("--keep-after-hours", type=float, default=6,
                   help="keep recording this long after the listed end (covers in-play on Polymarket)")
    p.add_argument("--refresh-min", type=float, default=30, help="minutes between market-list refreshes")
    p.add_argument("--pairs", help="CSV with columns kalshi_ticker, poly_token_id to always track")
    p.add_argument("--kalshi-max-pages", type=int, default=400)
    p.add_argument("--smoke-pages", type=int, default=40, help="page limit per venue in the smoke test")
    p.add_argument("--poly-max-pages", type=int, default=300)
    p.add_argument("--kalshi-rate", type=float, default=10.0, help="requests per second (limit is 20)")
    p.add_argument("--poly-rate", type=float, default=5.0)
    p.add_argument("--seed", type=int, default=20261116)
    p.add_argument("--log-every", type=int, default=15, help="write a log line every N snapshots")
    p.add_argument("--max-cycles", type=int, default=0, help="stop after N snapshots (0 = never)")
    p.add_argument("--kalshi-base", default=os.environ.get("KALSHI_BASE", KALSHI_BASE))
    p.add_argument("--gamma-base", default=os.environ.get("GAMMA_BASE", GAMMA_BASE))
    p.add_argument("--clob-base", default=os.environ.get("CLOB_BASE", CLOB_BASE))
    a = p.parse_args(argv)
    return cmd_smoke(a) if a.command == "smoke" else cmd_run(a)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except BaseException:
        import traceback
        with open("last_error.txt", "w", encoding="utf-8") as f:
            f.write(iso(now_utc()) + "\n" + traceback.format_exc())
        traceback.print_exc()
        sys.exit(2)
