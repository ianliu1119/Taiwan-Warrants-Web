"""Real-time market-data feed (live intraday quotes).

Runs inside the (single) web process alongside the APScheduler refresh jobs.
A background `FeedSource` pushes ticks into a module-level `_live_quotes` cache;
routes read from it via `get_quotes()`. The concrete source is chosen by the
`FEED_SOURCE` env var:

  * "mock"  (default) — a stdlib-only random-walk simulator, so the whole
    pipeline (cache → endpoint → frontend) is verifiable without any vendor SDK
    or credentials.
  * "fubon" — the Fubon Neo securities websocket, written to the documented SDK
    spec. `fubon_neo` is imported lazily inside `connect()` so this module still
    imports fine when the SDK is not installed.

Started once from wsgi.py (production) or app.py __main__ (dev), right after
scheduler.start(). Mirrors scheduler.py's singleton / start-lock / never-let-a
-background-loop-die conventions.
"""
import hashlib
import os
import random
import threading
import time
import traceback
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from services import scheduler  # for DEFAULT_WARRANT_STOCKS (single source of truth)

# ── Live quote cache ─────────────────────────────────────────────────────────
# symbol -> {"price","bid","ask","size","volume","ts"}; ts is float epoch seconds
# stamped when the tick is received. Guarded by _live_lock (mirror the
# warrant_logic _warrant_cache + lock pattern).
_live_quotes: dict = {}
_live_lock = threading.Lock()

STALE_SECONDS = 10  # a quote older than this is flagged stale in get_quotes()

_TAIPEI = ZoneInfo("Asia/Taipei")
_MARKET_OPEN = dtime(9, 0)
_MARKET_CLOSE = dtime(13, 30)

# Runtime state for feed_status().
_source = None            # the active FeedSource instance
_start_lock = threading.Lock()
_last_tick_ts = None      # float epoch seconds of the most recent tick received

# On-demand symbols requested via /live_quotes that are outside the base
# universe. Kept subscribed while actively polled (symbol -> last-request epoch),
# then reaped by the reconcile loop after ONDEMAND_TTL so subscriptions don't
# grow without bound.
_ondemand: dict = {}
_ondemand_lock = threading.Lock()
ONDEMAND_TTL = 300


def update_quote(symbol, fields):
    """Merge `fields` into the cached quote for `symbol` and stamp ts=now.

    Called by whichever FeedSource is running (the mock thread or the Fubon
    websocket callback). Thread-safe.
    """
    global _last_tick_ts
    now = time.time()
    with _live_lock:
        q = _live_quotes.get(symbol)
        if q is None:
            q = {}
            _live_quotes[symbol] = q
        q.update(fields)
        q["ts"] = now
        _last_tick_ts = now


def get_quotes(symbols):
    """Return cached quotes for `symbols`, each annotated with `stale` + `age`.

    Symbols with no data yet are omitted. `age` is seconds since the last tick,
    `stale` is True once age exceeds STALE_SECONDS.
    """
    now = time.time()
    out = {}
    with _live_lock:
        for sym in symbols:
            q = _live_quotes.get(sym)
            if not q or "ts" not in q:
                continue
            age = now - q["ts"]
            snap = dict(q)
            snap["age"] = round(age, 3)
            snap["stale"] = age > STALE_SECONDS
            out[sym] = snap
    return out


def is_market_open(now=None):
    """True during the TWSE regular session: 09:00–13:30 Asia/Taipei, Mon–Fri."""
    now = now or datetime.now(_TAIPEI)
    if now.tzinfo is None:
        now = now.replace(tzinfo=_TAIPEI)
    else:
        now = now.astimezone(_TAIPEI)
    if now.weekday() >= 5:  # Sat/Sun
        return False
    return _MARKET_OPEN <= now.time() <= _MARKET_CLOSE


def feed_status():
    """Snapshot of the feed for the frontend status line."""
    src = _source
    with _live_lock:
        symbols = len(_live_quotes)
        last = _last_tick_ts
    return {
        "source": os.environ.get("FEED_SOURCE", "mock").strip().lower() or "mock",
        "market_open": is_market_open(),
        "connected": bool(src and src.is_connected()),
        "symbols": symbols,
        "last_tick_ts": datetime.fromtimestamp(last).isoformat() if last else None,
    }


# ── Feed source interface ────────────────────────────────────────────────────
class FeedSource:
    """Abstract market-data source.

    A source receives an `on_tick(symbol, fields)` callback at construction and
    calls it for every tick. Concrete sources implement connect/subscribe/
    unsubscribe/close and maintain their own subscription set.
    """

    def __init__(self, on_tick):
        self._on_tick = on_tick

    def connect(self):
        raise NotImplementedError

    def subscribe(self, symbols):
        raise NotImplementedError

    def unsubscribe(self, symbols):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def is_connected(self):
        raise NotImplementedError

    def subscribed(self):
        """Current subscription set (copy)."""
        raise NotImplementedError


# ── Mock source ──────────────────────────────────────────────────────────────
class MockFeedSource(FeedSource):
    """Stdlib-only random-walk simulator.

    A daemon thread emits a small price move for every currently-subscribed
    symbol roughly every 0.5–1s. Each symbol's starting price is seeded
    deterministically from a stable hash so restarts look sane (~50–700 TWD).
    """

    _TICK_STEP = 0.5  # TWD half-spread around price for bid/ask

    def __init__(self, on_tick, interval=0.7):
        super().__init__(on_tick)
        self._interval = interval
        self._subs = set()
        self._lock = threading.Lock()
        self._state = {}  # symbol -> {"price","volume"}
        self._connected = False
        self._stop = threading.Event()
        self._thread = None

    def _seed_price(self, symbol):
        h = hashlib.sha256(symbol.encode()).digest()
        n = int.from_bytes(h[:4], "big")
        return round(50 + (n % 65000) / 100.0, 2)  # ~50.00–700.00 TWD

    def _ensure_state(self, symbol):
        st = self._state.get(symbol)
        if st is None:
            st = {"price": self._seed_price(symbol), "volume": random.randint(1000, 50000)}
            self._state[symbol] = st
        return st

    def connect(self):
        if self._connected:
            return
        self._connected = True
        self._thread = threading.Thread(target=self._run, name="mock-feed", daemon=True)
        self._thread.start()
        print("FEED: mock source connected", flush=True)

    def subscribe(self, symbols):
        with self._lock:
            for s in symbols:
                self._subs.add(s)
                self._ensure_state(s)

    def unsubscribe(self, symbols):
        with self._lock:
            for s in symbols:
                self._subs.discard(s)

    def close(self):
        self._stop.set()
        self._connected = False

    def is_connected(self):
        return self._connected

    def subscribed(self):
        with self._lock:
            return set(self._subs)

    def _run(self):
        while not self._stop.is_set():
            with self._lock:
                syms = list(self._subs)
                states = {s: self._state[s] for s in syms}
            for sym in syms:
                st = states[sym]
                # Random walk: ±~0.3% move, floored so price stays positive.
                move = st["price"] * random.uniform(-0.003, 0.003)
                price = max(1.0, round(st["price"] + move, 2))
                st["price"] = price
                size = random.randint(1, 200)
                st["volume"] += size
                self._on_tick(sym, {
                    "price": price,
                    "bid": round(price - self._TICK_STEP, 2),
                    "ask": round(price + self._TICK_STEP, 2),
                    "size": size,
                    "volume": st["volume"],
                })
            self._stop.wait(self._interval)


# ── Fubon source ─────────────────────────────────────────────────────────────
class FubonFeedSource(FeedSource):
    """Fubon Neo securities websocket source (written to the documented spec).

    Not exercised in the mock path — present and coherent so the real SDK drops
    in without touching the cache / endpoint / frontend. `fubon_neo` is imported
    lazily inside connect() so this module imports without the SDK installed.

    Flow (per Fubon docs):
        sdk.login(id, pwd, cert_path, cert_pwd)
        sdk.init_realtime()
        ws = sdk.marketdata.websocket_client.stock
        ws.on('message', cb); ws.connect()
        ws.subscribe({'channel': 'trades', 'symbol': [...]})
    """

    _SUB_BATCH = 100  # symbols per subscribe call

    def __init__(self, on_tick):
        super().__init__(on_tick)
        self._sdk = None
        self._ws = None
        self._subs = set()
        self._lock = threading.Lock()
        self._connected = False

    def connect(self):
        # Lazy import: keep module import working without the SDK installed.
        try:
            from fubon_neo.sdk import FubonSDK
        except Exception as e:
            raise RuntimeError(
                "fubon_neo SDK not installed; cannot use FEED_SOURCE=fubon"
            ) from e

        fid = os.environ.get("FUBON_ID")
        pwd = os.environ.get("FUBON_PWD")
        cert_path = os.environ.get("FUBON_CERT_PATH")
        cert_pwd = os.environ.get("FUBON_CERT_PWD")
        if not all([fid, pwd, cert_path]):
            raise RuntimeError(
                "FUBON_ID / FUBON_PWD / FUBON_CERT_PATH must be set for FEED_SOURCE=fubon"
            )

        sdk = FubonSDK()
        sdk.login(fid, pwd, cert_path, cert_pwd)
        sdk.init_realtime()
        ws = sdk.marketdata.websocket_client.stock
        ws.on("message", self._on_message)
        ws.connect()
        self._sdk = sdk
        self._ws = ws
        self._connected = True
        print("FEED: fubon source connected", flush=True)
        # Re-subscribe anything requested before (re)connect.
        with self._lock:
            existing = list(self._subs)
        if existing:
            self._ws_subscribe(existing)

    def _on_message(self, message):
        """Parse a raw websocket message and push trade ticks into the cache."""
        try:
            import json
            data = json.loads(message) if isinstance(message, (str, bytes)) else message
            if data.get("event") != "data" or data.get("channel") != "trades":
                return
            d = data.get("data") or {}
            symbol = d.get("symbol")
            if not symbol:
                return
            fields = {}
            for k in ("price", "bid", "ask", "size", "volume"):
                if d.get(k) is not None:
                    fields[k] = d[k]
            self._on_tick(symbol, fields)
        except Exception as e:
            print(f"FEED: fubon message parse error: {e}", flush=True)

    def _ws_subscribe(self, symbols):
        for i in range(0, len(symbols), self._SUB_BATCH):
            batch = symbols[i:i + self._SUB_BATCH]
            self._ws.subscribe({"channel": "trades", "symbol": batch})

    def subscribe(self, symbols):
        new = []
        with self._lock:
            for s in symbols:
                if s not in self._subs:
                    self._subs.add(s)
                    new.append(s)
        if new and self._connected and self._ws is not None:
            self._ws_subscribe(new)

    def unsubscribe(self, symbols):
        gone = []
        with self._lock:
            for s in symbols:
                if s in self._subs:
                    self._subs.discard(s)
                    gone.append(s)
        if gone and self._connected and self._ws is not None:
            for i in range(0, len(gone), self._SUB_BATCH):
                batch = gone[i:i + self._SUB_BATCH]
                self._ws.unsubscribe({"channel": "trades", "symbol": batch})

    def close(self):
        self._connected = False
        try:
            if self._ws is not None:
                self._ws.disconnect()
        except Exception:
            pass

    def is_connected(self):
        return self._connected

    def subscribed(self):
        with self._lock:
            return set(self._subs)


# ── Source selection + lifecycle ─────────────────────────────────────────────
def _make_source():
    kind = os.environ.get("FEED_SOURCE", "mock").strip().lower() or "mock"
    if kind == "fubon":
        return FubonFeedSource(update_quote)
    return MockFeedSource(update_quote)


def _portfolio_symbols():
    """All portfolio TW codes across users; [] on any failure (mirror scheduler)."""
    try:
        from services import db
        return db.all_portfolio_symbols()
    except Exception as e:
        print(f"FEED: portfolio symbols unavailable: {e}", flush=True)
        return []


def _custom_symbols():
    try:
        from services import db
        return db.all_custom_stock_codes()
    except Exception as e:
        print(f"FEED: custom stock codes unavailable: {e}", flush=True)
        return []


def universe():
    """DEFAULT_WARRANT_STOCKS ∪ all custom codes ∪ all portfolio symbols."""
    return set(scheduler.DEFAULT_WARRANT_STOCKS) | set(_custom_symbols()) | set(_portfolio_symbols())


def _active_ondemand():
    """On-demand symbols requested within ONDEMAND_TTL (reaping expired ones)."""
    now = time.time()
    with _ondemand_lock:
        for sym in [s for s, ts in _ondemand.items() if now - ts > ONDEMAND_TTL]:
            del _ondemand[sym]
        return set(_ondemand)


def desired():
    """Full set the feed should be subscribed to: base universe + active on-demand."""
    return universe() | _active_ondemand()


def subscribe_symbols(symbols):
    """Subscribe the running source to `symbols` not already subscribed.

    Used by the /live_quotes route for on-demand symbols. Records each symbol's
    request time so the reconcile loop keeps it subscribed while actively polled
    (and reaps it after ONDEMAND_TTL). No-op if not started.
    """
    src = _source
    now = time.time()
    with _ondemand_lock:
        for s in symbols:
            if s:
                _ondemand[s] = now
    if not src:
        return
    have = src.subscribed()
    new = [s for s in symbols if s and s not in have]
    if new:
        try:
            src.subscribe(new)
        except Exception as e:
            print(f"FEED: on-demand subscribe failed: {e}", flush=True)


RECONCILE_SECONDS = 60


def _reconcile_once():
    """Diff the live universe against current subscriptions; sub new / unsub gone."""
    src = _source
    if not src:
        return
    if not src.is_connected():
        # Auto-reconnect (matters for the fubon path; mock stays connected).
        try:
            src.connect()
        except Exception as e:
            print(f"FEED: reconnect failed: {e}", flush=True)
            return
    want = desired()
    have = src.subscribed()
    add = list(want - have)
    drop = list(have - want)
    if add:
        src.subscribe(add)
    if drop:
        src.unsubscribe(drop)
    if add or drop:
        print(f"FEED: reconcile +{len(add)} -{len(drop)} (universe={len(want)})", flush=True)


def _reconcile_loop():
    while True:
        try:
            _reconcile_once()
        except Exception as e:
            print(f"FEED: reconcile FAILED: {e}", flush=True)
            traceback.print_exc()
        time.sleep(RECONCILE_SECONDS)


def start():
    """Start the feed exactly once per process (singleton, like scheduler.start)."""
    global _source
    with _start_lock:
        if _source is not None:
            return _source
        src = _make_source()
        try:
            src.connect()
        except Exception as e:
            # Don't take the whole app down; report and leave _source set so the
            # reconcile loop can retry connect() (auto-reconnect path).
            print(f"FEED: initial connect failed: {e}", flush=True)
            traceback.print_exc()
        _source = src
        # Initial subscribe to the universe.
        try:
            src.subscribe(list(desired()))
        except Exception as e:
            print(f"FEED: initial subscribe failed: {e}", flush=True)
        threading.Thread(target=_reconcile_loop, name="feed-reconcile", daemon=True).start()
        print(f"FEED: started (source={os.environ.get('FEED_SOURCE', 'mock')})", flush=True)
        return src
