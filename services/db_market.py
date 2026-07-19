"""Supabase Postgres access (service-role) for SERVER-ONLY market-data snapshots.

Mirrors services/db.py exactly: no top-level `supabase` import, the client is
reused via `db.client()`, and every query goes through `db._run(build)` (the
transport-retry wrapper). Following db.py, the core functions carry NO internal
try/except — callers decide how to handle failures.

Snapshot model (see supabase/schema.sql): each category is a set of rows tagged
by a batch_id; md_batches holds the current-generation batch_id per category.
write_snapshot does INSERT (new batch) -> SWAP POINTER (md_batches upsert) ->
DELETE-OLD (prior batches), never delete-first. supabase-py has no transactions,
so the pointer flip IS the atomicity mechanism: a concurrent reader always sees
one complete batch (old or new), never an empty or half-written one.

This module MAY import pandas (it is a market-data module).
"""
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd

from services import applog, db

# category -> (table name, code column for the read/delete filter, ORDER-BY
# columns for deterministic pagination). The md_* tables have NO single-column
# primary key (see supabase/schema.sql), so ranged reads must carry an explicit,
# stable ORDER BY or PostgREST may return the same row on two different pages.
# The order columns below form a stable total order per batch: warrant_code and
# universe `code` are unique per row; an option row is uniquely keyed by
# (stock_code, contract, type).
_CATEGORY = {
    "warrants":          ("md_warrants",          "underlying_code", ("warrant_code",)),
    "tw_options":        ("md_tw_options",        "stock_code",      ("stock_code", "contract", "type")),
    "us_options":        ("md_us_options",        "stock_code",      ("stock_code", "contract", "type")),
    "warrant_universe":  ("md_warrant_universe",  "code",            ("code",)),
}

_INSERT_CHUNK = 500   # rows per insert request
_READ_PAGE = 1000     # requested rows per read page (.range window)
_READ_WORKERS = 6     # concurrent range fetches for a multi-page snapshot


def _now():
    return datetime.now(timezone.utc).isoformat()


def _records(df, batch_id):
    """DataFrame slice -> list of JSON-safe records with batch_id attached.

    NaN/NaT must become None: supabase-py serializes to JSON and a bare NaN is
    not valid JSON (and 'nan' as a string would corrupt the column). astype
    (object).where(notnull, None) replaces every missing cell with None.

    Call this per INSERT chunk, not on the whole frame: astype(object) plus
    to_dict each materialize a full copy, so converting all rows at once triples
    peak memory over a large snapshot (the ~140MB universe-write spike that
    pushed the 512MB Render instance into an OOM restart). A 500-row slice caps
    that transient flat regardless of total row count.
    """
    clean = df.astype(object).where(pd.notnull(df), None)
    records = clean.to_dict(orient="records")
    for r in records:
        r["batch_id"] = batch_id
    return records


def write_snapshot(category, df):
    """Insert df as a brand-new batch for `category`, atomically swap it in, then
    drop the old batch. Returns the new batch_id.

    Order is INSERT -> SWAP POINTER -> DELETE-OLD. The pointer flip is the
    commit point; never delete before it.
    """
    table, _code_col, _order_cols = _CATEGORY[category]
    batch_id = str(uuid.uuid4())
    t0 = time.perf_counter()

    # 1. INSERT the new batch, chunked so no single request is oversized AND so
    # the object-dtype/to_dict conversion only ever holds one chunk in memory
    # (see _records: converting the whole frame at once is the universe-write
    # memory spike). Slice the frame, convert just that slice, insert, discard.
    n = len(df)
    chunks = 0
    for i in range(0, n, _INSERT_CHUNK):
        chunk = _records(df.iloc[i:i + _INSERT_CHUNK], batch_id)
        db._run(lambda c, chunk=chunk: c.table(table).insert(chunk).execute())
        chunks += 1

    # 2. SWAP POINTER — this upsert is the atomic commit of the new batch.
    db._run(
        lambda c: c.table("md_batches")
        .upsert(
            {"category": category, "batch_id": batch_id, "created_at": _now()},
            on_conflict="category",
        )
        .execute()
    )

    # 3. DELETE-OLD — everything in this table that is not the new batch.
    db._run(
        lambda c: c.table(table).delete().neq("batch_id", batch_id).execute()
    )
    applog.log(
        "DB",
        f"{category} wrote {n} rows in {time.perf_counter() - t0:.1f}s ({chunks} chunks)",
    )
    return batch_id


def read_snapshot(category, codes=None):
    """Return (DataFrame, created_at_iso_or_None) for the current batch.

    Reads are paginated and filtered to the current batch_id; if `codes` is given
    they are also filtered on the category's code column. No other filtering is
    applied — callers filter in pandas.

    Pagination is count-driven, NOT early-break-on-short-page. PostgREST enforces
    a server-side max-rows cap (db-max-rows, default 1000): a `.range()` window
    wider than the cap silently returns only the cap, so we can never trust the
    REQUESTED page size (_READ_PAGE) as the real one. Instead the first request
    carries count="exact" — that returns page 1 AND the true total row count for
    the batch — and we measure page 1's length to learn the effective server page
    size. The remaining ranges are then fetched CONCURRENTLY (a several-thousand-
    row snapshot used to cost N serial Supabase round-trips; that serial paging is
    the "Loading market data from database…" latency on every fetch). Every ranged
    query carries an explicit ORDER BY (the md_* tables have no single-column PK)
    so pages never overlap or drop rows; results are assembled in offset order so
    the returned frame is positionally deterministic.
    """
    table, code_col, order_cols = _CATEGORY[category]
    t0 = time.perf_counter()

    ptr = db._run(
        lambda c: c.table("md_batches")
        .select("batch_id, created_at")
        .eq("category", category)
        .execute()
    )
    if not ptr.data:
        return pd.DataFrame(), None
    batch_id = ptr.data[0]["batch_id"]
    created_at = ptr.data[0].get("created_at")

    def fetch(offset, page, count=None):
        """One ranged read of the current batch: [offset, offset+page-1],
        ordered for deterministic pagination and filtered to `codes` if given.
        Pass count="exact" to also get the batch's total row count back on the
        response (.count). db._run is the transport-retry wrapper; each build
        constructs its own independent query object, so this is safe to call
        concurrently from the pool below (the shared client's httpx transport
        handles parallel requests; a rare concurrent transport-retry only rebuilds
        the client redundantly, never corrupts a read)."""
        def build(c):
            q = c.table(table).select("*", count=count) if count else \
                c.table(table).select("*")
            q = q.eq("batch_id", batch_id)
            if codes is not None:
                q = q.in_(code_col, list(codes))
            for col in order_cols:
                q = q.order(col)
            return q.range(offset, offset + page - 1).execute()

        return db._run(build)

    # 1. First page WITH an exact count: gives page 1's rows and the true total.
    first = fetch(0, _READ_PAGE, count="exact")
    rows = list(first.data or [])
    page_size = len(rows)   # the EFFECTIVE server page size (capped, not _READ_PAGE)
    total = first.count     # exact row count for this batch + codes filter
    pages = 1               # round-trips made (page 1 above); grows below

    if page_size and total and total > page_size:
        # 2. Fetch every remaining range concurrently, then assemble in offset
        # order so the frame stays positionally deterministic. Page size is the
        # measured server cap, so range windows never exceed it (no truncation).
        offsets = list(range(page_size, total, page_size))
        pages += len(offsets)
        by_offset = {}
        with ThreadPoolExecutor(max_workers=min(_READ_WORKERS, len(offsets))) as ex:
            futures = {ex.submit(fetch, off, page_size): off for off in offsets}
            for fut in as_completed(futures):
                off = futures[fut]
                by_offset[off] = fut.result().data or []
        for off in offsets:
            rows.extend(by_offset[off])
    elif page_size and total is None:
        # Fallback: count unavailable. Page sequentially and break on the MEASURED
        # page size — never on _READ_PAGE — so a server cap below _READ_PAGE can
        # never be mistaken for end-of-data and truncate the snapshot.
        offset = page_size
        while True:
            page = fetch(offset, page_size)
            pages += 1
            batch = page.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

    df = pd.DataFrame(rows)
    if not df.empty and "batch_id" in df.columns:
        df = df.drop(columns=["batch_id"])
    applog.log(
        "DB",
        f"{category} read {len(df)} rows in {time.perf_counter() - t0:.1f}s ({pages} pages)",
    )
    return df, created_at


def snapshot_enabled():
    """True when readers should serve the Supabase snapshot first.

    Gated on MARKET_SOURCE=supabase; read at call time so the env can be
    toggled per call (the parity gate relies on this).
    """
    return os.environ.get("MARKET_SOURCE") == "supabase"


def snapshot_as_of(category):
    """Return the current batch's md_batches.created_at ISO string, or None."""
    ptr = db._run(
        lambda c: c.table("md_batches")
        .select("created_at")
        .eq("category", category)
        .execute()
    )
    if not ptr.data:
        return None
    return ptr.data[0].get("created_at")


def get_key():
    """Return the stored CMoney key string, or None if unset."""
    r = db._run(
        lambda c: c.table("cmoney_key").select("key").eq("id", 1).execute()
    )
    if r.data:
        return r.data[0].get("key")
    return None


def set_key(key):
    """Upsert the single-row CMoney key store (id=1)."""
    db._run(
        lambda c: c.table("cmoney_key")
        .upsert({"id": 1, "key": key, "updated_at": _now()})
        .execute()
    )
    return True
