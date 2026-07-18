#!/usr/bin/env python3
"""One-shot: seed a local portfolio.json into Supabase under one user_id.

Additive by design. It does NOT call db.save_portfolio(): that function deletes
every row not present in the posted array, which would erase any Supabase rows
missing from this local file -- the exact data-loss bug two-way sync fixes. This
script only upserts, never deletes, so seeding is safe to run against a table
that already holds rows (yours or the other user's).

Dry run by default (prints what it would upsert, makes zero network calls). Pass
--commit to actually write. The target user is --user-id or the LOCAL_USER_ID
env var.

  python scripts/migrate_portfolio_to_supabase.py                 # dry run
  python scripts/migrate_portfolio_to_supabase.py --commit        # write
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE = os.path.join(ROOT, "portfolio.json")


def _load_dotenv():
    """Populate os.environ from a .env in the repo root so LOCAL_USER_ID and the
    SUPABASE_* vars set there are visible. Runs before argparse resolves the
    default user id (db.py loads .env too, but only on the --commit path, which is
    too late for the user-id check). Minimal; mirrors db._load_dotenv. Silent on
    any error."""
    path = os.path.join(ROOT, ".env")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"note: .env load skipped: {e}")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _rows(entries, user_id):
    """Build one portfolio row per entry. Missing ids are fatal: the id is the
    primary key and the sync merge key, so a null would collide/overwrite."""
    now = _now()
    rows = []
    for e in entries or []:
        eid = e.get("id")
        if not eid:
            raise SystemExit(f"entry has no id, cannot seed: {json.dumps(e)[:120]}")
        rows.append({"user_id": user_id, "id": str(eid), "payload": e, "updated_at": now})
    return rows


def main():
    _load_dotenv()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true", help="actually write (default: dry run)")
    ap.add_argument("--user-id", default=None,
                    help="target Supabase user UUID (default: $LOCAL_USER_ID from env or .env)")
    ap.add_argument("--file", default=DEFAULT_FILE, help=f"portfolio json (default: {DEFAULT_FILE})")
    args = ap.parse_args()

    user_id = args.user_id or os.environ.get("LOCAL_USER_ID")
    if not user_id:
        raise SystemExit("no user id: pass --user-id or set LOCAL_USER_ID")

    try:
        with open(args.file) as f:
            entries = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"no portfolio file at {args.file}")
    if not isinstance(entries, list):
        raise SystemExit(f"expected a JSON array in {args.file}, got {type(entries).__name__}")

    rows = _rows(entries, user_id)
    closed = sum(1 for e in entries if e.get("closed"))
    print(f"portfolio: {len(rows)} entries ({closed} closed) -> user {user_id}")

    if not args.commit:
        for r in rows:
            print(f"  would upsert id={r['id']}  {str(r['payload'].get('title'))[:60]}")
        print("dry run: nothing written. re-run with --commit to apply.")
        return

    # Import db lazily so a dry run never touches Supabase / needs env vars.
    sys.path.insert(0, ROOT)
    import db
    db.client().table("portfolio").upsert(rows).execute()
    print(f"committed: upserted {len(rows)} rows.")


if __name__ == "__main__":
    main()
