# Running a local redundancy instance

This guide sets up a **localhost copy** of the app that runs with **no login** and
keeps your portfolio in sync with the shared Supabase database. It also loads your
existing local `portfolio.json` (trade history / PnL) into Supabase the first time.

The model: **Supabase is the single source of truth.** The Render deploy and your
local copy are two doors into the *same* database — a trade added on either side
lands in Supabase, and the other side sees it on its next page load. Your local
`portfolio.json` is kept as a readable backup mirror so you can still view your
portfolio if Render is down.

### Two ways to use this guide

- **Just sync my trades** (most people): do Part A and Part B steps 1–6. This uploads
  your existing `portfolio.json` into Supabase once; afterward your trades show on the
  Render app. You can then **delete this folder** — your data lives in Supabase, not
  here. You do *not* need to run the app.
- **Also run a local backup app**: additionally do Part B step 7. This runs a
  standalone localhost copy so you can still use your portfolio when Render is down.
  In this case, **keep the folder** — the app runs from it.

> ⚠️ **Do the seed (Part B, steps 4–6) BEFORE you ever run the app locally.** Once
> local mode runs, it reads Supabase and *overwrites* `portfolio.json` with the
> database contents. If you launch the app before seeding, your local-only trades get
> wiped by the (empty or partial) Supabase copy. **Seed first, run second.**

---

## Part A — One-time coordination (with the Supabase project owner)

1. **Owner adds your email to the allow-list.** In the Supabase SQL editor:
   ```sql
   insert into allowed_users (email, note) values ('you@example.com', 'your name');
   ```
2. **Log into the live site once** (`https://warrant-scanner.onrender.com`) with that
   email via the magic link. This creates your row in Supabase `auth.users` — your
   portfolio rows must reference a real user, so this step is required even though
   local mode itself won't use login.
3. **Owner looks up your UUID:** Supabase dashboard → **Authentication → Users** →
   your email → copy the **User UID** (a UUID). They send it to you.
4. **Owner shares the Supabase secrets with you — securely** (password-manager share
   or another encrypted channel, **not** plain chat/email):
   - `SUPABASE_URL`
   - `SUPABASE_ANON_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_JWT_SECRET` (if used)

   > 🔒 The `service_role` key grants full database access. Only share it with a
   > trusted co-owner of the project.

---

## Part B — Local setup (you run these)

1. **Clone this branch** into a fresh folder (this does not touch any repo you
   already have):
   ```bash
   git clone -b worktree-web-deploy https://github.com/ianliu1119/Taiwan-Warrants-Web.git warrant-sync
   cd warrant-sync
   ```
   Verify you're on the right branch — the seed script and `db.py` exist **only** on
   `worktree-web-deploy`, not on `main`:
   ```bash
   git branch --show-current              # must print: worktree-web-deploy
   ls db.py scripts/migrate_portfolio_to_supabase.py   # both must be listed
   ```
   > If `db.py` is missing you're on the wrong branch. Fix it with
   > `git checkout worktree-web-deploy`, then re-check.

2. **Create a Python environment and install dependencies.**
   - For **seeding only** (Two ways → "Just sync my trades"), you just need the
     Supabase client:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate            # Windows: .venv\Scripts\activate
     pip install supabase==2.31.0
     ```
   - To **also run the app** later (step 7), install everything instead:
     ```bash
     pip install -r requirements.txt
     ```

3. **Create `.env`** from the template and fill it in:
   ```bash
   cp .env.example .env
   ```
   ```
   SUPABASE_URL=...
   SUPABASE_ANON_KEY=...
   SUPABASE_SERVICE_ROLE_KEY=...
   SUPABASE_JWT_SECRET=...

   LOCAL_USER_ID=<your UUID from Part A step 3>
   LOCAL_USER_EMAIL=you@example.com
   ```
   `LOCAL_USER_ID` set + `RENDER` unset is what enables no-login local mode.

4. **Bring your real `portfolio.json` into this folder, then back it up.** This is two
   actions — the fresh clone does **not** contain a `portfolio.json` (it's gitignored
   personal data), so you must copy in your own file first:
   ```bash
   # 4a. copy YOUR existing trade file (from wherever your app keeps it) into this folder:
   cp /path/to/your-app/portfolio.json ./portfolio.json
   # 4b. then make a safety backup of it:
   cp portfolio.json portfolio.json.backup
   ```
   > If you skip 4a, the seed will find zero trades (or the backup line errors with
   > "No such file"). Nothing to sync means the file isn't here yet.

5. **Preview the seed (dry run — writes nothing):**
   ```bash
   python scripts/migrate_portfolio_to_supabase.py
   ```
   It reads `LOCAL_USER_ID` from your `.env` and prints every trade it *would* upload.
   **Check the `portfolio: N entries` line** — `N` should match your real trade count.
   If it's `0`, your file isn't in the folder yet (go back to 4a). If you see
   `entry has no id`, stop — your portfolio format lacks per-trade IDs; ask the owner.
   > `LOCAL_USER_ID` not picked up? Pass it explicitly:
   > `python scripts/migrate_portfolio_to_supabase.py --user-id <YOUR_UUID>`

6. **Commit the seed for real:**
   ```bash
   python scripts/migrate_portfolio_to_supabase.py --commit
   ```
   Expected final line: `committed: upserted N rows.` This is **additive** — it only
   inserts/updates your rows and never deletes, so it safely *combines* your local
   `portfolio.json` with anything already in your Supabase account (e.g. trades you
   made on the live site). Your trades now show on the Render app.

   **If you only wanted to sync your trades, you're done here** — you can delete this
   folder; your data is safely in Supabase. Continue to step 7 only if you also want a
   local backup app.

7. **(Optional) Run the app locally** — only if you want a standalone backup instance:
   ```bash
   python wsgi.py     # or your usual local launch command
   ```
   It opens with no login and shows your combined portfolio. From here,
   `portfolio.json` is kept as a live backup mirror of Supabase. **Keep this folder**
   as long as you use the local app.

---

## What you get

- Add a trade **locally** or on the **live site** → it lands in the shared Supabase;
  the other side sees it on the next page load (synced on load, not real-time across
  already-open tabs).
- `portfolio.json` stays as a local backup, so you can view your portfolio even when
  Render is down.

### Caveat (by design)

This setup handles a **Render** outage, not a **Supabase** outage. If Supabase itself
is unreachable, a trade you add locally is written to the `portfolio.json` backup but
is **not** auto-buffered and replayed to Supabase later — re-enter it once you're back
online. (The "full offline reconcile" behavior was intentionally left out of scope.)
