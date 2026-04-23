#!/usr/bin/env python3
"""
Pull real production data from the Railway-hosted Consolidated Report API
into the local preview SQLite so the dashboard has live numbers without
requiring the unfinished endpoints to be deployed.

What this does
  1. Authenticates against Railway with a pre-issued Bearer token.
  2. Lists the org's companies.
  3. Upserts each company into the local SQLite (matching the schema that
     init_db() expects) under the local preview user's org_id.
  4. Pulls and caches each company's chart of accounts (including
     current_balance — this is what powers cash-on-hand).
  5. Pulls YTD P&L, Cash Flow, and Balance Sheet per company and writes
     them into the local `company_reports` cache so the dashboard's
     `_get_live_report_for_company` fallback can serve them.

Usage
  python3 scripts/sync_from_railway.py \\
    --local-db /path/to/qbo_enterprise.db \\
    --org-id <org_id-of-local-user> \\
    --token <RAILWAY_BEARER_TOKEN> \\
    [--base-url https://overflowing-ambition-production-4b7e.up.railway.app]
"""
from __future__ import annotations
import argparse
import datetime as _dt
import json
import sqlite3
import sys
import uuid

import requests


def _ymd(d: _dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def fetch_json(base: str, path: str, token: str, method: str = "GET", body: dict | None = None):
    url = base.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}"}
    if method == "GET":
        r = requests.get(url, headers=headers, timeout=60)
    else:
        r = requests.request(method, url, headers={**headers, "Content-Type": "application/json"}, json=body or {}, timeout=120)
    if not r.ok:
        raise RuntimeError(f"{method} {path} → {r.status_code} {r.text[:400]}")
    return r.json() if r.text else {}


def upsert_company(db: sqlite3.Connection, org_id: str, row: dict) -> str:
    """Mirror the row into local `companies`. Keeps remote UUIDs so we can
    cross-reference dividend events later.

    IMPORTANT: always force org_id + legal_name to the synced values so a
    pre-existing row that was seeded under a different org (e.g. the demo
    `org-default` row from init_db's `_seed_from_cached_files`) gets
    re-homed to the current user's org. Without this, the dashboard's
    org-scoped queries silently drop the company from every aggregation.
    """
    cid = row["id"]
    db.execute(
        """INSERT OR IGNORE INTO companies
           (id, name, org_id, legal_name, qbo_company_id, qbo_realm_id,
            access_token, refresh_token, token_expires_at, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, '', ?, '', ?, datetime('now'))""",
        (cid, row.get("name") or "", org_id,
         row.get("legal_name") or "", row.get("qbo_company_id") or "",
         row.get("qbo_realm_id") or "",
         # Stub refresh_token so the local dashboard still takes the
         # 'live' path (which will fall through to cached reports since
         # it can't actually call QBO from the local preview).
         "synced-from-railway",
         row.get("status") or "connected"),
    )
    db.execute(
        """UPDATE companies
           SET name = ?, org_id = ?, status = ?, legal_name = COALESCE(?, legal_name)
           WHERE id = ?""",
        (row.get("name") or "", org_id, row.get("status") or "connected",
         row.get("legal_name"), cid),
    )
    db.commit()
    return cid


def cache_accounts(db: sqlite3.Connection, company_id: str, accounts: list):
    db.execute("DELETE FROM company_accounts WHERE company_id = ?", (company_id,))
    for a in accounts:
        db.execute(
            """INSERT INTO company_accounts
               (id, company_id, qbo_account_id, name, fully_qualified_name,
                account_type, account_sub_type, classification, current_balance,
                active, cached_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (str(uuid.uuid4()), company_id,
             str(a.get("qbo_account_id") or a.get("Id") or ""),
             a.get("name") or a.get("Name") or "",
             a.get("fully_qualified_name") or a.get("FullyQualifiedName") or a.get("name") or "",
             a.get("account_type") or a.get("AccountType") or "",
             a.get("account_sub_type") or a.get("AccountSubType") or "",
             a.get("classification") or a.get("Classification") or "",
             float(a.get("current_balance") or a.get("CurrentBalance") or 0),
             1 if (a.get("active") is None or a.get("active") or a.get("Active")) else 0),
        )
    db.commit()


def cache_report(db: sqlite3.Connection, company_id: str, report_type: str,
                 period_key: str, data: dict):
    if not data:
        return
    rid = str(uuid.uuid4())
    db.execute(
        """INSERT OR REPLACE INTO company_reports
           (id, company_id, report_type, period_key, data_json, cached_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (rid, company_id, report_type, period_key, json.dumps(data)),
    )
    db.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--local-db", required=True)
    ap.add_argument("--org-id", required=True, help="org_id of the local user whose preview we're populating")
    ap.add_argument("--token", required=True, help="Railway API Bearer token")
    ap.add_argument("--base-url", default="https://overflowing-ambition-production-4b7e.up.railway.app")
    ap.add_argument("--period-start", default=None, help="defaults to YYYY-01-01 of current year")
    ap.add_argument("--period-end", default=None, help="defaults to today")
    args = ap.parse_args()

    today = _dt.date.today()
    start = args.period_start or f"{today.year}-01-01"
    end = args.period_end or _ymd(today)
    period_key = f"{today.year}-ytd"

    # Sanity check auth
    me = fetch_json(args.base_url, "/api/auth/me", args.token)
    print(f"authed as {me.get('email') or me.get('id')} (role={me.get('role')}) org={me.get('org_name')}")

    companies = fetch_json(args.base_url, "/api/companies", args.token)
    print(f"remote companies: {len(companies)}")

    db = sqlite3.connect(args.local_db)
    db.row_factory = sqlite3.Row

    # Wipe any hand-seeded local companies so we don't mix them with the
    # real ones. (Preserves shareholders / events / allocations tables.)
    db.execute("DELETE FROM company_accounts")
    db.execute("DELETE FROM company_reports")
    db.execute("DELETE FROM companies WHERE org_id = ?", (args.org_id,))
    db.commit()

    for c in companies:
        cid = upsert_company(db, args.org_id, c)
        label = c.get("name") or cid
        print(f"\n→ {label}  ({cid[:8]}…)")
        # Accounts
        try:
            accts = fetch_json(args.base_url, f"/api/companies/{cid}/accounts", args.token)
            acct_list = accts.get("accounts") if isinstance(accts, dict) else accts
            if isinstance(acct_list, list):
                cache_accounts(db, cid, acct_list)
                print(f"  · accounts cached: {len(acct_list)}")
            else:
                print(f"  · accounts payload unexpected shape: {type(accts).__name__}")
        except Exception as e:
            print(f"  · accounts failed: {e}")
        # Reports
        for (report_name, report_type) in (
            ("profit-loss", "profit_loss"),
            ("cash-flow", "cash_flow"),
            ("balance-sheet", "balance_sheet"),
        ):
            try:
                body = {"company_id": cid, "start_date": start, "end_date": end}
                data = fetch_json(args.base_url, f"/api/reports/{report_name}", args.token, method="POST", body=body)
                # Handler returns {current: <raw qbo report>, ...}; cache the inner report so our extractors can read it
                raw = data.get("current") if isinstance(data, dict) else None
                if raw:
                    cache_report(db, cid, report_type, period_key, raw)
                    print(f"  · {report_type} cached ({period_key})")
                else:
                    print(f"  · {report_type}: no current block in response")
            except Exception as e:
                print(f"  · {report_type} failed: {e}")

    db.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
