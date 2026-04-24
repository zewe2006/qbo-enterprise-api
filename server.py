#!/usr/bin/env python3
"""Consolidated Report — Backend API Server.
Multi-Company Reporting for QuickBooks.

Architecture:
- Direct QuickBooks Online API integration via OAuth 2.0.
- Each company gets its own OAuth tokens (access_token, refresh_token, realm_id).
- All companies can be connected simultaneously.
- Tokens are stored in SQLite and auto-refreshed when expired.
- Reports are pulled directly from QBO API and cached locally.

Self-hosted setup:
  1. pip install fastapi uvicorn httpx
  2. Set environment variables (or edit defaults below):
       QBO_CLIENT_ID, QBO_CLIENT_SECRET, QBO_REDIRECT_URI, QBO_ENVIRONMENT
  3. python server.py
  Server runs on http://localhost:8000
"""
import base64
import calendar
import csv
import io
import hashlib
import json
import logging
import os
import re as _re
import secrets
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone, date
from urllib.parse import urlencode

# ---------- Logging ----------

# Structured logging — all QBO API interactions, errors, and intuit_tid values
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("consolidatedreport")

import httpx
from fastapi import FastAPI, HTTPException, Header, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional

# ---------- AI Chat Config ----------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
AI_MODEL = os.environ.get("AI_MODEL", "gemini-2.5-flash-lite")

# ---------- Stripe Config ----------

STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_BUSINESS_MONTHLY = os.environ.get("STRIPE_PRICE_BUSINESS_MONTHLY", "")  # price_xxx from Stripe dashboard

# ---------- QBO Config ----------

QBO_CLIENT_ID = os.environ.get("QBO_CLIENT_ID", "")
QBO_CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET", "")
QBO_ENVIRONMENT = os.environ.get("QBO_ENVIRONMENT", "sandbox")  # "sandbox" or "production"

# Intuit OAuth endpoints
QBO_AUTH_URL = "https://appcenter.intuit.com/connect/oauth2"
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_REVOKE_URL = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"

# API base URL
if QBO_ENVIRONMENT == "sandbox":
    QBO_API_BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company"
else:
    QBO_API_BASE = "https://quickbooks.api.intuit.com/v3/company"

QBO_SCOPES = "com.intuit.quickbooks.accounting"

# Fixed OAuth redirect URI — must match exactly what is registered in Intuit Developer portal.
# Set this env var on Railway to your Railway public URL + /api/qbo/callback
# Example: https://your-app.up.railway.app/api/qbo/callback
QBO_REDIRECT_URI = os.environ.get(
    "QBO_REDIRECT_URI",
    "https://overflowing-ambition-production-4b7e.up.railway.app/api/qbo/callback"
)

# Frontend origin for CORS — set to your Netlify URL
FRONTEND_ORIGIN = os.environ.get(
    "FRONTEND_ORIGIN", "https://consolidatedreport.app"
)

# ---------- Database ----------

# Use Railway persistent volume if available, otherwise local directory
_here = os.path.dirname(__file__)
_volume_path = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
if _volume_path and os.path.isdir(_volume_path):
    DB_PATH = os.path.join(_volume_path, "qbo_enterprise.db")
    DATA_DIR = os.path.join(_volume_path, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
else:
    DB_PATH = os.path.join(_here, "qbo_enterprise.db")
    DATA_DIR = os.path.join(_here, "data")
    if not os.path.isdir(DATA_DIR):
        DATA_DIR = os.path.join(os.path.dirname(_here), "data")


def get_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS organizations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT UNIQUE,
            owner_id TEXT,
            plan TEXT DEFAULT 'free',
            max_companies INTEGER DEFAULT 5,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT,
            role TEXT DEFAULT 'user',
            org_id TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS companies (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            org_id TEXT,
            legal_name TEXT,
            qbo_company_id TEXT,
            qbo_realm_id TEXT,
            access_token TEXT,
            refresh_token TEXT,
            token_expires_at TEXT,
            status TEXT DEFAULT 'disconnected',
            last_synced TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS company_reports (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            report_type TEXT NOT NULL,
            period_key TEXT NOT NULL,
            params_json TEXT,
            data_json TEXT NOT NULL,
            cached_at TEXT DEFAULT (datetime('now')),
            UNIQUE(company_id, report_type, period_key),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        );
        CREATE TABLE IF NOT EXISTS company_accounts (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            qbo_account_id TEXT NOT NULL,
            name TEXT NOT NULL,
            fully_qualified_name TEXT,
            account_type TEXT,
            account_sub_type TEXT,
            classification TEXT,
            current_balance REAL DEFAULT 0,
            active INTEGER DEFAULT 1,
            cached_at TEXT DEFAULT (datetime('now')),
            UNIQUE(company_id, qbo_account_id),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        );
        CREATE TABLE IF NOT EXISTS account_mappings (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            qbo_account_id TEXT NOT NULL,
            qbo_account_name TEXT NOT NULL,
            consolidated_category TEXT NOT NULL,
            consolidated_subcategory TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        );
        CREATE TABLE IF NOT EXISTS intercompany_entries (
            id TEXT PRIMARY KEY,
            org_id TEXT,
            source_company_id TEXT NOT NULL,
            dest_company_id TEXT NOT NULL,
            entry_type TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            date TEXT NOT NULL,
            source_debit_account TEXT,
            source_credit_account TEXT,
            dest_debit_account TEXT,
            dest_credit_account TEXT,
            source_debit_entity_id TEXT,
            source_credit_entity_id TEXT,
            dest_debit_entity_id TEXT,
            dest_credit_entity_id TEXT,
            source_je_id TEXT,
            dest_je_id TEXT,
            status TEXT DEFAULT 'pending',
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (source_company_id) REFERENCES companies(id),
            FOREIGN KEY (dest_company_id) REFERENCES companies(id)
        );
        CREATE TABLE IF NOT EXISTS ic_entry_lines (
            id TEXT PRIMARY KEY,
            entry_id TEXT NOT NULL,
            side TEXT NOT NULL,
            posting_type TEXT NOT NULL,
            account_name TEXT NOT NULL,
            amount REAL NOT NULL,
            entity_id TEXT,
            description TEXT,
            FOREIGN KEY (entry_id) REFERENCES intercompany_entries(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS ic_templates (
            id TEXT PRIMARY KEY,
            org_id TEXT,
            name TEXT NOT NULL,
            source_company_id TEXT,
            dest_company_id TEXT,
            entry_type TEXT,
            source_debit_account TEXT,
            source_credit_account TEXT,
            dest_debit_account TEXT,
            dest_credit_account TEXT,
            description TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'general',
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (org_id) REFERENCES organizations(id)
        );
        CREATE TABLE IF NOT EXISTS oauth_states (
            state TEXT PRIMARY KEY,
            redirect_uri TEXT,
            org_id TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS user_company_access (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, company_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS delivery_mappings (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            platform TEXT NOT NULL DEFAULT 'ubereats',
            org_id TEXT NOT NULL,
            mapping TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(company_id, platform, org_id)
        );

        CREATE TABLE IF NOT EXISTS delivery_import_history (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            store_name TEXT DEFAULT '',
            statement_period TEXT DEFAULT '',
            payout_count INTEGER DEFAULT 0,
            entry_count INTEGER DEFAULT 0,
            prefix TEXT DEFAULT '',
            mapping TEXT DEFAULT '{}',
            status TEXT DEFAULT 'csv_downloaded',
            qbo_je_ids TEXT DEFAULT '[]',
            csv_content TEXT DEFAULT '',
            created_by TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)

    # Safe column additions
    for col, ctype in [
        ("address", "TEXT"), ("phone", "TEXT"), ("email", "TEXT"),
        ("industry", "TEXT"), ("qbo_plan", "TEXT"),
        ("access_token", "TEXT"), ("refresh_token", "TEXT"),
        ("token_expires_at", "TEXT"),
        # Manual + Plaid support
        ("source", "TEXT DEFAULT 'qbo'"),           # 'qbo' | 'manual'
        ("supabase_company_id", "TEXT"),            # UUID mirror into Supabase
        ("fiscal_year_start", "INTEGER DEFAULT 1"), # month 1-12
        ("base_currency", "TEXT DEFAULT 'USD'"),
        ("ein", "TEXT"),
    ]:
        _add_column_safe(db, "companies", col, ctype)

    # Backfill source for any pre-existing rows that have a QBO realm but no source set
    db.execute("UPDATE companies SET source='qbo' WHERE source IS NULL AND qbo_realm_id IS NOT NULL")
    db.execute("UPDATE companies SET source='qbo' WHERE source IS NULL")
    db.commit()

    # Entity ID columns for IC entries (AR/AP need Customer/Vendor refs)
    for col in ["source_debit_entity_id", "source_credit_entity_id",
                "dest_debit_entity_id", "dest_credit_entity_id"]:
        _add_column_safe(db, "intercompany_entries", col, "TEXT")

    # Multi-tenant migration
    _add_column_safe(db, "users", "org_id", "TEXT")
    _add_column_safe(db, "companies", "org_id", "TEXT")
    _add_column_safe(db, "intercompany_entries", "org_id", "TEXT")
    _add_column_safe(db, "ic_templates", "org_id", "TEXT")
    _add_column_safe(db, "oauth_states", "org_id", "TEXT")

    # Stripe billing migration
    _add_column_safe(db, "organizations", "stripe_customer_id", "TEXT")
    _add_column_safe(db, "organizations", "stripe_subscription_id", "TEXT")
    _add_column_safe(db, "organizations", "subscription_status", "TEXT DEFAULT 'none'")  # none, active, past_due, canceled
    _add_column_safe(db, "organizations", "trial_ends_at", "TEXT")
    _add_column_safe(db, "organizations", "trial_started_at", "TEXT")

    # Create default org for existing data if not exists
    existing_org = db.execute("SELECT id FROM organizations LIMIT 1").fetchone()
    if not existing_org:
        first_admin = db.execute("SELECT id FROM users WHERE role = 'admin' LIMIT 1").fetchone()
        if first_admin:
            default_org_id = "org-default"
            db.execute(
                "INSERT OR IGNORE INTO organizations (id, name, slug, owner_id) VALUES (?, ?, ?, ?)",
                (default_org_id, "Default Organization", "default", first_admin["id"]),
            )
            db.execute("UPDATE users SET org_id = ? WHERE org_id IS NULL", (default_org_id,))
            db.execute("UPDATE companies SET org_id = ? WHERE org_id IS NULL", (default_org_id,))
            db.execute("UPDATE intercompany_entries SET org_id = ? WHERE org_id IS NULL", (default_org_id,))
            db.execute("UPDATE ic_templates SET org_id = ? WHERE org_id IS NULL", (default_org_id,))
            db.commit()

    # Ensure orgs with >3 companies are on business plan (grandfathered)
    for org_row in db.execute(
        "SELECT o.id, o.plan, COUNT(c.id) as cnt FROM organizations o "
        "LEFT JOIN companies c ON c.org_id = o.id "
        "GROUP BY o.id HAVING cnt > 3 AND o.plan = 'free'"
    ).fetchall():
        db.execute(
            "UPDATE organizations SET plan = 'business', max_companies = 50, subscription_status = 'active' WHERE id = ?",
            (org_row["id"],),
        )
        logger.info("Grandfathered org %s to business plan (%d companies)", org_row["id"], org_row["cnt"])
    db.commit()

    # Default admin user
    existing = db.execute("SELECT id FROM users LIMIT 1").fetchone()
    if not existing:
        admin_id = str(uuid.uuid4())
        default_org_id = "org-default"
        pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
        db.execute(
            "INSERT OR IGNORE INTO organizations (id, name, slug, owner_id) VALUES (?, ?, ?, ?)",
            (default_org_id, "Default Organization", "default", admin_id),
        )
        db.execute(
            "INSERT INTO users (id, email, password_hash, name, role, org_id) VALUES (?, ?, ?, ?, ?, ?)",
            (admin_id, "admin@enterpriseledger.local", pw_hash, "Admin", "admin", default_org_id),
        )
        db.commit()

    # Seed from cached JSON if available
    _seed_from_cached_files(db)

    # Seed default knowledge base entries
    _seed_knowledge_base(db)
    db.close()


def _add_column_safe(db, table, column, col_type):
    try:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        db.commit()
    except sqlite3.OperationalError:
        pass


def _seed_from_cached_files(db):
    """One-time import of pre-cached JSON data from data/ directory."""
    if not os.path.isdir(DATA_DIR):
        return
    company_file = os.path.join(DATA_DIR, "farm_noodle_company.json")
    if not os.path.exists(company_file):
        return

    with open(company_file, "r") as f:
        data = json.load(f)
    info = data.get("CompanyInfo", data)
    legal = info.get("LegalName", info.get("CompanyName", ""))

    existing = db.execute("SELECT id FROM companies WHERE legal_name = ?", (legal,)).fetchone()
    if existing:
        cid = existing["id"]
    else:
        cid = str(uuid.uuid4())

    _upsert_company_from_info(db, cid, info, "connected", org_id="org-default")

    report_files = {
        "farm_noodle_pl_ytd.json": ("profit_loss", "2026-ytd"),
        "farm_noodle_pl_2025.json": ("profit_loss", "2025-full"),
        "farm_noodle_bs.json": ("balance_sheet", "2026-ytd"),
        "farm_noodle_cf.json": ("cash_flow", "2026-ytd"),
    }
    for fname, (rtype, period) in report_files.items():
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath, "r") as f:
                rdata = json.load(f)
            rid = str(uuid.uuid4())
            db.execute(
                """INSERT OR REPLACE INTO company_reports
                   (id, company_id, report_type, period_key, data_json, cached_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                (rid, cid, rtype, period, json.dumps(rdata)),
            )
    db.commit()

    acct_file = os.path.join(DATA_DIR, "farm_noodle_accounts.json")
    if os.path.exists(acct_file):
        with open(acct_file, "r") as f:
            adata = json.load(f)
        accounts = []
        if isinstance(adata, dict):
            qr = adata.get("QueryResponse", adata)
            accounts = qr.get("Account", [])
        elif isinstance(adata, list):
            accounts = adata
        _cache_accounts(db, cid, accounts)


def _seed_knowledge_base(db):
    """Seed default knowledge base entries for all orgs that don't have any yet."""
    orgs = db.execute("SELECT id FROM organizations").fetchall()
    for org in orgs:
        oid = org["id"]
        existing = db.execute("SELECT COUNT(*) as cnt FROM knowledge_base WHERE org_id = ?", (oid,)).fetchone()
        if existing["cnt"] > 0:
            continue
        defaults = [
            ("app_guide", "How to connect a QuickBooks company",
             "1. Go to the Companies page\n2. Click 'Connect New Company'\n3. Sign in with your QuickBooks credentials\n4. Authorize access\n5. The company will appear in your list as 'Connected'"),
            ("app_guide", "How to create an intercompany journal entry",
             "1. Go to the Intercompany page\n2. Click 'New Entry'\n3. Select the source company and destination company\n4. Choose the entry type (e.g., Management Fee, Loan, Expense Reimbursement)\n5. Add line items with accounts and amounts — debits must equal credits on each side\n6. Click 'Create Entry' to save as pending, or 'Post to QBO' to push to QuickBooks"),
            ("app_guide", "How to run financial reports",
             "1. Go to the Dashboard or Reports section\n2. Select the report type: Profit & Loss, Balance Sheet, or Cash Flow\n3. Choose the date range (Last Month, Year to Date, or Custom)\n4. View consolidated totals across all companies, or click a company column to drill down\n5. Click any dollar amount to see the transaction detail behind that number"),
            ("app_guide", "How to manage users",
             "1. Go to the Users page (admin only)\n2. Click 'Add User' and enter their email and name\n3. Assign a role: Admin (full access) or Viewer (read-only)\n4. Assign which companies they can access\n5. They will receive a login with the credentials you set"),
            ("app_guide", "How to use the AI chat assistant",
             "Click the chat icon in the bottom-right corner. You can ask the AI to:\n- Create intercompany journal entries (just describe what you need in plain English)\n- Pull financial reports for any company or date range\n- Analyze financial data and compare companies\n- Navigate to any page in the app\n\nThe AI knows your connected companies and chart of accounts."),
            ("accounting_rules", "Management Fee entries",
             "Management fees are charged from the parent company (Sweet Hut Group LLC) to subsidiary locations. Typical entry:\n- Source (subsidiary): Debit Management Fee Expense, Credit Due to Parent/Accounts Payable\n- Destination (parent): Debit Due from Subsidiary/Accounts Receivable, Credit Management Fee Income"),
            ("accounting_rules", "Intercompany loan entries",
             "Loans between related companies should be recorded as:\n- Lending company: Debit Due from [Borrower] (or Intercompany Receivable), Credit Cash/Bank\n- Borrowing company: Debit Cash/Bank, Credit Due to [Lender] (or Intercompany Payable)\n\nMake sure both sides balance and use consistent account names."),
            ("accounting_rules", "Expense reimbursement entries",
             "When one company pays an expense on behalf of another:\n- Paying company: Debit Due from [Other Company], Credit Cash/Bank\n- Benefiting company: Debit the actual Expense account, Credit Due to [Paying Company]"),
        ]
        for i, (cat, title, content) in enumerate(defaults):
            db.execute(
                "INSERT INTO knowledge_base (id, org_id, category, title, content, sort_order) VALUES (?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), oid, cat, title, content, i),
            )
        db.commit()


def _upsert_company_from_info(db, cid, info, status, realm_id=None,
                                access_token=None, refresh_token=None,
                                token_expires_at=None, org_id=None):
    """Insert or update a company record from QBO CompanyInfo."""
    name = info.get("CompanyName", "Unknown")
    legal = info.get("LegalName", "")
    qbo_id = info.get("Id", "")
    addr = info.get("CompanyAddr", {})
    address_str = ", ".join(
        filter(None, [addr.get("Line1", ""), addr.get("City", ""),
                      addr.get("CountrySubDivisionCode", ""), addr.get("PostalCode", "")])
    )
    phone = info.get("PrimaryPhone", {}).get("FreeFormNumber", "")
    email_addr = info.get("Email", {}).get("Address", "")
    nv = {item["Name"]: item["Value"] for item in info.get("NameValue", [])}
    industry = nv.get("IndustryType", nv.get("QBOIndustryType", ""))
    qbo_plan = nv.get("OfferingSku", "")

    existing = db.execute("SELECT id FROM companies WHERE id = ?", (cid,)).fetchone()
    if existing:
        sql = """UPDATE companies SET name=?, legal_name=?, qbo_company_id=?,
                 address=?, phone=?, email=?, industry=?, qbo_plan=?,
                 status=?, last_synced=datetime('now')"""
        params = [name, legal, qbo_id, address_str, phone, email_addr,
                  industry, qbo_plan, status]
        if realm_id is not None:
            sql += ", qbo_realm_id=?"
            params.append(realm_id)
        if access_token is not None:
            sql += ", access_token=?"
            params.append(access_token)
        if refresh_token is not None:
            sql += ", refresh_token=?"
            params.append(refresh_token)
        if token_expires_at is not None:
            sql += ", token_expires_at=?"
            params.append(token_expires_at)
        if org_id is not None:
            sql += ", org_id=?"
            params.append(org_id)
        sql += " WHERE id=?"
        params.append(cid)
        db.execute(sql, params)
    else:
        db.execute(
            """INSERT INTO companies
               (id, name, org_id, legal_name, qbo_company_id, qbo_realm_id,
                access_token, refresh_token, token_expires_at,
                address, phone, email, industry, qbo_plan, status, last_synced)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (cid, name, org_id or "", legal, qbo_id, realm_id or "",
             access_token or "", refresh_token or "", token_expires_at or "",
             address_str, phone, email_addr, industry, qbo_plan, status),
        )
    db.commit()
    return cid


def _cache_accounts(db, company_id, accounts):
    db.execute("DELETE FROM company_accounts WHERE company_id = ?", (company_id,))
    for a in accounts:
        aid = str(uuid.uuid4())
        db.execute(
            """INSERT INTO company_accounts
               (id, company_id, qbo_account_id, name, fully_qualified_name,
                account_type, account_sub_type, classification, current_balance, active, cached_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (aid, company_id, str(a.get("Id", "")), a.get("Name", ""),
             a.get("FullyQualifiedName", a.get("Name", "")),
             a.get("AccountType", ""), a.get("AccountSubType", ""),
             a.get("Classification", ""), a.get("CurrentBalance", 0),
             1 if a.get("Active", True) else 0),
        )
    db.commit()


# ---------- Direct QBO API Client ----------

async def _get_valid_token(db, company_id: str) -> tuple:
    """Get a valid access token for a company, refreshing if needed.
    Returns (access_token, realm_id) or raises HTTPException."""
    row = db.execute(
        "SELECT access_token, refresh_token, token_expires_at, qbo_realm_id FROM companies WHERE id = ?",
        (company_id,)
    ).fetchone()
    if not row or not row["refresh_token"]:
        raise HTTPException(status_code=400, detail="Company not connected to QBO. Please authorize first.")

    realm_id = row["qbo_realm_id"]
    access_token = row["access_token"]
    expires_at = row["token_expires_at"]

    # Check if token is expired or about to expire (5-min buffer)
    needs_refresh = True
    if expires_at:
        try:
            exp_dt = datetime.fromisoformat(expires_at)
            if exp_dt > datetime.now(timezone.utc) + timedelta(minutes=5):
                needs_refresh = False
        except (ValueError, TypeError):
            pass

    if needs_refresh:
        access_token = await _refresh_access_token(db, company_id, row["refresh_token"])

    return access_token, realm_id


async def _refresh_access_token(db, company_id: str, refresh_token: str) -> str:
    """Refresh an expired access token and store the new tokens.

    Only marks the company as `auth_expired` on REAL authentication failures
    (QBO's `invalid_grant` / `invalid_client` responses). Transient issues
    (429 rate limit, 5xx server errors, network errors) are retried with
    exponential backoff and — critically — do NOT flip status, so heavy
    workloads like a multi-month GL import don't accidentally disconnect
    the company.
    """
    import asyncio

    auth_header = base64.b64encode(
        f"{QBO_CLIENT_ID}:{QBO_CLIENT_SECRET}".encode()
    ).decode()

    _AUTH_FATAL_ERRORS = {
        "invalid_grant",       # refresh token revoked or expired past 100 days
        "invalid_client",      # client id/secret wrong
        "unauthorized_client", # app not allowed
    }

    last_err = None
    for attempt in range(4):  # 0..3 → up to 4 tries total
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    QBO_TOKEN_URL,
                    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                    headers={
                        "Authorization": f"Basic {auth_header}",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json",
                    },
                )
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            last_err = f"Network error: {e!s}"
            logger.warning("Token refresh transient network error (attempt %d): %s",
                           attempt + 1, last_err)
            await asyncio.sleep(2 ** attempt)
            continue

        intuit_tid = resp.headers.get("intuit_tid", "N/A")

        if resp.status_code == 200:
            tokens = resp.json()
            new_access = tokens["access_token"]
            new_refresh = tokens.get("refresh_token", refresh_token)
            expires_in = tokens.get("expires_in", 3600)
            expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
            logger.info(
                "Token refresh OK | company=%s | intuit_tid=%s | attempt=%d",
                company_id, intuit_tid, attempt + 1,
            )
            db.execute(
                """UPDATE companies SET access_token=?, refresh_token=?, token_expires_at=?,
                   status='connected' WHERE id=?""",
                (new_access, new_refresh, expires_at, company_id),
            )
            db.commit()
            return new_access

        body_text = resp.text or ""
        # Inspect QBO error payload to classify
        err_code = ""
        try:
            err_code = (resp.json() or {}).get("error") or ""
        except Exception:
            pass

        # Real auth failure — refresh token is dead. Mark expired and bail.
        if err_code in _AUTH_FATAL_ERRORS or (resp.status_code == 400 and err_code):
            db.execute(
                "UPDATE companies SET status='auth_expired' WHERE id=?",
                (company_id,),
            )
            db.commit()
            logger.error(
                "Token refresh FATAL | company=%s | status=%d | err=%s | intuit_tid=%s | body=%s",
                company_id, resp.status_code, err_code, intuit_tid, body_text[:300],
            )
            raise HTTPException(
                status_code=401,
                detail=f"QuickBooks session expired. Please re-connect this company. ({err_code or resp.status_code})",
            )

        # Transient — 429 rate limit, 5xx, or unexpected 4xx without a fatal code
        last_err = f"HTTP {resp.status_code}: {body_text[:200]}"
        retry_after = int(resp.headers.get("Retry-After", "0") or 0)
        backoff = retry_after if retry_after > 0 else (2 ** attempt)
        logger.warning(
            "Token refresh TRANSIENT | company=%s | status=%d | intuit_tid=%s | attempt=%d | backoff=%ds | body=%s",
            company_id, resp.status_code, intuit_tid, attempt + 1, backoff, body_text[:200],
        )
        await asyncio.sleep(backoff)

    # Exhausted retries on transient errors — do NOT flip status; bubble a 503
    logger.error("Token refresh exhausted retries | company=%s | last=%s", company_id, last_err)
    raise HTTPException(
        status_code=503,
        detail=f"QuickBooks is temporarily unreachable. Try again in a minute. ({last_err})",
    )


async def qbo_api_call(db, company_id: str, endpoint: str,
                        method: str = "GET", params: dict = None) -> dict:
    """Make an authenticated QBO API call for a specific company."""
    access_token, realm_id = await _get_valid_token(db, company_id)

    url = f"{QBO_API_BASE}/{realm_id}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        if method == "GET":
            resp = await client.get(url, headers=headers, params=params or {})
        else:
            resp = await client.post(url, headers=headers, json=params or {})

    intuit_tid = resp.headers.get("intuit_tid", "N/A")

    if resp.status_code == 401:
        # Token might be stale despite our check — try one refresh
        logger.warning(
            "QBO 401 (retrying) | company=%s | endpoint=%s | intuit_tid=%s",
            company_id, endpoint, intuit_tid,
        )
        access_token = await _refresh_access_token(
            db, company_id,
            db.execute("SELECT refresh_token FROM companies WHERE id=?", (company_id,)).fetchone()["refresh_token"]
        )
        headers["Authorization"] = f"Bearer {access_token}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method == "GET":
                resp = await client.get(url, headers=headers, params=params or {})
            else:
                resp = await client.post(url, headers=headers, json=params or {})
        intuit_tid = resp.headers.get("intuit_tid", "N/A")

    if resp.status_code != 200:
        logger.error(
            "QBO API ERROR | company=%s | %s %s | status=%d | intuit_tid=%s | body=%s",
            company_id, method, endpoint, resp.status_code, intuit_tid, resp.text[:500],
        )
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"QBO API error: {resp.text[:300]}"
        )

    logger.info(
        "QBO API OK | company=%s | %s %s | intuit_tid=%s",
        company_id, method, endpoint, intuit_tid,
    )
    return resp.json()


async def qbo_get_report(db, company_id: str, report_name: str,
                          params: dict = None) -> dict:
    """Get a QBO report (P&L, Balance Sheet, Cash Flow) for a specific company."""
    endpoint = f"reports/{report_name}"
    return await qbo_api_call(db, company_id, endpoint, method="GET", params=params)


async def qbo_query(db, company_id: str, query: str) -> dict:
    """Run a QBO query (e.g., SELECT * FROM Account)."""
    endpoint = "query"
    return await qbo_api_call(db, company_id, endpoint, method="GET",
                               params={"query": query})


async def qbo_get_company_info(db, company_id_db: str, realm_id: str = None) -> dict:
    """Get company info from QBO. If realm_id provided, uses it directly."""
    if realm_id:
        # Direct call with realm_id (for initial connect before DB has tokens)
        row = db.execute(
            "SELECT access_token FROM companies WHERE id=?", (company_id_db,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")
        url = f"{QBO_API_BASE}/{realm_id}/companyinfo/{realm_id}"
        headers = {
            "Authorization": f"Bearer {row['access_token']}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers=headers)
        intuit_tid = resp.headers.get("intuit_tid", "N/A")
        if resp.status_code != 200:
            logger.error(
                "QBO companyinfo ERROR | company=%s | realm=%s | status=%d | intuit_tid=%s | body=%s",
                company_id_db, realm_id, resp.status_code, intuit_tid, resp.text[:300],
            )
            raise HTTPException(status_code=resp.status_code, detail=resp.text[:300])
        logger.info("QBO companyinfo OK | company=%s | realm=%s | intuit_tid=%s", company_id_db, realm_id, intuit_tid)
        return resp.json()
    else:
        realm = db.execute(
            "SELECT qbo_realm_id FROM companies WHERE id=?", (company_id_db,)
        ).fetchone()
        if not realm or not realm["qbo_realm_id"]:
            raise HTTPException(status_code=400, detail="No realm ID for this company")
        rid = realm["qbo_realm_id"]
        return await qbo_api_call(db, company_id_db, f"companyinfo/{rid}")


# ---------- FastAPI App ----------

@asynccontextmanager
async def lifespan(app):
    init_db()
    logger.info(
        "Consolidated Report started | env=%s | db=%s | volume=%s",
        QBO_ENVIRONMENT, DB_PATH, bool(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")),
    )
    yield
    logger.info("Consolidated Report shutting down")

app = FastAPI(lifespan=lifespan, title="Consolidated Report API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_ORIGIN,
        "https://consolidatedreport.app",
        "https://v2.consolidatedreport.app",
        "https://hub.consolidatedreport.app",
        "http://localhost:3000",
        "http://localhost:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Auth ----------

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    org_name: str = ""

class CreateUserRequest(BaseModel):
    email: str
    password: str
    name: str
    role: str = "viewer"  # "admin" or "viewer"
    company_ids: List[str] = []  # company IDs this user can access

class UpdateUserRequest(BaseModel):
    email: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None
    company_ids: Optional[List[str]] = None

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


def _extract_token(authorization: str) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="No token")
    return authorization.replace("Bearer ", "")


def get_current_user(token: str):
    db = get_db()
    session = db.execute("SELECT user_id FROM sessions WHERE token = ?", (token,)).fetchone()
    if not session:
        db.close()
        raise HTTPException(status_code=401, detail="Invalid session")
    user = db.execute("SELECT * FROM users WHERE id = ?", (session["user_id"],)).fetchone()
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="User not found")
    # Get company access list
    access_rows = db.execute(
        "SELECT company_id FROM user_company_access WHERE user_id = ?", (user["id"],)
    ).fetchall()
    org = db.execute("SELECT * FROM organizations WHERE id = ?", (user["org_id"],)).fetchone() if user["org_id"] else None
    db.close()
    u = dict(user)
    u["company_ids"] = [r["company_id"] for r in access_rows]
    u["org_id"] = u.get("org_id", "")
    u["org_name"] = org["name"] if org else ""
    return u


def require_admin(user: dict):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")


def get_org_id(user):
    """Get the org_id for the current user. All data queries must use this."""
    org_id = user.get("org_id")
    if not org_id:
        raise HTTPException(status_code=403, detail="User is not associated with an organization.")
    return org_id


def get_effective_plan(org_dict: dict) -> dict:
    """Determine the effective plan for an org, accounting for trial status.
    Returns dict with: plan, max_companies, trial_active, trial_days_remaining, trial_ends_at
    """
    plan = org_dict.get("plan", "free")
    sub_status = org_dict.get("subscription_status", "none")
    trial_ends_at = org_dict.get("trial_ends_at", "")
    trial_started_at = org_dict.get("trial_started_at", "")

    # If they have an active paid subscription, they're on Business
    if plan == "business" and sub_status in ("active", "trialing"):
        return {
            "plan": "business",
            "max_companies": 50,
            "trial_active": False,
            "trial_days_remaining": 0,
            "trial_ends_at": "",
        }

    # Check if in-app trial is active
    if trial_ends_at:
        try:
            trial_end = datetime.fromisoformat(trial_ends_at.replace("Z", "+00:00")) if "+" in trial_ends_at or "Z" in trial_ends_at else datetime.fromisoformat(trial_ends_at).replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if now < trial_end:
                days_left = (trial_end - now).days + 1  # round up
                return {
                    "plan": "business",
                    "max_companies": 50,
                    "trial_active": True,
                    "trial_days_remaining": max(days_left, 1),
                    "trial_ends_at": trial_ends_at,
                }
            else:
                # Trial expired — ensure plan is downgraded
                if plan != "free" and sub_status not in ("active", "trialing"):
                    try:
                        db = get_db()
                        db.execute(
                            "UPDATE organizations SET plan = 'free', max_companies = 3 WHERE id = ?",
                            (org_dict.get("id"),),
                        )
                        db.commit()
                        db.close()
                    except Exception:
                        pass
                return {
                    "plan": "free",
                    "max_companies": 3,
                    "trial_active": False,
                    "trial_days_remaining": 0,
                    "trial_ends_at": trial_ends_at,
                    "trial_expired": True,
                }

        except (ValueError, TypeError):
            pass

    # Default: free plan
    return {
        "plan": plan if plan == "business" and sub_status in ("active",) else "free",
        "max_companies": org_dict.get("max_companies", 3) if plan == "business" and sub_status in ("active",) else 3,
        "trial_active": False,
        "trial_days_remaining": 0,
        "trial_ends_at": "",
    }


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    db = get_db()
    pw_hash = hashlib.sha256(req.password.encode()).hexdigest()
    user = db.execute(
        "SELECT * FROM users WHERE email = ? AND password_hash = ?",
        (req.email, pw_hash),
    ).fetchone()
    if not user:
        db.close()
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = str(uuid.uuid4())
    db.execute("INSERT INTO sessions (token, user_id) VALUES (?, ?)", (token, user["id"]))
    db.commit()
    # Get company access
    access_rows = db.execute(
        "SELECT company_id FROM user_company_access WHERE user_id = ?", (user["id"],)
    ).fetchall()
    org = db.execute("SELECT * FROM organizations WHERE id = ?", (user["org_id"],)).fetchone() if user["org_id"] else None
    db.close()
    plan_info = get_effective_plan(dict(org)) if org else {}
    return {
        "token": token,
        "user": {
            "id": user["id"], "email": user["email"], "name": user["name"],
            "role": user["role"],
            "company_ids": [r["company_id"] for r in access_rows],
            "org_id": user["org_id"] if user["org_id"] else "", "org_name": org["name"] if org else "",
            "plan": plan_info.get("plan", "free"),
            "trial_active": plan_info.get("trial_active", False),
            "trial_days_remaining": plan_info.get("trial_days_remaining", 0),
            "trial_expired": plan_info.get("trial_expired", False),
            "max_companies": plan_info.get("max_companies", 3),
        },
    }

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    if not req.email or not req.password or not req.name:
        raise HTTPException(status_code=400, detail="Email, password, and name are required.")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")
    email = req.email.strip().lower()
    org_name = (req.org_name or "").strip() or f"{req.name.strip()}'s Organization"
    db = get_db()
    existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        db.close()
        raise HTTPException(status_code=409, detail="An account with this email already exists.")
    user_id = str(uuid.uuid4())
    org_id = str(uuid.uuid4())
    org_slug = org_name.lower().replace(" ", "-").replace("'", "")[:50]
    # Check slug uniqueness, append random if taken
    if db.execute("SELECT id FROM organizations WHERE slug = ?", (org_slug,)).fetchone():
        org_slug = org_slug[:40] + "-" + secrets.token_hex(4)
    pw_hash = hashlib.sha256(req.password.encode()).hexdigest()
    # Create organization with 14-day Business trial
    trial_start = datetime.now(timezone.utc).isoformat()
    trial_end = (datetime.now(timezone.utc) + timedelta(days=14)).isoformat()
    db.execute(
        "INSERT INTO organizations (id, name, slug, owner_id, plan, max_companies, trial_started_at, trial_ends_at) VALUES (?, ?, ?, ?, 'business', 50, ?, ?)",
        (org_id, org_name, org_slug, user_id, trial_start, trial_end),
    )
    # Create user as org admin
    db.execute(
        "INSERT INTO users (id, email, password_hash, name, role, org_id) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, email, pw_hash, req.name.strip(), "admin", org_id),
    )
    # Auto-login
    token = str(uuid.uuid4())
    db.execute("INSERT INTO sessions (token, user_id) VALUES (?, ?)", (token, user_id))
    db.commit()
    db.close()
    return {
        "token": token,
        "user": {
            "id": user_id, "email": email, "name": req.name.strip(), "role": "admin",
            "company_ids": [], "org_id": org_id, "org_name": org_name,
            "plan": "business", "trial_active": True, "trial_days_remaining": 14,
            "trial_expired": False, "max_companies": 50,
        },
    }

@app.get("/api/auth/me")
async def get_me(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    # Include trial/plan info
    plan_info = {}
    if user.get("org_id"):
        db = get_db()
        org = db.execute("SELECT * FROM organizations WHERE id = ?", (user["org_id"],)).fetchone()
        db.close()
        if org:
            plan_info = get_effective_plan(dict(org))
    return {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "role": user["role"], "company_ids": user.get("company_ids", []),
        "org_id": user.get("org_id", ""), "org_name": user.get("org_name", ""),
        "plan": plan_info.get("plan", "free"),
        "trial_active": plan_info.get("trial_active", False),
        "trial_days_remaining": plan_info.get("trial_days_remaining", 0),
        "trial_expired": plan_info.get("trial_expired", False),
        "max_companies": plan_info.get("max_companies", 3),
    }

@app.post("/api/auth/logout")
async def logout(authorization: str = Header(None)):
    if authorization:
        token = authorization.replace("Bearer ", "")
        db = get_db()
        db.execute("DELETE FROM sessions WHERE token = ?", (token,))
        db.commit()
        db.close()
    return {"ok": True}

@app.post("/api/auth/change-password")
async def change_password(req: ChangePasswordRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    db = get_db()
    old_hash = hashlib.sha256(req.current_password.encode()).hexdigest()
    if user["password_hash"] != old_hash:
        db.close()
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    new_hash = hashlib.sha256(req.new_password.encode()).hexdigest()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user["id"]))
    db.commit()
    db.close()
    return {"ok": True}


# =====================================================================
#  USER MANAGEMENT (Admin Only)
# =====================================================================

@app.get("/api/users")
async def list_users(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute("SELECT id, email, name, role, created_at FROM users WHERE org_id = ? ORDER BY created_at", (org_id,)).fetchall()
    users = []
    for r in rows:
        u = dict(r)
        access = db.execute(
            "SELECT company_id FROM user_company_access WHERE user_id = ?", (u["id"],)
        ).fetchall()
        u["company_ids"] = [a["company_id"] for a in access]
        users.append(u)
    db.close()
    return users

@app.post("/api/users")
async def create_user(req: CreateUserRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    admin = get_current_user(token)
    require_admin(admin)
    org_id = get_org_id(admin)
    if req.role not in ("admin", "viewer"):
        raise HTTPException(status_code=400, detail="Role must be 'admin' or 'viewer'")
    db = get_db()
    existing = db.execute("SELECT id FROM users WHERE email = ?", (req.email,)).fetchone()
    if existing:
        db.close()
        raise HTTPException(status_code=409, detail="Email already registered")
    user_id = str(uuid.uuid4())
    pw_hash = hashlib.sha256(req.password.encode()).hexdigest()
    db.execute(
        "INSERT INTO users (id, email, password_hash, name, role, org_id) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, req.email, pw_hash, req.name, req.role, org_id),
    )
    # Assign company access
    for cid in req.company_ids:
        db.execute(
            "INSERT OR IGNORE INTO user_company_access (id, user_id, company_id) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), user_id, cid),
        )
    db.commit()
    db.close()
    return {"id": user_id, "email": req.email, "name": req.name, "role": req.role, "company_ids": req.company_ids}

@app.put("/api/users/{user_id}")
async def update_user(user_id: str, req: UpdateUserRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    admin = get_current_user(token)
    require_admin(admin)
    org_id = get_org_id(admin)
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ? AND org_id = ?", (user_id, org_id)).fetchone()
    if not user:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    if req.email is not None:
        dup = db.execute("SELECT id FROM users WHERE email = ? AND id != ?", (req.email, user_id)).fetchone()
        if dup:
            db.close()
            raise HTTPException(status_code=409, detail="Email already in use")
        db.execute("UPDATE users SET email = ? WHERE id = ?", (req.email, user_id))
    if req.name is not None:
        db.execute("UPDATE users SET name = ? WHERE id = ?", (req.name, user_id))
    if req.role is not None:
        if req.role not in ("admin", "viewer"):
            db.close()
            raise HTTPException(status_code=400, detail="Role must be 'admin' or 'viewer'")
        db.execute("UPDATE users SET role = ? WHERE id = ?", (req.role, user_id))
    if req.password is not None:
        pw_hash = hashlib.sha256(req.password.encode()).hexdigest()
        db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pw_hash, user_id))
    if req.company_ids is not None:
        db.execute("DELETE FROM user_company_access WHERE user_id = ?", (user_id,))
        for cid in req.company_ids:
            db.execute(
                "INSERT OR IGNORE INTO user_company_access (id, user_id, company_id) VALUES (?, ?, ?)",
                (str(uuid.uuid4()), user_id, cid),
            )
    db.commit()
    # Return updated user
    updated = db.execute("SELECT id, email, name, role, created_at FROM users WHERE id = ?", (user_id,)).fetchone()
    access = db.execute("SELECT company_id FROM user_company_access WHERE user_id = ?", (user_id,)).fetchall()
    db.close()
    result = dict(updated)
    result["company_ids"] = [a["company_id"] for a in access]
    return result

@app.delete("/api/users/{user_id}")
async def delete_user(user_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    admin = get_current_user(token)
    require_admin(admin)
    org_id = get_org_id(admin)
    if user_id == admin["id"]:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    db = get_db()
    target = db.execute("SELECT id FROM users WHERE id = ? AND org_id = ?", (user_id, org_id)).fetchone()
    if not target:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    db.execute("DELETE FROM user_company_access WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ? AND org_id = ?", (user_id, org_id))
    db.commit()
    db.close()
    return {"deleted": user_id}


@app.post("/api/users/{user_id}/generate-token")
async def generate_user_token(user_id: str, authorization: str = Header(None)):
    """Admin-only: mint a new API session token for any user in the org.

    Used for creating long-lived API tokens to share with integrations
    without needing the user's password.
    """
    token_in = _extract_token(authorization)
    admin = get_current_user(token_in)
    require_admin(admin)
    org_id = get_org_id(admin)
    db = get_db()
    target = db.execute(
        "SELECT id, email, name, role FROM users WHERE id = ? AND org_id = ?",
        (user_id, org_id),
    ).fetchone()
    if not target:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    new_token = str(uuid.uuid4())
    db.execute("INSERT INTO sessions (token, user_id) VALUES (?, ?)", (new_token, user_id))
    db.commit()
    db.close()
    return {
        "token": new_token,
        "user": {
            "id": target["id"],
            "email": target["email"],
            "name": target["name"],
            "role": target["role"],
        },
    }


@app.get("/api/users/{user_id}/sessions")
async def list_user_sessions(user_id: str, authorization: str = Header(None)):
    """Admin-only: list all active API session tokens for a user."""
    token_in = _extract_token(authorization)
    admin = get_current_user(token_in)
    require_admin(admin)
    org_id = get_org_id(admin)
    db = get_db()
    target = db.execute(
        "SELECT id FROM users WHERE id = ? AND org_id = ?", (user_id, org_id)
    ).fetchone()
    if not target:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    rows = db.execute(
        "SELECT token FROM sessions WHERE user_id = ?", (user_id,)
    ).fetchall()
    db.close()
    # Return masked tokens (first 8 + last 4 chars) for display; full token only visible on creation
    return {
        "sessions": [
            {
                "token_preview": f"{r['token'][:8]}...{r['token'][-4:]}",
                "token_full": r["token"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@app.delete("/api/users/{user_id}/sessions")
async def revoke_user_sessions(user_id: str, authorization: str = Header(None)):
    """Admin-only: revoke ALL active session tokens for a user."""
    token_in = _extract_token(authorization)
    admin = get_current_user(token_in)
    require_admin(admin)
    org_id = get_org_id(admin)
    db = get_db()
    target = db.execute(
        "SELECT id FROM users WHERE id = ? AND org_id = ?", (user_id, org_id)
    ).fetchone()
    if not target:
        db.close()
        raise HTTPException(status_code=404, detail="User not found")
    result = db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    revoked = result.rowcount
    db.commit()
    db.close()
    return {"revoked": revoked}


# =====================================================================
#  QBO OAuth 2.0 — Direct Integration
# =====================================================================

class AuthorizeRequest(BaseModel):
    frontend_origin: Optional[str] = None


@app.post("/api/qbo/authorize")
async def qbo_authorize(request: Request, body: AuthorizeRequest = None, authorization: str = Header(None)):
    """Generate a QBO OAuth authorization URL.

    The frontend opens this URL in a popup window. After the user signs in
    and selects a company, QBO redirects to our callback endpoint.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    # Use the fixed redirect URI if configured (recommended for production).
    # This MUST match exactly what is registered in the Intuit Developer portal.
    if QBO_REDIRECT_URI:
        redirect_uri = QBO_REDIRECT_URI
    elif body and body.frontend_origin:
        # Fallback: build from the frontend's origin
        base = body.frontend_origin.rstrip("/")
        redirect_uri = f"{base}/port/8000/api/qbo/callback"
    else:
        # Local dev fallback
        host = request.headers.get("x-forwarded-host") or request.headers.get("host", "localhost:8000")
        scheme = request.headers.get("x-forwarded-proto", "http")
        redirect_uri = f"{scheme}://{host}/api/qbo/callback"

    state = secrets.token_urlsafe(32)

    # Store state + redirect_uri + org_id for validation in the callback
    db = get_db()
    db.execute(
        "INSERT INTO oauth_states (state, redirect_uri, org_id) VALUES (?, ?, ?)",
        (state, redirect_uri, org_id),
    )
    # Clean up old states (older than 1 hour)
    db.execute("DELETE FROM oauth_states WHERE created_at < datetime('now', '-60 minutes')")
    db.commit()
    db.close()

    params = {
        "client_id": QBO_CLIENT_ID,
        "scope": QBO_SCOPES,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "state": state,
    }
    auth_url = f"{QBO_AUTH_URL}?{urlencode(params)}"

    return {"auth_url": auth_url, "redirect_uri": redirect_uri, "state": state}


@app.get("/api/qbo/callback")
async def qbo_callback(request: Request, code: str = None, state: str = None,
                         realmId: str = None, error: str = None):
    """OAuth callback from Intuit. Exchanges code for tokens, detects company,
    and returns an HTML page that closes the popup and notifies the parent."""

    if error:
        return HTMLResponse(content=f"""<html><body>
            <h2>Authorization Failed</h2><p>{error}</p>
            <script>
                if (window.opener) {{
                    window.opener.postMessage({{type:'qbo_auth_error', error:'{error}'}}, '*');
                    window.close();
                }}
            </script></body></html>""")

    if not code or not realmId:
        return HTMLResponse(content="""<html><body>
            <h2>Missing Parameters</h2><p>No authorization code received.</p>
            <script>
                if (window.opener) {
                    window.opener.postMessage({type:'qbo_auth_error', error:'Missing code or realmId'}, '*');
                    window.close();
                }
            </script></body></html>""")

    # Validate state and retrieve the stored redirect_uri
    db = get_db()
    state_row = db.execute(
        "SELECT state, redirect_uri, org_id FROM oauth_states WHERE state = ?", (state,)
    ).fetchone()
    if not state_row:
        # Log all existing states for debugging
        all_states = db.execute("SELECT state, created_at FROM oauth_states").fetchall()
        logger.error(
            "OAuth callback INVALID STATE | received_state=%s | stored_states=%d | states=%s",
            state, len(all_states),
            [(s["state"][:20] + "...", s["created_at"]) for s in all_states],
        )
        db.close()
        return HTMLResponse(content="""<html><body>
            <h2>Invalid State</h2><p>Security check failed. Please try again.</p>
            <script>
                if (window.opener) {
                    window.opener.postMessage({type:'qbo_auth_error', error:'Invalid state'}, '*');
                    window.close();
                }
            </script></body></html>""")

    # Use the redirect_uri stored when we created the authorize URL
    # This MUST match exactly what was sent to Intuit
    redirect_uri = state_row["redirect_uri"]
    oauth_org_id = state_row["org_id"]

    db.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
    db.commit()

    # Fallback if redirect_uri was not stored (shouldn't happen)
    if not redirect_uri:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host", "localhost:8000")
        scheme = request.headers.get("x-forwarded-proto", "http")
        forwarded_prefix = request.headers.get("x-forwarded-prefix", "")
        if forwarded_prefix:
            redirect_uri = f"{scheme}://{host}{forwarded_prefix}/api/qbo/callback"
        else:
            redirect_uri = f"{scheme}://{host}/api/qbo/callback"

    # Exchange code for tokens
    auth_header = base64.b64encode(
        f"{QBO_CLIENT_ID}:{QBO_CLIENT_SECRET}".encode()
    ).decode()

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            QBO_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            headers={
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )

    token_tid = token_resp.headers.get("intuit_tid", "N/A")

    if token_resp.status_code != 200:
        err_detail = token_resp.text[:300]
        logger.error(
            "OAuth token exchange FAILED | realm=%s | status=%d | intuit_tid=%s | body=%s",
            realmId, token_resp.status_code, token_tid, err_detail,
        )
        db.close()
        return HTMLResponse(content=f"""<html><body>
            <h2>Token Exchange Failed</h2><p>{err_detail}</p>
            <script>
                if (window.opener) {{
                    window.opener.postMessage({{type:'qbo_auth_error', error:'Token exchange failed'}}, '*');
                    window.close();
                }}
            </script></body></html>""")

    logger.info("OAuth token exchange OK | realm=%s | intuit_tid=%s", realmId, token_tid)
    tokens = token_resp.json()
    access_token = tokens["access_token"]
    refresh_token_val = tokens["refresh_token"]
    expires_in = tokens.get("expires_in", 3600)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    # Get company info using the new token
    company_url = f"{QBO_API_BASE}/{realmId}/companyinfo/{realmId}"
    async with httpx.AsyncClient() as client:
        info_resp = await client.get(
            company_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )

    info_tid = info_resp.headers.get("intuit_tid", "N/A")

    if info_resp.status_code != 200:
        logger.error(
            "OAuth companyinfo fetch FAILED | realm=%s | status=%d | intuit_tid=%s | body=%s",
            realmId, info_resp.status_code, info_tid, info_resp.text[:300],
        )
        db.close()
        return HTMLResponse(content=f"""<html><body>
            <h2>Connected but could not fetch company info</h2>
            <script>
                if (window.opener) {{
                    window.opener.postMessage({{type:'qbo_auth_error', error:'Could not fetch company info'}}, '*');
                    window.close();
                }}
            </script></body></html>""")

    logger.info("OAuth companyinfo OK | realm=%s | intuit_tid=%s", realmId, info_tid)
    company_data = info_resp.json()
    info = company_data.get("CompanyInfo", company_data)
    company_name = info.get("CompanyName", "Unknown")
    legal_name = info.get("LegalName", company_name)

    # Check if company already exists by realm_id (unique across all QBO subscriptions)
    existing = db.execute(
        "SELECT id FROM companies WHERE qbo_realm_id = ?", (realmId,)
    ).fetchone()

    if existing:
        cid = existing["id"]
    else:
        # Enforce company limit for new companies
        org = db.execute("SELECT * FROM organizations WHERE id = ?", (oauth_org_id,)).fetchone()
        if org:
            org_d = dict(org)
            effective = get_effective_plan(org_d)
            current_count = db.execute(
                "SELECT COUNT(*) as cnt FROM companies WHERE org_id = ?", (oauth_org_id,)
            ).fetchone()["cnt"]
            if current_count >= effective["max_companies"]:
                db.close()
                limit_msg = f"Your plan allows up to {effective['max_companies']} companies. Please upgrade to connect more."
                return HTMLResponse(content=f"""<html><body>
                    <h2>Company Limit Reached</h2><p>{limit_msg}</p>
                    <script>
                        if (window.opener) {{
                            window.opener.postMessage({{type:'qbo_auth_error', error:'{limit_msg}'}}, '*');
                            window.close();
                        }}
                    </script></body></html>""")
        cid = str(uuid.uuid4())

    # Save company with tokens
    _upsert_company_from_info(
        db, cid, info, "connected",
        realm_id=realmId,
        access_token=access_token,
        refresh_token=refresh_token_val,
        token_expires_at=expires_at,
        org_id=oauth_org_id,
    )
    db.close()

    # Return HTML that sends message to parent window and closes
    return HTMLResponse(content=f"""<!DOCTYPE html>
<html><head><title>QuickBooks Connected</title>
<style>
    body {{ font-family: -apple-system, sans-serif; display: flex; align-items: center;
           justify-content: center; min-height: 100vh; margin: 0; background: #f5f5f0; }}
    .card {{ background: white; border-radius: 12px; padding: 2rem; text-align: center;
             box-shadow: 0 2px 12px rgba(0,0,0,0.1); max-width: 400px; }}
    .check {{ color: #0f766e; font-size: 48px; }}
    h2 {{ color: #1a1a1a; margin: 0.5rem 0; }}
    p {{ color: #666; }}
</style></head>
<body><div class="card">
    <div class="check">&#10003;</div>
    <h2>Connected!</h2>
    <p><strong>{company_name}</strong></p>
    <p>{legal_name}</p>
    <p style="font-size:13px;color:#999;">This window will close automatically...</p>
</div>
<script>
    if (window.opener) {{
        window.opener.postMessage({{
            type: 'qbo_auth_success',
            company_id: '{cid}',
            company_name: '{company_name.replace(chr(39), "")}',
            legal_name: '{legal_name.replace(chr(39), "")}',
            realm_id: '{realmId}'
        }}, '*');
        setTimeout(() => window.close(), 2000);
    }}
</script></body></html>""")


# =====================================================================
#  COMPANIES — List, Sync, Delete
# =====================================================================

@app.get("/api/companies")
async def list_companies(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute(
        """SELECT id, name, legal_name, qbo_company_id, qbo_realm_id,
                  status, last_synced, created_at, address, phone, email,
                  industry, qbo_plan,
                  source, supabase_company_id, base_currency, fiscal_year_start, ein,
                  CASE WHEN access_token IS NOT NULL AND access_token != '' THEN 1 ELSE 0 END as has_token
           FROM companies WHERE org_id = ? ORDER BY name""",
        (org_id,),
    ).fetchall()
    db.close()
    companies = [dict(r) for r in rows]
    # Non-admin users only see companies they have access to
    if user["role"] != "admin":
        allowed = set(user.get("company_ids", []))
        companies = [c for c in companies if c["id"] in allowed]

    # Hydrate manual companies with their Plaid status (items + account count).
    # Best-effort: failures here should never break the list response.
    manual_sb_ids = [c["supabase_company_id"] for c in companies
                     if c.get("source") == "manual" and c.get("supabase_company_id")]
    if manual_sb_ids and _sb_configured():
        try:
            items = await _sb_select("plaid_items", {
                "company_id": f"in.({','.join(manual_sb_ids)})",
                "select": "id,company_id,institution_id,institution_name,status,last_synced_at",
            })
            accounts = await _sb_select("accounts", {
                "company_id": f"in.({','.join(manual_sb_ids)})",
                "select": "id,plaid_item_id,mask,name,type,current_balance",
            })
            items_by_company: dict = {}
            for it in items:
                items_by_company.setdefault(it["company_id"], []).append(it)
            accounts_by_item: dict = {}
            for a in accounts:
                if a.get("plaid_item_id"):
                    accounts_by_item.setdefault(a["plaid_item_id"], []).append(a)
            for c in companies:
                sbid = c.get("supabase_company_id")
                if not sbid:
                    continue
                its = items_by_company.get(sbid, [])
                hydrated = []
                for it in its:
                    accts = accounts_by_item.get(it["id"], [])
                    mask_preview = ""
                    if accts:
                        masks = [a.get("mask") for a in accts if a.get("mask")]
                        if masks:
                            mask_preview = masks[0]
                    hydrated.append({
                        "id": it["id"],
                        "institution_name": it.get("institution_name"),
                        "institution_id": it.get("institution_id"),
                        "status": it.get("status"),
                        "last_synced_at": it.get("last_synced_at"),
                        "accounts_count": len(accts),
                        "mask_preview": mask_preview,
                    })
                c["plaid_items"] = hydrated
        except Exception as e:
            logger.warning("Company list Plaid hydration failed: %s", str(e)[:200])

    return companies


@app.get("/api/companies/connected")
async def get_connected_company(authorization: str = Header(None)):
    """Get the first connected company with valid tokens for the header badge."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        """SELECT id, name, qbo_realm_id, access_token, refresh_token
           FROM companies WHERE status='connected' AND qbo_realm_id IS NOT NULL
           AND qbo_realm_id != '' AND org_id = ? LIMIT 1""",
        (org_id,),
    ).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=404, detail="No connected company")

    return {
        "company": {"CompanyName": row["name"]},
        "company_db_id": row["id"],
    }


@app.post("/api/companies/{company_id}/sync")
async def sync_company(company_id: str, authorization: str = Header(None)):
    """Pull ALL data from a specific company using its stored tokens."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()

    company = db.execute("SELECT * FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    if not company["qbo_realm_id"] or not company["refresh_token"]:
        db.close()
        raise HTTPException(status_code=400, detail="Company not authorized. Please connect via OAuth first.")

    db.execute("UPDATE companies SET status='syncing' WHERE id=?", (company_id,))
    db.commit()

    errors = []
    now = datetime.now()
    year = now.year

    # Pull reports
    report_tasks = [
        ("profit_loss", f"{year}-mtd", "ProfitAndLoss",
         {"date_macro": "This Month-to-date", "accounting_method": "Accrual"}),
        ("profit_loss", f"{year}-ytd", "ProfitAndLoss",
         {"start_date": f"{year}-01-01", "end_date": now.strftime("%Y-%m-%d"), "accounting_method": "Accrual"}),
        ("profit_loss", f"{year}-last-month", "ProfitAndLoss",
         {"date_macro": "Last Month", "accounting_method": "Accrual"}),
        ("profit_loss", f"{year - 1}-full", "ProfitAndLoss",
         {"start_date": f"{year - 1}-01-01", "end_date": f"{year - 1}-12-31", "accounting_method": "Accrual"}),
        ("balance_sheet", f"{year}-current", "BalanceSheet",
         {"date_macro": "Today"}),
        ("balance_sheet", f"{year}-ytd", "BalanceSheet",
         {"date_macro": "This Fiscal Year-to-date"}),
        ("cash_flow", f"{year}-mtd", "CashFlow",
         {"date_macro": "This Month-to-date"}),
        ("cash_flow", f"{year}-ytd", "CashFlow",
         {"start_date": f"{year}-01-01", "end_date": now.strftime("%Y-%m-%d")}),
    ]

    synced_count = 0
    for rtype, period, report_name, rparams in report_tasks:
        try:
            data = await qbo_get_report(db, company_id, report_name, rparams)
            rid = str(uuid.uuid4())
            db.execute(
                """INSERT OR REPLACE INTO company_reports
                   (id, company_id, report_type, period_key, params_json, data_json, cached_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                (rid, company_id, rtype, period, json.dumps(rparams), json.dumps(data)),
            )
            db.commit()
            synced_count += 1
        except Exception as e:
            errors.append(f"{rtype}/{period}: {str(e)[:120]}")

    # Pull Chart of Accounts
    accounts_count = 0
    try:
        acct_data = await qbo_query(db, company_id, "SELECT * FROM Account WHERE Active = true MAXRESULTS 1000")
        accounts = []
        if isinstance(acct_data, dict):
            qr = acct_data.get("QueryResponse", acct_data)
            accounts = qr.get("Account", [])
        _cache_accounts(db, company_id, accounts)
        accounts_count = len(accounts)
    except Exception as e:
        errors.append(f"accounts: {str(e)[:120]}")

    # Update company info from QBO
    try:
        info_data = await qbo_get_company_info(db, company_id)
        info = info_data.get("CompanyInfo", info_data)
        realm = company["qbo_realm_id"]
        _upsert_company_from_info(db, company_id, info, "connected", realm_id=realm)
    except Exception as e:
        errors.append(f"company_info: {str(e)[:120]}")

    db.execute(
        "UPDATE companies SET status='connected', last_synced=datetime('now') WHERE id=?",
        (company_id,),
    )
    db.commit()
    db.close()

    return {
        "company_id": company_id,
        "company_name": company["name"],
        "legal_name": company["legal_name"],
        "is_new_company": False,
        "reports_cached": synced_count,
        "accounts_cached": accounts_count,
        "errors": errors or None,
        "status": "synced",
    }


@app.delete("/api/companies/{company_id}")
async def delete_company(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute(
        "SELECT id, source, supabase_company_id FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    company_dict = dict(company)
    db.close()

    # For manual+Plaid companies, clean up Supabase + disconnect Plaid items.
    if company_dict.get("source") == "manual" and company_dict.get("supabase_company_id"):
        sb_id = company_dict["supabase_company_id"]
        # 1) Disconnect Plaid items (stops billing + removes access on their side)
        try:
            items = await _sb_select("plaid_items", {
                "company_id": f"eq.{sb_id}", "select": "id,plaid_item_id",
            })
            for it in items:
                try:
                    access_token = await _sb_rpc("plaid_access_token", {"p_item_id": it["id"]})
                    if access_token and isinstance(access_token, str):
                        await _plaid_post("/item/remove", {"access_token": access_token})
                except Exception as e:
                    logger.warning("Plaid item/remove failed for %s during company delete: %s",
                                   it.get("plaid_item_id"), str(e)[:200])
        except Exception as e:
            logger.warning("Could not list plaid_items during delete: %s", str(e)[:200])

        # 2) Delete Supabase tables in dependency order. Most have ON DELETE CASCADE
        #    from companies/plaid_items, so a single delete of the company row
        #    cascades everything. But we do an explicit tidy for journal_lines
        #    (linked to journal_entries but also reachable via CoA), then delete
        #    the company.
        for table in ["transactions", "rules", "journal_entries",
                      "categories", "chart_of_accounts", "accounts", "plaid_items"]:
            try:
                await _sb_delete(table, {"company_id": f"eq.{sb_id}"})
            except HTTPException as e:
                # Log, but don't block the company delete
                logger.warning("Supabase delete %s failed during company delete: %s",
                               table, e.detail)
        try:
            await _sb_delete("companies", {"id": f"eq.{sb_id}"})
        except HTTPException as e:
            logger.warning("Supabase delete companies failed during company delete: %s", e.detail)

    # Delete SQLite rows (regardless of source)
    db = get_db()
    try:
        db.execute("DELETE FROM company_reports WHERE company_id = ?", (company_id,))
        db.execute("DELETE FROM company_accounts WHERE company_id = ?", (company_id,))
        db.execute("DELETE FROM account_mappings WHERE company_id = ?", (company_id,))
        db.execute("DELETE FROM user_company_access WHERE company_id = ?", (company_id,))
        db.execute("DELETE FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id))
        db.commit()
    finally:
        db.close()
    return {"deleted": company_id}


# ---------- Per-account delete ----------

@app.delete("/api/accounts/{account_id}")
async def delete_account(account_id: str, authorization: str = Header(None)):
    """Delete a single Plaid-linked bank account from v2's Supabase. Removes
    the account row AND all of its transactions. Plaid's side is unaffected
    (the account still exists in Plaid for the parent item)."""
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("accounts", {
        "id": f"eq.{account_id}",
        "select": "id,company_id,name,mask",
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Account not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    # Delete transactions first (FK cascade should handle it, but be explicit)
    try:
        await _sb_delete("transactions", {"account_id": f"eq.{account_id}"})
    except HTTPException as e:
        logger.warning("Account txn delete failed: %s", e.detail)
    await _sb_delete("accounts", {"id": f"eq.{account_id}"})
    return {"ok": True}


@app.get("/api/companies/{company_id}/accounts")
async def get_company_accounts(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    rows = db.execute(
        """SELECT * FROM company_accounts WHERE company_id = ? AND active = 1
           ORDER BY classification, account_type, name""",
        (company_id,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# =====================================================================
#  CHART OF ACCOUNTS - CRUD
# =====================================================================

# Valid QBO AccountType values (enforced by QBO API)
_VALID_ACCOUNT_TYPES = {
    "Bank", "Other Current Asset", "Fixed Asset", "Other Asset",
    "Accounts Receivable", "Equity", "Expense", "Other Expense",
    "Cost of Goods Sold", "Accounts Payable", "Credit Card",
    "Long Term Liability", "Other Current Liability", "Income",
    "Other Income",
}


class CreateAccountRequest(BaseModel):
    name: str
    account_type: str                          # One of _VALID_ACCOUNT_TYPES
    account_sub_type: Optional[str] = None     # e.g. "Checking", "Rent"
    description: Optional[str] = None
    parent_name: Optional[str] = None          # Create as sub-account of this parent (by name)
    parent_qbo_id: Optional[str] = None        # Or by QBO ID directly
    currency: Optional[str] = None             # e.g. "USD"
    acct_num: Optional[str] = None             # Optional account number


class UpdateAccountRequest(BaseModel):
    name: Optional[str] = None
    account_sub_type: Optional[str] = None
    description: Optional[str] = None
    acct_num: Optional[str] = None


def _upsert_account_cache(db, company_id: str, qbo_account: dict):
    """Insert or update the local company_accounts cache from a QBO Account dict."""
    qbo_id = qbo_account.get("Id")
    if not qbo_id:
        return
    name = qbo_account.get("Name", "")
    fqn = qbo_account.get("FullyQualifiedName") or name
    acct_type = qbo_account.get("AccountType")
    sub_type = qbo_account.get("AccountSubType")
    classification = qbo_account.get("Classification")
    balance = float(qbo_account.get("CurrentBalance", 0) or 0)
    active = 1 if qbo_account.get("Active", True) else 0

    # Check if row exists
    existing = db.execute(
        "SELECT id FROM company_accounts WHERE company_id = ? AND qbo_account_id = ?",
        (company_id, qbo_id),
    ).fetchone()
    if existing:
        db.execute(
            """UPDATE company_accounts
               SET name = ?, fully_qualified_name = ?, account_type = ?,
                   account_sub_type = ?, classification = ?, current_balance = ?,
                   active = ?, cached_at = datetime('now')
               WHERE id = ?""",
            (name, fqn, acct_type, sub_type, classification, balance, active, existing["id"]),
        )
    else:
        db.execute(
            """INSERT INTO company_accounts
               (id, company_id, qbo_account_id, name, fully_qualified_name,
                account_type, account_sub_type, classification, current_balance, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid.uuid4()), company_id, qbo_id, name, fqn, acct_type,
             sub_type, classification, balance, active),
        )
    db.commit()


@app.post("/api/companies/{company_id}/accounts")
async def create_company_account(
    company_id: str,
    req: CreateAccountRequest,
    authorization: str = Header(None),
):
    """Create a new account in the company's QBO chart of accounts.

    Admin only. The new account is also added to the local cache so it can be
    referenced immediately by JE endpoints without needing a full sync.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    if req.account_type not in _VALID_ACCOUNT_TYPES:
        valid = ", ".join(sorted(_VALID_ACCOUNT_TYPES))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid account_type '{req.account_type}'. Must be one of: {valid}",
        )

    db = get_db()
    company = db.execute(
        "SELECT id, name FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    # Resolve parent account reference first (needed for duplicate check)
    parent_ref = None
    parent_fqn = None
    if req.parent_qbo_id:
        parent_ref = req.parent_qbo_id
        prow = db.execute(
            """SELECT fully_qualified_name FROM company_accounts
               WHERE company_id = ? AND qbo_account_id = ? AND active = 1 LIMIT 1""",
            (company_id, req.parent_qbo_id),
        ).fetchone()
        if prow:
            parent_fqn = prow["fully_qualified_name"]
    elif req.parent_name:
        prow = db.execute(
            """SELECT qbo_account_id, fully_qualified_name FROM company_accounts
               WHERE company_id = ? AND (fully_qualified_name = ? OR name = ?) AND active = 1
               LIMIT 1""",
            (company_id, req.parent_name, req.parent_name),
        ).fetchone()
        if not prow:
            db.close()
            raise HTTPException(
                status_code=400,
                detail=f"Parent account '{req.parent_name}' not found",
            )
        parent_ref = prow["qbo_account_id"]
        parent_fqn = prow["fully_qualified_name"]

    # Check for duplicate — scoped to the same parent.
    # Two accounts CAN share a short name if their parents differ
    # (e.g. Sales:Bread and Purchase:Bread are both valid in QBO).
    expected_fqn = f"{parent_fqn}:{req.name}" if parent_fqn else req.name
    existing = db.execute(
        """SELECT id, fully_qualified_name FROM company_accounts
           WHERE company_id = ? AND LOWER(fully_qualified_name) = LOWER(?) AND active = 1""",
        (company_id, expected_fqn),
    ).fetchone()
    if existing:
        db.close()
        raise HTTPException(
            status_code=409,
            detail=f"An account '{existing['fully_qualified_name']}' already exists in {company['name']}",
        )

    # Build QBO payload
    payload = {
        "Name": req.name,
        "AccountType": req.account_type,
    }
    if req.account_sub_type:
        payload["AccountSubType"] = req.account_sub_type
    if req.description:
        payload["Description"] = req.description
    if req.acct_num:
        payload["AcctNum"] = req.acct_num
    if req.currency:
        payload["CurrencyRef"] = {"value": req.currency}
    if parent_ref:
        payload["SubAccount"] = True
        payload["ParentRef"] = {"value": parent_ref}

    try:
        result = await qbo_api_call(
            db, company_id, "account?minorversion=65",
            method="POST", params=payload,
        )
        acct = result.get("Account", {})
        _upsert_account_cache(db, company_id, acct)
        db.close()
        return {
            "status": "created",
            "company_id": company_id,
            "qbo_account_id": acct.get("Id"),
            "name": acct.get("Name"),
            "fully_qualified_name": acct.get("FullyQualifiedName"),
            "account_type": acct.get("AccountType"),
            "account_sub_type": acct.get("AccountSubType"),
            "classification": acct.get("Classification"),
            "active": acct.get("Active", True),
        }
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=f"Failed to create account: {str(e)}")


async def _qbo_get_account(db, company_id: str, qbo_account_id: str) -> dict:
    """Fetch current state of a single account from QBO (needed for update sync token)."""
    result = await qbo_api_call(
        db, company_id, f"account/{qbo_account_id}?minorversion=65", method="GET",
    )
    return result.get("Account", {})


@app.patch("/api/companies/{company_id}/accounts/{qbo_account_id}")
async def update_company_account(
    company_id: str,
    qbo_account_id: str,
    req: UpdateAccountRequest,
    authorization: str = Header(None),
):
    """Update an existing account's name, description, sub_type, or account number.

    Admin only. Uses QBO sparse update so only provided fields are modified.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    company = db.execute(
        "SELECT id, name FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    # Need to fetch current SyncToken for the update
    try:
        current = await _qbo_get_account(db, company_id, qbo_account_id)
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=404, detail=f"Account not found in QBO: {he.detail}")

    if not current:
        db.close()
        raise HTTPException(status_code=404, detail="Account not found in QBO")

    # Build sparse update payload
    payload = {
        "Id": current["Id"],
        "SyncToken": current["SyncToken"],
        "sparse": True,
    }
    changed = False
    if req.name is not None:
        payload["Name"] = req.name
        changed = True
    if req.account_sub_type is not None:
        payload["AccountSubType"] = req.account_sub_type
        changed = True
    if req.description is not None:
        payload["Description"] = req.description
        changed = True
    if req.acct_num is not None:
        payload["AcctNum"] = req.acct_num
        changed = True

    if not changed:
        db.close()
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        result = await qbo_api_call(
            db, company_id, "account?minorversion=65",
            method="POST", params=payload,
        )
        acct = result.get("Account", {})
        _upsert_account_cache(db, company_id, acct)
        db.close()
        return {
            "status": "updated",
            "qbo_account_id": acct.get("Id"),
            "name": acct.get("Name"),
            "fully_qualified_name": acct.get("FullyQualifiedName"),
            "account_type": acct.get("AccountType"),
            "account_sub_type": acct.get("AccountSubType"),
            "active": acct.get("Active", True),
        }
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=f"Failed to update account: {str(e)}")


@app.delete("/api/companies/{company_id}/accounts/{qbo_account_id}")
async def deactivate_company_account(
    company_id: str,
    qbo_account_id: str,
    authorization: str = Header(None),
):
    """Deactivate (soft-delete) an account.

    QBO does not allow true deletion of accounts. This sets Active=false.
    The account will no longer appear in active account lists or JE dropdowns,
    but historical transactions remain intact. Admin only.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    company = db.execute(
        "SELECT id FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    try:
        current = await _qbo_get_account(db, company_id, qbo_account_id)
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=404, detail=f"Account not found in QBO: {he.detail}")

    if not current:
        db.close()
        raise HTTPException(status_code=404, detail="Account not found in QBO")

    if not current.get("Active", True):
        db.close()
        return {
            "status": "already_inactive",
            "qbo_account_id": qbo_account_id,
            "name": current.get("Name"),
        }

    payload = {
        "Id": current["Id"],
        "SyncToken": current["SyncToken"],
        "sparse": True,
        "Active": False,
        # QBO requires these even in sparse deactivation
        "Name": current.get("Name"),
        "AccountType": current.get("AccountType"),
    }

    try:
        result = await qbo_api_call(
            db, company_id, "account?minorversion=65",
            method="POST", params=payload,
        )
        acct = result.get("Account", {})
        _upsert_account_cache(db, company_id, acct)
        db.close()
        return {
            "status": "deactivated",
            "qbo_account_id": acct.get("Id"),
            "name": acct.get("Name"),
            "active": acct.get("Active", False),
        }
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=f"Failed to deactivate account: {str(e)}")


async def _fetch_all_qbo_entities(db, company_id: str, entity_name: str):
    """Fetch all pages of a QBO entity using STARTPOSITION/MAXRESULTS pagination.

    QBO caps each query response at 1000 rows. This paginates through them all.
    Returns the combined list of entity dicts.
    """
    all_items = []
    start = 1
    page_size = 1000
    max_pages = 50  # safety cap (50k entities)
    for _ in range(max_pages):
        query = (
            f"SELECT * FROM {entity_name} WHERE Active = true "
            f"STARTPOSITION {start} MAXRESULTS {page_size}"
        )
        result = await qbo_api_call(
            db, company_id, "query", method="GET",
            params={"query": query, "minorversion": "65"},
        )
        items = result.get("QueryResponse", {}).get(entity_name, [])
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break  # last page
        start += page_size
    return all_items


@app.get("/api/companies/{company_id}/customers")
async def get_company_customers(company_id: str, authorization: str = Header(None)):
    """Fetch all active customers from QBO for a specific company (paginated)."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        customers = await _fetch_all_qbo_entities(db, company_id, "Customer")
        return [
            {"id": c["Id"], "name": c.get("DisplayName", c.get("CompanyName", "")), "type": "Customer"}
            for c in customers
        ]
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"Error fetching customers: {str(ex)}")
    finally:
        db.close()


@app.get("/api/companies/{company_id}/vendors")
async def get_company_vendors(company_id: str, authorization: str = Header(None)):
    """Fetch all active vendors from QBO for a specific company (paginated)."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        vendors = await _fetch_all_qbo_entities(db, company_id, "Vendor")
        return [
            {"id": v["Id"], "name": v.get("DisplayName", v.get("CompanyName", "")), "type": "Vendor"}
            for v in vendors
        ]
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"Error fetching vendors: {str(ex)}")
    finally:
        db.close()


@app.get("/api/companies/{company_id}/reports")
async def get_company_report_list(company_id: str):
    db = get_db()
    rows = db.execute(
        """SELECT id, report_type, period_key, cached_at FROM company_reports
           WHERE company_id = ? ORDER BY report_type, cached_at DESC""",
        (company_id,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# =====================================================================
#  REPORTS — Live (per-company), Cached, or Consolidated
# =====================================================================

class ReportParams(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_macro: Optional[str] = None
    accounting_method: Optional[str] = "Accrual"
    compare_prior_year: Optional[bool] = False
    compare_prior_month: Optional[bool] = False
    company_id: Optional[str] = None  # specific company UUID | "all" for consolidated
    company_ids: Optional[list] = None  # list of company UUIDs for multi-select consolidated
    by_company: Optional[bool] = False  # return per-company breakdown alongside consolidated total
    summarize_column_by: Optional[str] = None  # "Month" | "Quarter" | "Year" | None (Total only)


async def _manual_company_by_id(company_id: str, org_id: str) -> Optional[dict]:
    db = get_db()
    try:
        row = db.execute(
            "SELECT * FROM companies WHERE id = ? AND org_id = ? AND source = 'manual'",
            (company_id, org_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


def _plaid_period(params) -> tuple:
    start = params.start_date or "1900-01-01"
    end = params.end_date or datetime.now().strftime("%Y-%m-%d")
    return start, end


async def _manual_companies_in_org(org_id: str, company_ids_filter: Optional[list] = None) -> list:
    """Return manual companies visible for org; optionally filter by a list of v2 ids."""
    db = get_db()
    try:
        rows = db.execute(
            "SELECT id, name, supabase_company_id FROM companies "
            "WHERE org_id = ? AND source = 'manual' AND supabase_company_id IS NOT NULL",
            (org_id,),
        ).fetchall()
        companies = [dict(r) for r in rows]
        if company_ids_filter:
            allowed = set(company_ids_filter)
            companies = [c for c in companies if c["id"] in allowed]
        return companies
    finally:
        db.close()


async def _collect_plaid_reports(
    org_id: str, report_kind: str, params, company_ids_filter: Optional[list] = None
) -> list:
    """Return a list of {company_id, name, report} for manual companies in the org."""
    manual = await _manual_companies_in_org(org_id, company_ids_filter)
    out = []
    start, end = _plaid_period(params)
    for c in manual:
        try:
            if report_kind == "profit_loss":
                rpt = await _plaid_pl(
                    c["supabase_company_id"], start, end,
                    summarize_column_by=getattr(params, "summarize_column_by", None),
                )
            elif report_kind == "balance_sheet":
                rpt = await _plaid_balance_sheet(c["supabase_company_id"], end)
            elif report_kind == "cash_flow":
                rpt = await _plaid_cash_flow(c["supabase_company_id"], start, end)
            else:
                continue
            out.append({"company_id": c["id"], "name": c["name"], "report": rpt})
        except Exception as e:
            logger.warning("Plaid report failed for %s: %s", c["name"], str(e)[:200])
    return out


@app.post("/api/reports/profit-loss")
async def get_profit_loss(params: ReportParams, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    # Single manual company → Plaid-sourced report
    if params.company_id and params.company_id != "all":
        mc = await _manual_company_by_id(params.company_id, org_id)
        if mc:
            start, end = _plaid_period(params)
            rpt = await _plaid_pl(
                mc["supabase_company_id"], start, end,
                summarize_column_by=params.summarize_column_by,
            )
            return {"current": rpt, "source": "plaid",
                    "companies": [{"name": mc["name"], "company_id": mc["id"]}]}

    if params.company_id == "all":
        try:
            result = await _get_live_consolidated(params, "ProfitAndLoss", "profit_loss", org_id)
        except Exception:
            result = None
        if not result or result.get("current") is None:
            result = _get_cached_report(params, "profit_loss", org_id)
        # Append Plaid-sourced reports for manual companies
        try:
            plaid_reports = await _collect_plaid_reports(
                org_id, "profit_loss", params, params.company_ids,
            )
            if plaid_reports:
                result = dict(result or {})
                result["plaid_reports"] = plaid_reports
        except Exception as e:
            logger.info("Plaid consolidated P&L skipped: %s", str(e)[:200])
        return result

    if params.company_id:
        return await _get_live_report_for_company(params, "ProfitAndLoss", "profit_loss")
    return {"current": None, "message": "Select a company"}


async def _get_live_consolidated(params, qbo_report_name, report_type, org_id=None):
    """Pull live reports from selected (or all) connected companies and merge them."""
    db = get_db()
    if org_id:
        companies = db.execute(
            "SELECT id, name, qbo_realm_id, refresh_token FROM companies WHERE status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != '' AND org_id = ?",
            (org_id,),
        ).fetchall()
    else:
        companies = db.execute(
            "SELECT id, name, qbo_realm_id, refresh_token FROM companies WHERE status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != ''"
        ).fetchall()
    db.close()

    # Filter by selected company_ids if provided
    if params.company_ids and len(params.company_ids) > 0:
        selected = set(params.company_ids)
        companies = [c for c in companies if c["id"] in selected]

    if not companies:
        return {"current": None, "consolidated": True, "companies": [], "message": "No connected companies. Connect and sync companies first."}

    reports = []
    prior_year_reports = []
    prior_month_reports = []
    company_names = []
    # Per-company data for by_company view
    per_company_reports = {}  # keyed by company name
    per_company_prior_reports = {}  # keyed by company name (prior year/month)
    for company in companies:
        try:
            result = await _get_live_report_for_company(
                ReportParams(
                    start_date=params.start_date,
                    end_date=params.end_date,
                    date_macro=params.date_macro,
                    accounting_method=params.accounting_method,
                    compare_prior_year=params.compare_prior_year,
                    compare_prior_month=params.compare_prior_month,
                    company_id=company["id"],
                ),
                qbo_report_name,
                report_type,
            )
            if result.get("current") and _has_report_data(result["current"]):
                reports.append(result["current"])
                company_names.append({"name": company["name"], "company_id": company["id"]})
                # Store per-company report for by_company view
                if params.by_company:
                    per_company_reports[company["name"]] = result["current"]
            # Don't filter comparison reports — empty prior period ($0) is valid data
            if result.get("prior_year"):
                prior_year_reports.append(result["prior_year"])
                if params.by_company:
                    per_company_prior_reports[company["name"]] = result["prior_year"]
            if result.get("prior_month"):
                prior_month_reports.append(result["prior_month"])
                if params.by_company and company["name"] not in per_company_prior_reports:
                    per_company_prior_reports[company["name"]] = result["prior_month"]
        except Exception:
            pass

    if not reports:
        return {"current": None, "consolidated": True, "companies": [], "message": "Could not pull live data from any company."}

    merged = _merge_reports(reports)
    out = {"current": merged, "consolidated": True, "companies": company_names}

    # Include per-company breakdown for by_company view
    if params.by_company and per_company_reports:
        # Build a flat lookup for each company: account_name -> value
        company_breakdowns = {}
        for cname, creport in per_company_reports.items():
            company_breakdowns[cname] = _build_flat_lookup(creport)
        out["company_breakdowns"] = company_breakdowns
        # Include per-company prior period breakdown for by_company comparison view
        if per_company_prior_reports:
            company_breakdowns_prior = {}
            for cname, creport in per_company_prior_reports.items():
                company_breakdowns_prior[cname] = _build_flat_lookup(creport)
            out["company_breakdowns_prior"] = company_breakdowns_prior

    if params.compare_prior_year:
        if prior_year_reports:
            m = _merge_reports(prior_year_reports) if len(prior_year_reports) > 1 else prior_year_reports[0]
            out["prior_year"] = m if m else _zero_report(merged)
        else:
            out["prior_year"] = _zero_report(merged)
    if params.compare_prior_month:
        if prior_month_reports:
            m = _merge_reports(prior_month_reports) if len(prior_month_reports) > 1 else prior_month_reports[0]
            out["prior_month"] = m if m else _zero_report(merged)
        else:
            out["prior_month"] = _zero_report(merged)
    return out


@app.post("/api/reports/balance-sheet")
async def get_balance_sheet(params: ReportParams, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    if params.company_id and params.company_id != "all":
        mc = await _manual_company_by_id(params.company_id, org_id)
        if mc:
            _, end = _plaid_period(params)
            rpt = await _plaid_balance_sheet(mc["supabase_company_id"], end)
            return {"current": rpt, "source": "plaid",
                    "companies": [{"name": mc["name"], "company_id": mc["id"]}]}

    if params.company_id == "all":
        try:
            result = await _get_live_consolidated(params, "BalanceSheet", "balance_sheet", org_id)
        except Exception:
            result = None
        if not result or result.get("current") is None:
            result = _get_cached_report(params, "balance_sheet", org_id)
        try:
            plaid_reports = await _collect_plaid_reports(
                org_id, "balance_sheet", params, params.company_ids,
            )
            if plaid_reports:
                result = dict(result or {})
                result["plaid_reports"] = plaid_reports
        except Exception as e:
            logger.info("Plaid consolidated BS skipped: %s", str(e)[:200])
        return result

    if params.company_id:
        return await _get_live_report_for_company(params, "BalanceSheet", "balance_sheet")
    return {"current": None, "message": "Select a company"}


@app.post("/api/reports/cash-flow")
async def get_cash_flow(params: ReportParams, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    if params.company_id and params.company_id != "all":
        mc = await _manual_company_by_id(params.company_id, org_id)
        if mc:
            start, end = _plaid_period(params)
            rpt = await _plaid_cash_flow(mc["supabase_company_id"], start, end)
            return {"current": rpt, "source": "plaid",
                    "companies": [{"name": mc["name"], "company_id": mc["id"]}]}

    if params.company_id == "all":
        try:
            result = await _get_live_consolidated(params, "CashFlow", "cash_flow", org_id)
        except Exception:
            result = None
        if not result or result.get("current") is None:
            result = _get_cached_report(params, "cash_flow", org_id)
        try:
            plaid_reports = await _collect_plaid_reports(
                org_id, "cash_flow", params, params.company_ids,
            )
            if plaid_reports:
                result = dict(result or {})
                result["plaid_reports"] = plaid_reports
        except Exception as e:
            logger.info("Plaid consolidated CF skipped: %s", str(e)[:200])
        return result

    if params.company_id:
        return await _get_live_report_for_company(params, "CashFlow", "cash_flow")
    return {"current": None, "message": "Select a company"}


class TransactionDetailParams(BaseModel):
    account_name: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_macro: Optional[str] = None
    accounting_method: Optional[str] = "Accrual"
    company_id: Optional[str] = None  # specific company UUID
    company_ids: Optional[list] = None  # for consolidated drill-down


@app.post("/api/reports/transaction-detail")
async def get_transaction_detail(params: TransactionDetailParams, authorization: str = Header(None)):
    """Drill down into a specific account — returns transaction-level detail from the QBO
    GeneralLedger report, across one or more companies."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()

    # Determine which companies to query
    if params.company_id == "all" or (not params.company_id):
        companies = db.execute(
            "SELECT id, name, qbo_realm_id, refresh_token FROM companies "
            "WHERE status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != '' AND org_id = ?",
            (org_id,),
        ).fetchall()
        if params.company_ids and len(params.company_ids) > 0:
            selected = set(params.company_ids)
            companies = [c for c in companies if c["id"] in selected]
    elif params.company_id:
        companies = db.execute(
            "SELECT id, name, qbo_realm_id, refresh_token FROM companies WHERE id=? AND org_id = ?",
            (params.company_id, org_id),
        ).fetchall()
    else:
        db.close()
        return {"transactions": [], "message": "Select a company"}

    all_transactions = []
    for company in companies:
        try:
            # Step 1: Look up account ID by name
            safe_name = params.account_name.replace("'", "\\'")
            acct_data = await qbo_query(db, company["id"],
                f"SELECT Id, Name FROM Account WHERE Name = '{safe_name}' AND Active = true MAXRESULTS 5")
            acct_list = acct_data.get("QueryResponse", {}).get("Account", [])
            if not acct_list:
                logger.info("Account '%s' not found in %s", params.account_name, company["name"])
                continue

            # Use first matching account's ID
            account_id = acct_list[0]["Id"]

            qbo_params = {
                "account": account_id,
                "columns": "tx_date,txn_type,doc_num,name,memo,account_name,subt_nat_amount,rbal_nat_amount,debt_amt,credit_amt",
                "sort_by": "tx_date",
                "sort_order": "ascend",
            }
            if params.accounting_method:
                qbo_params["accounting_method"] = params.accounting_method
            if params.date_macro:
                qbo_params["date_macro"] = params.date_macro
            if params.start_date:
                qbo_params["start_date"] = params.start_date
            if params.end_date:
                qbo_params["end_date"] = params.end_date

            report = await qbo_get_report(db, company["id"], "GeneralLedger", qbo_params)
            transactions = _parse_gl_transactions(report, company["name"], params.account_name)
            all_transactions.extend(transactions)
        except Exception as e:
            logger.warning("GL drill-down failed for %s: %s", company["name"], str(e)[:200])

    db.close()

    # Sort all transactions by date
    all_transactions.sort(key=lambda t: t.get("date", ""))

    return {
        "transactions": all_transactions,
        "account_name": params.account_name,
        "count": len(all_transactions),
    }


def _parse_gl_transactions(report: dict, company_name: str, filter_account: str = None) -> list:
    """Parse a QBO GeneralLedger report into flat transaction rows.
    The GL report is structured as sections by account. Each section has:
      - Header with account name
      - Data rows with individual transactions
      - Summary with totals
    If filter_account is provided, only rows from matching account sections are returned.
    """
    transactions = []
    if not report:
        return transactions

    # Get column definitions
    columns = []
    for col in report.get("Columns", {}).get("Column", []):
        col_type = col.get("ColTitle", "").strip()
        columns.append(col_type)

    filter_lower = filter_account.lower().strip() if filter_account else None

    def _extract_section_name(row):
        """Extract account name from a section header."""
        header = row.get("Header", {})
        cols = header.get("ColData", [])
        if cols:
            return cols[0].get("value", "").strip()
        return ""

    def _section_matches(section_name):
        if not filter_lower:
            return True
        return section_name.lower().strip() == filter_lower

    def walk_rows(rows_obj, in_matching_section=False):
        for row in rows_obj.get("Row", []):
            row_type = row.get("type", "")

            # Section rows contain Header + Rows + Summary
            if row_type == "Section":
                section_name = _extract_section_name(row)
                matches = _section_matches(section_name)
                # Recurse into this section's rows
                nested_rows = row.get("Rows", {})
                if nested_rows:
                    walk_rows(nested_rows, in_matching_section=matches)
            elif row.get("Header"):
                # Some sections don't have type=Section but do have Header
                section_name = _extract_section_name(row)
                matches = _section_matches(section_name)
                if row.get("Rows"):
                    walk_rows(row["Rows"], in_matching_section=matches)
            else:
                # Data row
                if in_matching_section and row.get("ColData"):
                    txn = {"company": company_name}
                    for i, cd in enumerate(row["ColData"]):
                        val = cd.get("value", "")
                        col_title = columns[i] if i < len(columns) else f"col_{i}"
                        txn[col_title] = val
                    transactions.append(txn)
                # Also recurse if nested
                if row.get("Rows"):
                    walk_rows(row["Rows"], in_matching_section=in_matching_section)

    rows = report.get("Rows", {})
    walk_rows(rows)
    return transactions


def _resolve_date_macro(date_macro: str, start_date: str = None, end_date: str = None):
    """Resolve a QBO date_macro to explicit start/end dates for comparison calculations.
    Returns (start_date, end_date) as YYYY-MM-DD strings."""
    if start_date and end_date:
        return start_date, end_date
    now = datetime.now()
    y, m, d = now.year, now.month, now.day
    macros = {
        "This Month": (f"{y}-{m:02d}-01", f"{y}-{m:02d}-{d:02d}"),
        "Last Month": (
            f"{y if m > 1 else y-1}-{(m-1 or 12):02d}-01",
            f"{y if m > 1 else y-1}-{(m-1 or 12):02d}-{calendar.monthrange(y if m > 1 else y-1, m-1 or 12)[1]:02d}",
        ),
        "This Month-to-date": (f"{y}-{m:02d}-01", f"{y}-{m:02d}-{d:02d}"),
        "This Fiscal Quarter": (
            f"{y}-{((m-1)//3)*3+1:02d}-01",
            f"{y}-{m:02d}-{d:02d}",
        ),
        "Last Fiscal Quarter": (
            f"{y if ((m-1)//3)*3+1 > 1 else y-1}-{(((m-1)//3)*3-2 if ((m-1)//3)*3-2 > 0 else ((m-1)//3)*3+10):02d}-01",
            f"{y}-{((m-1)//3)*3:02d}-{calendar.monthrange(y, ((m-1)//3)*3 or 12)[1]:02d}",
        ),
        "This Fiscal Quarter-to-date": (
            f"{y}-{((m-1)//3)*3+1:02d}-01",
            f"{y}-{m:02d}-{d:02d}",
        ),
        "This Fiscal Year": (f"{y}-01-01", f"{y}-{m:02d}-{d:02d}"),
        "Last Fiscal Year": (f"{y-1}-01-01", f"{y-1}-12-31"),
        "This Fiscal Year-to-date": (f"{y}-01-01", f"{y}-{m:02d}-{d:02d}"),
        "Today": (f"{y}-{m:02d}-{d:02d}", f"{y}-{m:02d}-{d:02d}"),
    }
    if date_macro and date_macro in macros:
        return macros[date_macro]
    # Fallback: current month
    return f"{y}-{m:02d}-01", f"{y}-{m:02d}-{d:02d}"


async def _get_live_report_for_company(params, qbo_report_name, report_type):
    """Pull a live report from a specific company using its own tokens."""
    db = get_db()

    # Check if company has tokens
    company = db.execute(
        "SELECT id, qbo_realm_id, refresh_token FROM companies WHERE id=?",
        (params.company_id,)
    ).fetchone()

    if not company or not company["refresh_token"]:
        # Fall back to cached
        db.close()
        return _get_cached_report(params, report_type)

    try:
        qbo_params = {}
        if params.accounting_method and qbo_report_name != "CashFlow":
            qbo_params["accounting_method"] = params.accounting_method
        if params.date_macro:
            qbo_params["date_macro"] = params.date_macro
        if params.start_date:
            qbo_params["start_date"] = params.start_date
        if params.end_date:
            qbo_params["end_date"] = params.end_date
        if params.summarize_column_by:
            qbo_params["summarize_column_by"] = params.summarize_column_by

        current = await qbo_get_report(db, params.company_id, qbo_report_name, qbo_params)
        result = {"current": current}

        # Resolve effective dates for comparison — use explicit dates or derive from date_macro
        eff_start = params.start_date
        eff_end = params.end_date
        if not eff_start or not eff_end:
            eff_start, eff_end = _resolve_date_macro(
                params.date_macro, params.start_date, params.end_date
            )

        if params.compare_prior_year and eff_start and eff_end:
            prior_params = dict(qbo_params)
            start = datetime.strptime(eff_start, "%Y-%m-%d")
            end = datetime.strptime(eff_end, "%Y-%m-%d")
            prior_params["start_date"] = start.replace(year=start.year - 1).strftime("%Y-%m-%d")
            prior_params["end_date"] = end.replace(year=end.year - 1).strftime("%Y-%m-%d")
            prior_params.pop("date_macro", None)
            try:
                result["prior_year"] = await qbo_get_report(
                    db, params.company_id, qbo_report_name, prior_params
                )
            except Exception:
                result["prior_year"] = None

        if params.compare_prior_month and eff_start and eff_end:
            prior_params = dict(qbo_params)
            start = datetime.strptime(eff_start, "%Y-%m-%d")
            end = datetime.strptime(eff_end, "%Y-%m-%d")
            # Calculate prior month start
            m = start.month - 1 or 12
            y = start.year if start.month > 1 else start.year - 1
            pm_start = start.replace(year=y, month=m, day=1)
            # Calculate prior month end (last day of prior month)
            last_day = calendar.monthrange(y, m)[1]
            pm_end = start.replace(year=y, month=m, day=min(end.day, last_day))
            prior_params["start_date"] = pm_start.strftime("%Y-%m-%d")
            prior_params["end_date"] = pm_end.strftime("%Y-%m-%d")
            prior_params.pop("date_macro", None)
            try:
                result["prior_month"] = await qbo_get_report(
                    db, params.company_id, qbo_report_name, prior_params
                )
            except Exception:
                result["prior_month"] = None

        db.close()
        return result

    except Exception as e:
        db.close()
        # Fall back to cached
        cached = _get_cached_report(params, report_type)
        cached["live_error"] = str(e)[:200]
        return cached


def _get_cached_report(params, report_type, org_id=None):
    """Return cached report(s) — single company or consolidated across all."""
    db = get_db()
    year = datetime.now().year

    macro_map = {
        "This Month-to-date": f"{year}-mtd",
        "This Month": f"{year}-mtd",
        "Last Month": f"{year}-last-month",
        "This Fiscal Year-to-date": f"{year}-ytd",
        "This Fiscal Year": f"{year}-ytd",
        "This Fiscal Quarter": f"{year}-qtd",
        "Last Fiscal Quarter": f"{year}-last-quarter",
        "This Fiscal Quarter-to-date": f"{year}-qtd",
        "Last Fiscal Year": f"{year - 1}-full",
        "Today": f"{year}-current",
    }
    period_key = macro_map.get(params.date_macro or "", f"{year}-ytd")
    # Build fallback chain: exact match → ytd → mtd → any available
    fallback_keys = [period_key]
    for fb in [f"{year}-ytd", f"{year}-mtd", f"{year}-last-month"]:
        if fb not in fallback_keys:
            fallback_keys.append(fb)

    def _find_rows_consolidated(rt, keys):
        for pk in keys:
            if org_id:
                rows = db.execute(
                    """SELECT cr.data_json, c.name AS company_name, c.id AS company_id, cr.period_key
                       FROM company_reports cr
                       JOIN companies c ON cr.company_id = c.id
                       WHERE cr.report_type = ? AND cr.period_key = ? AND c.org_id = ?""",
                    (rt, pk, org_id),
                ).fetchall()
            else:
                rows = db.execute(
                    """SELECT cr.data_json, c.name AS company_name, c.id AS company_id, cr.period_key
                       FROM company_reports cr
                       JOIN companies c ON cr.company_id = c.id
                       WHERE cr.report_type = ? AND cr.period_key = ?""",
                    (rt, pk),
                ).fetchall()
            if rows:
                return rows, pk
        return [], None

    def _find_row_single(cid, rt, keys):
        for pk in keys:
            row = db.execute(
                """SELECT data_json, cached_at, period_key FROM company_reports
                   WHERE company_id = ? AND report_type = ? AND period_key = ?
                   ORDER BY cached_at DESC LIMIT 1""",
                (cid, rt, pk),
            ).fetchone()
            if row:
                return row
        return None

    if params.company_id == "all":
        rows, matched_key = _find_rows_consolidated(report_type, fallback_keys)
        # Filter by company_ids if provided
        if params.company_ids and len(params.company_ids) > 0:
            selected = set(params.company_ids)
            rows = [r for r in rows if r["company_id"] in selected]
        db.close()

        if not rows:
            return {"current": None, "consolidated": True, "companies": [],
                    "message": "No cached data. Sync companies first."}

        reports = [json.loads(r["data_json"]) for r in rows]
        merged = _merge_reports(reports)
        return {
            "current": merged,
            "consolidated": True,
            "companies": [{"name": r["company_name"], "company_id": r["company_id"]} for r in rows],
        }
    else:
        row = _find_row_single(params.company_id, report_type, fallback_keys)
        db.close()
        if not row:
            return {"current": None, "message": "No cached data for this period. Sync the company first."}
        return {"current": json.loads(row["data_json"]), "cached_at": row["cached_at"]}


def _zero_report(report):
    """Create a zeroed-out copy of a report structure for comparison when prior period has no data."""
    if not report:
        return None
    zeroed = json.loads(json.dumps(report))

    def _zero_rows(rows):
        for row in rows:
            if row.get("ColData"):
                for i in range(1, len(row["ColData"])):
                    row["ColData"][i]["value"] = "0"
            if row.get("Summary", {}).get("ColData"):
                for i in range(1, len(row["Summary"]["ColData"])):
                    row["Summary"]["ColData"][i]["value"] = "0"
            if row.get("Header", {}).get("ColData"):
                pass  # Keep header labels
            if row.get("Rows", {}).get("Row"):
                _zero_rows(row["Rows"]["Row"])

    _zero_rows(zeroed.get("Rows", {}).get("Row", []))
    return zeroed


def _has_report_data(report):
    """Check if a QBO report actually contains numeric data (not an empty skeleton)."""
    if not report:
        return False
    header = report.get("Header", {})
    for opt in header.get("Option", []):
        if opt.get("Name") == "NoReportData" and str(opt.get("Value", "")).lower() == "true":
            return False
    # Also check if Columns has a Money/Amount column
    cols = report.get("Columns", {}).get("Column", [])
    if len(cols) < 2:
        return False
    return True


def _build_flat_lookup(report):
    """Build a flat dict mapping account names to their numeric values from a QBO report.
    Used for the by-company view to provide per-company breakdowns."""
    lookup = {}
    if not report:
        return lookup
    def walk(rows):
        for row in rows:
            if row.get("ColData"):
                name = row["ColData"][0].get("value", "")
                val = row["ColData"][1].get("value", "0") if len(row["ColData"]) > 1 else "0"
                if name:
                    try:
                        lookup[name] = float(val or "0")
                    except (ValueError, TypeError):
                        lookup[name] = 0.0
            if row.get("Summary", {}).get("ColData"):
                sc = row["Summary"]["ColData"]
                name = sc[0].get("value", "") if sc else ""
                val = sc[1].get("value", "0") if len(sc) > 1 else "0"
                if name:
                    try:
                        lookup[name] = float(val or "0")
                    except (ValueError, TypeError):
                        lookup[name] = 0.0
            if row.get("Rows", {}).get("Row"):
                walk(row["Rows"]["Row"])
    walk((report.get("Rows", {}) or {}).get("Row", []))
    return lookup


def _merge_reports(reports):
    # Filter out empty/skeleton reports that have no actual data
    reports = [r for r in reports if _has_report_data(r)]
    if not reports:
        return None
    if len(reports) == 1:
        return reports[0]
    base = json.loads(json.dumps(reports[0]))
    for additional in reports[1:]:
        _add_report_values(base, additional)
    if "Header" in base:
        base["Header"]["ReportName"] = base["Header"].get("ReportName", "Report") + " (Consolidated)"
    return base


def _add_report_values(base, addition):
    base_rows = base.get("Rows", {}).get("Row", [])
    add_rows = addition.get("Rows", {}).get("Row", [])

    add_lookup = {}
    for row in add_rows:
        key = row.get("group", "")
        if not key and row.get("Header", {}).get("ColData"):
            key = row["Header"]["ColData"][0].get("value", "")
        if not key and row.get("ColData"):
            key = row["ColData"][0].get("value", "")
        if key:
            add_lookup[key] = row

    for base_row in base_rows:
        key = base_row.get("group", "")
        if not key and base_row.get("Header", {}).get("ColData"):
            key = base_row["Header"]["ColData"][0].get("value", "")
        if not key and base_row.get("ColData"):
            key = base_row["ColData"][0].get("value", "")

        add_row = add_lookup.get(key)
        if not add_row:
            continue

        if base_row.get("ColData") and add_row.get("ColData"):
            for i in range(1, min(len(base_row["ColData"]), len(add_row["ColData"]))):
                try:
                    bv = float(base_row["ColData"][i].get("value", "0") or "0")
                    av = float(add_row["ColData"][i].get("value", "0") or "0")
                    base_row["ColData"][i]["value"] = str(round(bv + av, 2))
                except (ValueError, TypeError):
                    pass

        if base_row.get("Summary") and add_row.get("Summary"):
            bc = base_row["Summary"].get("ColData", [])
            ac = add_row["Summary"].get("ColData", [])
            for i in range(1, min(len(bc), len(ac))):
                try:
                    bv = float(bc[i].get("value", "0") or "0")
                    av = float(ac[i].get("value", "0") or "0")
                    bc[i]["value"] = str(round(bv + av, 2))
                except (ValueError, TypeError):
                    pass

        if base_row.get("Rows") and add_row.get("Rows"):
            _add_report_values(base_row, add_row)


# =====================================================================
#  ACCOUNTS — Cached
# =====================================================================

@app.get("/api/accounts/cached")
async def list_cached_accounts(company_id: str = None, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    if company_id:
        company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
        if not company:
            db.close()
            raise HTTPException(status_code=404, detail="Company not found")
        rows = db.execute(
            """SELECT ca.*, c.name AS company_name FROM company_accounts ca
               JOIN companies c ON ca.company_id = c.id
               WHERE ca.company_id = ? AND ca.active = 1
               ORDER BY ca.classification, ca.account_type, ca.name""",
            (company_id,),
        ).fetchall()
    else:
        rows = db.execute(
            """SELECT ca.*, c.name AS company_name FROM company_accounts ca
               JOIN companies c ON ca.company_id = c.id
               WHERE ca.active = 1 AND c.org_id = ?
               ORDER BY c.name, ca.classification, ca.account_type, ca.name""",
            (org_id,),
        ).fetchall()
    db.close()
    return [dict(r) for r in rows]


# =====================================================================
#  ACCOUNT MAPPINGS
# =====================================================================

class AccountMappingRequest(BaseModel):
    company_id: str
    qbo_account_id: str
    qbo_account_name: str
    consolidated_category: str
    consolidated_subcategory: Optional[str] = None

@app.get("/api/account-mappings")
async def list_account_mappings(company_id: str = None, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    if company_id:
        rows = db.execute(
            """SELECT am.*, c.name AS company_name FROM account_mappings am
               LEFT JOIN companies c ON am.company_id = c.id
               WHERE am.company_id = ? AND c.org_id = ?
               ORDER BY am.consolidated_category""",
            (company_id, org_id),
        ).fetchall()
    else:
        rows = db.execute(
            """SELECT am.*, c.name AS company_name FROM account_mappings am
               LEFT JOIN companies c ON am.company_id = c.id
               WHERE c.org_id = ?
               ORDER BY c.name, am.consolidated_category""",
            (org_id,),
        ).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/api/account-mappings")
async def create_account_mapping(req: AccountMappingRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (req.company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    mid = str(uuid.uuid4())
    db.execute(
        """INSERT INTO account_mappings
           (id, company_id, qbo_account_id, qbo_account_name, consolidated_category, consolidated_subcategory)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (mid, req.company_id, req.qbo_account_id, req.qbo_account_name,
         req.consolidated_category, req.consolidated_subcategory),
    )
    db.commit()
    db.close()
    return {"id": mid}

@app.delete("/api/account-mappings/{mapping_id}")
async def delete_account_mapping(mapping_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    mapping = db.execute(
        """SELECT am.id FROM account_mappings am
           JOIN companies c ON am.company_id = c.id
           WHERE am.id = ? AND c.org_id = ?""",
        (mapping_id, org_id),
    ).fetchone()
    if not mapping:
        db.close()
        raise HTTPException(status_code=404, detail="Mapping not found")
    db.execute("DELETE FROM account_mappings WHERE id = ?", (mapping_id,))
    db.commit()
    db.close()
    return {"deleted": mapping_id}


# =====================================================================
#  INTERCOMPANY JOURNAL ENTRIES
# =====================================================================

class ICEntryLine(BaseModel):
    side: str  # "source" or "dest"
    posting_type: str  # "Debit" or "Credit"
    account_name: str
    amount: float
    entity_id: Optional[str] = None
    description: Optional[str] = None


class ICEntryRequest(BaseModel):
    source_company_id: str
    dest_company_id: str
    entry_type: str
    description: str
    date: str
    lines: List[ICEntryLine]
    # Legacy single-line fields (kept for backward compat)
    amount: Optional[float] = None
    source_debit_account: Optional[str] = None
    source_credit_account: Optional[str] = None
    dest_debit_account: Optional[str] = None
    dest_credit_account: Optional[str] = None
    source_debit_entity_id: Optional[str] = None
    source_credit_entity_id: Optional[str] = None
    dest_debit_entity_id: Optional[str] = None
    dest_credit_entity_id: Optional[str] = None

@app.get("/api/intercompany")
async def list_ic_entries(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute(
        """SELECT ie.*,
           sc.name AS source_company_name, dc.name AS dest_company_name
           FROM intercompany_entries ie
           LEFT JOIN companies sc ON ie.source_company_id = sc.id
           LEFT JOIN companies dc ON ie.dest_company_id = dc.id
           WHERE ie.org_id = ?
           ORDER BY ie.created_at DESC""",
        (org_id,),
    ).fetchall()
    entries = []
    for r in rows:
        entry = dict(r)
        lines = db.execute(
            "SELECT * FROM ic_entry_lines WHERE entry_id = ? ORDER BY side, posting_type",
            (entry["id"],)
        ).fetchall()
        entry["lines"] = [dict(l) for l in lines]
        entries.append(entry)
    db.close()
    return entries

@app.post("/api/intercompany")
async def create_ic_entry(req: ICEntryRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    lines = req.lines

    # Validate debit/credit balance per side
    for side in ["source", "dest"]:
        side_lines = [l for l in lines if l.side == side]
        if not side_lines:
            continue
        total_debit = sum(l.amount for l in side_lines if l.posting_type == "Debit")
        total_credit = sum(l.amount for l in side_lines if l.posting_type == "Credit")
        if round(total_debit, 2) != round(total_credit, 2):
            db.close()
            raise HTTPException(
                status_code=400,
                detail=f"{side.capitalize()} side is unbalanced: Debits ${total_debit:.2f} != Credits ${total_credit:.2f}"
            )

    # Total amount = sum of debits on source side (for display)
    total_amount = sum(l.amount for l in lines if l.posting_type == "Debit" and l.side == "source")
    if total_amount == 0:
        total_amount = sum(l.amount for l in lines if l.posting_type == "Debit")

    entry_id = str(uuid.uuid4())
    db.execute(
        """INSERT INTO intercompany_entries
           (id, org_id, source_company_id, dest_company_id, entry_type, amount, description, date, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
        (entry_id, org_id, req.source_company_id, req.dest_company_id, req.entry_type,
         total_amount, req.description, req.date),
    )

    # Insert lines
    for line in lines:
        line_id = str(uuid.uuid4())
        db.execute(
            """INSERT INTO ic_entry_lines (id, entry_id, side, posting_type, account_name, amount, entity_id, description)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (line_id, entry_id, line.side, line.posting_type, line.account_name,
             line.amount, line.entity_id, line.description),
        )

    db.commit()
    db.close()
    return {"id": entry_id, "status": "pending"}

@app.post("/api/intercompany/{entry_id}/post")
async def post_ic_entry(entry_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ? AND org_id = ?", (entry_id, org_id)).fetchone()
    if not entry:
        db.close()
        raise HTTPException(status_code=404, detail="Entry not found")
    entry = dict(entry)

    # Fetch lines for this entry
    lines = db.execute(
        "SELECT * FROM ic_entry_lines WHERE entry_id = ? ORDER BY side, posting_type",
        (entry_id,)
    ).fetchall()
    lines = [dict(l) for l in lines]

    errors = []
    source_je_id = None
    dest_je_id = None

    # --- Helper: look up QBO account ID + type from cached account name ---
    def find_account_info(company_id, account_name):
        if not account_name:
            return None, None
        row = db.execute(
            """SELECT qbo_account_id, account_type FROM company_accounts
               WHERE company_id = ? AND (fully_qualified_name = ? OR name = ?) AND active = 1
               LIMIT 1""",
            (company_id, account_name, account_name)
        ).fetchone()
        if row:
            return row["qbo_account_id"], row["account_type"]
        return None, None

    # --- Helper: build a single QBO JE line ---
    def build_je_line(posting_type, amount, account_ref, account_type, entity_id, description):
        detail = {
            "PostingType": posting_type,
            "AccountRef": {"value": account_ref}
        }
        if account_type == "Accounts Receivable" and entity_id:
            detail["Entity"] = {"EntityRef": {"value": entity_id}, "Type": "Customer"}
        elif account_type == "Accounts Payable" and entity_id:
            detail["Entity"] = {"EntityRef": {"value": entity_id}, "Type": "Vendor"}
        return {
            "DetailType": "JournalEntryLineDetail",
            "Amount": round(abs(amount), 2),
            "Description": description or "",
            "JournalEntryLineDetail": detail
        }

    # --- Build and post JE per side ---
    for side, company_id_key in [("source", "source_company_id"), ("dest", "dest_company_id")]:
        side_lines = [l for l in lines if l["side"] == side]
        if not side_lines:
            # Fallback: legacy entries without lines table
            if side == "source" and entry.get("source_debit_account") and entry.get("source_credit_account"):
                side_lines = [
                    {"posting_type": "Debit", "account_name": entry["source_debit_account"],
                     "amount": entry["amount"], "entity_id": entry.get("source_debit_entity_id"), "description": entry["description"]},
                    {"posting_type": "Credit", "account_name": entry["source_credit_account"],
                     "amount": entry["amount"], "entity_id": entry.get("source_credit_entity_id"), "description": entry["description"]},
                ]
            elif side == "dest" and entry.get("dest_debit_account") and entry.get("dest_credit_account"):
                side_lines = [
                    {"posting_type": "Debit", "account_name": entry["dest_debit_account"],
                     "amount": entry["amount"], "entity_id": entry.get("dest_debit_entity_id"), "description": entry["description"]},
                    {"posting_type": "Credit", "account_name": entry["dest_credit_account"],
                     "amount": entry["amount"], "entity_id": entry.get("dest_credit_entity_id"), "description": entry["description"]},
                ]
            else:
                continue

        company_id = entry[company_id_key]
        je_lines = []
        missing_accounts = []

        for sl in side_lines:
            acct_id, acct_type = find_account_info(company_id, sl["account_name"])
            if not acct_id:
                missing_accounts.append(sl["account_name"])
                continue
            je_lines.append(build_je_line(
                sl["posting_type"], sl["amount"], acct_id, acct_type,
                sl.get("entity_id"), sl.get("description") or entry["description"]
            ))

        if missing_accounts:
            errors.append(f"{side.capitalize()}: account(s) not found: {', '.join(missing_accounts)}")
            continue

        if not je_lines:
            continue

        payload = {
            "TxnDate": entry["date"],
            "Line": je_lines,
            "PrivateNote": f"Intercompany: {entry['description'] or entry['entry_type']}"
        }

        try:
            result = await qbo_api_call(
                db, company_id,
                "journalentry?minorversion=65",
                method="POST", params=payload
            )
            je_id = result.get("JournalEntry", {}).get("Id")
            if side == "source":
                source_je_id = je_id
            else:
                dest_je_id = je_id
        except HTTPException as he:
            errors.append(f"{side.capitalize()} QBO error: {he.detail}")
        except Exception as ex:
            errors.append(f"{side.capitalize()} error: {str(ex)}")

    # Update status based on results
    if errors and not source_je_id and not dest_je_id:
        db.close()
        raise HTTPException(status_code=400, detail="Failed to post: " + "; ".join(errors))

    new_status = "posted" if not errors else "partial"
    db.execute(
        "UPDATE intercompany_entries SET status = ?, source_je_id = ?, dest_je_id = ? WHERE id = ?",
        (new_status, source_je_id, dest_je_id, entry_id)
    )
    db.commit()
    db.close()

    return {
        "id": entry_id,
        "status": new_status,
        "source_je_id": source_je_id,
        "dest_je_id": dest_je_id,
        "errors": errors if errors else None
    }


@app.put("/api/intercompany/{entry_id}")
async def update_ic_entry(entry_id: str, req: ICEntryRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ? AND org_id = ?", (entry_id, org_id)).fetchone()
    if not entry:
        db.close()
        raise HTTPException(status_code=404, detail="Entry not found")
    if entry["status"] == "posted":
        db.close()
        raise HTTPException(status_code=400, detail="Cannot edit a posted entry")

    lines = req.lines

    # Validate debit/credit balance per side
    for side in ["source", "dest"]:
        side_lines = [l for l in lines if l.side == side]
        if not side_lines:
            continue
        total_debit = sum(l.amount for l in side_lines if l.posting_type == "Debit")
        total_credit = sum(l.amount for l in side_lines if l.posting_type == "Credit")
        if round(total_debit, 2) != round(total_credit, 2):
            db.close()
            raise HTTPException(
                status_code=400,
                detail=f"{side.capitalize()} side is unbalanced: Debits ${total_debit:.2f} != Credits ${total_credit:.2f}"
            )

    total_amount = sum(l.amount for l in lines if l.posting_type == "Debit" and l.side == "source")
    if total_amount == 0:
        total_amount = sum(l.amount for l in lines if l.posting_type == "Debit")

    # Update header
    db.execute(
        """UPDATE intercompany_entries
           SET source_company_id=?, dest_company_id=?, entry_type=?, amount=?,
               description=?, date=?
           WHERE id=?""",
        (req.source_company_id, req.dest_company_id, req.entry_type,
         total_amount, req.description, req.date, entry_id),
    )

    # Replace lines
    db.execute("DELETE FROM ic_entry_lines WHERE entry_id = ?", (entry_id,))
    for line in lines:
        line_id = str(uuid.uuid4())
        db.execute(
            """INSERT INTO ic_entry_lines (id, entry_id, side, posting_type, account_name, amount, entity_id, description)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (line_id, entry_id, line.side, line.posting_type, line.account_name,
             line.amount, line.entity_id, line.description),
        )

    db.commit()
    db.close()
    return {"id": entry_id, "status": "pending"}


@app.delete("/api/intercompany/{entry_id}")
async def delete_ic_entry(entry_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ? AND org_id = ?", (entry_id, org_id)).fetchone()
    if not entry:
        db.close()
        raise HTTPException(status_code=404, detail="Entry not found")
    db.execute("DELETE FROM ic_entry_lines WHERE entry_id = ?", (entry_id,))
    db.execute("DELETE FROM intercompany_entries WHERE id = ?", (entry_id,))
    db.commit()
    db.close()
    return {"id": entry_id, "deleted": True}


# =====================================================================
#  SINGLE-COMPANY JOURNAL ENTRIES (REST)
# =====================================================================

class JournalEntryLine(BaseModel):
    posting_type: str  # "Debit" or "Credit"
    account_name: str  # must match an active account (name or fully_qualified_name)
    amount: float
    entity_id: Optional[str] = None  # Customer/Vendor ID (required for A/R, A/P)
    entity_type: Optional[str] = None  # "Customer" or "Vendor" (auto-detected if omitted)
    description: Optional[str] = None
    class_id: Optional[str] = None  # Optional QBO class reference


class JournalEntryRequest(BaseModel):
    date: str  # YYYY-MM-DD
    lines: List[JournalEntryLine]
    doc_number: Optional[str] = None  # optional journal number
    private_note: Optional[str] = None  # memo / private note
    currency: Optional[str] = None  # e.g. "USD"


class BulkJournalEntryRequest(BaseModel):
    entries: List[JournalEntryRequest]
    stop_on_error: Optional[bool] = False  # if True, abort remaining entries after first failure


# --- Shared helpers for single + bulk JE creation ---

def _find_account_in_company(db, company_id: str, account_name: str):
    """Look up an account by name or fully_qualified_name. Returns (qbo_id, type) or (None, None)."""
    if not account_name:
        return None, None
    row = db.execute(
        """SELECT qbo_account_id, account_type FROM company_accounts
           WHERE company_id = ? AND (fully_qualified_name = ? OR name = ?) AND active = 1
           LIMIT 1""",
        (company_id, account_name, account_name),
    ).fetchone()
    if row:
        return row["qbo_account_id"], row["account_type"]
    return None, None


def _build_je_payload(db, company_id: str, company_name: str, req: "JournalEntryRequest"):
    """Validate a single JE request and build the QBO payload.

    Returns (payload, total_debits, line_count) on success.
    Raises HTTPException on validation failure.
    """
    if not req.lines or len(req.lines) < 2:
        raise HTTPException(status_code=400, detail="At least 2 lines required")

    total_debits = round(sum(abs(l.amount) for l in req.lines if l.posting_type.lower() == "debit"), 2)
    total_credits = round(sum(abs(l.amount) for l in req.lines if l.posting_type.lower() == "credit"), 2)
    if abs(total_debits - total_credits) > 0.01:
        raise HTTPException(
            status_code=400,
            detail=f"Journal entry does not balance: debits={total_debits} credits={total_credits}",
        )

    je_lines = []
    missing_accounts = []
    for line in req.lines:
        posting = line.posting_type.strip().capitalize()
        if posting not in ("Debit", "Credit"):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid posting_type '{line.posting_type}' (must be 'Debit' or 'Credit')",
            )
        acct_id, acct_type = _find_account_in_company(db, company_id, line.account_name)
        if not acct_id:
            missing_accounts.append(line.account_name)
            continue

        detail = {"PostingType": posting, "AccountRef": {"value": acct_id}}

        # Auto-attach entity for A/R and A/P
        if line.entity_id:
            ent_type = line.entity_type
            if not ent_type:
                if acct_type == "Accounts Receivable":
                    ent_type = "Customer"
                elif acct_type == "Accounts Payable":
                    ent_type = "Vendor"
            if ent_type:
                detail["Entity"] = {"EntityRef": {"value": line.entity_id}, "Type": ent_type}

        if line.class_id:
            detail["ClassRef"] = {"value": line.class_id}

        je_lines.append({
            "DetailType": "JournalEntryLineDetail",
            "Amount": round(abs(line.amount), 2),
            "Description": line.description or "",
            "JournalEntryLineDetail": detail,
        })

    if missing_accounts:
        raise HTTPException(
            status_code=400,
            detail=f"Account(s) not found in {company_name}: {', '.join(missing_accounts)}. Sync the company to refresh the chart of accounts.",
        )

    payload = {"TxnDate": req.date, "Line": je_lines}
    if req.doc_number:
        payload["DocNumber"] = req.doc_number
    if req.private_note:
        payload["PrivateNote"] = req.private_note
    if req.currency:
        payload["CurrencyRef"] = {"value": req.currency}

    return payload, total_debits, len(je_lines)


async def _post_je_to_qbo(db, company_id: str, payload: dict):
    """POST a built payload to QBO. Returns the JournalEntry dict. Raises HTTPException on failure."""
    result = await qbo_api_call(
        db, company_id, "journalentry?minorversion=65",
        method="POST", params=payload,
    )
    return result.get("JournalEntry", {})


@app.post("/api/companies/{company_id}/journal-entries")
async def create_journal_entry(
    company_id: str,
    req: JournalEntryRequest,
    authorization: str = Header(None),
):
    """Create a single journal entry directly in a company's QBO book.

    Admin access required. The lines must balance (sum of debits == sum of credits).
    Accounts are looked up by name in the cached chart of accounts — run a
    company sync first if newly added accounts are missing.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    company = db.execute(
        "SELECT id, name FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    try:
        payload, total_debits, line_count = _build_je_payload(db, company_id, company["name"], req)
    except HTTPException:
        db.close()
        raise

    try:
        je = await _post_je_to_qbo(db, company_id, payload)
        db.close()
        return {
            "status": "posted",
            "company_id": company_id,
            "company_name": company["name"],
            "journal_entry_id": je.get("Id"),
            "doc_number": je.get("DocNumber"),
            "date": je.get("TxnDate"),
            "total": total_debits,
            "line_count": line_count,
        }
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=f"Failed to post journal entry: {str(e)}")


@app.post("/api/companies/{company_id}/journal-entries/bulk")
async def create_journal_entries_bulk(
    company_id: str,
    req: BulkJournalEntryRequest,
    authorization: str = Header(None),
):
    """Create multiple journal entries in a single request.

    Each entry is validated and posted to QBO independently. By default, failures
    on one entry do NOT stop the batch — all entries are attempted and per-entry
    results returned. Set `stop_on_error: true` to abort on the first failure.

    Max batch size: 100 entries.

    Response includes a summary and a per-entry result array with:
    - `index` — position in the request array
    - `status` — "posted", "validation_error", or "qbo_error"
    - `journal_entry_id` — QBO ID if posted
    - `doc_number`, `date`, `total`, `line_count` — entry details if posted
    - `error` — error message if failed
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    if not req.entries:
        raise HTTPException(status_code=400, detail="No entries provided")
    if len(req.entries) > 100:
        raise HTTPException(status_code=400, detail=f"Batch size {len(req.entries)} exceeds max of 100")

    db = get_db()
    company = db.execute(
        "SELECT id, name FROM companies WHERE id = ? AND org_id = ?",
        (company_id, org_id),
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    results = []
    posted_count = 0
    failed_count = 0
    aborted_count = 0

    for idx, entry_req in enumerate(req.entries):
        # Phase 1: validate + build payload
        try:
            payload, total_debits, line_count = _build_je_payload(
                db, company_id, company["name"], entry_req
            )
        except HTTPException as he:
            results.append({
                "index": idx,
                "status": "validation_error",
                "error": he.detail,
                "doc_number": entry_req.doc_number,
                "date": entry_req.date,
            })
            failed_count += 1
            if req.stop_on_error:
                # Mark remaining entries as aborted
                for ridx in range(idx + 1, len(req.entries)):
                    rentry = req.entries[ridx]
                    results.append({
                        "index": ridx,
                        "status": "aborted",
                        "error": "Batch stopped on prior error",
                        "doc_number": rentry.doc_number,
                        "date": rentry.date,
                    })
                    aborted_count += 1
                break
            continue

        # Phase 2: post to QBO
        try:
            je = await _post_je_to_qbo(db, company_id, payload)
            results.append({
                "index": idx,
                "status": "posted",
                "journal_entry_id": je.get("Id"),
                "doc_number": je.get("DocNumber") or entry_req.doc_number,
                "date": je.get("TxnDate") or entry_req.date,
                "total": total_debits,
                "line_count": line_count,
            })
            posted_count += 1
        except HTTPException as he:
            results.append({
                "index": idx,
                "status": "qbo_error",
                "error": str(he.detail),
                "doc_number": entry_req.doc_number,
                "date": entry_req.date,
            })
            failed_count += 1
            if req.stop_on_error:
                for ridx in range(idx + 1, len(req.entries)):
                    rentry = req.entries[ridx]
                    results.append({
                        "index": ridx,
                        "status": "aborted",
                        "error": "Batch stopped on prior error",
                        "doc_number": rentry.doc_number,
                        "date": rentry.date,
                    })
                    aborted_count += 1
                break
        except Exception as e:
            results.append({
                "index": idx,
                "status": "qbo_error",
                "error": f"Unexpected error: {str(e)}",
                "doc_number": entry_req.doc_number,
                "date": entry_req.date,
            })
            failed_count += 1
            if req.stop_on_error:
                for ridx in range(idx + 1, len(req.entries)):
                    rentry = req.entries[ridx]
                    results.append({
                        "index": ridx,
                        "status": "aborted",
                        "error": "Batch stopped on prior error",
                        "doc_number": rentry.doc_number,
                        "date": rentry.date,
                    })
                    aborted_count += 1
                break

    db.close()

    if posted_count == len(req.entries):
        overall = "success"
    elif posted_count == 0:
        overall = "failed"
    else:
        overall = "partial"

    return {
        "status": overall,
        "company_id": company_id,
        "company_name": company["name"],
        "total": len(req.entries),
        "posted": posted_count,
        "failed": failed_count,
        "aborted": aborted_count,
        "results": results,
    }


@app.get("/api/companies/{company_id}/journal-entries/{je_id}")
async def get_journal_entry(
    company_id: str,
    je_id: str,
    authorization: str = Header(None),
):
    """Fetch a journal entry by its QBO ID for verification/display."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute(
        "SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)
    ).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        result = await qbo_api_call(
            db, company_id, f"journalentry/{je_id}?minorversion=65", method="GET",
        )
        db.close()
        return result.get("JournalEntry", result)
    except HTTPException as he:
        db.close()
        raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")


# =====================================================================
#  IC TEMPLATES
# =====================================================================

class ICTemplateRequest(BaseModel):
    name: str
    source_company_id: Optional[str] = None
    dest_company_id: Optional[str] = None
    entry_type: Optional[str] = None
    source_debit_account: Optional[str] = None
    source_credit_account: Optional[str] = None
    dest_debit_account: Optional[str] = None
    dest_credit_account: Optional[str] = None
    description: Optional[str] = None

@app.get("/api/intercompany/templates")
async def list_ic_templates(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute("SELECT * FROM ic_templates WHERE org_id = ? ORDER BY name", (org_id,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/api/intercompany/templates")
async def create_ic_template(req: ICTemplateRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    tid = str(uuid.uuid4())
    db.execute(
        """INSERT INTO ic_templates
           (id, org_id, name, source_company_id, dest_company_id, entry_type,
            source_debit_account, source_credit_account, dest_debit_account, dest_credit_account, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (tid, org_id, req.name, req.source_company_id, req.dest_company_id, req.entry_type,
         req.source_debit_account, req.source_credit_account,
         req.dest_debit_account, req.dest_credit_account, req.description),
    )
    db.commit()
    db.close()
    return {"id": tid}


@app.put("/api/intercompany/templates/{template_id}")
async def update_ic_template(template_id: str, req: ICTemplateRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    existing = db.execute("SELECT id FROM ic_templates WHERE id = ? AND org_id = ?", (template_id, org_id)).fetchone()
    if not existing:
        db.close()
        raise HTTPException(status_code=404, detail="Template not found")
    db.execute(
        """UPDATE ic_templates SET name=?, source_company_id=?, dest_company_id=?, entry_type=?,
           source_debit_account=?, source_credit_account=?, dest_debit_account=?, dest_credit_account=?, description=?
           WHERE id=? AND org_id=?""",
        (req.name, req.source_company_id, req.dest_company_id, req.entry_type,
         req.source_debit_account, req.source_credit_account,
         req.dest_debit_account, req.dest_credit_account, req.description, template_id, org_id),
    )
    db.commit()
    db.close()
    return {"ok": True}


@app.delete("/api/intercompany/templates/{template_id}")
async def delete_ic_template(template_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    existing = db.execute("SELECT id FROM ic_templates WHERE id = ? AND org_id = ?", (template_id, org_id)).fetchone()
    if not existing:
        db.close()
        raise HTTPException(status_code=404, detail="Template not found")
    db.execute("DELETE FROM ic_templates WHERE id = ? AND org_id = ?", (template_id, org_id))
    db.commit()
    db.close()
    return {"ok": True}


# =====================================================================
#  DASHBOARD SUMMARY
# =====================================================================

@app.get("/api/dashboard/summary")
async def dashboard_summary(
    period: str = "last_month",
    start_date: str = None,
    end_date: str = None,
    company_ids: str = None,
    authorization: str = Header(None),
):
    """KPI data for home page. Pulls live data from connected companies.

    period: last_month (default), ytd_last_month, custom
    start_date/end_date: required when period=custom (YYYY-MM-DD)
    company_ids: comma-separated company UUIDs to filter (omit for all)
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company_count = db.execute("SELECT COUNT(*) FROM companies WHERE org_id = ?", (org_id,)).fetchone()[0]
    connected = db.execute(
        "SELECT id, name FROM companies WHERE status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != '' AND org_id = ?",
        (org_id,),
    ).fetchall()
    db.close()

    # Filter by selected companies
    if company_ids:
        selected = set(company_ids.split(","))
        connected = [c for c in connected if c["id"] in selected]

    now = datetime.now()
    y, m = now.year, now.month

    # Resolve date ranges based on period selection
    if period == "ytd_last_month":
        # Jan 1 through end of last month
        lm = m - 1 or 12
        ly = y if m > 1 else y - 1
        last_day = calendar.monthrange(ly, lm)[1]
        main_start = f"{ly if lm == 12 and m == 1 else y}-01-01"
        main_end = f"{ly}-{lm:02d}-{last_day:02d}"
        # Prior = same range, year before
        prior_start = f"{int(main_start[:4])-1}{main_start[4:]}"
        prior_end = f"{ly-1}-{lm:02d}-{last_day:02d}"
        period_label = f"YTD through {calendar.month_abbr[lm]} {ly}"
    elif period == "custom" and start_date and end_date:
        main_start = start_date
        main_end = end_date
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        prior_start = s.replace(year=s.year - 1).strftime("%Y-%m-%d")
        prior_end = e.replace(year=e.year - 1).strftime("%Y-%m-%d")
        period_label = f"{start_date} to {end_date}"
    else:
        # Default: last month
        lm = m - 1 or 12
        ly = y if m > 1 else y - 1
        last_day = calendar.monthrange(ly, lm)[1]
        main_start = f"{ly}-{lm:02d}-01"
        main_end = f"{ly}-{lm:02d}-{last_day:02d}"
        prior_start = f"{ly-1}-{lm:02d}-01"
        prior_end = f"{ly-1}-{lm:02d}-{last_day:02d}"
        period_label = f"{calendar.month_name[lm]} {ly}"

    live_data = {
        "company_count": company_count,
        "period_label": period_label,
        "period": period,
        "start_date": main_start,
        "end_date": main_end,
    }

    # Pull live P&L for main period + prior period
    main_reports = []
    prior_reports = []
    bs_reports = []
    for company in connected:
        try:
            rp = await qbo_get_report(get_db(), company["id"], "ProfitAndLoss", {
                "start_date": main_start, "end_date": main_end, "accounting_method": "Accrual",
            })
            if _has_report_data(rp):
                main_reports.append(rp)
        except Exception:
            pass
        try:
            rp2 = await qbo_get_report(get_db(), company["id"], "ProfitAndLoss", {
                "start_date": prior_start, "end_date": prior_end, "accounting_method": "Accrual",
            })
            if _has_report_data(rp2):
                prior_reports.append(rp2)
        except Exception:
            pass
        try:
            bs = await qbo_get_report(get_db(), company["id"], "BalanceSheet", {
                "start_date": main_start, "end_date": main_end,
            })
            if _has_report_data(bs):
                bs_reports.append(bs)
        except Exception:
            pass

    live_data["current_pl"] = _merge_reports(main_reports) if main_reports else None
    live_data["prior_pl"] = _merge_reports(prior_reports) if prior_reports else None
    live_data["balance_sheet"] = _merge_reports(bs_reports) if bs_reports else None

    return live_data


@app.get("/api/dashboard/revenue-trend")
async def revenue_trend(
    months: int = 12,
    company_ids: str = None,
    authorization: str = Header(None),
):
    """Return monthly revenue + expenses for the trailing N months.
    Used by the dashboard revenue trend chart."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    connected = db.execute(
        "SELECT id, name FROM companies WHERE status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != '' AND org_id = ?",
        (org_id,),
    ).fetchall()
    db.close()

    if company_ids:
        selected = set(company_ids.split(","))
        connected = [c for c in connected if c["id"] in selected]

    now = datetime.now()
    results = []

    # Last complete month as reference point
    lm_month = (now.month - 1) or 12
    lm_year = now.year if now.month > 1 else now.year - 1

    for i in range(months - 1, -1, -1):
        # Go back i months from last complete month
        total_months_back = lm_year * 12 + lm_month - i
        y = (total_months_back - 1) // 12
        m = ((total_months_back - 1) % 12) + 1

        last_day = calendar.monthrange(y, m)[1]
        start = f"{y}-{m:02d}-01"
        end = f"{y}-{m:02d}-{last_day:02d}"

        month_reports = []
        for company in connected:
            try:
                rp = await qbo_get_report(get_db(), company["id"], "ProfitAndLoss", {
                    "start_date": start, "end_date": end, "accounting_method": "Accrual",
                })
                if _has_report_data(rp):
                    month_reports.append(rp)
            except Exception:
                pass

        revenue = 0
        expenses = 0
        net_income = 0
        if month_reports:
            merged = _merge_reports(month_reports)
            for sec in (merged.get("Rows") or {}).get("Row", []):
                grp = sec.get("group", "")
                try:
                    val = float(sec.get("Summary", {}).get("ColData", [{}])[1].get("value", 0))
                except (IndexError, ValueError, TypeError):
                    val = 0
                if grp == "Income":
                    revenue = val
                elif grp in ("Expenses", "COGS", "OtherExpenses"):
                    # Total expenses = Operating Expenses + COGS + Other Expenses
                    expenses += abs(val)
                elif grp == "NetIncome":
                    net_income = val

        results.append({
            "month": calendar.month_abbr[m],
            "year": y,
            "label": f"{calendar.month_abbr[m]} {y}",
            "revenue": round(revenue, 2),
            "expenses": round(expenses, 2),
            "net_income": round(net_income, 2),
        })

    return {"months": results}


# =====================================================================
#  DELIVERY PLATFORM IMPORT (Uber Eats / DoorDash)
# =====================================================================

def _parse_ubereats_pdf(pdf_bytes: bytes) -> dict:
    """Parse an Uber Eats monthly statement PDF into structured payout data."""
    import pdfplumber
    pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    pdf.close()

    # Extract store name and period from page 1
    store_name = ""
    statement_period = ""
    lines = full_text.split("\n")
    for i, line in enumerate(lines):
        if line.strip() == "Monthly" and i + 1 < len(lines) and lines[i + 1].strip() == "Statement":
            # Next non-empty line after date header should have store name nearby
            pass
        if _re.match(r'^[A-Z][a-z]+ \d{4}$', line.strip()):
            statement_period = line.strip()
        # Store name: line before an address-like line
        addr_match = _re.match(r'^\d+\s+.+,\s*$', line.strip())
        if addr_match and i > 0 and not store_name:
            candidate = lines[i - 1].strip()
            if candidate and not _re.match(r'(Monthly|Statement|Food Terminal|Payout|Note)', candidate):
                pass

    # Better store name extraction: look for a line right before an address ("1234 Street Name")
    for i, line in enumerate(lines):
        if i + 1 < len(lines) and _re.match(r'^\d+\s+[A-Z]', lines[i + 1].strip()):
            candidate = line.strip()
            # Must look like a business name, not a dollar amount or generic label
            if candidate and not _re.match(r'(\$|Tax|Total|Net|Monthly|Statement|Earnings|Fees|Marketing|Payout)', candidate) and len(candidate) > 3:
                store_name = candidate
                break
    if not store_name:
        # Fallback: try matching "Food Terminal (...)" or similar store patterns
        name_match = _re.search(r'((?:Food|Restaurant|Kitchen|Cafe|Bakery|Bar|Grill|Deli)[^\n]{3,40})', full_text)
        if name_match:
            store_name = name_match.group(1).strip()

    # Split into sections: Consolidated Monthly Summary + individual payouts
    # We want the individual payout sections (pages 2+)
    sections = _re.split(r'Payout Period\s*:\s*\n?', full_text)

    payouts = []
    for sec_idx, section in enumerate(sections[1:], 1):  # skip first (consolidated summary)
        payout = {"lines": []}

        # Extract payout period
        period_match = _re.search(r'([A-Za-z]+ \d{1,2},\s*\d{4})\s*-\s*([A-Za-z]+ \d{1,2},\s*\d{4})', section)
        if period_match:
            payout["period_start"] = period_match.group(1).strip()
            payout["period_end"] = period_match.group(2).strip()

        # Extract deposit date — columns may be interleaved so the date line may not
        # be directly after "Deposit Initiated :". Search for the date line near it.
        deposit_match = _re.search(r'Deposit Initiated\s*:\s*\n?([A-Za-z]+ \d{1,2},\s*\d{4})', section)
        if deposit_match:
            payout["deposit_date"] = deposit_match.group(1).strip()
        else:
            # Fallback: find "Deposit Initiated" then search within next few lines for a date
            di_pos = section.find('Deposit Initiated')
            if di_pos >= 0:
                nearby = section[di_pos:di_pos+200]
                date_m = _re.search(r'\n([A-Z][a-z]{2} \d{1,2},\s*\d{4})\n', nearby)
                if date_m:
                    payout["deposit_date"] = date_m.group(1).strip()

        # Extract payout ref ID
        ref_match = _re.search(r'Payout Ref\.? ID\s*:\s*\n?([A-Z0-9]+)', section)
        if ref_match:
            payout["ref_id"] = ref_match.group(1).strip()

        # Helper: extract dollar amount from line like "Sales (62 Orders)\n$2,961.70" or "Marketplace Fees\n-$704.43"
        def extract_amount(pattern):
            m = _re.search(pattern + r'\s*\n?\s*(-?\$[\d,]+\.\d{2})', section)
            if m:
                val = m.group(1).replace('$', '').replace(',', '')
                return float(val)
            return 0.0

        # Extract key line items
        sales = extract_amount(r'Sales \(\d+ Orders?\)')
        tax_on_sales = extract_amount(r'Tax on Sales')
        tips = extract_amount(r'Tips')
        container_fees = extract_amount(r'Container Fees(?!\n.*Tax)')
        other_earnings = extract_amount(r'Other Earnings(?!\n.*Tax)')
        marketplace_fees = extract_amount(r'Marketplace Fees(?!\n.*Tax)')
        other_charges = extract_amount(r'Other Charges(?!\n.*Tax)')
        offers_on_items = extract_amount(r'Offers On Items')
        marketing_adjustment = extract_amount(r'Marketing Adjustment')
        other_offer_charges = extract_amount(r'Other Offer Charges')
        tax_on_offers = extract_amount(r'Tax on offer spends')
        ad_spends = extract_amount(r'Ad Spends')
        ad_credits = extract_amount(r'Ad Credits')
        chargeback = extract_amount(r'Net Chargeback Amount')
        chargeback_tax = extract_amount(r'Net Tax On Chargeback')
        marketplace_facilitator_tax = extract_amount(r'Marketplace Facilitator Tax')
        adjustments = extract_amount(r'Adjustments(?!\n.*Tax)')
        net_payout = extract_amount(r'Net Payout')

        # Total uber fees
        total_uber_fees = abs(marketplace_fees) + abs(other_charges)
        # Total marketing
        total_marketing = abs(offers_on_items) + abs(other_offer_charges) + abs(ad_spends) - abs(marketing_adjustment) - abs(ad_credits) + abs(tax_on_offers)
        # Total chargebacks
        total_chargebacks = abs(chargeback) + abs(chargeback_tax)

        payout["sales"] = round(sales, 2)
        payout["tax_on_sales"] = round(tax_on_sales, 2)
        payout["tips"] = round(tips, 2)
        payout["container_fees"] = round(container_fees, 2)
        payout["other_earnings"] = round(other_earnings, 2)
        payout["marketplace_fees"] = round(abs(marketplace_fees), 2)
        payout["other_charges"] = round(abs(other_charges), 2)
        payout["total_uber_fees"] = round(total_uber_fees, 2)
        payout["offers_on_items"] = round(abs(offers_on_items), 2)
        payout["marketing_adjustment"] = round(abs(marketing_adjustment), 2)
        payout["other_offer_charges"] = round(abs(other_offer_charges), 2)
        payout["tax_on_offers"] = round(abs(tax_on_offers), 2)
        payout["ad_spends"] = round(abs(ad_spends), 2)
        payout["ad_credits"] = round(abs(ad_credits), 2)
        payout["total_marketing"] = round(total_marketing, 2)
        payout["chargeback"] = round(abs(chargeback), 2)
        payout["chargeback_tax"] = round(abs(chargeback_tax), 2)
        payout["total_chargebacks"] = round(total_chargebacks, 2)
        payout["marketplace_facilitator_tax"] = round(abs(marketplace_facilitator_tax), 2)
        payout["adjustments"] = round(abs(adjustments), 2)
        payout["net_payout"] = round(abs(net_payout), 2)

        if payout.get("deposit_date") or payout.get("net_payout"):
            payouts.append(payout)

    return {
        "platform": "ubereats",
        "store_name": store_name,
        "statement_period": statement_period,
        "payouts": payouts,
    }


def _parse_doordash_pdf(pdf_bytes: bytes) -> dict:
    """Parse a DoorDash monthly statement PDF into structured payout data."""
    import pdfplumber
    pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    pdf.close()

    # Extract store name: appears after "Store Sales (xxx)" on line below
    store_name = ""
    store_match = _re.search(r'(?:Store\s+.*?\n)(.+?)\n', full_text)
    if store_match:
        store_name = store_match.group(1).strip()

    # Extract statement period
    period = ""
    period_match = _re.search(r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w* \d{4}) Statement', full_text)
    if period_match:
        period = period_match.group(1)

    # Split by "Payout #XXXXXXXXX" sections
    sections = _re.split(r'Payout #(\d+)', full_text)

    payouts = []
    for i in range(1, len(sections), 2):
        payout_id = sections[i]
        body = sections[i + 1] if i + 1 < len(sections) else ""
        payout = {"payout_id": payout_id}

        # Extract dollar amount: "Label $X,XXX.XX" or "Label -$X,XXX.XX"
        def extract_amt(pattern):
            m = _re.search(pattern + r'\s+(-?\$[\d,]+\.\d{2})', body)
            if m:
                return float(m.group(1).replace('$', '').replace(',', ''))
            return 0.0

        # Deposit date
        dm = _re.search(r'Deposit Initiated\s+([A-Za-z]+ \d{1,2},?\s*\d{4})', body)
        if dm:
            payout["deposit_date"] = dm.group(1).strip()

        # Transaction dates
        tm = _re.search(r'Transaction Dates\s+([A-Za-z]+ \d{1,2},?\s*\d{4})\s*-\s*([A-Za-z]+ \d{1,2},?\s*\d{4})', body)
        if tm:
            payout["period_start"] = tm.group(1).strip()
            payout["period_end"] = tm.group(2).strip()

        payout["subtotal"] = round(extract_amt(r'Subtotal'), 2)
        payout["tax_subtotal"] = round(extract_amt(r'Tax \(subtotal\)'), 2)
        payout["staff_tips"] = round(extract_amt(r'Staff tips'), 2)
        payout["customer_fees"] = round(extract_amt(r'Customer fees(?!\s+\$)'), 2)
        payout["commission"] = round(abs(extract_amt(r'Commission(?!\s+&)')), 2)
        payout["merchant_fees"] = round(abs(extract_amt(r'Merchant fees')), 2)
        payout["tax_merchant_fees"] = round(abs(extract_amt(r'Tax \(merchant fees\)')), 2)
        payout["marketing_fees"] = round(abs(extract_amt(r'Marketing fees')), 2)
        payout["customer_discounts_you"] = round(abs(extract_amt(r'Customer discounts funded by you')), 2)
        payout["customer_discounts_dd"] = round(abs(extract_amt(r'Customer discounts funded by DoorDash')), 2)
        payout["marketing_credit"] = round(abs(extract_amt(r'Marketing credit')), 2)
        payout["error_charges"] = round(abs(extract_amt(r'Error charges')), 2)
        payout["adjustments"] = round(extract_amt(r'Adjustments'), 2)
        payout["net_payout"] = round(extract_amt(r'Net payout'), 2)

        # Calculated totals
        payout["total_commission_fees"] = round(payout["commission"] + payout["merchant_fees"], 2)
        payout["total_marketing"] = round(payout["marketing_fees"] + payout["customer_discounts_you"] - payout["marketing_credit"], 2)
        payout["sales"] = round(payout["subtotal"] + payout["staff_tips"], 2)

        if payout.get("deposit_date") or payout.get("net_payout"):
            payouts.append(payout)

    return {
        "platform": "doordash",
        "store_name": store_name,
        "statement_period": period,
        "payouts": payouts,
    }


def _detect_platform(text: str) -> str:
    """Detect whether a PDF is from Uber Eats or DoorDash."""
    text_lower = text.lower()
    if "uber eats" in text_lower or "marketplace fees" in text_lower and "uber" in text_lower:
        return "ubereats"
    if "doordash" in text_lower or "dasher" in text_lower:
        return "doordash"
    # Heuristic: Uber Eats uses "Marketplace Fees", DoorDash uses "Commission"
    if "marketplace fees" in text_lower:
        return "ubereats"
    if "commission" in text_lower:
        return "doordash"
    return "unknown"


def _generate_journal_entries(parsed: dict, mapping: dict, prefix: str = "UBER") -> list:
    """Convert parsed payout data into journal entry rows.
    mapping = {category: qbo_account_name} e.g. {"bank": "Metro City Bank", "income": "Ubereats", ...}
    Returns list of dicts with: journal_no, journal_date, account, debit, credit, description
    """
    entries = []
    platform_label = "Uber Eats" if parsed["platform"] == "ubereats" else "DoorDash"

    for idx, payout in enumerate(parsed["payouts"], 1):
        journal_no = f"{prefix}-{idx}"
        # Parse deposit date
        date_str = payout.get("deposit_date", "")
        try:
            dt = datetime.strptime(date_str.replace(",", ""), "%b %d %Y")
            formatted_date = dt.strftime("%-m/%-d/%y")
        except (ValueError, TypeError):
            formatted_date = date_str

        period_desc = ""
        if payout.get("period_start") and payout.get("period_end"):
            period_desc = f"{payout['period_start']} - {payout['period_end']}"

        # DEBIT: Bank account (net payout)
        if payout.get("net_payout", 0) > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("bank", "Checking"),
                "debit": round(payout["net_payout"], 2),
                "credit": "",
                "description": f"{platform_label} Payout {period_desc}".strip(),
            })

        # DEBIT: Platform fees
        fee_amount = 0
        if parsed["platform"] == "ubereats":
            fee_amount = payout.get("total_uber_fees", 0)
        else:
            fee_amount = payout.get("total_commission_fees", 0)
        if fee_amount > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("fees", "Delivery Fee"),
                "debit": round(fee_amount, 2),
                "credit": "",
                "description": f"{platform_label} Marketplace Fees",
            })

        # DEBIT: Marketing
        mkt_amount = payout.get("total_marketing", 0)
        if mkt_amount > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("marketing", "Advertising & Marketing:Online Order Marketing"),
                "debit": round(mkt_amount, 2),
                "credit": "",
                "description": f"{platform_label} Marketing Spends",
            })

        # DEBIT: Chargebacks (Uber Eats)
        cb_amount = payout.get("total_chargebacks", 0)
        if cb_amount > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("chargeback", "Chargeback"),
                "debit": round(cb_amount, 2),
                "credit": "",
                "description": f"{platform_label} Chargebacks",
            })

        # DEBIT: Error charges (DoorDash)
        err_amount = payout.get("error_charges", 0)
        if err_amount > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("chargeback", "Chargeback"),
                "debit": round(err_amount, 2),
                "credit": "",
                "description": f"{platform_label} Error Charges",
            })

        # DEBIT: Adjustments
        adj_amount = payout.get("adjustments", 0)
        if adj_amount > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("adjustments", "Other Expense"),
                "debit": round(adj_amount, 2),
                "credit": "",
                "description": f"{platform_label} Adjustments",
            })

        # CREDIT: Total income (Sales amount = sum of all debits)
        total_debits = sum(e["debit"] for e in entries if e["journal_no"] == journal_no and e["debit"])
        if total_debits > 0:
            entries.append({
                "journal_no": journal_no,
                "journal_date": formatted_date,
                "account": mapping.get("income", "Ubereats"),
                "debit": "",
                "credit": round(total_debits, 2),
                "description": f"Total {platform_label} Income",
            })

    return entries


class DeliveryMappingUpdate(BaseModel):
    company_id: str
    platform: str  # "ubereats" or "doordash"
    mapping: dict  # {"bank": "Metro City Bank", "income": "Ubereats", ...}


@app.post("/api/delivery-import/parse")
async def parse_delivery_statement(
    file: UploadFile = File(...),
    authorization: str = Header(None),
):
    """Upload and parse an Uber Eats or DoorDash PDF statement.
    Returns parsed payout data and detected platform."""
    token = _extract_token(authorization)
    user = get_current_user(token)

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    pdf_bytes = await file.read()
    if len(pdf_bytes) > 10 * 1024 * 1024:  # 10MB limit
        raise HTTPException(status_code=400, detail="File too large (max 10MB)")

    # Detect platform
    import pdfplumber
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        first_page_text = pdf.pages[0].extract_text() or ""
        pdf.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read PDF: {str(e)}")

    platform = _detect_platform(first_page_text)
    if platform == "unknown":
        raise HTTPException(status_code=400, detail="Could not detect platform. Please upload an Uber Eats or DoorDash monthly statement.")

    try:
        if platform == "ubereats":
            parsed = _parse_ubereats_pdf(pdf_bytes)
        else:
            parsed = _parse_doordash_pdf(pdf_bytes)
    except Exception as e:
        logger.error("Delivery import parse error: %s", str(e), exc_info=True)
        raise HTTPException(status_code=400, detail=f"Error parsing PDF: {str(e)}")

    return parsed


@app.post("/api/delivery-import/generate-csv")
async def generate_delivery_csv(
    authorization: str = Header(None),
):
    """Generate QBO-compatible journal entry CSV from parsed data.
    Expects JSON body with: parsed (from /parse), mapping (account mapping), prefix (journal number prefix)."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    return {}  # Placeholder — actual CSV generation handled in the combined endpoint below


class GenerateCSVRequest(BaseModel):
    parsed: dict
    mapping: dict
    prefix: str = "UBER"
    company_id: str = ""


@app.post("/api/delivery-import/csv")
async def delivery_csv(
    req: GenerateCSVRequest,
    authorization: str = Header(None),
):
    """Generate QBO journal entry CSV from parsed payout data + account mapping."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    entries = _generate_journal_entries(req.parsed, req.mapping, req.prefix)
    if not entries:
        raise HTTPException(status_code=400, detail="No journal entries could be generated")

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Journal No", "Journal Date", "Account", "Debit", "Credit", "Description"])
    for e in entries:
        writer.writerow([e["journal_no"], e["journal_date"], e["account"],
                         e["debit"] if e["debit"] else "", e["credit"] if e["credit"] else "",
                         e["description"]])

    csv_content = output.getvalue()

    # Save to history
    if req.company_id:
        try:
            history_id = str(uuid.uuid4())
            db = get_db()
            db.execute(
                """INSERT INTO delivery_import_history
                   (id, org_id, company_id, platform, store_name, statement_period,
                    payout_count, entry_count, prefix, mapping, status, qbo_je_ids,
                    csv_content, created_by)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (history_id, org_id, req.company_id, req.parsed.get("platform", ""),
                 req.parsed.get("store_name", ""), req.parsed.get("statement_period", ""),
                 len(req.parsed.get("payouts", [])), len(entries), req.prefix,
                 json.dumps(req.mapping), "csv_downloaded", "[]",
                 csv_content, user["email"]),
            )
            db.commit()
            db.close()
        except Exception as e:
            logger.warning("Failed to save delivery import history: %s", str(e))

    return {
        "csv_content": csv_content,
        "entries": entries,
        "entry_count": len(entries),
        "payout_count": len(req.parsed.get("payouts", [])),
    }


@app.get("/api/delivery-import/mapping")
async def get_delivery_mapping(
    company_id: str,
    platform: str = "ubereats",
    authorization: str = Header(None),
):
    """Get saved account mapping for a company/platform."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    row = db.execute(
        "SELECT mapping FROM delivery_mappings WHERE company_id = ? AND platform = ? AND org_id = ?",
        (company_id, platform, org_id),
    ).fetchone()
    db.close()

    if row:
        return {"mapping": json.loads(row[0])}

    # Return defaults
    if platform == "ubereats":
        return {"mapping": {
            "bank": "Checking",
            "income": "Ubereats",
            "fees": "Delivery Fee",
            "marketing": "Advertising & Marketing:Online Order Marketing",
            "chargeback": "Chargeback",
            "adjustments": "Other Expense",
        }}
    else:
        return {"mapping": {
            "bank": "Checking",
            "income": "DoorDash",
            "fees": "Delivery Fee",
            "marketing": "Advertising & Marketing:Online Order Marketing",
            "chargeback": "Chargeback",
            "adjustments": "Other Expense",
        }}


@app.post("/api/delivery-import/mapping")
async def save_delivery_mapping(
    req: DeliveryMappingUpdate,
    authorization: str = Header(None),
):
    """Save account mapping for a company/platform."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    db.execute(
        "INSERT INTO delivery_mappings (id, company_id, platform, org_id, mapping, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(company_id, platform, org_id) DO UPDATE SET mapping = excluded.mapping, updated_at = excluded.updated_at",
        (str(uuid.uuid4()), req.company_id, req.platform, org_id,
         json.dumps(req.mapping), datetime.now().isoformat()),
    )
    db.commit()
    db.close()

    return {"ok": True}


class ExportQBORequest(BaseModel):
    company_id: str
    parsed: dict
    mapping: dict
    prefix: str = "UBER"


@app.post("/api/delivery-import/export-qbo")
async def export_delivery_to_qbo(
    req: ExportQBORequest,
    authorization: str = Header(None),
):
    """Push delivery import journal entries directly into QuickBooks via API."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    entries = _generate_journal_entries(req.parsed, req.mapping, req.prefix)
    if not entries:
        raise HTTPException(status_code=400, detail="No journal entries could be generated")

    db = get_db()

    # Helper: look up QBO account ID + type from cached account name
    def find_account_info(company_id, account_name):
        if not account_name:
            return None, None
        row = db.execute(
            """SELECT qbo_account_id, account_type FROM company_accounts
               WHERE company_id = ? AND (fully_qualified_name = ? OR name = ?) AND active = 1
               LIMIT 1""",
            (company_id, account_name, account_name)
        ).fetchone()
        if row:
            return row["qbo_account_id"], row["account_type"]
        return None, None

    # Group entries by journal_no (each payout = one JE)
    from collections import OrderedDict
    je_groups = OrderedDict()
    for e in entries:
        jno = e["journal_no"]
        if jno not in je_groups:
            je_groups[jno] = {"date": e["journal_date"], "lines": []}
        je_groups[jno]["lines"].append(e)

    platform_label = "Uber Eats" if req.parsed["platform"] == "ubereats" else "DoorDash"
    posted_je_ids = []
    errors = []

    for jno, group in je_groups.items():
        je_lines = []
        missing_accounts = []

        for line in group["lines"]:
            acct_id, acct_type = find_account_info(req.company_id, line["account"])
            if not acct_id:
                missing_accounts.append(line["account"])
                continue

            posting_type = "Debit" if line["debit"] else "Credit"
            amount = line["debit"] if line["debit"] else line["credit"]

            detail = {
                "PostingType": posting_type,
                "AccountRef": {"value": acct_id}
            }
            je_lines.append({
                "DetailType": "JournalEntryLineDetail",
                "Amount": round(abs(float(amount)), 2),
                "Description": line.get("description", ""),
                "JournalEntryLineDetail": detail
            })

        if missing_accounts:
            errors.append(f"{jno}: account(s) not found in QBO: {', '.join(missing_accounts)}")
            continue

        if not je_lines:
            continue

        # Parse the journal date to QBO format (YYYY-MM-DD)
        date_str = group["date"]
        try:
            dt = datetime.strptime(date_str, "%m/%d/%y")
            qbo_date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            try:
                dt = datetime.strptime(date_str.replace(",", ""), "%b %d %Y")
                qbo_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                qbo_date = date_str

        payload = {
            "TxnDate": qbo_date,
            "Line": je_lines,
            "PrivateNote": f"{platform_label} Import: {jno} - {req.parsed.get('statement_period', '')}"
        }

        try:
            result = await qbo_api_call(
                db, req.company_id,
                "journalentry?minorversion=65",
                method="POST", params=payload
            )
            je_id = result.get("JournalEntry", {}).get("Id")
            if je_id:
                posted_je_ids.append({"journal_no": jno, "qbo_id": je_id})
        except HTTPException as he:
            errors.append(f"{jno}: QBO error - {he.detail[:200]}")
        except Exception as ex:
            errors.append(f"{jno}: error - {str(ex)[:200]}")

    # Build CSV content for history
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Journal No", "Journal Date", "Account", "Debit", "Credit", "Description"])
    for e in entries:
        writer.writerow([e["journal_no"], e["journal_date"], e["account"],
                         e["debit"] if e["debit"] else "", e["credit"] if e["credit"] else "",
                         e["description"]])

    # Save to history
    status = "exported" if posted_je_ids and not errors else "partial" if posted_je_ids else "failed"
    history_id = str(uuid.uuid4())
    db.execute(
        """INSERT INTO delivery_import_history
           (id, org_id, company_id, platform, store_name, statement_period,
            payout_count, entry_count, prefix, mapping, status, qbo_je_ids,
            csv_content, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (history_id, org_id, req.company_id, req.parsed["platform"],
         req.parsed.get("store_name", ""), req.parsed.get("statement_period", ""),
         len(req.parsed.get("payouts", [])), len(entries), req.prefix,
         json.dumps(req.mapping), status, json.dumps(posted_je_ids),
         output.getvalue(), user["email"]),
    )
    db.commit()
    db.close()

    return {
        "status": status,
        "history_id": history_id,
        "posted": posted_je_ids,
        "posted_count": len(posted_je_ids),
        "total_count": len(je_groups),
        "errors": errors if errors else None,
    }


@app.get("/api/delivery-import/history")
async def get_delivery_import_history(
    company_id: str = None,
    authorization: str = Header(None),
):
    """Get delivery import history for the org, optionally filtered by company."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    if company_id:
        rows = db.execute(
            """SELECT id, company_id, platform, store_name, statement_period,
                      payout_count, entry_count, prefix, status, qbo_je_ids,
                      created_by, created_at
               FROM delivery_import_history
               WHERE org_id = ? AND company_id = ?
               ORDER BY created_at DESC LIMIT 50""",
            (org_id, company_id),
        ).fetchall()
    else:
        rows = db.execute(
            """SELECT id, company_id, platform, store_name, statement_period,
                      payout_count, entry_count, prefix, status, qbo_je_ids,
                      created_by, created_at
               FROM delivery_import_history
               WHERE org_id = ?
               ORDER BY created_at DESC LIMIT 50""",
            (org_id,),
        ).fetchall()
    db.close()

    history = []
    for r in rows:
        row = dict(r)
        row["qbo_je_ids"] = json.loads(row.get("qbo_je_ids", "[]"))
        history.append(row)

    return {"history": history}


@app.get("/api/delivery-import/history/{history_id}/csv")
async def download_history_csv(
    history_id: str,
    authorization: str = Header(None),
):
    """Download the CSV from a past import."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    row = db.execute(
        "SELECT csv_content, platform, statement_period FROM delivery_import_history WHERE id = ? AND org_id = ?",
        (history_id, org_id),
    ).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=404, detail="Import not found")

    return {
        "csv_content": row["csv_content"],
        "platform": row["platform"],
        "statement_period": row["statement_period"],
    }


# =====================================================================
#  STRIPE BILLING
# =====================================================================

@app.get("/api/billing/plans")
async def get_plans():
    """Return available plans (public, no auth needed)."""
    return {
        "plans": [
            {
                "id": "free",
                "name": "Starter",
                "price": 0,
                "interval": None,
                "max_companies": 3,
                "features": ["Up to 3 companies", "P&L, Balance Sheet, Cash Flow", "Transaction drill-down", "1 admin user"],
            },
            {
                "id": "business",
                "name": "Business",
                "price": 4900,  # cents
                "interval": "month",
                "max_companies": 50,
                "features": ["Up to 50 companies", "All financial reports", "Period comparison", "Intercompany journals", "Account mapping", "Unlimited team members"],
            },
        ]
    }


@app.get("/api/billing/subscription")
async def get_subscription(authorization: str = Header(None)):
    """Get current org's subscription status, with trial-aware plan resolution."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    org = db.execute("SELECT * FROM organizations WHERE id = ?", (org_id,)).fetchone()
    db.close()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    org_dict = dict(org)
    effective = get_effective_plan(org_dict)
    return {
        "plan": effective["plan"],
        "max_companies": effective["max_companies"],
        "subscription_status": org_dict.get("subscription_status", "none"),
        "stripe_customer_id": org_dict.get("stripe_customer_id", ""),
        "trial_ends_at": effective.get("trial_ends_at", ""),
        "trial_active": effective.get("trial_active", False),
        "trial_days_remaining": effective.get("trial_days_remaining", 0),
        "trial_expired": effective.get("trial_expired", False),
    }


@app.post("/api/billing/create-checkout")
async def create_checkout_session(authorization: str = Header(None)):
    """Create a Stripe Checkout session for upgrading to Business plan."""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe is not configured.")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    org = db.execute("SELECT * FROM organizations WHERE id = ?", (org_id,)).fetchone()
    org_dict = dict(org)

    # Get or create Stripe customer
    stripe_customer_id = org_dict.get("stripe_customer_id")
    if not stripe_customer_id:
        customer = stripe.Customer.create(
            email=user["email"],
            name=org_dict.get("name", ""),
            metadata={"org_id": org_id},
        )
        stripe_customer_id = customer.id
        db.execute("UPDATE organizations SET stripe_customer_id = ? WHERE id = ?", (stripe_customer_id, org_id))
        db.commit()
    db.close()

    # Create Checkout session
    session = stripe.checkout.Session.create(
        customer=stripe_customer_id,
        payment_method_types=["card"],
        line_items=[{
            "price": STRIPE_PRICE_BUSINESS_MONTHLY,
            "quantity": 1,
        }],
        mode="subscription",
        success_url=FRONTEND_ORIGIN + "?billing=success",
        cancel_url=FRONTEND_ORIGIN + "?billing=canceled",
        subscription_data={
            "trial_period_days": 14,
            "metadata": {"org_id": org_id},
        },
        metadata={"org_id": org_id},
    )
    return {"checkout_url": session.url}


@app.post("/api/billing/portal")
async def create_billing_portal(authorization: str = Header(None)):
    """Create a Stripe Billing Portal session for managing subscription."""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe is not configured.")
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    org = db.execute("SELECT stripe_customer_id FROM organizations WHERE id = ?", (org_id,)).fetchone()
    db.close()

    if not org or not org["stripe_customer_id"]:
        raise HTTPException(status_code=400, detail="No billing account found. Please upgrade first.")

    session = stripe.billing_portal.Session.create(
        customer=org["stripe_customer_id"],
        return_url=FRONTEND_ORIGIN,
    )
    return {"portal_url": session.url}


@app.post("/api/billing/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events to update subscription status."""
    if not STRIPE_SECRET_KEY:
        return {"status": "stripe not configured"}
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    body = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(body, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(body)
    except Exception as e:
        logger.error("Stripe webhook signature verification failed: %s", e)
        raise HTTPException(status_code=400, detail="Invalid signature")

    event_type = event.get("type", "") if isinstance(event, dict) else event.type
    data = event.get("data", {}).get("object", {}) if isinstance(event, dict) else event.data.object

    logger.info("Stripe webhook: %s", event_type)

    db = get_db()
    try:
        if event_type == "checkout.session.completed":
            org_id = data.get("metadata", {}).get("org_id") if isinstance(data, dict) else data.metadata.get("org_id")
            sub_id = data.get("subscription") if isinstance(data, dict) else data.subscription
            cust_id = data.get("customer") if isinstance(data, dict) else data.customer
            if org_id:
                db.execute(
                    "UPDATE organizations SET plan = 'business', max_companies = 50, stripe_subscription_id = ?, stripe_customer_id = ?, subscription_status = 'active' WHERE id = ?",
                    (sub_id, cust_id, org_id),
                )
                db.commit()
                logger.info("Org %s upgraded to Business plan", org_id)

        elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
            sub_obj = data
            sub_status = sub_obj.get("status") if isinstance(sub_obj, dict) else sub_obj.status
            sub_id = sub_obj.get("id") if isinstance(sub_obj, dict) else sub_obj.id
            org_id = (sub_obj.get("metadata", {}) if isinstance(sub_obj, dict) else sub_obj.metadata).get("org_id")

            if not org_id:
                # Fallback: find org by stripe_subscription_id
                row = db.execute("SELECT id FROM organizations WHERE stripe_subscription_id = ?", (sub_id,)).fetchone()
                org_id = row["id"] if row else None

            if org_id:
                if sub_status in ("active", "trialing"):
                    db.execute(
                        "UPDATE organizations SET subscription_status = 'active', plan = 'business', max_companies = 50 WHERE id = ?",
                        (org_id,),
                    )
                elif sub_status == "past_due":
                    db.execute(
                        "UPDATE organizations SET subscription_status = 'past_due' WHERE id = ?",
                        (org_id,),
                    )
                elif sub_status in ("canceled", "unpaid", "incomplete_expired"):
                    db.execute(
                        "UPDATE organizations SET subscription_status = 'canceled', plan = 'free', max_companies = 3 WHERE id = ?",
                        (org_id,),
                    )
                db.commit()
                logger.info("Org %s subscription updated: %s", org_id, sub_status)

        elif event_type == "invoice.payment_failed":
            cust_id = data.get("customer") if isinstance(data, dict) else data.customer
            row = db.execute("SELECT id FROM organizations WHERE stripe_customer_id = ?", (cust_id,)).fetchone()
            if row:
                db.execute("UPDATE organizations SET subscription_status = 'past_due' WHERE id = ?", (row["id"],))
                db.commit()
                logger.warning("Payment failed for org %s", row["id"])
    finally:
        db.close()

    return {"status": "ok"}


# =====================================================================
#  KNOWLEDGE BASE
# =====================================================================

class KBEntryCreate(BaseModel):
    category: str = "general"
    title: str
    content: str
    enabled: bool = True
    sort_order: int = 0

class KBEntryUpdate(BaseModel):
    category: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    enabled: Optional[bool] = None
    sort_order: Optional[int] = None


@app.get("/api/knowledge-base")
async def list_kb_entries(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute(
        "SELECT * FROM knowledge_base WHERE org_id = ? ORDER BY category, sort_order, title",
        (org_id,),
    ).fetchall()
    db.close()
    return [{**dict(r), "enabled": bool(r["enabled"])} for r in rows]


@app.post("/api/knowledge-base")
async def create_kb_entry(req: KBEntryCreate, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    if user["role"] not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin access required")
    org_id = get_org_id(user)
    entry_id = str(uuid.uuid4())
    db = get_db()
    db.execute(
        """INSERT INTO knowledge_base (id, org_id, category, title, content, enabled, sort_order)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (entry_id, org_id, req.category, req.title, req.content, int(req.enabled), req.sort_order),
    )
    db.commit()
    row = db.execute("SELECT * FROM knowledge_base WHERE id = ?", (entry_id,)).fetchone()
    db.close()
    return {**dict(row), "enabled": bool(row["enabled"])}


@app.put("/api/knowledge-base/{entry_id}")
async def update_kb_entry(entry_id: str, req: KBEntryUpdate, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    if user["role"] not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin access required")
    org_id = get_org_id(user)
    db = get_db()
    existing = db.execute("SELECT * FROM knowledge_base WHERE id = ? AND org_id = ?", (entry_id, org_id)).fetchone()
    if not existing:
        db.close()
        raise HTTPException(status_code=404, detail="Entry not found")
    updates = []
    params = []
    for field in ["category", "title", "content", "enabled", "sort_order"]:
        val = getattr(req, field)
        if val is not None:
            if field == "enabled":
                val = int(val)
            updates.append(f"{field} = ?")
            params.append(val)
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(entry_id)
        params.append(org_id)
        db.execute(f"UPDATE knowledge_base SET {', '.join(updates)} WHERE id = ? AND org_id = ?", params)
        db.commit()
    row = db.execute("SELECT * FROM knowledge_base WHERE id = ?", (entry_id,)).fetchone()
    db.close()
    return {**dict(row), "enabled": bool(row["enabled"])}


@app.delete("/api/knowledge-base/{entry_id}")
async def delete_kb_entry(entry_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    if user["role"] not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin access required")
    org_id = get_org_id(user)
    db = get_db()
    existing = db.execute("SELECT id FROM knowledge_base WHERE id = ? AND org_id = ?", (entry_id, org_id)).fetchone()
    if not existing:
        db.close()
        raise HTTPException(status_code=404, detail="Entry not found")
    db.execute("DELETE FROM knowledge_base WHERE id = ? AND org_id = ?", (entry_id, org_id))
    db.commit()
    db.close()
    return {"status": "deleted"}


# =====================================================================
#  AI CHAT ASSISTANT
# =====================================================================

class ChatMessage(BaseModel):
    message: str
    conversation: Optional[list] = None  # previous messages [{role, content}]


def _build_company_context(org_id: str) -> str:
    """Build a context string with all companies for this org."""
    db = get_db()
    companies = db.execute(
        "SELECT id, name, status, source FROM companies WHERE org_id = ? ORDER BY name", (org_id,)
    ).fetchall()
    db.close()
    if not companies:
        return "No companies connected yet."
    lines = []
    for c in companies:
        src = (c["source"] or "qbo").lower()
        src_label = "QuickBooks" if src == "qbo" else "Manual+Plaid"
        lines.append(f"- {c['name']} (id: {c['id']}, status: {c['status']}, source: {src_label})")
    return "Connected companies:\n" + "\n".join(lines)


def _build_kb_context(org_id: str) -> str:
    """Build knowledge base context for the AI chat."""
    db = get_db()
    entries = db.execute(
        """SELECT category, title, content FROM knowledge_base
           WHERE org_id = ? AND enabled = 1
           ORDER BY category, sort_order, title
           LIMIT 50""",
        (org_id,),
    ).fetchall()
    db.close()
    if not entries:
        return ""
    lines = []
    current_cat = ""
    for e in entries:
        cat_label = e["category"].replace("_", " ").title()
        if cat_label != current_cat:
            current_cat = cat_label
            lines.append(f"\n## {current_cat}")
        lines.append(f"### {e['title']}\n{e['content']}")
    return "Knowledge Base:\n" + "\n".join(lines)


def _build_accounts_context(org_id: str) -> str:
    """Build cached accounts list for context."""
    db = get_db()
    accounts = db.execute(
        """SELECT ca.name, ca.account_type, c.name as company_name
           FROM company_accounts ca
           JOIN companies c ON ca.company_id = c.id
           WHERE c.org_id = ? AND ca.active = 1
           ORDER BY c.name, ca.account_type, ca.name
           LIMIT 200""",
        (org_id,),
    ).fetchall()
    db.close()
    if not accounts:
        return "No chart of accounts data cached yet."
    lines = []
    current_company = ""
    for a in accounts:
        if a["company_name"] != current_company:
            current_company = a["company_name"]
            lines.append(f"\n{current_company}:")
        lines.append(f"  - {a['name']} ({a['account_type']})")
    return "Chart of Accounts (sample):\n" + "\n".join(lines)


CHAT_SYSTEM_PROMPT = """You are the AI assistant for Consolidated Report, a multi-company QuickBooks Online reporting dashboard.

You help users with:
1. **Creating intercompany journal entries** — Ask for source company, destination company, entry type, amount, accounts, and date. Then provide the structured JSON to create it.
2. **Answering financial questions** — When users ask about revenue, expenses, net income, balances, etc., respond with real numbers from the financial data provided.
3. **Analyzing financial data** — Compare companies, identify trends, break down expenses.
4. **App navigation and help** — Guide users on how to use features.

When the user asks to create a journal entry, gather the necessary info and respond with a special JSON block that the frontend will parse:
```action:create_je
{{"source_company_id": "...", "dest_company_id": "...", "entry_type": "...", "description": "...", "date": "YYYY-MM-DD", "amount": 0, "lines": [{{"side": "source", "posting_type": "Debit", "account_name": "...", "amount": 0}}, ...]}}
```

When the user asks to SEE/VIEW/OPEN a report (not asking a question about data), respond with:
```action:show_report
{{"report_type": "profit-loss|balance-sheet|cash-flow", "company_id": "all|<specific-id>", "period": "last_month|ytd|custom", "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}}
```

When the user asks to navigate somewhere:
```action:navigate
{{"page": "dashboard|companies|intercompany|account-mapping|users|billing|knowledge-base"}}
```

Always be concise and helpful. Format currency with dollar signs and commas. When financial data is provided in the context, use those exact numbers to answer the question. Include per-company breakdowns when the data is available. After answering a financial question, you may optionally include an action:show_report block so the user can view the full report.

Use the company and account context below to resolve company names to IDs.
For journal entries, each side (source and dest) must balance: total debits = total credits on that side.
Common entry types: Management Fee, Loan, Expense Reimbursement, Revenue Transfer, Cost Allocation.
Today's date: {today}.

{company_context}

{accounts_context}

{kb_context}

{financial_context}"""


async def _call_gemini(system_msg: str, contents: list, max_tokens: int = 2000) -> str:
    """Call Gemini API and return the text reply."""
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{AI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            gemini_url,
            headers={"Content-Type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": system_msg}]},
                "contents": contents,
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": max_tokens,
                },
                "tool_config": {"function_calling_config": {"mode": "NONE"}},
            },
        )
    if resp.status_code != 200:
        logger.error("Gemini API error: %s %s", resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail="AI service error. Please try again.")
    data = resp.json()
    # Check for blocked/empty responses
    candidates = data.get("candidates", [])
    if not candidates:
        block_reason = data.get("promptFeedback", {}).get("blockReason", "unknown")
        logger.error("Gemini: no candidates. blockReason=%s, response=%s", block_reason, json.dumps(data)[:1000])
        raise HTTPException(status_code=502, detail=f"AI response was blocked ({block_reason}). Please try rephrasing.")
    candidate = candidates[0]
    finish_reason = candidate.get("finishReason", "")
    # Try to extract text from any part
    parts = candidate.get("content", {}).get("parts", [])
    text_parts = []
    for part in parts:
        if "text" in part:
            text_parts.append(part["text"])
        elif "functionCall" in part:
            # Model tried to make a function call — log it and continue
            fc = part["functionCall"]
            logger.warning("Gemini made unexpected function call: %s(%s)", fc.get("name"), json.dumps(fc.get("args", {}))[:200])
    if text_parts:
        return "\n".join(text_parts)
    # If no text at all
    logger.error("Gemini no text parts (finishReason=%s): %s", finish_reason, json.dumps(data)[:1000])
    raise HTTPException(status_code=502, detail=f"AI returned an unexpected response (reason: {finish_reason}).")


async def _call_gemini_with_retry(system_msg: str, contents: list, max_tokens: int = 2000) -> str:
    """Call Gemini with a retry on UNEXPECTED_TOOL_CALL errors."""
    try:
        return await _call_gemini(system_msg, contents, max_tokens)
    except HTTPException as e:
        if "UNEXPECTED_TOOL_CALL" in str(e.detail):
            # Retry with a prefixed instruction to avoid function calling
            logger.info("Retrying Gemini call with anti-tool-call prefix")
            modified_contents = list(contents)
            # Prefix the user message with an explicit instruction
            if modified_contents:
                last = modified_contents[-1]
                if last.get("role") == "user" and last.get("parts"):
                    original_text = last["parts"][0].get("text", "")
                    modified_contents[-1] = {
                        "role": "user",
                        "parts": [{"text": f"(Important: respond only with plain text, do not call any functions or tools.)\n\n{original_text}"}]
                    }
            return await _call_gemini(system_msg, modified_contents, max_tokens)
        raise


async def _call_gemini_safe(system_msg: str, contents: list, max_tokens: int = 2000) -> str:
    """Like _call_gemini but returns None instead of raising on failure."""
    try:
        return await _call_gemini(system_msg, contents, max_tokens)
    except Exception as e:
        logger.error("_call_gemini_safe failed: %s", str(e))
        return None


async def _chat_fetch_report_data(fetch_params: dict, org_id: str) -> str:
    """Internally fetch report data for the AI chat and return a text summary."""
    db = get_db()
    report_type = fetch_params.get("report_type", "profit-loss")
    company_id = fetch_params.get("company_id", "all")
    start_date = fetch_params.get("start_date")
    end_date = fetch_params.get("end_date")
    by_company = fetch_params.get("by_company", False)

    qbo_report_map = {
        "profit-loss": "ProfitAndLoss",
        "balance-sheet": "BalanceSheet",
        "cash-flow": "CashFlow",
    }
    qbo_report_name = qbo_report_map.get(report_type, "ProfitAndLoss")

    # Determine which companies to query
    if company_id and company_id != "all":
        companies = db.execute(
            "SELECT id, name FROM companies WHERE id = ? AND org_id = ? AND status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != ''",
            (company_id, org_id),
        ).fetchall()
    else:
        companies = db.execute(
            "SELECT id, name FROM companies WHERE org_id = ? AND status IN ('connected','synced') AND refresh_token IS NOT NULL AND refresh_token != '' ORDER BY name",
            (org_id,),
        ).fetchall()
    db.close()

    if not companies:
        return "No connected companies found."

    # Fetch reports for each company
    all_reports = []
    per_company = {}  # name -> flat lookup
    for company in companies:
        try:
            qbo_params = {"accounting_method": "Accrual"}
            if start_date:
                qbo_params["start_date"] = start_date
            if end_date:
                qbo_params["end_date"] = end_date
            report = await qbo_get_report(get_db(), company["id"], qbo_report_name, qbo_params)
            if _has_report_data(report):
                all_reports.append(report)
                if by_company:
                    per_company[company["name"]] = _build_flat_lookup(report)
        except Exception as e:
            logger.warning("Chat fetch report error for %s: %s", company["name"], str(e))

    if not all_reports:
        return f"No {report_type} data available for the requested period ({start_date} to {end_date})."

    # Build consolidated totals
    consolidated = _merge_reports(all_reports)
    consolidated_lookup = _build_flat_lookup(consolidated) if consolidated else {}

    # Format the data as readable text for the AI
    lines = []
    lines.append(f"=== {report_type.upper()} DATA ({start_date} to {end_date}) ===")
    lines.append(f"Accounting Method: Accrual")
    lines.append(f"Companies included: {len(all_reports)}")
    lines.append("")

    # Consolidated totals
    lines.append("--- CONSOLIDATED TOTALS ---")
    for key in sorted(consolidated_lookup.keys()):
        val = consolidated_lookup[key]
        if val != 0:
            lines.append(f"  {key}: ${val:,.2f}")

    # Per-company breakdown
    if by_company and per_company:
        lines.append("")
        lines.append("--- PER-COMPANY BREAKDOWN ---")
        for cname in sorted(per_company.keys()):
            lines.append(f"")
            lines.append(f"  [{cname}]")
            lookup = per_company[cname]
            for key in sorted(lookup.keys()):
                val = lookup[key]
                if val != 0:
                    lines.append(f"    {key}: ${val:,.2f}")

    return "\n".join(lines)


# Financial question keywords for proactive data fetching
_FINANCIAL_KEYWORDS = [
    "net income", "revenue", "income", "expense", "profit", "loss",
    "cogs", "cost of goods", "gross profit", "operating income",
    "balance sheet", "assets", "liabilities", "equity",
    "cash flow", "how much", "what is my", "what are my",
    "total sales", "payroll", "rent", "utilities", "how did",
    "compare", "breakdown", "which store", "which company",
    "best performing", "worst performing", "highest", "lowest",
]

_MONTH_NAMES = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
    "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}


def _parse_period(msg_lower: str, now: datetime) -> tuple:
    """Parse a date period from a chat message. Returns (start_date, end_date) strings."""

    # Check if there's a specific month mentioned (before checking year-level patterns)
    has_month = any(m in msg_lower for m in _MONTH_NAMES)

    # "last year" / "prior year" (only if no specific month is also mentioned)
    if not has_month and ("last year" in msg_lower or "prior year" in msg_lower):
        y = now.year - 1
        return f"{y}-01-01", f"{y}-12-31"

    # "this year" / "year to date" / "ytd" (only if no specific month)
    if not has_month and ("this year" in msg_lower or "year to date" in msg_lower or "ytd" in msg_lower):
        return f"{now.year}-01-01", now.strftime("%Y-%m-%d")

    # "last month"
    if "last month" in msg_lower:
        lm = (now.month - 1) or 12
        ly = now.year if now.month > 1 else now.year - 1
        last_day = calendar.monthrange(ly, lm)[1]
        return f"{ly}-{lm:02d}-01", f"{ly}-{lm:02d}-{last_day:02d}"

    # "this month"
    if "this month" in msg_lower:
        return f"{now.year}-{now.month:02d}-01", now.strftime("%Y-%m-%d")

    # Quarters: "q1", "q2", "q3", "q4" optionally with year
    q_match = _re.search(r'q([1-4])\s*(\d{4})?', msg_lower)
    if q_match:
        q = int(q_match.group(1))
        y = int(q_match.group(2)) if q_match.group(2) else now.year
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        last_day = calendar.monthrange(y, end_month)[1]
        return f"{y}-{start_month:02d}-01", f"{y}-{end_month:02d}-{last_day:02d}"

    # Specific month + optional year: "january 2025", "feb", "march 2026", "december last year"
    for month_name, month_num in _MONTH_NAMES.items():
        if month_name in msg_lower:
            # Check for explicit year
            year_match = _re.search(month_name + r'\s+(\d{4})', msg_lower)
            if year_match:
                y = int(year_match.group(1))
            elif "last year" in msg_lower or "prior year" in msg_lower:
                y = now.year - 1
            else:
                # If the month is in the future this year, assume last year
                y = now.year
                if month_num > now.month:
                    y -= 1
                # If the month is the current month, use this year
            last_day = calendar.monthrange(y, month_num)[1]
            return f"{y}-{month_num:02d}-01", f"{y}-{month_num:02d}-{last_day:02d}"

    # Explicit year only: "2025", "in 2024"
    year_only = _re.search(r'\b(20[0-9]{2})\b', msg_lower)
    if year_only:
        y = int(year_only.group(1))
        if y != now.year:  # Only if it's not the current year (avoid matching random numbers)
            return f"{y}-01-01", f"{y}-12-31"

    # Default: last month
    lm = (now.month - 1) or 12
    ly = now.year if now.month > 1 else now.year - 1
    last_day = calendar.monthrange(ly, lm)[1]
    return f"{ly}-{lm:02d}-01", f"{ly}-{lm:02d}-{last_day:02d}"


def _detect_financial_query(message: str) -> Optional[dict]:
    """Detect if a message is asking a financial question and extract parameters.
    Returns fetch params dict or None."""
    msg_lower = message.lower()
    is_financial = any(kw in msg_lower for kw in _FINANCIAL_KEYWORDS)
    if not is_financial:
        return None

    now = datetime.now()
    start_date, end_date = _parse_period(msg_lower, now)

    # Detect report type
    report_type = "profit-loss"  # default
    if "balance sheet" in msg_lower or "assets" in msg_lower or "liabilities" in msg_lower or "equity" in msg_lower:
        report_type = "balance-sheet"
    elif "cash flow" in msg_lower:
        report_type = "cash-flow"

    # Always fetch by_company for multi-store context
    return {
        "report_type": report_type,
        "company_id": "all",
        "start_date": start_date,
        "end_date": end_date,
        "by_company": True,
    }


@app.post("/api/chat")
async def chat(req: ChatMessage, authorization: str = Header(None)):
    """AI chat endpoint — processes user messages with Google Gemini.
    Proactively fetches financial data when it detects financial questions."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="AI chat is not configured. Set GEMINI_API_KEY on Railway.")

    try:
        token = _extract_token(authorization)
        user = get_current_user(token)
        org_id = get_org_id(user)

        # Build context
        company_context = _build_company_context(org_id)
        accounts_context = _build_accounts_context(org_id)
        kb_context = _build_kb_context(org_id)
        today = datetime.now().strftime("%Y-%m-%d")

        # Proactively detect financial questions and fetch data
        financial_context = ""
        fetch_params = _detect_financial_query(req.message)
        if fetch_params:
            try:
                logger.info("Chat: detected financial query, fetching data: %s", json.dumps(fetch_params))
                report_data = await _chat_fetch_report_data(fetch_params, org_id)
                if len(report_data) > 10000:
                    report_data = report_data[:10000] + "\n... (truncated)"
                financial_context = f"LIVE FINANCIAL DATA (use these numbers to answer the user's question):\nRequested period: {fetch_params.get('start_date')} to {fetch_params.get('end_date')}\n{report_data}"
                logger.info("Chat: fetched financial data for %s to %s, %d chars", fetch_params.get('start_date'), fetch_params.get('end_date'), len(financial_context))
            except Exception as e:
                logger.warning("Chat: failed to fetch financial data: %s", str(e))
                financial_context = "(Financial data could not be loaded. Suggest the user check the report page directly.)"

        system_msg = CHAT_SYSTEM_PROMPT.format(
            today=today,
            company_context=company_context,
            accounts_context=accounts_context,
            kb_context=kb_context,
            financial_context=financial_context,
        )

        # Build Gemini contents array
        contents = []
        if req.conversation:
            for msg in req.conversation[-10:]:
                role = "model" if msg.get("role") == "assistant" else "user"
                contents.append({"role": role, "parts": [{"text": msg.get("content", "")}]})
        contents.append({"role": "user", "parts": [{"text": req.message}]})

        # If we have financial context, use a simplified system prompt to avoid
        # UNEXPECTED_TOOL_CALL errors caused by action blocks in large contexts
        if financial_context:
            simple_system = f"""You are the AI assistant for Consolidated Report, a multi-company QuickBooks Online reporting dashboard.

The user asked a financial question. Answer using the data below. Be specific with actual dollar amounts formatted with $ signs and commas.
If per-company data is available and relevant, show a breakdown by company.
Be concise and helpful.
Today's date: {today}.

{financial_context}"""
            reply = await _call_gemini(simple_system, contents, max_tokens=4000)
        else:
            reply = await _call_gemini_with_retry(system_msg, contents, max_tokens=2000)

        return {"reply": reply}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat endpoint error: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")


# =====================================================================
#  SUPABASE + PLAID — manual companies with bank-feed reporting
# =====================================================================
#
# Design notes:
# - v2 mirrors manual companies into Supabase so we can reuse the hub's
#   Plaid schema (plaid_items with encrypted access tokens, transactions,
#   chart_of_accounts, categories).
# - All Supabase access is via the service-role key, which bypasses RLS.
#   Access control is enforced in this FastAPI layer via user_company_access.
# - Plaid items are encrypted at rest by a pgcrypto trigger on insert/update.
#   To read an access token, we call the SECURITY DEFINER RPC
#   plaid_access_token(item_id uuid).
# - Reports for manual companies aggregate transactions by
#   chart_of_accounts.type. Consolidated "all" reports merge QBO (live API)
#   with manual (Supabase query) data at the account-type level.

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "") or os.environ.get(
    "SUPABASE_SERVICE_ROLE_KEY", ""
)
SUPABASE_SYSTEM_USER_ID = os.environ.get("SUPABASE_SYSTEM_USER_ID", "")

PLAID_CLIENT_ID = os.environ.get("PLAID_CLIENT_ID", "")
PLAID_SECRET = os.environ.get("PLAID_SECRET", "")
PLAID_ENV = os.environ.get("PLAID_ENV", "sandbox").lower()
PLAID_PRODUCTS = [
    p.strip() for p in os.environ.get("PLAID_PRODUCTS", "transactions").split(",") if p.strip()
]
PLAID_COUNTRY_CODES = [
    c.strip() for c in os.environ.get("PLAID_COUNTRY_CODES", "US").split(",") if c.strip()
]
PLAID_WEBHOOK_URL = os.environ.get(
    "PLAID_WEBHOOK_URL",
    "https://overflowing-ambition-production-4b7e.up.railway.app/api/plaid/webhook",
)

_PLAID_HOSTS = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com",
}


def _plaid_configured() -> bool:
    return bool(PLAID_CLIENT_ID and PLAID_SECRET and PLAID_ENV in _PLAID_HOSTS)


def _plaid_base() -> str:
    if not _plaid_configured():
        raise HTTPException(status_code=500, detail="Plaid is not configured")
    return _PLAID_HOSTS[PLAID_ENV]


def _sb_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)


def _sb_headers(prefer: Optional[str] = None) -> dict:
    h = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


async def _sb_request(method: str, path: str, *, params: Optional[dict] = None,
                      json_body: Optional[dict] = None, prefer: Optional[str] = None) -> httpx.Response:
    if not _sb_configured():
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1{path}"
    async with httpx.AsyncClient(timeout=60) as client:
        return await client.request(
            method, url, headers=_sb_headers(prefer), params=params, json=json_body
        )


async def _sb_insert(table: str, row: dict) -> dict:
    resp = await _sb_request("POST", f"/{table}", json_body=row, prefer="return=representation")
    if resp.status_code >= 300:
        logger.error("Supabase insert %s failed %s: %s", table, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase insert failed ({resp.status_code})")
    data = resp.json()
    return data[0] if isinstance(data, list) and data else data


async def _sb_upsert(table: str, rows: list, on_conflict: str) -> list:
    if not rows:
        return []
    resp = await _sb_request(
        "POST", f"/{table}", params={"on_conflict": on_conflict},
        json_body=rows, prefer="return=representation,resolution=merge-duplicates",
    )
    if resp.status_code >= 300:
        logger.error("Supabase upsert %s failed %s: %s", table, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase upsert failed ({resp.status_code})")
    return resp.json()


async def _sb_select(table: str, params: dict) -> list:
    resp = await _sb_request("GET", f"/{table}", params=params)
    if resp.status_code >= 300:
        logger.error("Supabase select %s failed %s: %s", table, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase select failed ({resp.status_code})")
    return resp.json()


async def _sb_update(table: str, match_params: dict, patch: dict) -> list:
    resp = await _sb_request(
        "PATCH", f"/{table}", params=match_params, json_body=patch, prefer="return=representation"
    )
    if resp.status_code >= 300:
        logger.error("Supabase update %s failed %s: %s", table, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase update failed ({resp.status_code})")
    return resp.json()


async def _sb_delete(table: str, match_params: dict) -> None:
    resp = await _sb_request("DELETE", f"/{table}", params=match_params)
    if resp.status_code >= 300:
        logger.error("Supabase delete %s failed %s: %s", table, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase delete failed ({resp.status_code})")


async def _sb_rpc(fn_name: str, args: dict) -> any:
    if not _sb_configured():
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/rpc/{fn_name}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, headers=_sb_headers(), json=args)
    if resp.status_code >= 300:
        logger.error("Supabase RPC %s failed %s: %s", fn_name, resp.status_code, resp.text[:500])
        raise HTTPException(status_code=502, detail=f"Supabase RPC {fn_name} failed ({resp.status_code})")
    try:
        return resp.json()
    except Exception:
        return resp.text


# ---------- Plaid HTTP client (thin wrapper over REST) ----------
#
# We deliberately avoid the plaid-python SDK to keep imports light and the
# surface area small. Plaid's REST API is straightforward: POST /endpoint
# with {client_id, secret, ...params}. Documented at https://plaid.com/docs/api/

async def _plaid_post(path: str, payload: dict, timeout: int = 30) -> dict:
    url = f"{_plaid_base()}{path}"
    body = {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, **payload}
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=body, headers={"Content-Type": "application/json"})
    if resp.status_code >= 300:
        logger.error("Plaid %s failed %s: %s", path, resp.status_code, resp.text[:500])
        raise HTTPException(
            status_code=502,
            detail=f"Plaid {path} failed: {_plaid_err(resp)}",
        )
    return resp.json()


def _plaid_err(resp: httpx.Response) -> str:
    try:
        j = resp.json()
        return j.get("error_message") or j.get("error_code") or resp.text[:200]
    except Exception:
        return resp.text[:200]


# ---------- Plaid PFC → category-name map (port of plaid-pfc-map.ts) ----------

PLAID_PFC_TO_CATEGORY_NAME: dict = {
    "INCOME":                    "Sales Revenue",
    "TRANSFER_IN":               None,
    "TRANSFER_OUT":              None,
    "LOAN_PAYMENTS":             None,
    "BANK_FEES":                 "Bank Fees",
    "ENTERTAINMENT":             "Meals & Entertainment",
    "FOOD_AND_DRINK":            "Meals & Entertainment",
    "GENERAL_MERCHANDISE":       "Office Supplies",
    "HOME_IMPROVEMENT":          "Office Supplies",
    "MEDICAL":                   "Insurance",
    "PERSONAL_CARE":             None,
    "GENERAL_SERVICES":          "Professional Services",
    "GOVERNMENT_AND_NON_PROFIT": "Uncategorized",
    "TRANSPORTATION":            "Travel",
    "TRAVEL":                    "Travel",
    "RENT_AND_UTILITIES":        "Utilities",
}


def _categorize_by_pfc(plaid_pfc: Optional[str], category_by_name: dict) -> Optional[str]:
    if not plaid_pfc:
        return None
    target = PLAID_PFC_TO_CATEGORY_NAME.get(plaid_pfc)
    if not target:
        return None
    return category_by_name.get(target.lower())


# ---------- Rules engine (port of rules/engine.ts) ----------

def _apply_rule_to_tx(tx: dict, rule: dict) -> bool:
    match = rule.get("match") or {}
    merchant_q = match.get("merchant")
    if merchant_q:
        m = (tx.get("merchant_name") or "") + " " + (tx.get("description") or "")
        if merchant_q.lower() not in m.lower():
            return False
    desc_re = match.get("description_regex")
    if desc_re:
        try:
            if not _re.search(desc_re, tx.get("description") or "", _re.IGNORECASE):
                return False
        except Exception:
            return False
    amt = abs(float(tx.get("amount") or 0))
    if match.get("min") is not None and amt < float(match["min"]):
        return False
    if match.get("max") is not None and amt > float(match["max"]):
        return False
    if match.get("account_id") and tx.get("account_id") != match["account_id"]:
        return False
    return True


def _apply_rules(tx: dict, rules: list) -> Optional[dict]:
    """Return the action dict of the first matching rule, or None."""
    for r in sorted(rules, key=lambda x: (x.get("priority") or 100)):
        if not r.get("enabled", True):
            continue
        if _apply_rule_to_tx(tx, r):
            return r.get("action") or {}
    return None


# ---------- Transaction sync (port of lib/plaid/sync.ts) ----------

async def _plaid_sync_transactions(sb_item: dict) -> dict:
    """Sync a single plaid_item. sb_item is the Supabase plaid_items row.
    Returns a summary {added, modified, removed}."""
    item_id = sb_item["id"]
    sb_company_id = sb_item["company_id"]

    # Fetch plaintext access token via encryption RPC
    access_token = await _sb_rpc("plaid_access_token", {"p_item_id": item_id})
    if not access_token or not isinstance(access_token, str):
        raise HTTPException(status_code=500, detail="Could not decrypt Plaid access token")

    # Load the item's import_start_date so we can filter added rows. Plaid's
    # /transactions/sync pulls the full available history (up to 2 years) on
    # initial connect regardless of days_requested, so we enforce the window
    # client-side.
    item_rows = await _sb_select(
        "plaid_items",
        {"id": f"eq.{item_id}", "select": "import_start_date", "limit": "1"},
    )
    import_start = (item_rows[0].get("import_start_date")
                    if item_rows and item_rows[0].get("import_start_date")
                    else None)

    # Prepare categorization context: rules + categories lookup
    categories = await _sb_select(
        "categories",
        {"company_id": f"eq.{sb_company_id}", "select": "id,name,coa_account_id"},
    )
    category_by_name = {c["name"].lower(): c["id"] for c in categories if c.get("name")}

    rules = await _sb_select(
        "rules",
        {"company_id": f"eq.{sb_company_id}", "select": "*", "enabled": "eq.true"},
    )

    # Fetch accounts to map plaid_account_id → internal account row id
    accounts = await _sb_select(
        "accounts",
        {"company_id": f"eq.{sb_company_id}", "select": "id,plaid_account_id"},
    )
    acct_map = {a["plaid_account_id"]: a["id"] for a in accounts if a.get("plaid_account_id")}

    cursor = sb_item.get("cursor")
    totals = {"added": 0, "modified": 0, "removed": 0, "skipped_before_start": 0}
    has_more = True
    iterations = 0
    while has_more and iterations < 50:  # safety cap
        iterations += 1
        payload = {"access_token": access_token}
        if cursor:
            payload["cursor"] = cursor
        page = await _plaid_post("/transactions/sync", payload, timeout=60)

        # ---- added ----
        rows_to_insert = []
        for t in page.get("added", []):
            account_row_id = acct_map.get(t.get("account_id"))
            if not account_row_id:
                continue  # new account appeared mid-sync — skip until next sync
            tx_date = t.get("date") or ""
            if import_start and tx_date and tx_date < import_start:
                # User asked to import only from import_start onward.
                totals["skipped_before_start"] += 1
                continue
            plaid_pfc = (t.get("personal_finance_category") or {}).get("primary")
            base_row = {
                "id": str(uuid.uuid4()),
                "company_id": sb_company_id,
                "account_id": account_row_id,
                "plaid_txn_id": t.get("transaction_id"),
                "date": t.get("date"),
                "posted_date": t.get("authorized_date") or t.get("date"),
                "amount": t.get("amount"),
                "iso_currency": t.get("iso_currency_code") or "USD",
                "merchant_name": t.get("merchant_name"),
                "description": t.get("name"),
                "pending": t.get("pending", False),
                "plaid_pfc": plaid_pfc,
                "is_transfer": False,
                "categorized_by": None,
                "category_id": None,
            }
            # Rules first, then PFC fallback
            action = _apply_rules(base_row, rules)
            if action:
                if action.get("set_category_id"):
                    base_row["category_id"] = action["set_category_id"]
                    base_row["categorized_by"] = "rule"
                if action.get("set_vendor_id"):
                    base_row["vendor_id"] = action["set_vendor_id"]
                if action.get("mark_transfer"):
                    base_row["is_transfer"] = True
                if action.get("set_notes"):
                    base_row["notes"] = action["set_notes"]
            if not base_row["category_id"] and not base_row["is_transfer"]:
                cat = _categorize_by_pfc(plaid_pfc, category_by_name)
                if cat:
                    base_row["category_id"] = cat
                    base_row["categorized_by"] = "plaid"
            rows_to_insert.append(base_row)
        if rows_to_insert:
            # Chunk to avoid huge requests
            for i in range(0, len(rows_to_insert), 200):
                await _sb_upsert(
                    "transactions",
                    rows_to_insert[i:i + 200],
                    on_conflict="plaid_txn_id",
                )
            totals["added"] += len(rows_to_insert)

        # ---- modified (update Plaid-owned fields only) ----
        for t in page.get("modified", []):
            patch = {
                "amount": t.get("amount"),
                "date": t.get("date"),
                "posted_date": t.get("authorized_date") or t.get("date"),
                "merchant_name": t.get("merchant_name"),
                "description": t.get("name"),
                "pending": t.get("pending", False),
                "plaid_pfc": (t.get("personal_finance_category") or {}).get("primary"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await _sb_update(
                "transactions",
                {"plaid_txn_id": f"eq.{t.get('transaction_id')}"},
                patch,
            )
            totals["modified"] += 1

        # ---- removed ----
        removed_ids = [r.get("transaction_id") for r in page.get("removed", []) if r.get("transaction_id")]
        for tid in removed_ids:
            await _sb_delete("transactions", {"plaid_txn_id": f"eq.{tid}"})
        totals["removed"] += len(removed_ids)

        cursor = page.get("next_cursor") or cursor
        has_more = bool(page.get("has_more"))

    # Persist cursor + last_synced_at
    await _sb_update(
        "plaid_items", {"id": f"eq.{item_id}"},
        {
            "cursor": cursor,
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
            "status": "good",
        },
    )
    return totals


# ---------- Webhook JWS verification (port of verify-webhook.ts) ----------

_JWK_CACHE: dict = {}  # kid → { key_obj, fetched_at }


async def _verify_plaid_webhook(raw_body: bytes, jws_header: str) -> bool:
    """Verify a Plaid-Verification JWS header against the raw request body.
    Returns True if valid; False otherwise. Logs the reason on failure."""
    if not jws_header:
        logger.warning("Plaid webhook missing Plaid-Verification header")
        return False
    try:
        import jwt
        from jwt import PyJWKClient  # noqa: F401  (avail via pyjwt[crypto])
        from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePublicKey  # noqa: F401
    except Exception as e:
        logger.error("Webhook verification deps missing: %s", str(e))
        return False

    try:
        header = jwt.get_unverified_header(jws_header)
    except Exception as e:
        logger.warning("Plaid webhook JWS header decode failed: %s", str(e))
        return False

    kid = header.get("kid")
    alg = header.get("alg")
    if alg != "ES256" or not kid:
        logger.warning("Plaid webhook unexpected alg/kid: %s/%s", alg, kid)
        return False

    # Fetch or cache the JWK
    cache_entry = _JWK_CACHE.get(kid)
    now = datetime.now(timezone.utc)
    if not cache_entry or (now - cache_entry["fetched_at"]).total_seconds() > 3600:
        jwk_resp = await _plaid_post("/webhook_verification_key/get", {"key_id": kid})
        key_data = jwk_resp.get("key")
        if not key_data:
            logger.warning("Plaid webhook_verification_key_get returned no key for kid=%s", kid)
            return False
        try:
            from jwt.algorithms import ECAlgorithm
            key_obj = ECAlgorithm.from_jwk(json.dumps(key_data))
        except Exception as e:
            logger.warning("Failed to parse Plaid JWK: %s", str(e))
            return False
        _JWK_CACHE[kid] = {"key_obj": key_obj, "fetched_at": now}
        cache_entry = _JWK_CACHE[kid]

    try:
        claims = jwt.decode(
            jws_header, cache_entry["key_obj"], algorithms=["ES256"],
            options={"verify_aud": False},
        )
    except Exception as e:
        logger.warning("Plaid webhook JWS verification failed: %s", str(e))
        return False

    # Reject tokens older than 5 minutes
    iat = claims.get("iat")
    if not iat or (now.timestamp() - iat) > 300:
        logger.warning("Plaid webhook token too old or missing iat")
        return False

    body_hash = claims.get("request_body_sha256")
    computed = hashlib.sha256(raw_body).hexdigest()
    if body_hash != computed:
        logger.warning("Plaid webhook body hash mismatch")
        return False
    return True


# ---------- Helpers: ensure a company belongs to the user ----------

def _get_manual_company_for_user(company_id: str, user: dict) -> dict:
    """Raise unless company exists, is accessible, and is source='manual'.
    Returns the SQLite row as a dict (must have supabase_company_id)."""
    db = get_db()
    try:
        org_id = get_org_id(user)
        row = db.execute(
            """SELECT c.* FROM companies c
                 JOIN user_company_access uca ON uca.company_id = c.id
                WHERE c.id = ? AND uca.user_id = ? AND c.org_id = ?""",
            (company_id, user["id"], org_id),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")
        d = dict(row)
        if d.get("source") != "manual":
            raise HTTPException(status_code=400, detail="Not a manual company")
        if not d.get("supabase_company_id"):
            raise HTTPException(status_code=500, detail="Company is missing Supabase mirror")
        return d
    finally:
        db.close()


# ---------- Endpoint: create a manual company ----------

class ManualCompanyRequest(BaseModel):
    name: str
    legal_name: Optional[str] = None
    ein: Optional[str] = None
    fiscal_year_start: Optional[int] = 1  # month 1-12
    base_currency: Optional[str] = "USD"
    industry: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None


@app.post("/api/companies/manual")
async def create_manual_company(
    body: ManualCompanyRequest,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    if not body.name.strip():
        raise HTTPException(status_code=400, detail="name is required")
    if body.fiscal_year_start and not (1 <= body.fiscal_year_start <= 12):
        raise HTTPException(status_code=400, detail="fiscal_year_start must be 1-12")

    if not _sb_configured():
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    if not SUPABASE_SYSTEM_USER_ID:
        raise HTTPException(
            status_code=500,
            detail="SUPABASE_SYSTEM_USER_ID env var is required for manual companies",
        )

    # 1) Create in Supabase
    sb_row = await _sb_insert("companies", {
        "name": body.name.strip(),
        "legal_name": body.legal_name,
        "ein": body.ein,
        "fiscal_year_start": body.fiscal_year_start or 1,
        "base_currency": body.base_currency or "USD",
        "created_by": SUPABASE_SYSTEM_USER_ID,
    })
    sb_company_id = sb_row["id"]

    # 2) Seed the default chart of accounts (+ categories via trigger)
    try:
        await _sb_rpc("seed_default_coa", {"p_company_id": sb_company_id})
    except HTTPException as e:
        # Best-effort: roll back the company if seeding fails
        logger.error("seed_default_coa failed for %s: %s", sb_company_id, e.detail)
        await _sb_delete("companies", {"id": f"eq.{sb_company_id}"})
        raise

    # 3) Mirror into SQLite
    db = get_db()
    try:
        company_id = str(uuid.uuid4())
        db.execute(
            """INSERT INTO companies
                 (id, name, org_id, source, supabase_company_id,
                  legal_name, ein, fiscal_year_start, base_currency,
                  industry, address, phone, email, status, created_at)
               VALUES (?, ?, ?, 'manual', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', datetime('now'))""",
            (
                company_id, body.name.strip(), org_id, sb_company_id,
                body.legal_name, body.ein, body.fiscal_year_start or 1,
                body.base_currency or "USD",
                body.industry, body.address, body.phone, body.email,
            ),
        )
        db.execute(
            "INSERT OR IGNORE INTO user_company_access (id, user_id, company_id) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), user["id"], company_id),
        )
        db.commit()
        row = db.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        return {"company": dict(row) if row else None}
    finally:
        db.close()


# ---------- Endpoint: create a Plaid Link token ----------

class LinkTokenRequest(BaseModel):
    company_id: str


@app.post("/api/plaid/link-token")
async def create_link_token(
    body: LinkTokenRequest, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(body.company_id, user)

    payload = {
        "user": {"client_user_id": f"{user['id']}:{company['supabase_company_id']}"},
        "client_name": "Consolidated Report",
        "products": PLAID_PRODUCTS,
        "country_codes": PLAID_COUNTRY_CODES,
        "language": "en",
        "webhook": PLAID_WEBHOOK_URL,
        "transactions": {"days_requested": 730},
    }
    data = await _plaid_post("/link/token/create", payload)
    return {"link_token": data.get("link_token"), "expiration": data.get("expiration")}


# ---------- Endpoint: exchange public token and do initial sync ----------

class ExchangeTokenRequest(BaseModel):
    public_token: str
    company_id: str
    institution_id: Optional[str] = None
    institution_name: Optional[str] = None
    import_start_date: Optional[str] = None  # YYYY-MM-DD; older than sync's default window


@app.post("/api/plaid/exchange-token")
async def exchange_public_token(
    body: ExchangeTokenRequest, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(body.company_id, user)
    sb_company_id = company["supabase_company_id"]

    # 1) Exchange
    ex = await _plaid_post("/item/public_token/exchange", {"public_token": body.public_token})
    access_token = ex.get("access_token")
    plaid_item_id = ex.get("item_id")
    if not access_token or not plaid_item_id:
        raise HTTPException(status_code=502, detail="Plaid did not return access token")

    # 2) Insert plaid_items row (pgcrypto trigger encrypts access_token on insert)
    item_row = {
        "company_id": sb_company_id,
        "plaid_item_id": plaid_item_id,
        "institution_id": body.institution_id,
        "institution_name": body.institution_name,
        "access_token": access_token,
        "status": "good",
    }
    if body.import_start_date:
        item_row["import_start_date"] = body.import_start_date
    sb_item = await _sb_insert("plaid_items", item_row)

    # 3) Fetch accounts and upsert
    accounts_resp = await _plaid_post("/accounts/get", {"access_token": access_token})
    account_rows = []
    for a in accounts_resp.get("accounts", []):
        bal = a.get("balances") or {}
        account_rows.append({
            "id": str(uuid.uuid4()),
            "company_id": sb_company_id,
            "plaid_item_id": sb_item["id"],
            "plaid_account_id": a.get("account_id"),
            "name": a.get("name") or "",
            "official_name": a.get("official_name"),
            "mask": a.get("mask"),
            "type": a.get("type"),
            "subtype": a.get("subtype"),
            "current_balance": bal.get("current"),
            "available_balance": bal.get("available"),
            "currency": bal.get("iso_currency_code") or "USD",
        })
    if account_rows:
        await _sb_upsert("accounts", account_rows, on_conflict="plaid_account_id")

    # 4) Kick off initial transactions sync (best-effort; Plaid needs a minute
    #    after link before transactions are ready — webhook will also trigger
    #    a sync when INITIAL_UPDATE / HISTORICAL_UPDATE fire).
    try:
        fresh = await _sb_select(
            "plaid_items",
            {"id": f"eq.{sb_item['id']}", "select": "id,company_id,cursor"},
        )
        if fresh:
            await _plaid_sync_transactions(fresh[0])
    except Exception as e:
        logger.info("Initial sync will be retried via webhook: %s", str(e)[:200])

    # 5) If user asked for an older start date than /transactions/sync can cover,
    #    page /transactions/get to backfill. Best-effort; failures don't block.
    if body.import_start_date:
        try:
            await _plaid_backfill_history(sb_item["id"], body.import_start_date)
        except Exception as e:
            logger.warning("Plaid backfill failed (non-fatal): %s", str(e)[:300])

    return {
        "ok": True,
        "item": sb_item,
        "accounts": account_rows,
    }


async def _plaid_backfill_history(sb_item_id: str, start_date: str) -> dict:
    """Page /transactions/get to backfill history older than sync's default window.
    Upserts on plaid_txn_id so rows already captured by /transactions/sync aren't duplicated,
    and user category edits are preserved."""
    items = await _sb_select(
        "plaid_items",
        {"id": f"eq.{sb_item_id}", "select": "id,company_id"},
    )
    if not items:
        raise HTTPException(status_code=404, detail="Item not found for backfill")
    sb_item = items[0]
    sb_company_id = sb_item["company_id"]

    access_token = await _sb_rpc("plaid_access_token", {"p_item_id": sb_item_id})
    if not access_token or not isinstance(access_token, str):
        raise HTTPException(status_code=500, detail="Could not decrypt Plaid access token")

    accounts = await _sb_select(
        "accounts",
        {"company_id": f"eq.{sb_company_id}", "select": "id,plaid_account_id"},
    )
    acct_map = {a["plaid_account_id"]: a["id"] for a in accounts if a.get("plaid_account_id")}

    categories = await _sb_select(
        "categories",
        {"company_id": f"eq.{sb_company_id}", "select": "id,name"},
    )
    category_by_name = {c["name"].lower(): c["id"] for c in categories if c.get("name")}

    rules = await _sb_select(
        "rules",
        {"company_id": f"eq.{sb_company_id}", "select": "*", "enabled": "eq.true"},
    )

    end_date = datetime.now().strftime("%Y-%m-%d")
    offset = 0
    page_size = 500
    total_added = 0
    while True:
        resp = await _plaid_post(
            "/transactions/get",
            {
                "access_token": access_token,
                "start_date": start_date,
                "end_date": end_date,
                "options": {"count": page_size, "offset": offset},
            },
            timeout=90,
        )
        txs = resp.get("transactions", [])
        if not txs:
            break

        rows_to_insert = []
        for t in txs:
            account_row_id = acct_map.get(t.get("account_id"))
            if not account_row_id:
                continue
            plaid_pfc = (t.get("personal_finance_category") or {}).get("primary")
            base_row = {
                "id": str(uuid.uuid4()),
                "company_id": sb_company_id,
                "account_id": account_row_id,
                "plaid_txn_id": t.get("transaction_id"),
                "date": t.get("date"),
                "posted_date": t.get("authorized_date") or t.get("date"),
                "amount": t.get("amount"),
                "iso_currency": t.get("iso_currency_code") or "USD",
                "merchant_name": t.get("merchant_name"),
                "description": t.get("name"),
                "pending": t.get("pending", False),
                "plaid_pfc": plaid_pfc,
                "is_transfer": False,
                "categorized_by": None,
                "category_id": None,
            }
            action = _apply_rules(base_row, rules)
            if action:
                if action.get("set_category_id"):
                    base_row["category_id"] = action["set_category_id"]
                    base_row["categorized_by"] = "rule"
                if action.get("mark_transfer"):
                    base_row["is_transfer"] = True
                if action.get("set_notes"):
                    base_row["notes"] = action["set_notes"]
            if not base_row["category_id"] and not base_row["is_transfer"]:
                cat = _categorize_by_pfc(plaid_pfc, category_by_name)
                if cat:
                    base_row["category_id"] = cat
                    base_row["categorized_by"] = "plaid"
            rows_to_insert.append(base_row)

        if rows_to_insert:
            # Upsert on plaid_txn_id preserves prior sync-written rows (merge-duplicates).
            # For rows already present, this will overwrite Plaid-owned fields but also
            # wipe user-set category_id. Prefer ignore-duplicates via a header so existing
            # rows are untouched — PostgREST supports 'resolution=ignore-duplicates'.
            resp_headers_prefer = "return=representation,resolution=ignore-duplicates"
            for i in range(0, len(rows_to_insert), 200):
                chunk = rows_to_insert[i:i + 200]
                r = await _sb_request(
                    "POST", "/transactions",
                    params={"on_conflict": "plaid_txn_id"},
                    json_body=chunk,
                    prefer=resp_headers_prefer,
                )
                if r.status_code >= 300:
                    logger.warning("Backfill upsert chunk failed %s: %s",
                                   r.status_code, r.text[:300])
            total_added += len(rows_to_insert)

        total = resp.get("total_transactions", 0) or 0
        offset += len(txs)
        if offset >= total:
            break

    return {"backfilled": total_added, "through": end_date, "from": start_date}


# ---------- Endpoint: Plaid webhook ----------

@app.post("/api/plaid/webhook")
async def plaid_webhook(request: Request):
    raw = await request.body()
    jws = request.headers.get("plaid-verification") or request.headers.get("Plaid-Verification") or ""
    ok = await _verify_plaid_webhook(raw, jws)
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid Plaid webhook signature")

    try:
        body = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    wh_type = body.get("webhook_type")
    wh_code = body.get("webhook_code")
    plaid_item_id = body.get("item_id")
    if not plaid_item_id:
        return {"ok": True}  # nothing actionable

    items = await _sb_select(
        "plaid_items",
        {"plaid_item_id": f"eq.{plaid_item_id}", "select": "id,company_id,cursor,status"},
    )
    if not items:
        logger.info("Plaid webhook for unknown item %s (ignored)", plaid_item_id)
        return {"ok": True}
    sb_item = items[0]

    try:
        if wh_type == "TRANSACTIONS" and wh_code in {
            "SYNC_UPDATES_AVAILABLE", "DEFAULT_UPDATE",
            "INITIAL_UPDATE", "HISTORICAL_UPDATE",
        }:
            await _plaid_sync_transactions(sb_item)
        elif wh_type == "ITEM":
            status_patch = None
            if wh_code == "ERROR":
                status_patch = "error"
            elif wh_code == "LOGIN_REPAIRED":
                status_patch = "good"
            elif wh_code == "PENDING_EXPIRATION":
                status_patch = "login_required"
            if status_patch:
                await _sb_update(
                    "plaid_items", {"id": f"eq.{sb_item['id']}"}, {"status": status_patch},
                )
    except Exception as e:
        logger.error("Plaid webhook handler error: %s", str(e), exc_info=True)

    return {"ok": True}


# ---------- Endpoint: manual sync trigger ----------

@app.post("/api/plaid/sync/{company_id}")
async def manual_sync(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)

    items = await _sb_select(
        "plaid_items",
        {"company_id": f"eq.{company['supabase_company_id']}",
         "select": "id,company_id,cursor,status,plaid_item_id"},
    )
    summary = {"items_synced": 0, "totals": {"added": 0, "modified": 0, "removed": 0}}
    for it in items:
        try:
            t = await _plaid_sync_transactions(it)
            for k in summary["totals"]:
                summary["totals"][k] += t.get(k, 0)
            summary["items_synced"] += 1
        except Exception as e:
            logger.error("Sync failed for item %s: %s", it["id"], str(e)[:300])
    return summary


# ---------- Endpoint: disconnect a Plaid item ----------

@app.post("/api/plaid/disconnect/{item_id}")
async def disconnect_item(item_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)

    items = await _sb_select(
        "plaid_items", {"id": f"eq.{item_id}", "select": "id,company_id,plaid_item_id"},
    )
    if not items:
        raise HTTPException(status_code=404, detail="Item not found")
    sb_item = items[0]

    # Confirm the company is accessible to the user
    db = get_db()
    try:
        row = db.execute(
            """SELECT c.id FROM companies c
                 JOIN user_company_access uca ON uca.company_id = c.id
                WHERE c.supabase_company_id = ? AND uca.user_id = ?""",
            (sb_item["company_id"], user["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Item not found")
    finally:
        db.close()

    # Decrypt, call item/remove, then delete row (cascades)
    try:
        access_token = await _sb_rpc("plaid_access_token", {"p_item_id": item_id})
        if access_token and isinstance(access_token, str):
            await _plaid_post("/item/remove", {"access_token": access_token})
    except Exception as e:
        logger.warning("Plaid /item/remove failed (will still drop row): %s", str(e)[:200])

    await _sb_delete("plaid_items", {"id": f"eq.{item_id}"})
    return {"ok": True}


# ---------- Endpoint: list transactions for a manual company ----------

@app.get("/api/plaid/transactions/{company_id}")
async def list_plaid_transactions(
    company_id: str,
    limit: int = 100,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    uncategorized_only: bool = False,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))

    params = {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,date,posted_date,amount,merchant_name,description,pending,"
                   "plaid_pfc,is_transfer,category_id,notes,account_id,categorized_by",
        "order": "date.desc,created_at.desc",
        "limit": str(limit),
        "offset": str(offset),
    }
    if start_date:
        params["date"] = f"gte.{start_date}"
    if end_date:
        key = "and" if "date" in params else "date"
        if key == "and":
            # Already have a gte filter — combine via PostgREST 'and' syntax
            params["and"] = f"(date.gte.{start_date},date.lte.{end_date})"
            params.pop("date", None)
        else:
            params["date"] = f"lte.{end_date}"
    if uncategorized_only:
        params["category_id"] = "is.null"
        params["is_transfer"] = "eq.false"

    txs = await _sb_select("transactions", params)

    # Enrich with account + category name for the UI
    account_ids = list({t["account_id"] for t in txs if t.get("account_id")})
    category_ids = list({t["category_id"] for t in txs if t.get("category_id")})
    accounts_map = {}
    categories_map = {}
    if account_ids:
        id_filter = ",".join(account_ids)
        accs = await _sb_select("accounts", {
            "id": f"in.({id_filter})", "select": "id,name,mask,type,subtype",
        })
        accounts_map = {a["id"]: a for a in accs}
    if category_ids:
        id_filter = ",".join(category_ids)
        cats = await _sb_select("categories", {
            "id": f"in.({id_filter})", "select": "id,name,coa_account_id",
        })
        categories_map = {c["id"]: c for c in cats}

    for t in txs:
        t["account"] = accounts_map.get(t.get("account_id"))
        t["category"] = categories_map.get(t.get("category_id"))
    return {"transactions": txs, "count": len(txs)}


# ---------- Endpoint: list accounts for a manual company ----------

@app.get("/api/plaid/accounts/{company_id}")
async def list_plaid_accounts(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)

    accounts = await _sb_select(
        "accounts",
        {"company_id": f"eq.{company['supabase_company_id']}",
         "select": "id,plaid_item_id,plaid_account_id,name,official_name,mask,"
                    "type,subtype,current_balance,available_balance,currency"},
    )
    items = await _sb_select(
        "plaid_items",
        {"company_id": f"eq.{company['supabase_company_id']}",
         "select": "id,plaid_item_id,institution_id,institution_name,status,last_synced_at"},
    )
    return {"accounts": accounts, "items": items}


# =====================================================================
#  REPORTS — Plaid-sourced (for manual companies)
# =====================================================================
# Plaid amount convention: positive = outflow (money leaving the account).
# We flip signs so income appears positive and expense appears positive.

async def _load_plaid_coa_rows(sb_company_id: str) -> dict:
    """Return a map of coa_account_id → {code, name, type}."""
    coa = await _sb_select(
        "chart_of_accounts",
        {"company_id": f"eq.{sb_company_id}", "is_active": "eq.true",
         "select": "id,code,name,type"},
    )
    return {c["id"]: c for c in coa}


async def _sum_plaid_by_coa_type(
    sb_company_id: str, start_date: str, end_date: str,
) -> dict:
    """Return {'income':[{coa_id,name,code,total}], 'expense':[...], 'asset':..., 'liability':..., 'equity':...}."""
    # Supabase/PostgREST caps a single response at 1000 rows regardless of the
    # `limit` we send, so page through the full result set explicitly.
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "and": f"(date.gte.{start_date},date.lte.{end_date})",
            "select": "amount,category_id",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    cat_ids = list({t["category_id"] for t in txs if t.get("category_id")})
    cat_to_coa = {}
    if cat_ids:
        cats = await _sb_select("categories", {
            "id": f"in.({','.join(cat_ids)})", "select": "id,coa_account_id",
        })
        cat_to_coa = {c["id"]: c.get("coa_account_id") for c in cats}

    coa_map = await _load_plaid_coa_rows(sb_company_id)

    buckets: dict = {"income": {}, "expense": {}, "asset": {}, "liability": {}, "equity": {}}
    uncategorized_total = 0.0
    for t in txs:
        cid = t.get("category_id")
        coa_id = cat_to_coa.get(cid) if cid else None
        coa = coa_map.get(coa_id) if coa_id else None
        if not coa:
            # uncategorized — assume expense for conservative P&L
            uncategorized_total += float(t.get("amount") or 0)
            continue
        coa_type = coa["type"]
        if coa_type not in buckets:
            continue
        amt = float(t.get("amount") or 0)
        # Plaid: positive = outflow. For income, outflow is negative income, so flip.
        if coa_type == "income":
            amt = -amt  # inflow → positive income
        # For expense, outflow is positive expense, leave as-is
        key = coa_id
        bucket = buckets[coa_type]
        if key not in bucket:
            bucket[key] = {"coa_id": coa_id, "code": coa["code"], "name": coa["name"], "total": 0.0}
        bucket[key]["total"] += amt

    result = {k: list(v.values()) for k, v in buckets.items()}
    result["uncategorized_expense_total"] = uncategorized_total
    return result


# ---------- QBO-shape helpers (so the existing front-end renderer works) ----------

def _qbo_section(group_name: str, header_label: str, total_label: str,
                 line_rows: list, total_value: float, columns: int = 2) -> dict:
    """Build a QBO-style Section row. line_rows = [{name, total}]."""
    return {
        "type": "Section",
        "group": group_name,
        "Header": {"ColData": [{"value": header_label}]
                                + [{"value": ""}] * (columns - 1)},
        "Rows": {"Row": [
            {"ColData": [{"value": r["name"]}, {"value": f"{r['total']:.2f}"}]}
            for r in line_rows
        ]},
        "Summary": {"ColData": [
            {"value": total_label}, {"value": f"{total_value:.2f}"},
        ]},
    }


def _qbo_summary_row(group_name: str, label: str, total, columns: int = 2) -> dict:
    # `total` may be a scalar (Total-only column) or a list of per-period values
    # followed by an optional grand total. The ColData length is computed as
    # 1 label + N period values.
    if isinstance(total, (list, tuple)):
        cols = [{"value": label}] + [{"value": f"{v:.2f}"} for v in total]
    else:
        cols = [{"value": label}, {"value": f"{total:.2f}"}]
    return {
        "type": "Section",
        "group": group_name,
        "Summary": {"ColData": cols},
    }


def _period_buckets(start_date: str, end_date: str, summarize_by: Optional[str]) -> list:
    """Return a list of (period_start, period_end, label) tuples.

    - summarize_by=None/""  → single bucket covering [start_date, end_date]
    - summarize_by="Month"  → one bucket per calendar month
    - summarize_by="Quarter"→ one per calendar quarter
    - summarize_by="Year"   → one per calendar year
    All clipped to [start_date, end_date].
    """
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        return [(start_date, end_date, end_date)]
    mode = (summarize_by or "").lower()
    if mode not in ("month", "quarter", "year"):
        return [(start_date, end_date, end_date)]

    out: list = []
    if mode == "month":
        cur = s.replace(day=1)
        while cur <= e:
            last = calendar.monthrange(cur.year, cur.month)[1]
            pe = cur.replace(day=last)
            ps = max(cur, s); pe2 = min(pe, e)
            label = cur.strftime("%b %Y")
            out.append((ps.isoformat(), pe2.isoformat(), label))
            # advance
            ny = cur.year + (1 if cur.month == 12 else 0)
            nm = 1 if cur.month == 12 else cur.month + 1
            cur = cur.replace(year=ny, month=nm, day=1)
    elif mode == "quarter":
        q_start_month = ((s.month - 1) // 3) * 3 + 1
        cur = s.replace(month=q_start_month, day=1)
        while cur <= e:
            em = cur.month + 2
            ey = cur.year
            if em > 12:
                em -= 12; ey += 1
            last = calendar.monthrange(ey, em)[1]
            pe = date(ey, em, last)
            ps = max(cur, s); pe2 = min(pe, e)
            label = f"Q{(cur.month - 1)//3 + 1} {cur.year}"
            out.append((ps.isoformat(), pe2.isoformat(), label))
            # advance 3 months
            nm = cur.month + 3
            ny = cur.year
            if nm > 12:
                nm -= 12; ny += 1
            cur = date(ny, nm, 1)
    elif mode == "year":
        cur = date(s.year, 1, 1)
        while cur <= e:
            pe = date(cur.year, 12, 31)
            ps = max(cur, s); pe2 = min(pe, e)
            out.append((ps.isoformat(), pe2.isoformat(), str(cur.year)))
            cur = date(cur.year + 1, 1, 1)
    return out


async def _sum_plaid_by_coa_and_period(
    sb_company_id: str, periods: list,
) -> dict:
    """Return {'income': {coa_id: {name, code, totals:[..]}}, 'expense': {...}, 'asset': {...},
    'liability': {...}, 'equity': {...}, 'uncategorized_expense_totals': [..]}.
    totals[i] corresponds to periods[i].
    """
    overall_start = min(p[0] for p in periods)
    overall_end   = max(p[1] for p in periods)

    # Fetch all transactions across the overall span with pagination
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "and": f"(date.gte.{overall_start},date.lte.{overall_end})",
            "select": "amount,category_id,date",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000

    cat_ids = list({t["category_id"] for t in txs if t.get("category_id")})
    cat_to_coa: dict = {}
    if cat_ids:
        cats = await _sb_select("categories", {
            "id": f"in.({','.join(cat_ids)})", "select": "id,coa_account_id",
        })
        cat_to_coa = {c["id"]: c.get("coa_account_id") for c in cats}

    coa_map = await _load_plaid_coa_rows(sb_company_id)

    buckets: dict = {"income": {}, "expense": {}, "asset": {}, "liability": {}, "equity": {}}
    n = len(periods)
    uncat = [0.0] * n
    # Precompute period index lookup by date string comparison
    def _period_idx(d: str) -> int:
        for i, (ps, pe, _lbl) in enumerate(periods):
            if ps <= d <= pe:
                return i
        return -1

    for t in txs:
        idx = _period_idx(t.get("date") or "")
        if idx < 0:
            continue
        cid = t.get("category_id")
        coa_id = cat_to_coa.get(cid) if cid else None
        coa = coa_map.get(coa_id) if coa_id else None
        amt = float(t.get("amount") or 0)
        if not coa:
            uncat[idx] += amt
            continue
        coa_type = coa["type"]
        if coa_type not in buckets:
            continue
        # Plaid: positive = outflow. For income, flip so inflow = positive income.
        if coa_type == "income":
            amt = -amt
        key = coa_id
        b = buckets[coa_type]
        if key not in b:
            b[key] = {"coa_id": coa_id, "code": coa["code"],
                      "name": coa["name"], "totals": [0.0] * n}
        b[key]["totals"][idx] += amt

    return {"buckets": buckets, "uncategorized_expense_totals": uncat,
            "periods": periods}


async def _plaid_pl(sb_company_id: str, start_date: str, end_date: str,
                    summarize_column_by: Optional[str] = None) -> dict:
    """Return a QBO-shaped P&L dict for a manual company.
    When summarize_column_by is Month/Quarter/Year, returns one column per
    period PLUS a trailing Total column.
    """
    periods = _period_buckets(start_date, end_date, summarize_column_by)
    data = await _sum_plaid_by_coa_and_period(sb_company_id, periods)
    buckets = data["buckets"]
    uncat_totals = data["uncategorized_expense_totals"]
    n = len(periods)
    multi = bool(summarize_column_by) and n > 1

    def _make_rows(bucket: dict) -> list:
        rows = []
        for r in sorted(bucket.values(), key=lambda r: r["code"]):
            totals = r["totals"]
            row = {"name": r["name"], "totals": totals,
                   "grand": round(sum(totals), 2)}
            rows.append(row)
        return rows

    income_rows  = _make_rows(buckets["income"])
    expense_rows = _make_rows(buckets["expense"])
    if any(abs(v) > 0.005 for v in uncat_totals):
        expense_rows.append({"name": "Uncategorized",
                             "totals": list(uncat_totals),
                             "grand": round(sum(uncat_totals), 2)})

    def _section_totals(rows):
        totals_per_period = [0.0] * n
        for r in rows:
            for i, v in enumerate(r["totals"]):
                totals_per_period[i] += v
        return totals_per_period, round(sum(totals_per_period), 2)

    income_section_totals, total_income_grand = _section_totals(income_rows)
    expense_section_totals, total_expense_grand = _section_totals(expense_rows)
    net_section_totals = [round(income_section_totals[i] - expense_section_totals[i], 2)
                          for i in range(n)]
    net_income_grand = round(total_income_grand - total_expense_grand, 2)

    # Build Columns: [Account, <period labels...>, (Total if multi)]
    columns = [{"ColTitle": "", "ColType": "Account"}]
    for (_ps, _pe, label) in periods:
        columns.append({"ColTitle": label, "ColType": "Money"})
    if multi:
        columns.append({"ColTitle": "Total", "ColType": "Money"})

    def _row_coldata(row) -> list:
        vals = [{"value": row["name"]}] + [{"value": f"{v:.2f}"} for v in row["totals"]]
        if multi:
            vals.append({"value": f"{row['grand']:.2f}"})
        return vals

    def _section_block(group: str, header: str, total_label: str,
                       rows: list, per_period_totals: list, grand: float) -> dict:
        row_objs = [{"ColData": _row_coldata(r)} for r in rows]
        summary_vals = [{"value": total_label}] + [{"value": f"{v:.2f}"} for v in per_period_totals]
        if multi:
            summary_vals.append({"value": f"{grand:.2f}"})
        header_row = [{"value": header}] + [{"value": ""}] * (len(columns) - 1)
        return {
            "type": "Section",
            "group": group,
            "Header": {"ColData": header_row},
            "Rows": {"Row": row_objs},
            "Summary": {"ColData": summary_vals},
        }

    net_summary_vals = [{"value": "Net Income"}] + [{"value": f"{v:.2f}"} for v in net_section_totals]
    if multi:
        net_summary_vals.append({"value": f"{net_income_grand:.2f}"})

    return {
        "Header": {"StartPeriod": start_date, "EndPeriod": end_date,
                   "ReportName": "ProfitAndLoss", "Source": "plaid",
                   "Currency": "USD",
                   "SummarizeColumnsBy": summarize_column_by or "Total"},
        "Columns": {"Column": columns},
        "Rows": {"Row": [
            _section_block("Income", "Income", "Total Income", income_rows,
                           income_section_totals, total_income_grand),
            _section_block("Expenses", "Expenses", "Total Expenses", expense_rows,
                           expense_section_totals, total_expense_grand),
            {"type": "Section", "group": "NetIncome",
             "Summary": {"ColData": net_summary_vals}},
        ]},
    }


async def _plaid_balance_sheet(sb_company_id: str, as_of: str) -> dict:
    """QBO-shaped Balance Sheet aggregated from transactions by CoA type.

    Uses standard accounting sign conventions:
      - Assets: raw (debit positive, credit negative) → debit balance
      - Liabilities/Equity: flipped (credit positive, debit negative) → credit balance
      - Retained Earnings: Net Income (all-time up to as_of)

    Falls back to Plaid live account balances for bank accounts when the
    transaction stream doesn't cover opening balances.
    """
    # Pull all non-transfer transactions up to as_of, paging through
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "date": f"lte.{as_of}",
            "select": "amount,category_id",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000

    # Build category → CoA lookup (categories table is auto-mirrored from CoA)
    cats: list = []
    offset = 0
    while True:
        chunk = await _sb_select("categories", {
            "company_id": f"eq.{sb_company_id}",
            "select": "id,coa_account_id",
            "order": "id",
            "limit": "500",
            "offset": str(offset),
        })
        cats.extend(chunk)
        if len(chunk) < 500:
            break
        offset += 500
    cat_to_coa = {c["id"]: c.get("coa_account_id") for c in cats if c.get("coa_account_id")}

    coa = await _sb_select("chart_of_accounts", {
        "company_id": f"eq.{sb_company_id}",
        "is_active": "eq.true",
        "select": "id,code,name,type",
    })
    coa_by_id = {c["id"]: c for c in coa}

    # Accumulate by CoA with accounting sign convention
    buckets: dict = {"asset": {}, "liability": {}, "equity": {},
                     "income": {}, "expense": {}}
    for t in txs:
        try:
            amt = float(t.get("amount") or 0)
        except Exception:
            continue
        cid = t.get("category_id")
        coa_id = cat_to_coa.get(cid) if cid else None
        row = coa_by_id.get(coa_id) if coa_id else None
        if not row:
            continue
        typ = row["type"]
        if typ not in buckets:
            continue
        # Credit-side account balances (liab/equity/income) are the NEGATIVE of
        # sum(debit - credit), i.e. credit - debit.
        if typ in ("liability", "equity", "income"):
            amt = -amt
        b = buckets[typ]
        if coa_id not in b:
            b[coa_id] = {"name": row["name"], "code": row["code"], "total": 0.0}
        b[coa_id]["total"] += amt

    # Build display rows — filter out zero balances and sort by code
    def _rows(bucket: dict) -> list:
        return sorted(
            [{"name": r["name"], "code": r["code"],
              "total": round(r["total"], 2)}
             for r in bucket.values() if abs(r["total"]) > 0.005],
            key=lambda r: r["code"],
        )

    asset_rows     = _rows(buckets["asset"])
    liability_rows = _rows(buckets["liability"])
    equity_rows    = _rows(buckets["equity"])

    total_assets      = sum(r["total"] for r in asset_rows)
    total_liabilities = sum(r["total"] for r in liability_rows)
    total_equity_direct = sum(r["total"] for r in equity_rows)

    # Retained Earnings = Net Income all-time up to as_of
    total_income  = sum(r["total"] for r in buckets["income"].values())
    total_expense = sum(r["total"] for r in buckets["expense"].values())
    retained = round(total_income - total_expense, 2)

    equity_rows.append({"code": "3900", "name": "Retained Earnings",
                        "total": retained})
    total_equity = total_equity_direct + retained

    # --- Overlay AR/AP from invoices/bills (Phase 7) -------------------
    # When the sales/expenses module is in use, the authoritative AR/AP
    # balances live on `invoices.balance` / `bills.balance`. Overlay them
    # into the asset/liability rows, replacing any raw postings to the
    # seeded "Accounts Receivable" / "Accounts Payable" CoA to avoid
    # double-counting.
    try:
        open_invs = await _sb_select("invoices", {
            "company_id": f"eq.{sb_company_id}",
            "status": "not.in.(paid,void)",
            "date": f"lte.{as_of}",
            "select": "balance",
            "limit": "5000",
        })
        ar_total = round(sum(float(i.get("balance") or 0) for i in open_invs), 2)

        open_bills = await _sb_select("bills", {
            "company_id": f"eq.{sb_company_id}",
            "status": "not.in.(paid,void)",
            "date": f"lte.{as_of}",
            "select": "balance",
            "limit": "5000",
        })
        ap_total = round(sum(float(b.get("balance") or 0) for b in open_bills), 2)

        if ar_total:
            # Replace any existing "Accounts Receivable"-named asset row
            asset_rows = [r for r in asset_rows if "receivable" not in r["name"].lower()]
            asset_rows.append({"code": "1100", "name": "Accounts Receivable",
                               "total": ar_total})
            asset_rows.sort(key=lambda r: r["code"])
            total_assets = round(sum(r["total"] for r in asset_rows), 2)

        if ap_total:
            liability_rows = [r for r in liability_rows if "payable" not in r["name"].lower()]
            liability_rows.append({"code": "2000", "name": "Accounts Payable",
                                   "total": ap_total})
            liability_rows.sort(key=lambda r: r["code"])
            total_liabilities = round(sum(r["total"] for r in liability_rows), 2)
    except Exception as e:
        logger.warning("AR/AP overlay on BS failed: %s", str(e)[:200])

    return {
        "Header": {"EndPeriod": as_of, "ReportName": "BalanceSheet",
                   "Source": "plaid", "Currency": "USD"},
        "Columns": {"Column": [
            {"ColTitle": "", "ColType": "Account"},
            {"ColTitle": as_of, "ColType": "Money"},
        ]},
        "Rows": {"Row": [
            _qbo_section("Assets", "Assets", "Total Assets",
                         asset_rows, total_assets),
            _qbo_section("Liabilities", "Liabilities", "Total Liabilities",
                         liability_rows, total_liabilities),
            _qbo_section("Equity", "Equity", "Total Equity",
                         equity_rows, total_equity),
            _qbo_summary_row("LiabilitiesAndEquity",
                             "Total Liabilities and Equity",
                             round(total_liabilities + total_equity, 2)),
        ]},
    }


async def _plaid_cash_flow(sb_company_id: str, start_date: str, end_date: str) -> dict:
    """QBO-shaped Cash Flow: monthly inflow/outflow rolled into a single net column."""
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "and": f"(date.gte.{start_date},date.lte.{end_date})",
            "select": "date,amount",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    by_month: dict = {}
    for t in txs:
        d = (t.get("date") or "")[:7]  # YYYY-MM
        if not d:
            continue
        amt = float(t.get("amount") or 0)
        inflow = max(0.0, -amt)   # Plaid: negative = inflow
        outflow = max(0.0, amt)
        m = by_month.setdefault(d, {"inflow": 0.0, "outflow": 0.0})
        m["inflow"] += inflow
        m["outflow"] += outflow

    inflow_rows = [{"name": k, "total": round(v["inflow"], 2)}
                   for k, v in sorted(by_month.items())]
    outflow_rows = [{"name": k, "total": round(v["outflow"], 2)}
                    for k, v in sorted(by_month.items())]
    total_in = sum(r["total"] for r in inflow_rows)
    total_out = sum(r["total"] for r in outflow_rows)

    return {
        "Header": {"StartPeriod": start_date, "EndPeriod": end_date,
                   "ReportName": "CashFlow", "Source": "plaid",
                   "Currency": "USD"},
        "Columns": {"Column": [
            {"ColTitle": "", "ColType": "Account"},
            {"ColTitle": f"{start_date} → {end_date}", "ColType": "Money"},
        ]},
        "Rows": {"Row": [
            _qbo_section("CashInflow", "Cash Inflow (by month)",
                         "Total Inflow", inflow_rows, total_in),
            _qbo_section("CashOutflow", "Cash Outflow (by month)",
                         "Total Outflow", outflow_rows, total_out),
            _qbo_summary_row("NetCashIncrease", "Net Cash Increase for Period",
                             total_in - total_out),
        ]},
    }


# =====================================================================
#  V2 PAGES — Transactions, Chart of Accounts, Rules, Journal, Dashboard
# =====================================================================
#
# Per-company resources for manual/Plaid companies. Each endpoint:
#  1. Authorizes via existing user_company_access check (_get_manual_company_for_user
#     or a thin resolver that accepts a raw Supabase company id).
#  2. Reads/writes Supabase via the _sb_* helpers (service-role, bypasses RLS).
#  3. For QBO companies where the endpoint isn't meaningful (CoA, Rules, Journal),
#     returns 400. Reports and Transactions still have QBO-native paths elsewhere.


async def _resolve_manual_company_from_supabase_id(sb_company_id: str, user: dict) -> dict:
    """Given a Supabase companies.id, confirm the current user has access via
    user_company_access in SQLite. Returns the SQLite row (dict) or raises 404."""
    db = get_db()
    try:
        row = db.execute(
            """SELECT c.* FROM companies c
                 JOIN user_company_access uca ON uca.company_id = c.id
                WHERE c.supabase_company_id = ? AND uca.user_id = ?""",
            (sb_company_id, user["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Company not found")
        return dict(row)
    finally:
        db.close()


# ---------- Transactions CRUD ----------

@app.get("/api/transactions/{company_id}")
async def list_transactions(
    company_id: str,
    limit: int = 100,
    offset: int = 0,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    account_id: Optional[str] = None,
    plaid_item_id: Optional[str] = None,
    category_id: Optional[str] = None,
    uncategorized_only: bool = False,
    transfers_only: bool = False,
    include_transfers: bool = True,
    sort: str = "date.desc",
    authorization: str = Header(None),
):
    """Supersedes /api/plaid/transactions/{company_id} with richer filters."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))

    # If a plaid_item_id was passed, expand to all account ids under that bank.
    account_ids_filter = None
    if plaid_item_id:
        bank_accts = await _sb_select("accounts", {
            "plaid_item_id": f"eq.{plaid_item_id}",
            "company_id": f"eq.{sb_company_id}",
            "select": "id",
        })
        account_ids_filter = [a["id"] for a in bank_accts]
        if not account_ids_filter:
            # Bank has no accounts (shouldn't normally happen) — return empty set
            return {"transactions": [], "count": 0, "has_more": False}

    # Build PostgREST query params
    params: dict = {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,date,posted_date,amount,merchant_name,description,pending,plaid_pfc,"
                   "is_transfer,category_id,vendor_id,notes,account_id,categorized_by,split_parent_id",
        "limit": str(limit),
        "offset": str(offset),
        "order": sort,
    }
    and_clauses = []
    if date_from:
        and_clauses.append(f"date.gte.{date_from}")
    if date_to:
        and_clauses.append(f"date.lte.{date_to}")
    if account_id:
        and_clauses.append(f"account_id.eq.{account_id}")
    elif account_ids_filter:
        and_clauses.append(f"account_id.in.({','.join(account_ids_filter)})")
    if category_id:
        and_clauses.append(f"category_id.eq.{category_id}")
    if uncategorized_only:
        and_clauses.append("category_id.is.null")
        and_clauses.append("is_transfer.eq.false")
    elif transfers_only:
        and_clauses.append("is_transfer.eq.true")
    elif not include_transfers:
        and_clauses.append("is_transfer.eq.false")
    if search:
        # PostgREST OR syntax. Search text fields via ilike, and if the query
        # parses as a number, also match on exact amount (positive OR negative
        # sign since Plaid flips inflows).
        safe = search.replace("(", "").replace(")", "").replace(",", " ").strip()
        or_parts = [f"merchant_name.ilike.*{safe}*", f"description.ilike.*{safe}*"]
        # Numeric search: accept $247.65, 247.65, -247.65 → match both signs
        try:
            num = float(safe.lstrip("$").replace(",", ""))
            or_parts.append(f"amount.eq.{num}")
            or_parts.append(f"amount.eq.{-num}")
            # Also tolerate small rounding: abs match within 0.005
            # (skip for perf — exact match covers the common case)
        except ValueError:
            pass
        params["or"] = "(" + ",".join(or_parts) + ")"
    if and_clauses:
        params["and"] = "(" + ",".join(and_clauses) + ")"

    txs = await _sb_select("transactions", params)

    # Hydrate account + category + vendor names
    account_ids = list({t["account_id"] for t in txs if t.get("account_id")})
    category_ids = list({t["category_id"] for t in txs if t.get("category_id")})
    vendor_ids = list({t["vendor_id"] for t in txs if t.get("vendor_id")})
    accounts_map: dict = {}
    categories_map: dict = {}
    vendors_map: dict = {}
    if account_ids:
        accs = await _sb_select("accounts", {
            "id": f"in.({','.join(account_ids)})", "select": "id,name,mask,type,subtype",
        })
        accounts_map = {a["id"]: a for a in accs}
    if category_ids:
        cats = await _sb_select("categories", {
            "id": f"in.({','.join(category_ids)})", "select": "id,name,coa_account_id",
        })
        categories_map = {c["id"]: c for c in cats}
    if vendor_ids:
        vs = await _sb_select("vendors", {
            "id": f"in.({','.join(vendor_ids)})", "select": "id,display_name",
        })
        vendors_map = {v["id"]: v for v in vs}

    for t in txs:
        t["account"] = accounts_map.get(t.get("account_id"))
        t["category"] = categories_map.get(t.get("category_id"))
        t["vendor"] = vendors_map.get(t.get("vendor_id"))
    return {"transactions": txs, "count": len(txs),
            "has_more": len(txs) >= limit}


class TransactionPatch(BaseModel):
    category_id: Optional[str] = None
    vendor_id: Optional[str] = None
    is_transfer: Optional[bool] = None
    notes: Optional[str] = None
    clear_category: Optional[bool] = False  # if True, set category_id = null
    clear_vendor: Optional[bool] = False


@app.patch("/api/transactions/{txn_id}")
async def patch_transaction(
    txn_id: str, body: TransactionPatch, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)

    # Load tx, confirm company access
    rows = await _sb_select("transactions", {
        "id": f"eq.{txn_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Transaction not found")
    tx = rows[0]
    await _resolve_manual_company_from_supabase_id(tx["company_id"], user)

    patch: dict = {"updated_at": datetime.now(timezone.utc).isoformat()}
    if body.clear_category:
        patch["category_id"] = None
        patch["categorized_by"] = None
    elif body.category_id is not None:
        patch["category_id"] = body.category_id
        patch["categorized_by"] = "user"
    if body.clear_vendor:
        patch["vendor_id"] = None
    elif body.vendor_id is not None:
        patch["vendor_id"] = body.vendor_id
    if body.is_transfer is not None:
        patch["is_transfer"] = body.is_transfer
        if body.is_transfer:
            patch["category_id"] = None
    if body.notes is not None:
        patch["notes"] = body.notes

    await _sb_update("transactions", {"id": f"eq.{txn_id}"}, patch)
    updated = await _sb_select("transactions", {"id": f"eq.{txn_id}", "select": "*", "limit": "1"})
    return {"transaction": updated[0] if updated else None}


class SplitEntry(BaseModel):
    category_id: Optional[str] = None
    amount: float
    notes: Optional[str] = None


class SplitTransactionBody(BaseModel):
    splits: List[SplitEntry]


@app.post("/api/transactions/{txn_id}/split")
async def split_transaction(
    txn_id: str, body: SplitTransactionBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    if not body.splits or len(body.splits) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 splits")

    rows = await _sb_select("transactions", {
        "id": f"eq.{txn_id}",
        "select": "id,company_id,account_id,date,amount,merchant_name,description,iso_currency",
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Transaction not found")
    parent = rows[0]
    await _resolve_manual_company_from_supabase_id(parent["company_id"], user)

    parent_amt = float(parent.get("amount") or 0)
    split_total = sum(float(s.amount) for s in body.splits)
    if abs(split_total - parent_amt) > 0.005:
        raise HTTPException(
            status_code=400,
            detail=f"Splits must sum to {parent_amt:.2f} (got {split_total:.2f})",
        )

    # Create child rows, mark parent as split (clear its category, keep amount for totals)
    children = []
    for s in body.splits:
        children.append({
            "id": str(uuid.uuid4()),
            "company_id": parent["company_id"],
            "account_id": parent["account_id"],
            "plaid_txn_id": None,   # child has no Plaid id
            "split_parent_id": parent["id"],
            "date": parent["date"],
            "amount": s.amount,
            "iso_currency": parent.get("iso_currency") or "USD",
            "merchant_name": parent.get("merchant_name"),
            "description": parent.get("description"),
            "category_id": s.category_id,
            "categorized_by": "user",
            "notes": s.notes,
            "is_transfer": False,
            "pending": False,
        })
    await _sb_insert("transactions", children[0]) if len(children) == 1 else None
    # Insert all children (Supabase POST accepts an array)
    resp = await _sb_request("POST", "/transactions", json_body=children,
                             prefer="return=representation")
    if resp.status_code >= 300:
        logger.error("Split insert failed %s: %s", resp.status_code, resp.text[:300])
        raise HTTPException(status_code=502, detail="Split failed")

    # Null parent category (so it doesn't double-count in P&L)
    await _sb_update(
        "transactions", {"id": f"eq.{parent['id']}"},
        {"category_id": None, "categorized_by": "split_parent",
         "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    return {"ok": True, "children_count": len(children)}


@app.delete("/api/transactions/{txn_id}/split")
async def undo_split(txn_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("transactions", {
        "id": f"eq.{txn_id}", "select": "id,company_id,categorized_by", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Transaction not found")
    parent = rows[0]
    await _resolve_manual_company_from_supabase_id(parent["company_id"], user)

    # Delete child rows
    await _sb_delete("transactions", {"split_parent_id": f"eq.{txn_id}"})
    # Clear split_parent marker on the parent
    await _sb_update(
        "transactions", {"id": f"eq.{txn_id}"},
        {"categorized_by": None, "updated_at": datetime.now(timezone.utc).isoformat()},
    )
    return {"ok": True}


# ---------- Chart of Accounts ----------

async def _coa_ytd_totals(sb_company_id: str) -> dict:
    """Return dict coa_account_id -> ytd total amount. Positive = outflow (expense);
    for income rows the frontend flips sign."""
    year_start = f"{datetime.now().year}-01-01"
    # Aggregate transactions by category → CoA (paginate — Supabase caps at 1000/req)
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "date": f"gte.{year_start}",
            "select": "amount,category_id",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    cat_ids = list({t["category_id"] for t in txs if t.get("category_id")})
    cat_to_coa: dict = {}
    if cat_ids:
        cats = await _sb_select("categories", {
            "id": f"in.({','.join(cat_ids)})", "select": "id,coa_account_id",
        })
        cat_to_coa = {c["id"]: c.get("coa_account_id") for c in cats}
    totals: dict = {}
    for t in txs:
        coa_id = cat_to_coa.get(t.get("category_id"))
        if not coa_id:
            continue
        totals[coa_id] = totals.get(coa_id, 0.0) + float(t.get("amount") or 0)
    return totals


@app.get("/api/coa/{company_id}")
async def list_coa(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    coa = await _sb_select("chart_of_accounts", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,code,name,type,parent_id,is_active,created_at",
        "order": "code.asc",
    })
    ytd = await _coa_ytd_totals(sb_company_id)
    for row in coa:
        row["ytd_activity"] = round(ytd.get(row["id"], 0.0), 2)
    return {"accounts": coa}


class CoACreate(BaseModel):
    code: str
    name: str
    type: str  # asset | liability | equity | income | expense
    parent_id: Optional[str] = None


@app.post("/api/coa/{company_id}")
async def create_coa(
    company_id: str, body: CoACreate, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    if body.type not in ("asset", "liability", "equity", "income", "expense"):
        raise HTTPException(status_code=400, detail="Invalid type")

    row = await _sb_insert("chart_of_accounts", {
        "company_id": sb_company_id,
        "code": body.code.strip(),
        "name": body.name.strip(),
        "type": body.type,
        "parent_id": body.parent_id,
        "is_active": True,
    })
    return {"account": row}


class CoAPatch(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    parent_id: Optional[str] = None
    is_active: Optional[bool] = None


@app.patch("/api/coa/{coa_id}")
async def patch_coa(
    coa_id: str, body: CoAPatch, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("chart_of_accounts", {
        "id": f"eq.{coa_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Account not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    patch: dict = {}
    if body.code is not None: patch["code"] = body.code
    if body.name is not None: patch["name"] = body.name
    if body.type is not None:
        if body.type not in ("asset", "liability", "equity", "income", "expense"):
            raise HTTPException(status_code=400, detail="Invalid type")
        patch["type"] = body.type
    if body.parent_id is not None: patch["parent_id"] = body.parent_id or None
    if body.is_active is not None: patch["is_active"] = body.is_active
    if not patch:
        raise HTTPException(status_code=400, detail="No changes")

    updated = await _sb_update("chart_of_accounts", {"id": f"eq.{coa_id}"}, patch)
    return {"account": updated[0] if updated else None}


# ---------- Rules ----------

@app.get("/api/rules/{company_id}")
async def list_rules(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    rows = await _sb_select("rules", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,name,priority,match,action,enabled,created_at",
        "order": "priority.asc,created_at.asc",
    })
    return {"rules": rows}


class RuleBody(BaseModel):
    name: str
    priority: Optional[int] = 100
    match: dict  # { merchant?: str, description_regex?: str, min?: float, max?: float, account_id?: str }
    action: dict  # { set_category_id?: str, mark_transfer?: bool, set_notes?: str }
    enabled: Optional[bool] = True


@app.post("/api/rules/{company_id}")
async def create_rule(
    company_id: str, body: RuleBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)

    row = await _sb_insert("rules", {
        "company_id": company["supabase_company_id"],
        "name": body.name.strip(),
        "priority": int(body.priority or 100),
        "match": body.match,
        "action": body.action,
        "enabled": bool(body.enabled),
    })
    return {"rule": row}


@app.patch("/api/rules/{rule_id}")
async def patch_rule(
    rule_id: str, body: RuleBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("rules", {
        "id": f"eq.{rule_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Rule not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    patch = {
        "name": body.name.strip(),
        "priority": int(body.priority or 100),
        "match": body.match,
        "action": body.action,
        "enabled": bool(body.enabled),
    }
    updated = await _sb_update("rules", {"id": f"eq.{rule_id}"}, patch)
    return {"rule": updated[0] if updated else None}


@app.delete("/api/rules/{rule_id}")
async def delete_rule(rule_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("rules", {
        "id": f"eq.{rule_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Rule not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    await _sb_delete("rules", {"id": f"eq.{rule_id}"})
    return {"ok": True}


class RulePreviewBody(BaseModel):
    company_id: str
    match: dict


@app.post("/api/rules/preview")
async def preview_rule(body: RulePreviewBody, authorization: str = Header(None)):
    """Count how many uncategorized transactions this rule would match."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(body.company_id, user)

    # Pull uncategorized txns and evaluate in Python against the match dict
    txs = await _sb_select("transactions", {
        "company_id": f"eq.{company['supabase_company_id']}",
        "category_id": "is.null",
        "is_transfer": "eq.false",
        "select": "id,merchant_name,description,amount,account_id",
        "limit": "5000",
    })
    matches = 0
    for t in txs:
        if _apply_rule_to_tx(t, {"match": body.match}):
            matches += 1
    return {"matches": matches, "scanned": len(txs)}


@app.post("/api/rules/{company_id}/recategorize")
async def recategorize_all(company_id: str, authorization: str = Header(None)):
    """Re-run rules + PFC fallback across all uncategorized non-transfer transactions."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    rules = await _sb_select("rules", {
        "company_id": f"eq.{sb_company_id}",
        "enabled": "eq.true", "select": "*", "order": "priority.asc",
    })
    categories = await _sb_select("categories", {
        "company_id": f"eq.{sb_company_id}", "select": "id,name",
    })
    category_by_name = {c["name"].lower(): c["id"] for c in categories if c.get("name")}

    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "category_id": "is.null", "is_transfer": "eq.false",
            "select": "id,merchant_name,description,amount,account_id,plaid_pfc",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    counts = {"rule": 0, "plaid": 0, "skipped": 0}
    for t in txs:
        action = _apply_rules(t, rules)
        patch = {}
        if action:
            if action.get("set_category_id"):
                patch["category_id"] = action["set_category_id"]
                patch["categorized_by"] = "rule"
                counts["rule"] += 1
            if action.get("set_vendor_id"):
                patch["vendor_id"] = action["set_vendor_id"]
            if action.get("mark_transfer"):
                patch["is_transfer"] = True
            if action.get("set_notes"):
                patch["notes"] = action["set_notes"]
        if not patch.get("category_id"):
            cat = _categorize_by_pfc(t.get("plaid_pfc"), category_by_name)
            if cat:
                patch["category_id"] = cat
                patch["categorized_by"] = "plaid"
                counts["plaid"] += 1
        if patch:
            patch["updated_at"] = datetime.now(timezone.utc).isoformat()
            await _sb_update("transactions", {"id": f"eq.{t['id']}"}, patch)
        else:
            counts["skipped"] += 1
    return counts


# ---------- Journal Entries ----------

@app.get("/api/journal/{company_id}")
async def list_journal(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    entries = await _sb_select("journal_entries", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,date,memo,created_at,created_by",
        "order": "date.desc,created_at.desc",
        "limit": "500",
    })
    if not entries:
        return {"entries": []}

    entry_ids = [e["id"] for e in entries]
    lines = await _sb_select("journal_lines", {
        "journal_entry_id": f"in.({','.join(entry_ids)})",
        "select": "id,journal_entry_id,coa_account_id,debit,credit,description",
    })

    # Enrich CoA
    coa_ids = list({l["coa_account_id"] for l in lines if l.get("coa_account_id")})
    coa_map: dict = {}
    if coa_ids:
        coa = await _sb_select("chart_of_accounts", {
            "id": f"in.({','.join(coa_ids)})", "select": "id,code,name,type",
        })
        coa_map = {c["id"]: c for c in coa}

    lines_by_entry: dict = {}
    for l in lines:
        l["coa"] = coa_map.get(l["coa_account_id"])
        lines_by_entry.setdefault(l["journal_entry_id"], []).append(l)
    for e in entries:
        e["lines"] = lines_by_entry.get(e["id"], [])
    return {"entries": entries}


class JournalLineBody(BaseModel):
    coa_account_id: str
    debit: Optional[float] = 0
    credit: Optional[float] = 0
    description: Optional[str] = None


class JournalEntryBody(BaseModel):
    date: str  # YYYY-MM-DD
    memo: Optional[str] = None
    lines: List[JournalLineBody]


@app.post("/api/journal/{company_id}")
async def create_journal(
    company_id: str, body: JournalEntryBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    if len(body.lines) < 2:
        raise HTTPException(status_code=400, detail="Journal entry needs at least 2 lines")
    total_debit = sum(float(l.debit or 0) for l in body.lines)
    total_credit = sum(float(l.credit or 0) for l in body.lines)
    if abs(total_debit - total_credit) > 0.005:
        raise HTTPException(
            status_code=400,
            detail=f"Debits ({total_debit:.2f}) must equal credits ({total_credit:.2f})",
        )
    if total_debit <= 0:
        raise HTTPException(status_code=400, detail="Entry must have non-zero totals")

    entry = await _sb_insert("journal_entries", {
        "company_id": sb_company_id,
        "date": body.date,
        "memo": body.memo,
        "created_by": SUPABASE_SYSTEM_USER_ID or None,
    })
    # Insert lines in bulk
    line_rows = []
    for l in body.lines:
        line_rows.append({
            "journal_entry_id": entry["id"],
            "coa_account_id": l.coa_account_id,
            "debit": float(l.debit or 0),
            "credit": float(l.credit or 0),
            "description": l.description,
        })
    resp = await _sb_request(
        "POST", "/journal_lines", json_body=line_rows,
        prefer="return=representation",
    )
    if resp.status_code >= 300:
        logger.error("Journal lines insert failed %s: %s", resp.status_code, resp.text[:300])
        # Roll back the entry
        await _sb_delete("journal_entries", {"id": f"eq.{entry['id']}"})
        raise HTTPException(status_code=502, detail="Failed to save journal lines")
    return {"entry": entry, "lines_count": len(line_rows)}


@app.delete("/api/journal/{entry_id}")
async def delete_journal(entry_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("journal_entries", {
        "id": f"eq.{entry_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Journal entry not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    # Lines cascade via FK ON DELETE CASCADE (from the Supabase schema)
    await _sb_delete("journal_entries", {"id": f"eq.{entry_id}"})
    return {"ok": True}


# ---------- Bank Accounts (CoA mapping) + backfill ----------

class AccountPatch(BaseModel):
    coa_account_id: Optional[str] = None


@app.patch("/api/accounts/{account_id}")
async def patch_account(
    account_id: str, body: AccountPatch, authorization: str = Header(None),
):
    """Update the CoA mapping for a Plaid-linked bank account."""
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("accounts", {
        "id": f"eq.{account_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Account not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    patch: dict = {}
    if body.coa_account_id is not None:
        patch["coa_account_id"] = body.coa_account_id or None
    if not patch:
        raise HTTPException(status_code=400, detail="No changes")
    updated = await _sb_update("accounts", {"id": f"eq.{account_id}"}, patch)
    return {"account": updated[0] if updated else None}


class BackfillBody(BaseModel):
    start_date: str  # YYYY-MM-DD


@app.post("/api/plaid/backfill/{item_id}")
async def plaid_backfill(
    item_id: str, body: BackfillBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)

    rows = await _sb_select("plaid_items", {
        "id": f"eq.{item_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Item not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)

    # Also update plaid_items.import_start_date for visibility
    await _sb_update("plaid_items", {"id": f"eq.{item_id}"},
                     {"import_start_date": body.start_date})
    result = await _plaid_backfill_history(item_id, body.start_date)
    return result


# ---------- Dashboard ----------

@app.get("/api/dashboard/{company_id}")
async def get_company_dashboard(company_id: str, authorization: str = Header(None)):
    """Aggregated per-company dashboard widgets. Plaid-source only for now."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    today = datetime.now().date()
    year_start = today.replace(month=1, day=1).isoformat()
    twelve_months_ago = (today.replace(day=1) - timedelta(days=365)).replace(day=1).isoformat()

    # Accounts for "Cash on hand"
    accounts = await _sb_select("accounts", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,name,type,subtype,mask,current_balance",
    })
    cash_on_hand = sum(float(a.get("current_balance") or 0) for a in accounts
                       if (a.get("type") or "").lower() in ("depository",))

    # YTD P&L — use the raw bucket sums directly (skip the QBO-shape wrapper for speed)
    ytd_buckets = await _sum_plaid_by_coa_type(sb_company_id, year_start, today.isoformat())
    ytd_revenue = sum(r["total"] for r in ytd_buckets["income"])
    ytd_expense = sum(r["total"] for r in ytd_buckets["expense"]) + ytd_buckets["uncategorized_expense_total"]
    ytd_net     = ytd_revenue - ytd_expense

    # 12-month trend: monthly net (from raw cash flow aggregation)
    trend_txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"eq.{sb_company_id}",
            "is_transfer": "eq.false",
            "and": f"(date.gte.{twelve_months_ago},date.lte.{today.isoformat()})",
            "select": "date,amount",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        trend_txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000
    by_month: dict = {}
    for t in trend_txs:
        d = (t.get("date") or "")[:7]
        if not d: continue
        amt = float(t.get("amount") or 0)
        m = by_month.setdefault(d, {"inflow": 0.0, "outflow": 0.0})
        m["inflow"] += max(0.0, -amt)
        m["outflow"] += max(0.0, amt)
    months = [
        {"month": k, "inflow": round(v["inflow"], 2),
         "outflow": round(v["outflow"], 2),
         "net": round(v["inflow"] - v["outflow"], 2)}
        for k, v in sorted(by_month.items())
    ]

    # Top 5 expense categories from YTD
    top_expenses = sorted(ytd_buckets["expense"], key=lambda r: r.get("total", 0), reverse=True)[:5]

    # Uncategorized count
    uncats = await _sb_select("transactions", {
        "company_id": f"eq.{sb_company_id}",
        "category_id": "is.null", "is_transfer": "eq.false",
        "select": "id", "limit": "1001",
    })
    uncat_count = len(uncats)

    # Recent 10 transactions
    recent = await _sb_select("transactions", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,date,amount,merchant_name,description,category_id",
        "order": "date.desc,created_at.desc",
        "limit": "10",
    })

    return {
        "company": {"id": company["id"], "name": company["name"], "source": company["source"]},
        "kpi": {
            "cash_on_hand": round(cash_on_hand, 2),
            "ytd_revenue":  round(ytd_revenue, 2),
            "ytd_expense":  round(ytd_expense, 2),
            "ytd_net":      round(ytd_net, 2),
        },
        "trend_months": months,
        "top_expenses": top_expenses,
        "uncategorized_count": uncat_count,
        "accounts": accounts,
        "recent_transactions": recent,
    }


# ---------- QBO → Manual Company bulk import ----------
#
# Pulls every journal line from QBO's GeneralLedger report for a date range,
# auto-maps QBO accounts to the manual company's chart of accounts, and writes
# the transactions into Supabase under a synthetic "QBO Import" placeholder
# account. Idempotent via a synthetic plaid_txn_id of the form
# "qbo:{src_company_id}:{month}:{row_idx}" so rerunning is safe.


# QBO AccountType → manual CoA type fallback mapping
_QBO_TYPE_TO_COA_TYPE = {
    # Assets
    "Bank": "asset",
    "Accounts Receivable": "asset",
    "Other Current Asset": "asset",
    "Fixed Asset": "asset",
    "Other Asset": "asset",
    # Liabilities
    "Accounts Payable": "liability",
    "Credit Card": "liability",
    "Other Current Liability": "liability",
    "Long Term Liability": "liability",
    # Equity
    "Equity": "equity",
    # Income
    "Income": "income",
    "Other Income": "income",
    # Expense
    "Expense": "expense",
    "Other Expense": "expense",
    "Cost of Goods Sold": "expense",
}


_COA_CODE_PREFIX_BY_TYPE = {
    "asset":     1900,
    "liability": 2900,
    "equity":    3900,
    "income":    4900,
    "expense":   6900,
}


async def _auto_map_qbo_to_coa(
    sb_company_id: str, qbo_accounts: list, manual_coa: list, preview: bool = False,
) -> tuple:
    """Build a mapping from QBO account name (lowercased) → manual CoA id.
    Exact-name matches reuse an existing CoA row. For anything else, CREATE a
    new CoA row in Supabase so every QBO account has a real destination.

    When preview=True: does NOT write to Supabase. Returns simulated plan with
    placeholder ids of the form "PREVIEW:<code>" so callers can identify them.

    Returns (name_to_coa_id, created_accounts_summary).
    """
    coa_by_name = {c["name"].lower().strip(): c for c in manual_coa if c.get("is_active", True)}
    existing_codes = {c["code"] for c in manual_coa if c.get("code")}
    # Track next free code per type so we can auto-increment
    next_code_by_type = dict(_COA_CODE_PREFIX_BY_TYPE)

    name_to_coa_id: dict = {}
    created: list = []

    for qa in qbo_accounts:
        qname = (qa.get("Name") or "").strip()
        if not qname:
            continue
        # 1. Exact name match → reuse
        m = coa_by_name.get(qname.lower())
        if m:
            name_to_coa_id[qname.lower()] = m["id"]
            continue

        # 2. Otherwise create a new CoA row
        qtype = qa.get("AccountType") or ""
        coa_type = _QBO_TYPE_TO_COA_TYPE.get(qtype) or "expense"

        # Pick a code: prefer QBO's AcctNum if present and unique, else auto-increment
        code = None
        acct_num = (qa.get("AcctNum") or "").strip()
        if acct_num and acct_num not in existing_codes:
            code = acct_num
        else:
            while True:
                candidate = str(next_code_by_type.get(coa_type, 9000))
                next_code_by_type[coa_type] = next_code_by_type.get(coa_type, 9000) + 1
                if candidate not in existing_codes:
                    code = candidate
                    break

        if preview:
            new_id = f"PREVIEW:{code}"
        else:
            try:
                new_row = await _sb_insert("chart_of_accounts", {
                    "company_id": sb_company_id,
                    "code": code,
                    "name": qname,
                    "type": coa_type,
                    "is_active": True,
                })
            except HTTPException as e:
                logger.warning("CoA create failed for %s (%s): %s", qname, code, e.detail)
                continue
            new_id = new_row["id"]
            coa_by_name[qname.lower()] = new_row

        existing_codes.add(code)
        name_to_coa_id[qname.lower()] = new_id
        created.append({
            "qbo_name": qname,
            "qbo_type": qtype,
            "coa_code": code,
            "coa_type": coa_type,
        })

    return name_to_coa_id, created


async def _ensure_qbo_import_placeholder_account(sb_company_id: str, label: str) -> str:
    """Ensure a synthetic accounts row exists for QBO-imported transactions.
    Returns the account id."""
    name = f"QBO Import · {label}"
    existing = await _sb_select("accounts", {
        "company_id": f"eq.{sb_company_id}",
        "name": f"eq.{name}",
        "select": "id", "limit": "1",
    })
    if existing:
        return existing[0]["id"]
    row = await _sb_insert("accounts", {
        "id": str(uuid.uuid4()),
        "company_id": sb_company_id,
        "plaid_item_id": None,
        "plaid_account_id": None,
        "name": name,
        "official_name": name,
        "type": "depository",
        "subtype": "imported",
        "currency": "USD",
        "current_balance": 0,
        "available_balance": 0,
    })
    return row["id"]


def _parse_gl_for_import(report: dict) -> list:
    """Walk a QBO GeneralLedger report and return flat rows with the section's
    account name attached. Each row: {date, txn_type, doc_num, name, memo,
    account_name, debit, credit, amount}."""
    out: list = []
    if not report:
        return out
    columns = []
    for c in report.get("Columns", {}).get("Column", []):
        # Prefer machine-readable ColType; fall back to ColTitle
        col_type = (c.get("ColType") or c.get("ColTitle") or "").strip()
        columns.append(col_type.lower())

    def walk(rows_obj, current_account=""):
        for row in rows_obj.get("Row", []):
            row_type = row.get("type", "")
            if row_type == "Section" or row.get("Header"):
                header_cols = (row.get("Header") or {}).get("ColData", [])
                section_name = header_cols[0].get("value", "") if header_cols else ""
                next_acct = section_name or current_account
                nested = row.get("Rows", {})
                if nested:
                    walk(nested, current_account=next_acct)
            else:
                if row.get("ColData"):
                    txn = {"account_name": current_account}
                    for i, cd in enumerate(row["ColData"]):
                        key = columns[i] if i < len(columns) else f"col_{i}"
                        # Normalize keys we care about
                        val = cd.get("value", "")
                        txn[key] = val
                    out.append(txn)
                if row.get("Rows"):
                    walk(row["Rows"], current_account=current_account)

    walk(report.get("Rows", {}))
    return out


def _month_range(start_date: str, end_date: str) -> list:
    """Return a list of (month_start, month_end) tuples spanning start..end inclusive."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD")
    if end < start:
        raise HTTPException(status_code=400, detail="end_date is before start_date")

    out = []
    cur = start.replace(day=1)
    while cur <= end:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end = cur.replace(day=last_day)
        out.append((
            max(cur, start).strftime("%Y-%m-%d"),
            min(month_end, end).strftime("%Y-%m-%d"),
        ))
        # advance to next month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return out


class QboImportRequest(BaseModel):
    source_qbo_company_id: str
    dest_manual_company_id: str
    start_date: str  # YYYY-MM-DD
    end_date: str
    accounting_method: Optional[str] = "Accrual"
    preview: Optional[bool] = False  # dry-run; no Supabase writes


@app.post("/api/import/qbo-to-manual")
async def import_qbo_to_manual(body: QboImportRequest, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    try:
        src = db.execute(
            "SELECT id, name, source, qbo_realm_id, refresh_token FROM companies "
            "WHERE id = ? AND org_id = ? AND source = 'qbo'",
            (body.source_qbo_company_id, org_id),
        ).fetchone()
        if not src:
            raise HTTPException(status_code=404, detail="Source QBO company not found")
        if not src["refresh_token"]:
            raise HTTPException(status_code=400, detail="Source QBO company is not connected")

        dest = db.execute(
            "SELECT id, name, source, supabase_company_id FROM companies "
            "WHERE id = ? AND org_id = ? AND source = 'manual'",
            (body.dest_manual_company_id, org_id),
        ).fetchone()
        if not dest:
            raise HTTPException(status_code=404, detail="Destination manual company not found")
        if not dest["supabase_company_id"]:
            raise HTTPException(status_code=500, detail="Destination company is missing Supabase mirror")

        src_dict = dict(src)
        dest_dict = dict(dest)
    finally:
        db.close()

    sb_company_id = dest_dict["supabase_company_id"]

    # 1) Load QBO accounts (source of truth for the mapping)
    db = get_db()
    try:
        qbo_acct_data = await qbo_query(
            db, src_dict["id"],
            "SELECT Id, Name, AccountType, AccountSubType, Classification FROM Account "
            "WHERE Active = true MAXRESULTS 1000",
        )
    finally:
        db.close()
    qbo_accounts = qbo_acct_data.get("QueryResponse", {}).get("Account", []) or []

    # 2) Load manual CoA from Supabase
    manual_coa = await _sb_select("chart_of_accounts", {
        "company_id": f"eq.{sb_company_id}",
        "is_active": "eq.true",
        "select": "id,code,name,type",
    })

    # Compute months up-front (validates the range, used for both preview and real run)
    months = _month_range(body.start_date, body.end_date)

    # Preview mode: compute what we WOULD do, write nothing, return plan.
    if body.preview:
        _, planned_creates = await _auto_map_qbo_to_coa(
            sb_company_id, qbo_accounts, manual_coa, preview=True,
        )
        return {
            "preview": True,
            "source_company": src_dict["name"],
            "dest_company": dest_dict["name"],
            "start_date": body.start_date,
            "end_date": body.end_date,
            "months_to_process": len(months),
            "qbo_account_count": len(qbo_accounts),
            "existing_match_count": len(qbo_accounts) - len(planned_creates),
            "new_coa_count": len(planned_creates),
            "new_coas": planned_creates[:100],
        }

    # 3) Real run: build the mapping (auto-creates missing CoA rows)
    name_to_coa_id, created_accounts = await _auto_map_qbo_to_coa(
        sb_company_id, qbo_accounts, manual_coa, preview=False,
    )

    # 3a) Refresh categories after CoA creation — the CoA→category mirror
    #     trigger should have populated them, but we query after to be sure.
    categories = await _sb_select("categories", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,name,coa_account_id",
    })
    cat_by_coa = {c["coa_account_id"]: c["id"] for c in categories if c.get("coa_account_id")}

    # 4) Ensure placeholder account
    placeholder_id = await _ensure_qbo_import_placeholder_account(
        sb_company_id, src_dict["name"],
    )

    # 5) For each month chunk, pull GL and upsert
    import asyncio as _asyncio
    totals = {"imported": 0, "skipped": 0, "months_processed": 0}
    db = get_db()
    try:
        for idx_m, (m_start, m_end) in enumerate(months):
            # Small delay between months to stay under QBO's rate limit
            # (~500 req/min per realm). This is well below even with retries.
            if idx_m > 0:
                await _asyncio.sleep(0.4)
            try:
                report = await qbo_get_report(db, src_dict["id"], "GeneralLedger", {
                    "start_date": m_start,
                    "end_date": m_end,
                    "accounting_method": body.accounting_method or "Accrual",
                    "columns": "tx_date,txn_type,doc_num,name,memo,account_name,debt_amt,credit_amt",
                })
            except HTTPException as e:
                # Non-fatal — log and move on. Status_code 401 means token
                # genuinely expired; 503 is transient. In both cases we don't
                # abort the whole import, since already-imported months are
                # persisted and the idempotent plaid_txn_id lets the user retry.
                logger.warning("QBO GL fetch failed for %s..%s: %s", m_start, m_end, e.detail)
                continue
            except Exception as e:
                logger.warning("QBO GL fetch error for %s..%s: %s", m_start, m_end, str(e)[:200])
                continue

            rows = _parse_gl_for_import(report)
            batch: list = []
            for idx, r in enumerate(rows):
                acct_name = (r.get("account_name") or "").strip()
                if not acct_name:
                    totals["skipped"] += 1
                    continue
                # QBO GL reports sub-accounts as "Parent:Child" — try exact first,
                # then fall back to the leaf name.
                coa_id = name_to_coa_id.get(acct_name.lower())
                if not coa_id and ":" in acct_name:
                    leaf = acct_name.rsplit(":", 1)[-1].strip().lower()
                    coa_id = name_to_coa_id.get(leaf)
                category_id = cat_by_coa.get(coa_id) if coa_id else None

                # QBO GL column keys: look for common names
                def _num(d, *keys):
                    for k in keys:
                        v = d.get(k)
                        if v is None:
                            continue
                        try:
                            return float(v)
                        except Exception:
                            try:
                                return float(str(v).replace(",", "").replace("$", ""))
                            except Exception:
                                continue
                    return 0.0

                debit = _num(r, "debit", "debt_amt", "debit_amt")
                credit = _num(r, "credit", "credit_amt")
                if debit == 0.0 and credit == 0.0:
                    # Nothing posted — skip summary/rolling rows
                    totals["skipped"] += 1
                    continue

                date_str = r.get("date") or r.get("tx_date")
                if not date_str:
                    totals["skipped"] += 1
                    continue

                # Plaid convention: positive = outflow; amount = debit - credit
                amount = round(debit - credit, 2)

                # Synthetic stable id. Include the full account_name so sub-account
                # paths like "Purchase:Food" don't get truncated and lost during
                # later leaf-name lookups. Cap at 120 chars as a safety.
                tx_id = f"qbo:{src_dict['id']}:{m_start}:{idx}:{acct_name[:120]}"
                # The plaid_txn_id column has UNIQUE constraint across the whole
                # transactions table. To keep rerun idempotent we include company.
                plaid_txn_id = tx_id

                row = {
                    "id": str(uuid.uuid4()),
                    "company_id": sb_company_id,
                    "account_id": placeholder_id,
                    "plaid_txn_id": plaid_txn_id,
                    "date": date_str,
                    "posted_date": date_str,
                    "amount": amount,
                    "iso_currency": "USD",
                    "merchant_name": (r.get("name") or "")[:200] or None,
                    "description": (r.get("memo") or r.get("txn_type") or "")[:500] or None,
                    "pending": False,
                    "plaid_pfc": None,
                    "is_transfer": False,
                    "categorized_by": "qbo_import" if category_id else None,
                    "category_id": category_id,
                    "notes": None,
                }
                batch.append(row)

            # Upsert in chunks of 200
            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                resp = await _sb_request(
                    "POST", "/transactions",
                    params={"on_conflict": "plaid_txn_id"},
                    json_body=chunk,
                    prefer="return=minimal,resolution=merge-duplicates",
                )
                if resp.status_code >= 300:
                    logger.warning("QBO import upsert chunk failed %s: %s",
                                   resp.status_code, resp.text[:300])
                else:
                    totals["imported"] += len(chunk)
            totals["months_processed"] += 1
    finally:
        db.close()

    return {
        **totals,
        "placeholder_account_id": placeholder_id,
        "mapped_account_count": len(name_to_coa_id),
        "created_accounts": created_accounts[:50],  # cap response size
        "created_accounts_count": len(created_accounts),
        "source_company": src_dict["name"],
        "dest_company": dest_dict["name"],
    }


# ---------- QBO AR/AP Import (M3) ----------
#
# Mirror of the transaction-level import, but for Customers, Vendors,
# Invoices and Bills. Idempotent via qbo_id columns.


class QboArApImportRequest(BaseModel):
    source_qbo_company_id: str
    dest_manual_company_id: str
    start_date: str          # for Invoices/Bills only
    end_date: str
    preview: Optional[bool] = False


def _parse_qbo_address(addr: Optional[dict]) -> Optional[dict]:
    if not addr:
        return None
    return {
        "line1": addr.get("Line1") or "",
        "line2": addr.get("Line2") or "",
        "city": addr.get("City") or "",
        "region": addr.get("CountrySubDivisionCode") or addr.get("State") or "",
        "postal_code": addr.get("PostalCode") or "",
        "country": addr.get("Country") or "US",
    }


def _qbo_primary_email(e: dict) -> Optional[str]:
    pe = e.get("PrimaryEmailAddr") or {}
    return pe.get("Address") if isinstance(pe, dict) else None


def _qbo_primary_phone(e: dict) -> Optional[str]:
    p = e.get("PrimaryPhone") or {}
    return p.get("FreeFormNumber") if isinstance(p, dict) else None


async def _qbo_pull_all(db, src_company_id: str, entity: str,
                        extra_where: Optional[str] = None) -> list:
    """Page through a QBO entity list via STARTPOSITION/MAXRESULTS (max 1000)."""
    all_items: list = []
    start = 1
    page_size = 500
    while True:
        q = f"SELECT * FROM {entity}"
        if extra_where:
            q += f" WHERE {extra_where}"
        q += f" STARTPOSITION {start} MAXRESULTS {page_size}"
        try:
            data = await qbo_query(db, src_company_id, q)
        except HTTPException as e:
            logger.warning("QBO %s query failed: %s", entity, e.detail)
            break
        page = data.get("QueryResponse", {}).get(entity, []) or []
        if not page:
            break
        all_items.extend(page)
        if len(page) < page_size:
            break
        start += page_size
        # Safety cap
        if start > 50000:
            break
    return all_items


@app.post("/api/import/qbo-ar-ap")
async def import_qbo_ar_ap(
    body: QboArApImportRequest, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    db = get_db()
    try:
        src = db.execute(
            "SELECT id, name, source, qbo_realm_id, refresh_token FROM companies "
            "WHERE id = ? AND org_id = ? AND source = 'qbo'",
            (body.source_qbo_company_id, org_id),
        ).fetchone()
        if not src:
            raise HTTPException(status_code=404, detail="Source QBO company not found")
        if not src["refresh_token"]:
            raise HTTPException(status_code=400, detail="QBO company not connected")
        dest = db.execute(
            "SELECT id, name, source, supabase_company_id FROM companies "
            "WHERE id = ? AND org_id = ? AND source = 'manual'",
            (body.dest_manual_company_id, org_id),
        ).fetchone()
        if not dest:
            raise HTTPException(status_code=404, detail="Destination manual company not found")
        src_dict, dest_dict = dict(src), dict(dest)
    finally:
        db.close()

    sb_id = dest_dict["supabase_company_id"]

    # Load manual CoA for account mapping
    manual_coa = await _sb_select("chart_of_accounts", {
        "company_id": f"eq.{sb_id}",
        "is_active": "eq.true",
        "select": "id,code,name,type",
    })

    # ---- Pull QBO entities ----
    db = get_db()
    try:
        qbo_accounts = (await qbo_query(
            db, src_dict["id"],
            "SELECT Id, Name, AccountType, AccountSubType, AcctNum, FullyQualifiedName "
            "FROM Account WHERE Active = true MAXRESULTS 1000",
        )).get("QueryResponse", {}).get("Account", []) or []
        qbo_customers = await _qbo_pull_all(db, src_dict["id"], "Customer", "Active = true")
        qbo_vendors   = await _qbo_pull_all(db, src_dict["id"], "Vendor",   "Active = true")
        qbo_invoices  = await _qbo_pull_all(
            db, src_dict["id"], "Invoice",
            f"TxnDate >= '{body.start_date}' AND TxnDate <= '{body.end_date}'",
        )
        qbo_bills     = await _qbo_pull_all(
            db, src_dict["id"], "Bill",
            f"TxnDate >= '{body.start_date}' AND TxnDate <= '{body.end_date}'",
        )
    finally:
        db.close()

    # Account mapping — may need to create missing CoA. Preview doesn't write.
    name_to_coa_id, created_accounts = await _auto_map_qbo_to_coa(
        sb_id, qbo_accounts, manual_coa, preview=body.preview,
    )
    # Also index by QBO Account.Id for invoice/bill line resolution
    qbo_acct_id_to_name = {a.get("Id"): (a.get("Name") or "") for a in qbo_accounts}

    if body.preview:
        return {
            "preview": True,
            "source_company": src_dict["name"],
            "dest_company": dest_dict["name"],
            "start_date": body.start_date,
            "end_date": body.end_date,
            "counts": {
                "customers": len(qbo_customers),
                "vendors":   len(qbo_vendors),
                "invoices":  len(qbo_invoices),
                "bills":     len(qbo_bills),
                "new_coa":   len(created_accounts),
            },
            "new_coas": created_accounts[:50],
        }

    # Refresh categories after any new CoAs (mirror trigger creates them)
    categories = await _sb_select("categories", {
        "company_id": f"eq.{sb_id}", "select": "id,coa_account_id",
    })
    cat_by_coa = {c["coa_account_id"]: c["id"] for c in categories if c.get("coa_account_id")}

    summary = {"customers": 0, "vendors": 0, "invoices": 0, "bills": 0,
               "skipped_invoices": 0, "skipped_bills": 0}

    # ---- Customers ----
    qbo_cust_id_to_sb: dict = {}
    cust_rows = []
    for c in qbo_customers:
        qbo_id = c.get("Id")
        row = {
            "company_id": sb_id,
            "display_name": (c.get("DisplayName") or c.get("CompanyName") or "Unnamed").strip(),
            "company_name": c.get("CompanyName"),
            "email": _qbo_primary_email(c),
            "phone": _qbo_primary_phone(c),
            "billing_address": _parse_qbo_address(c.get("BillAddr")),
            "shipping_address": _parse_qbo_address(c.get("ShipAddr")),
            "terms_days": 30,
            "notes": c.get("Notes"),
            "is_active": bool(c.get("Active", True)),
            "qbo_id": qbo_id,
        }
        cust_rows.append(row)
    if cust_rows:
        resp = await _sb_request(
            "POST", "/customers",
            params={"on_conflict": "company_id,qbo_id"},
            json_body=cust_rows,
            prefer="return=representation,resolution=merge-duplicates",
        )
        if resp.status_code < 300:
            data = resp.json() if resp.text else []
            for r in data:
                if r.get("qbo_id"):
                    qbo_cust_id_to_sb[r["qbo_id"]] = r["id"]
            summary["customers"] = len(data)
        else:
            logger.warning("Customer upsert failed: %s", resp.text[:300])

    # If upsert didn't return ids for some (dedupe by display_name unique key may have skipped),
    # re-fetch to populate lookup
    if len(qbo_cust_id_to_sb) < len(qbo_customers):
        all_c = await _sb_select("customers", {
            "company_id": f"eq.{sb_id}",
            "select": "id,qbo_id", "limit": "5000",
        })
        for c in all_c:
            if c.get("qbo_id"):
                qbo_cust_id_to_sb.setdefault(c["qbo_id"], c["id"])

    # ---- Vendors ----
    qbo_vend_id_to_sb: dict = {}
    vend_rows = []
    for v in qbo_vendors:
        row = {
            "company_id": sb_id,
            "display_name": (v.get("DisplayName") or v.get("CompanyName") or "Unnamed").strip(),
            "company_name": v.get("CompanyName"),
            "email": _qbo_primary_email(v),
            "phone": _qbo_primary_phone(v),
            "billing_address": _parse_qbo_address(v.get("BillAddr")),
            "terms_days": 30,
            "is_1099": bool(v.get("Vendor1099", False)),
            "tax_id": v.get("TaxIdentifier"),
            "notes": v.get("Notes"),
            "is_active": bool(v.get("Active", True)),
            "qbo_id": v.get("Id"),
        }
        vend_rows.append(row)
    if vend_rows:
        resp = await _sb_request(
            "POST", "/vendors",
            params={"on_conflict": "company_id,qbo_id"},
            json_body=vend_rows,
            prefer="return=representation,resolution=merge-duplicates",
        )
        if resp.status_code < 300:
            data = resp.json() if resp.text else []
            for r in data:
                if r.get("qbo_id"):
                    qbo_vend_id_to_sb[r["qbo_id"]] = r["id"]
            summary["vendors"] = len(data)
    if len(qbo_vend_id_to_sb) < len(qbo_vendors):
        all_v = await _sb_select("vendors", {
            "company_id": f"eq.{sb_id}",
            "select": "id,qbo_id", "limit": "5000",
        })
        for v in all_v:
            if v.get("qbo_id"):
                qbo_vend_id_to_sb.setdefault(v["qbo_id"], v["id"])

    # ---- helper to map a QBO line to a CoA / category ----
    def _line_coa_id(qbo_account_id: Optional[str]) -> Optional[str]:
        if not qbo_account_id:
            return None
        acct_name = qbo_acct_id_to_name.get(qbo_account_id) or ""
        coa_id = name_to_coa_id.get(acct_name.lower())
        if not coa_id and ":" in acct_name:
            leaf = acct_name.rsplit(":", 1)[-1].strip().lower()
            coa_id = name_to_coa_id.get(leaf)
        return coa_id

    # ---- Invoices ----
    for inv in qbo_invoices:
        qbo_id = inv.get("Id")
        cust_ref = (inv.get("CustomerRef") or {}).get("value")
        sb_cust_id = qbo_cust_id_to_sb.get(cust_ref)
        if not sb_cust_id:
            summary["skipped_invoices"] += 1
            continue
        total = float(inv.get("TotalAmt") or 0)
        balance = float(inv.get("Balance") or 0)
        status = "paid" if balance < 0.005 else ("partially_paid" if balance < total - 0.005 else "sent")
        inv_row = {
            "company_id": sb_id,
            "customer_id": sb_cust_id,
            "number": (inv.get("DocNumber") or f"QBO-{qbo_id}")[:50],
            "date": inv.get("TxnDate"),
            "due_date": inv.get("DueDate"),
            "status": status,
            "memo": inv.get("CustomerMemo", {}).get("value") if isinstance(inv.get("CustomerMemo"), dict) else inv.get("PrivateNote"),
            "subtotal": round(total - float(inv.get("TxnTaxDetail", {}).get("TotalTax") or 0), 2),
            "tax_total": round(float(inv.get("TxnTaxDetail", {}).get("TotalTax") or 0), 2),
            "total": round(total, 2),
            "balance": round(balance, 2),
            "currency": (inv.get("CurrencyRef") or {}).get("value", "USD"),
            "qbo_id": qbo_id,
        }
        # Upsert invoice
        resp = await _sb_request(
            "POST", "/invoices",
            params={"on_conflict": "company_id,qbo_id"},
            json_body=inv_row,
            prefer="return=representation,resolution=merge-duplicates",
        )
        if resp.status_code >= 300:
            logger.warning("Invoice upsert failed: %s", resp.text[:300])
            summary["skipped_invoices"] += 1
            continue
        result = resp.json() if resp.text else []
        sb_invoice = result[0] if isinstance(result, list) and result else None
        if not sb_invoice:
            summary["skipped_invoices"] += 1
            continue
        # Replace lines
        await _sb_delete("invoice_lines", {"invoice_id": f"eq.{sb_invoice['id']}"})
        line_rows = []
        line_no = 0
        for ln in inv.get("Line") or []:
            if ln.get("DetailType") != "SalesItemLineDetail":
                continue
            d = ln.get("SalesItemLineDetail") or {}
            acct_ref = (d.get("IncomeAccountRef") or d.get("ItemAccountRef") or {}).get("value")
            qty = float(d.get("Qty") or 1)
            unit_price = float(d.get("UnitPrice") or 0)
            amount = float(ln.get("Amount") or (qty * unit_price))
            line_no += 1
            line_rows.append({
                "invoice_id": sb_invoice["id"],
                "line_no": line_no,
                "description": ln.get("Description"),
                "quantity": qty,
                "unit_price": unit_price,
                "amount": round(amount, 2),
                "tax_rate": 0,
                "tax_amount": 0,
                "coa_account_id": _line_coa_id(acct_ref),
            })
        if line_rows:
            await _sb_request("POST", "/invoice_lines", json_body=line_rows, prefer="return=minimal")
        summary["invoices"] += 1

    # ---- Bills ----
    for bill in qbo_bills:
        qbo_id = bill.get("Id")
        vend_ref = (bill.get("VendorRef") or {}).get("value")
        sb_vend_id = qbo_vend_id_to_sb.get(vend_ref)
        if not sb_vend_id:
            summary["skipped_bills"] += 1
            continue
        total = float(bill.get("TotalAmt") or 0)
        balance = float(bill.get("Balance") or 0)
        status = "paid" if balance < 0.005 else ("partially_paid" if balance < total - 0.005 else "open")
        bill_row = {
            "company_id": sb_id,
            "vendor_id": sb_vend_id,
            "number": (bill.get("DocNumber") or None),
            "date": bill.get("TxnDate"),
            "due_date": bill.get("DueDate"),
            "status": status,
            "memo": bill.get("PrivateNote"),
            "subtotal": round(total, 2),
            "tax_total": 0,
            "total": round(total, 2),
            "balance": round(balance, 2),
            "currency": (bill.get("CurrencyRef") or {}).get("value", "USD"),
            "qbo_id": qbo_id,
        }
        resp = await _sb_request(
            "POST", "/bills",
            params={"on_conflict": "company_id,qbo_id"},
            json_body=bill_row,
            prefer="return=representation,resolution=merge-duplicates",
        )
        if resp.status_code >= 300:
            logger.warning("Bill upsert failed: %s", resp.text[:300])
            summary["skipped_bills"] += 1
            continue
        result = resp.json() if resp.text else []
        sb_bill = result[0] if isinstance(result, list) and result else None
        if not sb_bill:
            summary["skipped_bills"] += 1
            continue
        await _sb_delete("bill_lines", {"bill_id": f"eq.{sb_bill['id']}"})
        line_rows = []
        line_no = 0
        for ln in bill.get("Line") or []:
            if ln.get("DetailType") != "AccountBasedExpenseLineDetail":
                continue
            d = ln.get("AccountBasedExpenseLineDetail") or {}
            acct_ref = (d.get("AccountRef") or {}).get("value")
            amount = float(ln.get("Amount") or 0)
            line_no += 1
            line_rows.append({
                "bill_id": sb_bill["id"],
                "line_no": line_no,
                "description": ln.get("Description"),
                "quantity": 1,
                "unit_price": amount,
                "amount": round(amount, 2),
                "tax_rate": 0,
                "tax_amount": 0,
                "coa_account_id": _line_coa_id(acct_ref),
            })
        if line_rows:
            await _sb_request("POST", "/bill_lines", json_body=line_rows, prefer="return=minimal")
        summary["bills"] += 1

    return {
        "preview": False,
        "source_company": src_dict["name"],
        "dest_company": dest_dict["name"],
        **summary,
        "new_coa_count": len(created_accounts),
    }


# ---------- Payment match suggestions (M4) ----------

@app.get("/api/payments/match-suggestions/{plaid_txn_id}")
async def match_suggestions(
    plaid_txn_id: str,
    kind: str = "invoice",  # "invoice" | "bill"
    date_window_days: int = 14,
    amount_tolerance_pct: float = 0.01,  # 1% of amount
    top_n: int = 5,
    authorization: str = Header(None),
):
    """Given a Plaid transaction id, suggest open invoices (for inflows) or
    bills (for outflows) that could be this payment."""
    token = _extract_token(authorization)
    user = get_current_user(token)

    # Load the transaction
    tx_rows = await _sb_select("transactions", {
        "id": f"eq.{plaid_txn_id}",
        "select": "id,company_id,date,amount,merchant_name,description",
        "limit": "1",
    })
    if not tx_rows:
        raise HTTPException(status_code=404, detail="Transaction not found")
    tx = tx_rows[0]
    await _resolve_manual_company_from_supabase_id(tx["company_id"], user)

    sb_id = tx["company_id"]
    tx_amt = abs(float(tx["amount"] or 0))
    tx_date = tx.get("date") or ""
    merch = (tx.get("merchant_name") or tx.get("description") or "").lower()

    tol = max(0.05, tx_amt * amount_tolerance_pct)
    lo = max(0.0, tx_amt - tol)
    hi = tx_amt + tol

    # Date window
    try:
        from datetime import date as _d
        tx_d = _d.fromisoformat(tx_date) if tx_date else None
    except Exception:
        tx_d = None

    if kind == "invoice":
        rows = await _sb_select("invoices", {
            "company_id": f"eq.{sb_id}",
            "status": "not.in.(paid,void)",
            "balance": f"gte.{lo}",
            "and": f"(balance.lte.{hi})",
            "select": "id,number,date,due_date,total,balance,customer_id",
            "order": "date.desc",
            "limit": "50",
        })
        # Hydrate customer names
        cust_ids = list({r["customer_id"] for r in rows if r.get("customer_id")})
        cmap: dict = {}
        if cust_ids:
            cs = await _sb_select("customers", {
                "id": f"in.({','.join(cust_ids)})",
                "select": "id,display_name",
            })
            cmap = {c["id"]: c for c in cs}
        for r in rows:
            r["party"] = cmap.get(r.get("customer_id"))
    else:  # bill
        rows = await _sb_select("bills", {
            "company_id": f"eq.{sb_id}",
            "status": "not.in.(paid,void)",
            "balance": f"gte.{lo}",
            "and": f"(balance.lte.{hi})",
            "select": "id,number,date,due_date,total,balance,vendor_id",
            "order": "date.desc",
            "limit": "50",
        })
        vend_ids = list({r["vendor_id"] for r in rows if r.get("vendor_id")})
        vmap: dict = {}
        if vend_ids:
            vs = await _sb_select("vendors", {
                "id": f"in.({','.join(vend_ids)})",
                "select": "id,display_name",
            })
            vmap = {v["id"]: v for v in vs}
        for r in rows:
            r["party"] = vmap.get(r.get("vendor_id"))

    # Score each candidate
    def score(r):
        amt_diff = abs(float(r["balance"]) - tx_amt)
        s = max(0.0, 1 - amt_diff / max(tol, 0.01)) * 0.5
        if tx_d and r.get("date"):
            try:
                from datetime import date as _d2
                r_date = _d2.fromisoformat(r["date"][:10])
                days = abs((tx_d - r_date).days)
                s += max(0.0, 1 - days / max(date_window_days, 1)) * 0.3
            except Exception:
                pass
        # Name proximity
        name = (r.get("party") or {}).get("display_name", "").lower()
        if merch and name and (name in merch or merch in name):
            s += 0.2
        return s

    scored = [(r, score(r)) for r in rows]
    scored.sort(key=lambda x: x[1], reverse=True)
    return {
        "transaction": {
            "id": tx["id"], "date": tx["date"], "amount": tx["amount"],
            "merchant_name": tx.get("merchant_name"),
        },
        "kind": kind,
        "candidates": [
            {**r, "score": round(s, 3)}
            for r, s in scored[:top_n]
        ],
    }


class MatchApplyBody(BaseModel):
    plaid_txn_id: str
    invoice_id: Optional[str] = None
    bill_id: Optional[str] = None
    amount: float
    payment_method: Optional[str] = None
    reference: Optional[str] = None
    memo: Optional[str] = None


@app.post("/api/payments/apply-match")
async def apply_match(body: MatchApplyBody, authorization: str = Header(None)):
    """One-click: create a payment + application from a Plaid transaction."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    if (body.invoice_id is None) == (body.bill_id is None):
        raise HTTPException(status_code=400, detail="Exactly one of invoice_id or bill_id required")
    tx_rows = await _sb_select("transactions", {
        "id": f"eq.{body.plaid_txn_id}",
        "select": "id,company_id,date,account_id", "limit": "1",
    })
    if not tx_rows:
        raise HTTPException(status_code=404, detail="Transaction not found")
    tx = tx_rows[0]
    company = await _resolve_manual_company_from_supabase_id(tx["company_id"], user)

    return await record_payment(
        company["id"],
        PaymentBody(
            date=tx["date"],
            amount=body.amount,
            kind="invoice_payment" if body.invoice_id else "bill_payment",
            bank_account_id=tx.get("account_id"),
            matched_transaction_id=tx["id"],
            payment_method=body.payment_method,
            reference=body.reference,
            memo=body.memo,
            applications=[PaymentApplyBody(
                invoice_id=body.invoice_id,
                bill_id=body.bill_id,
                amount=body.amount,
            )],
        ),
        authorization,
    )


# ---------- Credit Memos (M5) ----------
# LineBody is used by invoice/bill/credit-memo bodies. Defined here (before
# CreditMemoBody) because the M3/M4/M5 block was inserted above the M1/M2
# definitions. The later M1/M2 redefinition is harmless (same class shape).


class LineBody(BaseModel):
    description: Optional[str] = None
    quantity: float = 1
    unit_price: float = 0
    tax_rate: float = 0  # as decimal, e.g. 0.0825 = 8.25%
    coa_account_id: Optional[str] = None


class CreditMemoBody(BaseModel):
    customer_id: str
    number: Optional[str] = None
    date: str
    memo: Optional[str] = None
    lines: List[LineBody]


@app.get("/api/credit-memos/{company_id}")
async def list_credit_memos(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    rows = await _sb_select("credit_memos", {
        "company_id": f"eq.{company['supabase_company_id']}",
        "select": "*", "order": "date.desc", "limit": "500",
    })
    cust_ids = list({r["customer_id"] for r in rows if r.get("customer_id")})
    cmap: dict = {}
    if cust_ids:
        cs = await _sb_select("customers", {
            "id": f"in.({','.join(cust_ids)})", "select": "id,display_name",
        })
        cmap = {c["id"]: c for c in cs}
    for r in rows:
        r["customer"] = cmap.get(r.get("customer_id"))
    return {"credit_memos": rows}


@app.post("/api/credit-memos/{company_id}")
async def create_credit_memo(
    company_id: str, body: CreditMemoBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.lines:
        raise HTTPException(status_code=400, detail="Credit memo needs at least one line")
    subtotal, tax_total, total, lines = _compute_doc_totals(body.lines)
    cm = await _sb_insert("credit_memos", {
        "company_id": company["supabase_company_id"],
        "customer_id": body.customer_id,
        "number": body.number,
        "date": body.date,
        "status": "open",
        "memo": body.memo,
        "subtotal": subtotal, "tax_total": tax_total,
        "total": total, "balance": total,
    })
    for l in lines:
        l_row = {k: v for k, v in l.items() if k != "tax_rate" and k != "tax_amount"}
        l_row["credit_memo_id"] = cm["id"]
        l["credit_memo_id"] = cm["id"]
    if lines:
        await _sb_request(
            "POST", "/credit_memo_lines",
            json_body=[{k: v for k, v in l.items() if k not in ("tax_rate", "tax_amount")} for l in lines],
            prefer="return=minimal",
        )
    return {"credit_memo": cm}


class CreditMemoApplyBody(BaseModel):
    invoice_id: str
    amount: float


@app.post("/api/credit-memos/{credit_memo_id}/apply")
async def apply_credit_memo(
    credit_memo_id: str, body: CreditMemoApplyBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    cm_rows = await _sb_select("credit_memos", {
        "id": f"eq.{credit_memo_id}",
        "select": "id,company_id,balance,total", "limit": "1",
    })
    if not cm_rows:
        raise HTTPException(status_code=404, detail="Credit memo not found")
    cm = cm_rows[0]
    await _resolve_manual_company_from_supabase_id(cm["company_id"], user)
    if float(cm["balance"]) < body.amount - 0.005:
        raise HTTPException(status_code=400, detail="Credit memo balance insufficient")

    inv_rows = await _sb_select("invoices", {
        "id": f"eq.{body.invoice_id}",
        "select": "id,company_id,balance,total", "limit": "1",
    })
    if not inv_rows or inv_rows[0]["company_id"] != cm["company_id"]:
        raise HTTPException(status_code=404, detail="Invoice not found or mismatched company")
    inv = inv_rows[0]
    if float(inv["balance"]) < body.amount - 0.005:
        raise HTTPException(status_code=400, detail="Invoice balance insufficient")

    await _sb_insert("credit_memo_applications", {
        "credit_memo_id": credit_memo_id,
        "invoice_id": body.invoice_id,
        "amount": round(body.amount, 2),
    })
    # Reduce both balances
    new_cm_bal = round(float(cm["balance"]) - body.amount, 2)
    cm_status = "applied" if new_cm_bal < 0.005 else "partially_applied"
    await _sb_update("credit_memos", {"id": f"eq.{credit_memo_id}"},
                     {"balance": new_cm_bal, "status": cm_status})
    new_inv_bal = round(float(inv["balance"]) - body.amount, 2)
    inv_status = _new_status_after_payment(float(inv["total"]), new_inv_bal, False)
    await _sb_update("invoices", {"id": f"eq.{body.invoice_id}"},
                     {"balance": new_inv_bal, "status": inv_status,
                      "updated_at": datetime.now(timezone.utc).isoformat()})
    return {"ok": True, "credit_memo_balance": new_cm_bal, "invoice_balance": new_inv_bal}


@app.delete("/api/credit-memos/{credit_memo_id}")
async def delete_credit_memo(credit_memo_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("credit_memos", {
        "id": f"eq.{credit_memo_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Credit memo not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    # Reverse applications
    apps = await _sb_select("credit_memo_applications", {
        "credit_memo_id": f"eq.{credit_memo_id}", "select": "invoice_id,amount",
    })
    for a in apps:
        inv_rows = await _sb_select("invoices", {
            "id": f"eq.{a['invoice_id']}", "select": "id,total,balance", "limit": "1",
        })
        if inv_rows:
            inv = inv_rows[0]
            new_bal = round(float(inv["balance"]) + float(a["amount"]), 2)
            new_status = _new_status_after_payment(float(inv["total"]), new_bal, False)
            await _sb_update("invoices", {"id": f"eq.{a['invoice_id']}"},
                             {"balance": new_bal, "status": new_status})
    await _sb_delete("credit_memos", {"id": f"eq.{credit_memo_id}"})
    return {"ok": True}


# ---------- Recurring Invoices (M5) ----------

class RecurringInvoiceBody(BaseModel):
    customer_id: str
    name: str
    frequency: str  # weekly | monthly | quarterly | annual
    start_date: str
    end_date: Optional[str] = None
    template_json: dict   # { lines: [...], memo: "...", terms: "...", due_days_offset: 0 }


@app.get("/api/recurring-invoices/{company_id}")
async def list_recurring_invoices(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    rows = await _sb_select("recurring_invoices", {
        "company_id": f"eq.{company['supabase_company_id']}",
        "select": "*", "order": "next_run_date.asc",
    })
    cust_ids = list({r["customer_id"] for r in rows if r.get("customer_id")})
    cmap: dict = {}
    if cust_ids:
        cs = await _sb_select("customers", {
            "id": f"in.({','.join(cust_ids)})", "select": "id,display_name",
        })
        cmap = {c["id"]: c for c in cs}
    for r in rows:
        r["customer"] = cmap.get(r.get("customer_id"))
    return {"recurring_invoices": rows}


def _next_date(current_iso: str, frequency: str) -> str:
    from datetime import date as _d
    d = _d.fromisoformat(current_iso)
    if frequency == "weekly":
        nd = d + timedelta(days=7)
    elif frequency == "monthly":
        m = d.month + 1
        y = d.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        last = calendar.monthrange(y, m)[1]
        nd = d.replace(year=y, month=m, day=min(d.day, last))
    elif frequency == "quarterly":
        m = d.month + 3
        y = d.year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        last = calendar.monthrange(y, m)[1]
        nd = d.replace(year=y, month=m, day=min(d.day, last))
    elif frequency == "annual":
        try:
            nd = d.replace(year=d.year + 1)
        except ValueError:
            nd = d.replace(year=d.year + 1, day=28)
    else:
        nd = d + timedelta(days=30)
    return nd.isoformat()


@app.post("/api/recurring-invoices/{company_id}")
async def create_recurring_invoice(
    company_id: str, body: RecurringInvoiceBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if body.frequency not in ("weekly", "monthly", "quarterly", "annual"):
        raise HTTPException(status_code=400, detail="Invalid frequency")
    row = await _sb_insert("recurring_invoices", {
        "company_id": company["supabase_company_id"],
        "customer_id": body.customer_id,
        "name": body.name,
        "frequency": body.frequency,
        "start_date": body.start_date,
        "end_date": body.end_date,
        "next_run_date": body.start_date,
        "is_active": True,
        "template_json": body.template_json,
    })
    return {"recurring_invoice": row}


@app.delete("/api/recurring-invoices/{recurring_id}")
async def delete_recurring_invoice(recurring_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("recurring_invoices", {
        "id": f"eq.{recurring_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Recurring not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    await _sb_delete("recurring_invoices", {"id": f"eq.{recurring_id}"})
    return {"ok": True}


@app.post("/api/recurring-invoices/process")
async def process_recurring_invoices(authorization: str = Header(None)):
    """Materialize all recurring invoices whose next_run_date is <= today.
    Creates a draft invoice for each, then advances next_run_date."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    today = datetime.now().date().isoformat()

    # Find all active recurring invoices in this user's org-accessible manual companies
    db = get_db()
    try:
        sb_ids = [r["supabase_company_id"] for r in db.execute(
            "SELECT supabase_company_id FROM companies "
            "WHERE org_id = ? AND source = 'manual' AND supabase_company_id IS NOT NULL",
            (org_id,),
        ).fetchall() if r["supabase_company_id"]]
    finally:
        db.close()
    if not sb_ids:
        return {"processed": 0, "created": []}

    due = await _sb_select("recurring_invoices", {
        "company_id": f"in.({','.join(sb_ids)})",
        "is_active": "eq.true",
        "next_run_date": f"lte.{today}",
        "select": "*", "limit": "1000",
    })
    created = []
    for r in due:
        template = r.get("template_json") or {}
        lines_t = template.get("lines") or []
        if not lines_t:
            continue
        due_days = int(template.get("due_days_offset") or 30)
        try:
            tx_date = r["next_run_date"]
            due_date = (datetime.fromisoformat(tx_date) + timedelta(days=due_days)).date().isoformat()
            lines_body = [LineBody(**l) for l in lines_t]
            subtotal, tax_total, total, persisted = _compute_doc_totals(lines_body)
            inv_row = {
                "company_id": r["company_id"],
                "customer_id": r["customer_id"],
                "number": f"REC-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "date": tx_date,
                "due_date": due_date,
                "status": "draft",
                "memo": template.get("memo"),
                "terms": template.get("terms"),
                "subtotal": subtotal, "tax_total": tax_total,
                "total": total, "balance": total,
                "currency": template.get("currency", "USD"),
            }
            inv = await _sb_insert("invoices", inv_row)
            for l in persisted:
                l["invoice_id"] = inv["id"]
            if persisted:
                await _sb_request("POST", "/invoice_lines", json_body=persisted, prefer="return=minimal")
            new_next = _next_date(r["next_run_date"], r["frequency"])
            stop = r.get("end_date") and new_next > r["end_date"]
            await _sb_update("recurring_invoices", {"id": f"eq.{r['id']}"}, {
                "next_run_date": new_next,
                "is_active": not stop,
            })
            created.append({"recurring_id": r["id"], "invoice_id": inv["id"], "number": inv_row["number"]})
        except Exception as e:
            logger.warning("Recurring run failed for %s: %s", r.get("id"), str(e)[:200])
    return {"processed": len(due), "created": created}


# ---------- Email invoice (M5) ----------

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
INVOICE_PDF_FROM_EMAIL = os.environ.get(
    "INVOICE_PDF_FROM_EMAIL", "billing@consolidatedreport.app",
)


class EmailInvoiceBody(BaseModel):
    to_email: str
    subject: Optional[str] = None
    body_html: Optional[str] = None  # optional override; otherwise we use a default template


@app.post("/api/invoices/{invoice_id}/email")
async def email_invoice(
    invoice_id: str, body: EmailInvoiceBody, authorization: str = Header(None),
):
    if not RESEND_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Email not configured. Set RESEND_API_KEY on Railway to enable.",
        )
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("invoices", {
        "id": f"eq.{invoice_id}",
        "select": "id,company_id,number,date,due_date,total,balance,memo,customer_id",
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Invoice not found")
    inv = rows[0]
    company = await _resolve_manual_company_from_supabase_id(inv["company_id"], user)

    subject = body.subject or f"Invoice {inv.get('number') or ''} from {company['name']}".strip()
    if not body.body_html:
        body_html = (
            f"<p>Thanks for your business.</p>"
            f"<p>Invoice <strong>{inv.get('number') or ''}</strong> — total "
            f"<strong>${float(inv['total']):.2f}</strong> — due {inv.get('due_date') or 'on receipt'}.</p>"
            f"<p>Balance owing: <strong>${float(inv['balance']):.2f}</strong>.</p>"
            f"<p>— {company['name']}</p>"
        )
    else:
        body_html = body.body_html

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                     "Content-Type": "application/json"},
            json={
                "from": INVOICE_PDF_FROM_EMAIL,
                "to": body.to_email,
                "subject": subject,
                "html": body_html,
            },
        )
    if r.status_code >= 300:
        logger.error("Resend send failed %s: %s", r.status_code, r.text[:300])
        raise HTTPException(status_code=502, detail=f"Email send failed ({r.status_code})")
    return {"ok": True, "message_id": (r.json() or {}).get("id")}


# =====================================================================
#  AR / AP AGING REPORTS
# =====================================================================


def _age_bucket(days_overdue: int) -> str:
    if days_overdue <= 0: return "current"
    if days_overdue <= 30: return "d1_30"
    if days_overdue <= 60: return "d31_60"
    if days_overdue <= 90: return "d61_90"
    return "d90_plus"


async def _aging_report(
    sb_company_id: str,
    as_of: str,
    kind: str,  # "invoice" | "bill"
) -> dict:
    """Build an aging report by party (customer or vendor)."""
    from datetime import date as _d
    try:
        as_of_d = _d.fromisoformat(as_of)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid as_of date")

    if kind == "invoice":
        rows = await _sb_select("invoices", {
            "company_id": f"eq.{sb_company_id}",
            "status": "not.in.(paid,void)",
            "date": f"lte.{as_of}",
            "select": "id,number,date,due_date,total,balance,customer_id",
            "order": "date.asc",
            "limit": "5000",
        })
        party_ids = list({r["customer_id"] for r in rows if r.get("customer_id")})
        party_rows = await _sb_select("customers", {
            "id": f"in.({','.join(party_ids)})", "select": "id,display_name",
        }) if party_ids else []
    else:
        rows = await _sb_select("bills", {
            "company_id": f"eq.{sb_company_id}",
            "status": "not.in.(paid,void)",
            "date": f"lte.{as_of}",
            "select": "id,number,date,due_date,total,balance,vendor_id",
            "order": "date.asc",
            "limit": "5000",
        })
        party_ids = list({r["vendor_id"] for r in rows if r.get("vendor_id")})
        party_rows = await _sb_select("vendors", {
            "id": f"in.({','.join(party_ids)})", "select": "id,display_name",
        }) if party_ids else []

    party_map = {p["id"]: p["display_name"] for p in party_rows}

    # Group by party
    grouped: dict = {}
    totals = {"current": 0.0, "d1_30": 0.0, "d31_60": 0.0,
              "d61_90": 0.0, "d90_plus": 0.0, "total": 0.0}

    for r in rows:
        balance = float(r.get("balance") or 0)
        if balance <= 0.005:
            continue
        due_str = r.get("due_date") or r.get("date")
        try:
            due_d = _d.fromisoformat(due_str)
            days_overdue = (as_of_d - due_d).days
        except Exception:
            days_overdue = 0
        bucket = _age_bucket(days_overdue)
        pid = r.get("customer_id" if kind == "invoice" else "vendor_id")

        if pid not in grouped:
            grouped[pid] = {
                "party_id": pid,
                "party_name": party_map.get(pid, "(unknown)"),
                "current": 0.0, "d1_30": 0.0, "d31_60": 0.0,
                "d61_90": 0.0, "d90_plus": 0.0, "total": 0.0,
                "docs": [],
            }
        g = grouped[pid]
        g[bucket] += balance
        g["total"] += balance
        g["docs"].append({
            "id": r["id"], "number": r.get("number"),
            "date": r["date"], "due_date": r.get("due_date"),
            "total": float(r.get("total") or 0), "balance": balance,
            "days_overdue": days_overdue, "bucket": bucket,
        })
        totals[bucket] += balance
        totals["total"] += balance

    parties = sorted(grouped.values(), key=lambda g: g["total"], reverse=True)
    # Round all
    for g in parties:
        for k in ("current", "d1_30", "d31_60", "d61_90", "d90_plus", "total"):
            g[k] = round(g[k], 2)
    for k in totals:
        totals[k] = round(totals[k], 2)

    return {"as_of": as_of, "kind": kind, "parties": parties, "totals": totals}


@app.get("/api/reports/ar-aging/{company_id}")
async def ar_aging(
    company_id: str,
    as_of: Optional[str] = None,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    as_of = as_of or datetime.now().date().isoformat()
    return await _aging_report(company["supabase_company_id"], as_of, "invoice")


@app.get("/api/reports/ap-aging/{company_id}")
async def ap_aging(
    company_id: str,
    as_of: Optional[str] = None,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    as_of = as_of or datetime.now().date().isoformat()
    return await _aging_report(company["supabase_company_id"], as_of, "bill")


# =====================================================================
#  TRANSFER DETECTION (intercompany + intracompany)
# =====================================================================


class TransferDetectRequest(BaseModel):
    date_from: str
    date_to: str
    amount_tolerance_pct: Optional[float] = 0.005  # 0.5%
    date_window_days: Optional[int] = 3
    same_company_only: Optional[bool] = False


@app.post("/api/transfers/detect")
async def detect_transfers(
    body: TransferDetectRequest, authorization: str = Header(None),
):
    """Scan transactions across the user's accessible manual companies and
    suggest transfer pairs: an outflow in one account matched by an inflow
    of equal magnitude in another account within date_window_days.

    Returns suggested pairs — does NOT auto-mark. Use /api/transfers/confirm
    to commit a pair.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)

    # Pull all user-accessible manual companies' supabase ids
    db = get_db()
    try:
        rows = db.execute(
            """SELECT c.supabase_company_id, c.id as v2_id, c.name
                 FROM companies c
                 JOIN user_company_access uca ON uca.company_id = c.id
                WHERE c.org_id = ? AND uca.user_id = ?
                  AND c.source = 'manual' AND c.supabase_company_id IS NOT NULL""",
            (org_id, user["id"]),
        ).fetchall()
        companies = [dict(r) for r in rows]
    finally:
        db.close()
    if not companies:
        return {"pairs": [], "scanned": 0}

    sb_ids = [c["supabase_company_id"] for c in companies]
    sb_to_v2 = {c["supabase_company_id"]: c for c in companies}

    # Fetch all candidate transactions: non-transfer, no transfer_pair_id,
    # within date range, across these companies.
    txs: list = []
    offset = 0
    while True:
        chunk = await _sb_select("transactions", {
            "company_id": f"in.({','.join(sb_ids)})",
            "is_transfer": "eq.false",
            "transfer_pair_id": "is.null",
            "and": f"(date.gte.{body.date_from},date.lte.{body.date_to})",
            "select": "id,company_id,account_id,date,amount,merchant_name,description",
            "order": "id",
            "limit": "1000",
            "offset": str(offset),
        })
        txs.extend(chunk)
        if len(chunk) < 1000:
            break
        offset += 1000

    # Index by magnitude bucket for quick pairing
    from datetime import date as _d
    window = body.date_window_days or 3
    tol = body.amount_tolerance_pct or 0.005

    # Split into inflows (amount < 0 in Plaid convention) and outflows (amount > 0)
    outflows = [t for t in txs if float(t.get("amount") or 0) > 0.005]
    inflows  = [t for t in txs if float(t.get("amount") or 0) < -0.005]

    # Build inflow lookup by rounded magnitude to the cent
    from collections import defaultdict
    inflow_by_amt = defaultdict(list)
    for t in inflows:
        key = round(abs(float(t["amount"])), 2)
        inflow_by_amt[key].append(t)

    def _parse_d(s):
        try: return _d.fromisoformat(s[:10])
        except Exception: return None

    pairs: list = []
    used_inflow_ids = set()
    for out in outflows:
        out_amt = round(abs(float(out["amount"])), 2)
        out_date = _parse_d(out.get("date"))
        out_co = out.get("company_id")
        # Consider exact amount first, then amounts within tolerance
        candidate_amts = {out_amt}
        if tol > 0:
            delta = max(0.01, out_amt * tol)
            # Check a narrow band of possible inflow amounts
            for cents in range(max(1, int((out_amt - delta) * 100)),
                               int((out_amt + delta) * 100) + 1):
                candidate_amts.add(round(cents / 100, 2))
        best = None
        best_score = 0.0
        for amt in candidate_amts:
            for inc in inflow_by_amt.get(amt, []):
                if inc["id"] in used_inflow_ids:
                    continue
                inc_co = inc.get("company_id")
                if body.same_company_only and inc_co != out_co:
                    continue
                # At minimum, pair must be a DIFFERENT account (otherwise it's
                # just posting noise, not a transfer)
                if inc.get("account_id") == out.get("account_id"):
                    continue
                inc_date = _parse_d(inc.get("date"))
                if not out_date or not inc_date:
                    continue
                days_diff = abs((inc_date - out_date).days)
                if days_diff > window:
                    continue
                # Score: exact amount + closeness in date + merchant similarity
                s = 0.5  # base: amounts match within tolerance
                s += max(0, 1 - days_diff / max(window, 1)) * 0.3
                # Name similarity (cheap check)
                om = (out.get("merchant_name") or out.get("description") or "").lower()
                im = (inc.get("merchant_name") or inc.get("description") or "").lower()
                if om and im:
                    tokens_o = set(om.split())
                    tokens_i = set(im.split())
                    overlap = len(tokens_o & tokens_i)
                    if overlap:
                        s += min(0.2, overlap * 0.05)
                # Intercompany = natural transfer, bump score
                if out_co != inc_co:
                    s += 0.05
                if s > best_score:
                    best_score = s
                    best = inc
        if best:
            used_inflow_ids.add(best["id"])
            pairs.append({
                "score": round(best_score, 3),
                "days_diff": abs((_parse_d(out["date"]) - _parse_d(best["date"])).days),
                "outflow": {
                    "id": out["id"],
                    "date": out["date"],
                    "amount": float(out["amount"]),
                    "merchant": out.get("merchant_name") or out.get("description"),
                    "company_id": out["company_id"],
                    "company_name": sb_to_v2.get(out["company_id"], {}).get("name"),
                    "account_id": out.get("account_id"),
                },
                "inflow": {
                    "id": best["id"],
                    "date": best["date"],
                    "amount": float(best["amount"]),
                    "merchant": best.get("merchant_name") or best.get("description"),
                    "company_id": best["company_id"],
                    "company_name": sb_to_v2.get(best["company_id"], {}).get("name"),
                    "account_id": best.get("account_id"),
                },
                "is_intercompany": out["company_id"] != best["company_id"],
            })
    pairs.sort(key=lambda p: p["score"], reverse=True)
    return {"pairs": pairs[:500], "scanned": len(txs),
            "outflows_scanned": len(outflows),
            "inflows_scanned": len(inflows)}


class TransferConfirmBody(BaseModel):
    outflow_id: str
    inflow_id: str


@app.post("/api/transfers/confirm")
async def confirm_transfer(
    body: TransferConfirmBody, authorization: str = Header(None),
):
    """Mark both transactions as a transfer pair with a shared transfer_pair_id
    and is_transfer=true so they drop out of P&L."""
    token = _extract_token(authorization)
    user = get_current_user(token)

    # Validate both transactions belong to user-accessible companies
    rows = await _sb_select("transactions", {
        "id": f"in.({body.outflow_id},{body.inflow_id})",
        "select": "id,company_id", "limit": "2",
    })
    if len(rows) != 2:
        raise HTTPException(status_code=404, detail="Both transactions not found")
    for r in rows:
        await _resolve_manual_company_from_supabase_id(r["company_id"], user)

    pair_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    await _sb_update("transactions", {"id": f"in.({body.outflow_id},{body.inflow_id})"}, {
        "transfer_pair_id": pair_id,
        "is_transfer": True,
        "categorized_by": "transfer",
        "category_id": None,
        "updated_at": now,
    })
    return {"ok": True, "transfer_pair_id": pair_id}


@app.post("/api/transfers/unpair")
async def unpair_transfer(
    body: TransferConfirmBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("transactions", {
        "id": f"in.({body.outflow_id},{body.inflow_id})",
        "select": "id,company_id", "limit": "2",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Transactions not found")
    for r in rows:
        await _resolve_manual_company_from_supabase_id(r["company_id"], user)
    await _sb_update("transactions", {"id": f"in.({body.outflow_id},{body.inflow_id})"}, {
        "transfer_pair_id": None,
        "is_transfer": False,
        "categorized_by": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return {"ok": True}


# ---------- Categories list (for inline pickers) ----------

@app.get("/api/categories/{company_id}")
async def list_categories(company_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_company_id = company["supabase_company_id"]

    # Categories are auto-mirrored from CoA. Join to CoA for type info.
    cats = await _sb_select("categories", {
        "company_id": f"eq.{sb_company_id}",
        "select": "id,name,coa_account_id,parent_id",
        "order": "name.asc",
    })
    coa_ids = list({c["coa_account_id"] for c in cats if c.get("coa_account_id")})
    coa_map: dict = {}
    if coa_ids:
        coa = await _sb_select("chart_of_accounts", {
            "id": f"in.({','.join(coa_ids)})",
            "is_active": "eq.true",
            "select": "id,code,type",
        })
        coa_map = {c["id"]: c for c in coa}
    for c in cats:
        coa = coa_map.get(c.get("coa_account_id"))
        c["type"] = coa.get("type") if coa else None
        c["code"] = coa.get("code") if coa else None
    # Drop categories whose CoA was archived
    cats = [c for c in cats if c["type"]]
    return {"categories": cats}


# =====================================================================
#  SALES (Invoices) + EXPENSES (Bills) — AR/AP CRUD
# =====================================================================


# ---------- customers ----------

class CustomerBody(BaseModel):
    display_name: str
    company_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    billing_address: Optional[dict] = None
    shipping_address: Optional[dict] = None
    terms_days: Optional[int] = 30
    default_account_id: Optional[str] = None
    notes: Optional[str] = None
    is_active: Optional[bool] = True


@app.get("/api/customers/{company_id}")
async def list_customers(
    company_id: str,
    search: Optional[str] = None,
    active_only: bool = True,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_id = company["supabase_company_id"]
    params = {
        "company_id": f"eq.{sb_id}",
        "select": "id,display_name,company_name,email,phone,billing_address,shipping_address,"
                   "terms_days,default_account_id,notes,is_active,qbo_id,created_at",
        "order": "display_name.asc",
        "limit": "1000",
    }
    if active_only:
        params["is_active"] = "eq.true"
    if search:
        safe = search.replace("(", "").replace(")", "").replace(",", " ").strip()
        params["or"] = f"(display_name.ilike.*{safe}*,email.ilike.*{safe}*,company_name.ilike.*{safe}*)"
    rows = await _sb_select("customers", params)

    # Hydrate balance = sum of open invoice balances
    if rows:
        invs = await _sb_select("invoices", {
            "company_id": f"eq.{sb_id}",
            "status": "not.in.(paid,void)",
            "select": "customer_id,balance",
            "limit": "5000",
        })
        by_c: dict = {}
        for inv in invs:
            k = inv["customer_id"]
            by_c[k] = by_c.get(k, 0.0) + float(inv.get("balance") or 0)
        for r in rows:
            r["balance"] = round(by_c.get(r["id"], 0.0), 2)
    return {"customers": rows}


@app.post("/api/customers/{company_id}")
async def create_customer(
    company_id: str, body: CustomerBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.display_name.strip():
        raise HTTPException(status_code=400, detail="display_name required")
    row = await _sb_insert("customers", {
        "company_id": company["supabase_company_id"],
        **body.model_dump(exclude_none=True),
    })
    return {"customer": row}


@app.patch("/api/customers/{customer_id}")
async def patch_customer(
    customer_id: str, body: CustomerBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("customers", {
        "id": f"eq.{customer_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    patch = body.model_dump(exclude_none=True)
    patch["updated_at"] = datetime.now(timezone.utc).isoformat()
    updated = await _sb_update("customers", {"id": f"eq.{customer_id}"}, patch)
    return {"customer": updated[0] if updated else None}


@app.delete("/api/customers/{customer_id}")
async def delete_customer(customer_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("customers", {
        "id": f"eq.{customer_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    # Soft delete to avoid FK issues with existing invoices
    await _sb_update("customers", {"id": f"eq.{customer_id}"}, {"is_active": False})
    return {"ok": True}


# ---------- vendors ----------

class VendorBody(BaseModel):
    display_name: str
    company_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    billing_address: Optional[dict] = None
    shipping_address: Optional[dict] = None
    terms_days: Optional[int] = 30
    default_account_id: Optional[str] = None
    is_1099: Optional[bool] = False
    tax_id: Optional[str] = None
    notes: Optional[str] = None
    is_active: Optional[bool] = True


@app.get("/api/vendors/{company_id}")
async def list_vendors(
    company_id: str,
    search: Optional[str] = None,
    active_only: bool = True,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    sb_id = company["supabase_company_id"]
    params = {
        "company_id": f"eq.{sb_id}",
        "select": "id,display_name,company_name,email,phone,billing_address,shipping_address,"
                   "terms_days,default_account_id,is_1099,tax_id,notes,is_active,qbo_id,created_at",
        "order": "display_name.asc",
        "limit": "1000",
    }
    if active_only:
        params["is_active"] = "eq.true"
    if search:
        safe = search.replace("(", "").replace(")", "").replace(",", " ").strip()
        params["or"] = f"(display_name.ilike.*{safe}*,email.ilike.*{safe}*,company_name.ilike.*{safe}*)"
    rows = await _sb_select("vendors", params)
    if rows:
        bills = await _sb_select("bills", {
            "company_id": f"eq.{sb_id}",
            "status": "not.in.(paid,void)",
            "select": "vendor_id,balance",
            "limit": "5000",
        })
        by_v: dict = {}
        for b in bills:
            k = b["vendor_id"]
            by_v[k] = by_v.get(k, 0.0) + float(b.get("balance") or 0)
        for r in rows:
            r["balance"] = round(by_v.get(r["id"], 0.0), 2)
    return {"vendors": rows}


@app.post("/api/vendors/{company_id}")
async def create_vendor(
    company_id: str, body: VendorBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.display_name.strip():
        raise HTTPException(status_code=400, detail="display_name required")
    row = await _sb_insert("vendors", {
        "company_id": company["supabase_company_id"],
        **body.model_dump(exclude_none=True),
    })
    return {"vendor": row}


@app.patch("/api/vendors/{vendor_id}")
async def patch_vendor(
    vendor_id: str, body: VendorBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("vendors", {
        "id": f"eq.{vendor_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Vendor not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    patch = body.model_dump(exclude_none=True)
    patch["updated_at"] = datetime.now(timezone.utc).isoformat()
    updated = await _sb_update("vendors", {"id": f"eq.{vendor_id}"}, patch)
    return {"vendor": updated[0] if updated else None}


@app.delete("/api/vendors/{vendor_id}")
async def delete_vendor(vendor_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("vendors", {
        "id": f"eq.{vendor_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Vendor not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    await _sb_update("vendors", {"id": f"eq.{vendor_id}"}, {"is_active": False})
    return {"ok": True}


# ---------- invoices & bills — shared helpers ----------

class LineBody(BaseModel):
    description: Optional[str] = None
    quantity: float = 1
    unit_price: float = 0
    tax_rate: float = 0  # as decimal, e.g. 0.0825 = 8.25%
    coa_account_id: Optional[str] = None


class InvoiceBody(BaseModel):
    customer_id: str
    number: str
    date: str          # YYYY-MM-DD
    due_date: Optional[str] = None
    status: Optional[str] = "draft"
    memo: Optional[str] = None
    terms: Optional[str] = None
    currency: Optional[str] = "USD"
    lines: List[LineBody]


class BillBody(BaseModel):
    vendor_id: str
    number: Optional[str] = None
    date: str
    due_date: Optional[str] = None
    status: Optional[str] = "open"
    memo: Optional[str] = None
    terms: Optional[str] = None
    currency: Optional[str] = "USD"
    lines: List[LineBody]


def _compute_doc_totals(lines: list) -> tuple:
    subtotal = 0.0
    tax_total = 0.0
    persisted_lines = []
    for i, l in enumerate(lines):
        qty = float(l.quantity or 0)
        unit = float(l.unit_price or 0)
        amt = round(qty * unit, 2)
        tax_rate = float(l.tax_rate or 0)
        tax = round(amt * tax_rate, 2)
        subtotal += amt
        tax_total += tax
        persisted_lines.append({
            "line_no": i + 1,
            "description": l.description,
            "quantity": qty,
            "unit_price": unit,
            "amount": amt,
            "tax_rate": tax_rate,
            "tax_amount": tax,
            "coa_account_id": l.coa_account_id,
        })
    total = round(subtotal + tax_total, 2)
    return round(subtotal, 2), round(tax_total, 2), total, persisted_lines


# ---------- invoices ----------

@app.get("/api/invoices/{company_id}")
async def list_invoices(
    company_id: str,
    status: Optional[str] = None,
    customer_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    params = {
        "company_id": f"eq.{company['supabase_company_id']}",
        "select": "id,customer_id,number,date,due_date,status,subtotal,tax_total,total,balance,"
                   "currency,memo,qbo_id,created_at,updated_at",
        "order": "date.desc,created_at.desc",
        "limit": str(max(1, min(limit, 500))),
        "offset": str(max(0, offset)),
    }
    and_clauses = []
    if status:       and_clauses.append(f"status.eq.{status}")
    if customer_id:  and_clauses.append(f"customer_id.eq.{customer_id}")
    if date_from:    and_clauses.append(f"date.gte.{date_from}")
    if date_to:      and_clauses.append(f"date.lte.{date_to}")
    if and_clauses:
        params["and"] = "(" + ",".join(and_clauses) + ")"
    invs = await _sb_select("invoices", params)

    # Enrich customer display_name
    cust_ids = list({i["customer_id"] for i in invs if i.get("customer_id")})
    cmap: dict = {}
    if cust_ids:
        cs = await _sb_select("customers", {
            "id": f"in.({','.join(cust_ids)})",
            "select": "id,display_name,email",
        })
        cmap = {c["id"]: c for c in cs}
    for i in invs:
        i["customer"] = cmap.get(i.get("customer_id"))
    return {"invoices": invs, "count": len(invs)}


@app.get("/api/invoices/detail/{invoice_id}")
async def get_invoice(invoice_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("invoices", {
        "id": f"eq.{invoice_id}", "select": "*", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Invoice not found")
    inv = rows[0]
    await _resolve_manual_company_from_supabase_id(inv["company_id"], user)
    lines = await _sb_select("invoice_lines", {
        "invoice_id": f"eq.{invoice_id}", "select": "*", "order": "line_no.asc",
    })
    cust = await _sb_select("customers", {
        "id": f"eq.{inv['customer_id']}", "select": "*", "limit": "1",
    })
    # Payments + applications
    apps = await _sb_select("payment_applications", {
        "invoice_id": f"eq.{invoice_id}", "select": "id,payment_id,amount",
    })
    pay_ids = list({a["payment_id"] for a in apps})
    payments: list = []
    if pay_ids:
        payments = await _sb_select("payments", {
            "id": f"in.({','.join(pay_ids)})",
            "select": "id,date,amount,payment_method,reference,memo,bank_account_id",
            "order": "date.desc",
        })
    return {"invoice": inv, "lines": lines,
            "customer": cust[0] if cust else None,
            "payment_applications": apps, "payments": payments}


@app.post("/api/invoices/{company_id}")
async def create_invoice(
    company_id: str, body: InvoiceBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.lines:
        raise HTTPException(status_code=400, detail="Invoice must have at least one line")
    subtotal, tax_total, total, lines = _compute_doc_totals(body.lines)
    inv = await _sb_insert("invoices", {
        "company_id": company["supabase_company_id"],
        "customer_id": body.customer_id,
        "number": body.number.strip(),
        "date": body.date,
        "due_date": body.due_date,
        "status": body.status or "draft",
        "memo": body.memo,
        "terms": body.terms,
        "subtotal": subtotal,
        "tax_total": tax_total,
        "total": total,
        "balance": total,
        "currency": body.currency or "USD",
        "created_by": SUPABASE_SYSTEM_USER_ID or None,
    })
    for l in lines:
        l["invoice_id"] = inv["id"]
    if lines:
        resp = await _sb_request(
            "POST", "/invoice_lines", json_body=lines,
            prefer="return=minimal",
        )
        if resp.status_code >= 300:
            await _sb_delete("invoices", {"id": f"eq.{inv['id']}"})
            raise HTTPException(status_code=502, detail="Failed to save invoice lines")
    return {"invoice": inv, "lines_count": len(lines)}


@app.patch("/api/invoices/{invoice_id}")
async def patch_invoice(
    invoice_id: str, body: InvoiceBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("invoices", {
        "id": f"eq.{invoice_id}",
        "select": "id,company_id,status,balance,total",
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Invoice not found")
    existing = rows[0]
    await _resolve_manual_company_from_supabase_id(existing["company_id"], user)
    if existing["status"] in ("paid",):
        # Allow edits on paid invoices only for memo/status; reject line changes
        pass

    subtotal, tax_total, total, lines = _compute_doc_totals(body.lines or [])
    # Preserve amount already paid
    paid = float(existing.get("total") or 0) - float(existing.get("balance") or 0)
    new_balance = round(total - paid, 2)
    await _sb_update("invoices", {"id": f"eq.{invoice_id}"}, {
        "customer_id": body.customer_id,
        "number": body.number.strip(),
        "date": body.date,
        "due_date": body.due_date,
        "status": body.status or existing["status"],
        "memo": body.memo,
        "terms": body.terms,
        "subtotal": subtotal,
        "tax_total": tax_total,
        "total": total,
        "balance": new_balance,
        "currency": body.currency or "USD",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    await _sb_delete("invoice_lines", {"invoice_id": f"eq.{invoice_id}"})
    for l in lines:
        l["invoice_id"] = invoice_id
    if lines:
        await _sb_request("POST", "/invoice_lines", json_body=lines, prefer="return=minimal")
    return {"ok": True}


@app.delete("/api/invoices/{invoice_id}")
async def delete_invoice(invoice_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("invoices", {
        "id": f"eq.{invoice_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Invoice not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    await _sb_delete("invoices", {"id": f"eq.{invoice_id}"})
    return {"ok": True}


# ---------- bills ----------

@app.get("/api/bills/{company_id}")
async def list_bills(
    company_id: str,
    status: Optional[str] = None,
    vendor_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    params = {
        "company_id": f"eq.{company['supabase_company_id']}",
        "select": "id,vendor_id,number,date,due_date,status,subtotal,tax_total,total,balance,"
                   "currency,memo,qbo_id,created_at,updated_at",
        "order": "date.desc,created_at.desc",
        "limit": str(max(1, min(limit, 500))),
        "offset": str(max(0, offset)),
    }
    and_clauses = []
    if status:     and_clauses.append(f"status.eq.{status}")
    if vendor_id:  and_clauses.append(f"vendor_id.eq.{vendor_id}")
    if date_from:  and_clauses.append(f"date.gte.{date_from}")
    if date_to:    and_clauses.append(f"date.lte.{date_to}")
    if and_clauses:
        params["and"] = "(" + ",".join(and_clauses) + ")"
    bills = await _sb_select("bills", params)
    vendor_ids = list({b["vendor_id"] for b in bills if b.get("vendor_id")})
    vmap: dict = {}
    if vendor_ids:
        vs = await _sb_select("vendors", {
            "id": f"in.({','.join(vendor_ids)})",
            "select": "id,display_name,email",
        })
        vmap = {v["id"]: v for v in vs}
    for b in bills:
        b["vendor"] = vmap.get(b.get("vendor_id"))
    return {"bills": bills, "count": len(bills)}


@app.get("/api/bills/detail/{bill_id}")
async def get_bill(bill_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("bills", {
        "id": f"eq.{bill_id}", "select": "*", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Bill not found")
    bill = rows[0]
    await _resolve_manual_company_from_supabase_id(bill["company_id"], user)
    lines = await _sb_select("bill_lines", {
        "bill_id": f"eq.{bill_id}", "select": "*", "order": "line_no.asc",
    })
    vend = await _sb_select("vendors", {
        "id": f"eq.{bill['vendor_id']}", "select": "*", "limit": "1",
    })
    apps = await _sb_select("payment_applications", {
        "bill_id": f"eq.{bill_id}", "select": "id,payment_id,amount",
    })
    pay_ids = list({a["payment_id"] for a in apps})
    payments: list = []
    if pay_ids:
        payments = await _sb_select("payments", {
            "id": f"in.({','.join(pay_ids)})",
            "select": "id,date,amount,payment_method,reference,memo,bank_account_id",
            "order": "date.desc",
        })
    return {"bill": bill, "lines": lines,
            "vendor": vend[0] if vend else None,
            "payment_applications": apps, "payments": payments}


@app.post("/api/bills/{company_id}")
async def create_bill(
    company_id: str, body: BillBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.lines:
        raise HTTPException(status_code=400, detail="Bill must have at least one line")
    subtotal, tax_total, total, lines = _compute_doc_totals(body.lines)
    bill = await _sb_insert("bills", {
        "company_id": company["supabase_company_id"],
        "vendor_id": body.vendor_id,
        "number": (body.number or "").strip() or None,
        "date": body.date,
        "due_date": body.due_date,
        "status": body.status or "open",
        "memo": body.memo,
        "terms": body.terms,
        "subtotal": subtotal,
        "tax_total": tax_total,
        "total": total,
        "balance": total,
        "currency": body.currency or "USD",
        "created_by": SUPABASE_SYSTEM_USER_ID or None,
    })
    for l in lines:
        l["bill_id"] = bill["id"]
    if lines:
        resp = await _sb_request(
            "POST", "/bill_lines", json_body=lines, prefer="return=minimal",
        )
        if resp.status_code >= 300:
            await _sb_delete("bills", {"id": f"eq.{bill['id']}"})
            raise HTTPException(status_code=502, detail="Failed to save bill lines")
    return {"bill": bill, "lines_count": len(lines)}


@app.patch("/api/bills/{bill_id}")
async def patch_bill(
    bill_id: str, body: BillBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("bills", {
        "id": f"eq.{bill_id}", "select": "id,company_id,status,balance,total",
        "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Bill not found")
    existing = rows[0]
    await _resolve_manual_company_from_supabase_id(existing["company_id"], user)
    subtotal, tax_total, total, lines = _compute_doc_totals(body.lines or [])
    paid = float(existing.get("total") or 0) - float(existing.get("balance") or 0)
    new_balance = round(total - paid, 2)
    await _sb_update("bills", {"id": f"eq.{bill_id}"}, {
        "vendor_id": body.vendor_id,
        "number": (body.number or "").strip() or None,
        "date": body.date,
        "due_date": body.due_date,
        "status": body.status or existing["status"],
        "memo": body.memo,
        "terms": body.terms,
        "subtotal": subtotal,
        "tax_total": tax_total,
        "total": total,
        "balance": new_balance,
        "currency": body.currency or "USD",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    await _sb_delete("bill_lines", {"bill_id": f"eq.{bill_id}"})
    for l in lines:
        l["bill_id"] = bill_id
    if lines:
        await _sb_request("POST", "/bill_lines", json_body=lines, prefer="return=minimal")
    return {"ok": True}


@app.delete("/api/bills/{bill_id}")
async def delete_bill(bill_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("bills", {
        "id": f"eq.{bill_id}", "select": "id,company_id", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Bill not found")
    await _resolve_manual_company_from_supabase_id(rows[0]["company_id"], user)
    await _sb_delete("bills", {"id": f"eq.{bill_id}"})
    return {"ok": True}


# ---------- payments ----------

class PaymentApplyBody(BaseModel):
    invoice_id: Optional[str] = None
    bill_id: Optional[str] = None
    amount: float


class PaymentBody(BaseModel):
    date: str
    amount: float
    kind: str  # invoice_payment | bill_payment | refund | vendor_credit_apply | customer_credit_apply
    bank_account_id: Optional[str] = None
    matched_transaction_id: Optional[str] = None
    payment_method: Optional[str] = None
    reference: Optional[str] = None
    memo: Optional[str] = None
    applications: List[PaymentApplyBody]


def _new_status_after_payment(total: float, new_balance: float, is_bill: bool = False) -> str:
    if new_balance <= 0.005:
        return "paid"
    if new_balance < total - 0.005:
        return "partially_paid"
    return "open" if is_bill else "sent"


@app.post("/api/payments/{company_id}")
async def record_payment(
    company_id: str, body: PaymentBody, authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    company = _get_manual_company_for_user(company_id, user)
    if not body.applications:
        raise HTTPException(status_code=400, detail="At least one application required")

    total_applied = sum(a.amount for a in body.applications)
    if abs(total_applied - body.amount) > 0.005:
        raise HTTPException(
            status_code=400,
            detail=f"Applications sum to {total_applied:.2f} but payment is {body.amount:.2f}",
        )

    # Validate targets belong to this company & pull current balances
    invoice_targets: dict = {}
    bill_targets: dict = {}
    for a in body.applications:
        if (a.invoice_id is None) == (a.bill_id is None):
            raise HTTPException(
                status_code=400,
                detail="Each application needs exactly one of invoice_id or bill_id",
            )
        if a.invoice_id:
            rows = await _sb_select("invoices", {
                "id": f"eq.{a.invoice_id}",
                "select": "id,company_id,total,balance",
                "limit": "1",
            })
            if not rows or rows[0]["company_id"] != company["supabase_company_id"]:
                raise HTTPException(status_code=404, detail=f"Invoice {a.invoice_id} not found")
            invoice_targets[a.invoice_id] = rows[0]
        else:
            rows = await _sb_select("bills", {
                "id": f"eq.{a.bill_id}",
                "select": "id,company_id,total,balance",
                "limit": "1",
            })
            if not rows or rows[0]["company_id"] != company["supabase_company_id"]:
                raise HTTPException(status_code=404, detail=f"Bill {a.bill_id} not found")
            bill_targets[a.bill_id] = rows[0]

    # Insert payment
    pay = await _sb_insert("payments", {
        "company_id": company["supabase_company_id"],
        "date": body.date,
        "amount": round(float(body.amount), 2),
        "kind": body.kind,
        "bank_account_id": body.bank_account_id,
        "matched_transaction_id": body.matched_transaction_id,
        "payment_method": body.payment_method,
        "reference": body.reference,
        "memo": body.memo,
        "created_by": SUPABASE_SYSTEM_USER_ID or None,
    })

    # Insert applications in bulk
    app_rows = []
    for a in body.applications:
        app_rows.append({
            "payment_id": pay["id"],
            "invoice_id": a.invoice_id,
            "bill_id": a.bill_id,
            "amount": round(float(a.amount), 2),
        })
    if app_rows:
        resp = await _sb_request(
            "POST", "/payment_applications",
            json_body=app_rows, prefer="return=minimal",
        )
        if resp.status_code >= 300:
            await _sb_delete("payments", {"id": f"eq.{pay['id']}"})
            raise HTTPException(status_code=502, detail="Failed to save applications")

    # Update balances + status on each target
    for inv_id, inv in invoice_targets.items():
        applied = sum(a.amount for a in body.applications if a.invoice_id == inv_id)
        new_bal = round(float(inv["balance"]) - applied, 2)
        new_status = _new_status_after_payment(float(inv["total"]), new_bal, False)
        await _sb_update("invoices", {"id": f"eq.{inv_id}"}, {
            "balance": new_bal, "status": new_status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
    for bill_id, bill in bill_targets.items():
        applied = sum(a.amount for a in body.applications if a.bill_id == bill_id)
        new_bal = round(float(bill["balance"]) - applied, 2)
        new_status = _new_status_after_payment(float(bill["total"]), new_bal, True)
        await _sb_update("bills", {"id": f"eq.{bill_id}"}, {
            "balance": new_bal, "status": new_status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

    return {"payment": pay, "applications_count": len(app_rows)}


@app.delete("/api/payments/{payment_id}")
async def delete_payment(payment_id: str, authorization: str = Header(None)):
    """Void a payment. Reverses its effect on invoice/bill balances."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    rows = await _sb_select("payments", {
        "id": f"eq.{payment_id}", "select": "id,company_id,amount", "limit": "1",
    })
    if not rows:
        raise HTTPException(status_code=404, detail="Payment not found")
    pay = rows[0]
    await _resolve_manual_company_from_supabase_id(pay["company_id"], user)

    apps = await _sb_select("payment_applications", {
        "payment_id": f"eq.{payment_id}",
        "select": "id,invoice_id,bill_id,amount",
    })
    # Reverse each application
    for a in apps:
        if a.get("invoice_id"):
            inv_rows = await _sb_select("invoices", {
                "id": f"eq.{a['invoice_id']}",
                "select": "id,total,balance", "limit": "1",
            })
            if inv_rows:
                inv = inv_rows[0]
                new_bal = round(float(inv["balance"]) + float(a["amount"]), 2)
                new_status = _new_status_after_payment(float(inv["total"]), new_bal, False)
                await _sb_update("invoices", {"id": f"eq.{a['invoice_id']}"}, {
                    "balance": new_bal, "status": new_status,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
        elif a.get("bill_id"):
            bill_rows = await _sb_select("bills", {
                "id": f"eq.{a['bill_id']}",
                "select": "id,total,balance", "limit": "1",
            })
            if bill_rows:
                bill = bill_rows[0]
                new_bal = round(float(bill["balance"]) + float(a["amount"]), 2)
                new_status = _new_status_after_payment(float(bill["total"]), new_bal, True)
                await _sb_update("bills", {"id": f"eq.{a['bill_id']}"}, {
                    "balance": new_bal, "status": new_status,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
    await _sb_delete("payments", {"id": f"eq.{payment_id}"})
    return {"ok": True}


# =====================================================================
#  HEALTH
# =====================================================================

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "time": datetime.now().isoformat(),
        "qbo_env": QBO_ENVIRONMENT,
        "db_path": DB_PATH,
        "volume_mounted": bool(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")),
        "db_exists": os.path.isfile(DB_PATH),
    }



if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
