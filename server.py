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
from datetime import datetime, timedelta, timezone
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
        CREATE TABLE IF NOT EXISTS shareholders (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            short_name TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shareholders_org_name
            ON shareholders(org_id, lower(display_name));
        CREATE TABLE IF NOT EXISTS shareholder_account_links (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            shareholder_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            qbo_account_id TEXT NOT NULL,
            qbo_account_name TEXT NOT NULL,
            account_kind TEXT NOT NULL CHECK (account_kind IN ('drawing','dividend_payable')),
            is_default_for_writes INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (company_id, qbo_account_id)
        );
        CREATE INDEX IF NOT EXISTS idx_sal_shareholder_company
            ON shareholder_account_links(shareholder_id, company_id);
        CREATE TABLE IF NOT EXISTS shareholder_dividend_events (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            shareholder_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            event_date TEXT NOT NULL,
            amount REAL NOT NULL,
            kind TEXT NOT NULL DEFAULT 'payment'
                CHECK (kind IN ('payment','declaration','adjustment','managing_bonus')),
            drawing_account_name TEXT,
            cash_account_name TEXT,
            memo TEXT,
            source TEXT NOT NULL DEFAULT 'manual'
                CHECK (source IN ('manual','import','sync')),
            qbo_je_id TEXT,
            qbo_post_status TEXT NOT NULL DEFAULT 'pending'
                CHECK (qbo_post_status IN ('pending','posted','failed','skipped','voided')),
            qbo_post_error TEXT,
            qbo_request_json TEXT,
            qbo_response_json TEXT,
            created_by_user_id TEXT,
            created_by_name TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            posted_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sde_shareholder_date
            ON shareholder_dividend_events(shareholder_id, event_date DESC);
        CREATE INDEX IF NOT EXISTS idx_sde_org_company
            ON shareholder_dividend_events(org_id, company_id);
        CREATE TABLE IF NOT EXISTS shareholder_share_allocations (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            shareholder_id TEXT NOT NULL,
            effective_date TEXT NOT NULL,     -- YYYY-MM-DD; allocation valid from this date
            shares_held REAL NOT NULL DEFAULT 0,
            ownership_pct REAL,               -- optional cached fraction (0..1)
            dividend_per_share REAL,          -- DPS snapshot for the period
            mb_amount REAL DEFAULT 0,         -- managing-bonus allocation for the period
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (shareholder_id, effective_date)
        );
        CREATE INDEX IF NOT EXISTS idx_ssa_org_date
            ON shareholder_share_allocations(org_id, effective_date DESC);
        CREATE INDEX IF NOT EXISTS idx_ssa_shareholder
            ON shareholder_share_allocations(shareholder_id, effective_date DESC);
        CREATE TABLE IF NOT EXISTS shareholder_alias_map (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            alias_kind TEXT NOT NULL CHECK (alias_kind IN ('shareholder','company')),
            source_label TEXT NOT NULL,       -- e.g. 'Howie Ewe', 'SHG'
            target_id TEXT NOT NULL,          -- shareholder_id or company_id
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (org_id, alias_kind, source_label)
        );
    """)

    # Safe column additions
    for col, ctype in [
        ("address", "TEXT"), ("phone", "TEXT"), ("email", "TEXT"),
        ("industry", "TEXT"), ("qbo_plan", "TEXT"),
        ("access_token", "TEXT"), ("refresh_token", "TEXT"),
        ("token_expires_at", "TEXT"),
    ]:
        _add_column_safe(db, "companies", col, ctype)

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
    """Refresh an expired access token and store the new tokens."""
    auth_header = base64.b64encode(
        f"{QBO_CLIENT_ID}:{QBO_CLIENT_SECRET}".encode()
    ).decode()

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            QBO_TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            headers={
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )

    intuit_tid = resp.headers.get("intuit_tid", "N/A")

    if resp.status_code != 200:
        # Mark company as needing re-auth
        db.execute("UPDATE companies SET status='auth_expired' WHERE id=?", (company_id,))
        db.commit()
        logger.error(
            "Token refresh FAILED | company=%s | status=%d | intuit_tid=%s | body=%s",
            company_id, resp.status_code, intuit_tid, resp.text[:300],
        )
        raise HTTPException(
            status_code=401,
            detail=f"Token refresh failed. Please re-authorize this company. QBO response: {resp.text[:200]}"
        )

    tokens = resp.json()
    new_access = tokens["access_token"]
    new_refresh = tokens.get("refresh_token", refresh_token)
    expires_in = tokens.get("expires_in", 3600)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
    logger.info("Token refresh OK | company=%s | intuit_tid=%s", company_id, intuit_tid)

    db.execute(
        """UPDATE companies SET access_token=?, refresh_token=?, token_expires_at=?,
           status='connected' WHERE id=?""",
        (new_access, new_refresh, expires_at, company_id),
    )
    db.commit()
    return new_access


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
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    db.execute("DELETE FROM company_reports WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM company_accounts WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM account_mappings WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id))
    db.commit()
    db.close()
    return {"deleted": company_id}


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


@app.post("/api/reports/profit-loss")
async def get_profit_loss(params: ReportParams, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    if params.company_id == "all":
        # Always try live first for consolidated to avoid stale/empty cache
        try:
            result = await _get_live_consolidated(params, "ProfitAndLoss", "profit_loss", org_id)
            if result.get("current") is not None:
                return result
        except Exception:
            pass
        # Fall back to cache
        return _get_cached_report(params, "profit_loss", org_id)
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
    if params.company_id == "all":
        try:
            result = await _get_live_consolidated(params, "BalanceSheet", "balance_sheet", org_id)
            if result.get("current") is not None:
                return result
        except Exception:
            pass
        return _get_cached_report(params, "balance_sheet", org_id)
    if params.company_id:
        return await _get_live_report_for_company(params, "BalanceSheet", "balance_sheet")
    return {"current": None, "message": "Select a company"}


@app.post("/api/reports/cash-flow")
async def get_cash_flow(params: ReportParams, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    if params.company_id == "all":
        try:
            result = await _get_live_consolidated(params, "CashFlow", "cash_flow", org_id)
            if result.get("current") is not None:
                return result
        except Exception:
            pass
        return _get_cached_report(params, "cash_flow", org_id)
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
#  SHAREHOLDER DIVIDENDS
# =====================================================================
#
# Tracks distributions to shareholders across all QBO companies the org
# has connected. Each "A/C Drawing - [Person]" or "Dividend Payable -
# [Person]" QBO account is linked to a first-class shareholder record
# so amounts roll up across company files. Every recorded payment can
# optionally post a balanced JE to the source company's QBO book using
# the existing _build_je_payload / _post_je_to_qbo helpers.
#
# The reconciliation endpoint ties Net Income (P&L), Dividends Paid
# (this table), and Cash Flow (cash flow report) together per company.

class ShareholderIn(BaseModel):
    display_name: str
    short_name: Optional[str] = None
    active: Optional[bool] = True
    notes: Optional[str] = None


class AccountLinkIn(BaseModel):
    company_id: str
    qbo_account_id: str
    qbo_account_name: str
    account_kind: str = "drawing"  # 'drawing' | 'dividend_payable'
    is_default_for_writes: Optional[bool] = True


class DividendEventIn(BaseModel):
    shareholder_id: str
    company_id: str
    event_date: str  # YYYY-MM-DD
    amount: float
    kind: Optional[str] = "payment"  # 'payment'|'declaration'|'adjustment'|'managing_bonus'
    drawing_account_name: Optional[str] = None  # auto-resolved from default link if omitted
    cash_account_name: Optional[str] = None     # required if post_to_qbo=true
    memo: Optional[str] = None
    post_to_qbo: Optional[bool] = True
    source: Optional[str] = None                # 'manual'|'import'|'sync' — default inferred


class DividendImportRow(BaseModel):
    event_date: str
    shareholder_match: str   # display_name or short_name — resolved server-side
    company_match: str       # company name or id — resolved server-side
    amount: float
    memo: Optional[str] = None


class DividendImportRequest(BaseModel):
    rows: list  # list of DividendImportRow-shaped dicts
    commit: Optional[bool] = False


class ReconcileQuery(BaseModel):
    company_ids: Optional[list] = None  # omit for all connected companies
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_macro: Optional[str] = None


def _row_to_shareholder(row) -> dict:
    return {
        "id": row["id"],
        "display_name": row["display_name"],
        "short_name": row["short_name"],
        "active": bool(row["active"]),
        "notes": row["notes"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_account_link(row) -> dict:
    return {
        "id": row["id"],
        "shareholder_id": row["shareholder_id"],
        "company_id": row["company_id"],
        "qbo_account_id": row["qbo_account_id"],
        "qbo_account_name": row["qbo_account_name"],
        "account_kind": row["account_kind"],
        "is_default_for_writes": bool(row["is_default_for_writes"]),
        "created_at": row["created_at"],
    }


def _row_to_dividend_event(row) -> dict:
    return {
        "id": row["id"],
        "shareholder_id": row["shareholder_id"],
        "company_id": row["company_id"],
        "event_date": row["event_date"],
        "amount": row["amount"],
        "kind": row["kind"],
        "drawing_account_name": row["drawing_account_name"],
        "cash_account_name": row["cash_account_name"],
        "memo": row["memo"],
        "source": row["source"],
        "qbo_je_id": row["qbo_je_id"],
        "qbo_post_status": row["qbo_post_status"],
        "qbo_post_error": row["qbo_post_error"],
        "created_by_name": row["created_by_name"],
        "created_at": row["created_at"],
        "posted_at": row["posted_at"],
    }


def _resolve_default_drawing_account(db, shareholder_id: str, company_id: str):
    """Pick the default drawing account for (shareholder, company).

    Returns (qbo_account_id, qbo_account_name) or (None, None).
    """
    row = db.execute(
        """SELECT qbo_account_id, qbo_account_name FROM shareholder_account_links
           WHERE shareholder_id = ? AND company_id = ? AND account_kind = 'drawing'
                 AND is_default_for_writes = 1
           ORDER BY created_at ASC LIMIT 1""",
        (shareholder_id, company_id),
    ).fetchone()
    if row:
        return row["qbo_account_id"], row["qbo_account_name"]
    # Fall back to any linked drawing account
    row = db.execute(
        """SELECT qbo_account_id, qbo_account_name FROM shareholder_account_links
           WHERE shareholder_id = ? AND company_id = ? AND account_kind = 'drawing'
           ORDER BY created_at ASC LIMIT 1""",
        (shareholder_id, company_id),
    ).fetchone()
    if row:
        return row["qbo_account_id"], row["qbo_account_name"]
    return None, None


@app.get("/api/shareholders")
async def list_shareholders(
    include_inactive: bool = False,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    if include_inactive:
        rows = db.execute(
            "SELECT * FROM shareholders WHERE org_id = ? ORDER BY display_name",
            (org_id,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM shareholders WHERE org_id = ? AND active = 1 ORDER BY display_name",
            (org_id,),
        ).fetchall()
    # Pre-count links so the UI can show "3 accounts linked" badges.
    links_by_sh = {}
    all_links = db.execute(
        "SELECT shareholder_id, COUNT(*) AS n FROM shareholder_account_links WHERE org_id = ? GROUP BY shareholder_id",
        (org_id,),
    ).fetchall()
    for r in all_links:
        links_by_sh[r["shareholder_id"]] = r["n"]
    db.close()
    out = []
    for r in rows:
        s = _row_to_shareholder(r)
        s["linked_account_count"] = links_by_sh.get(s["id"], 0)
        out.append(s)
    return out


@app.post("/api/shareholders")
async def create_shareholder(req: ShareholderIn, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    if not req.display_name or not req.display_name.strip():
        raise HTTPException(status_code=400, detail="display_name is required")
    sid = str(uuid.uuid4())
    db = get_db()
    try:
        db.execute(
            """INSERT INTO shareholders (id, org_id, display_name, short_name, active, notes)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (sid, org_id, req.display_name.strip(),
             (req.short_name or "").strip() or None,
             1 if (req.active if req.active is not None else True) else 0,
             req.notes),
        )
        db.commit()
    except sqlite3.IntegrityError:
        db.close()
        raise HTTPException(status_code=409, detail="A shareholder with that display name already exists.")
    row = db.execute("SELECT * FROM shareholders WHERE id = ?", (sid,)).fetchone()
    db.close()
    return _row_to_shareholder(row)


@app.put("/api/shareholders/{shareholder_id}")
async def update_shareholder(
    shareholder_id: str, req: ShareholderIn, authorization: str = Header(None)
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        "SELECT id FROM shareholders WHERE id = ? AND org_id = ?",
        (shareholder_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found")
    db.execute(
        """UPDATE shareholders
           SET display_name = ?, short_name = ?, active = ?, notes = ?,
               updated_at = datetime('now')
           WHERE id = ? AND org_id = ?""",
        (req.display_name.strip(),
         (req.short_name or "").strip() or None,
         1 if (req.active if req.active is not None else True) else 0,
         req.notes, shareholder_id, org_id),
    )
    db.commit()
    out = db.execute("SELECT * FROM shareholders WHERE id = ?", (shareholder_id,)).fetchone()
    db.close()
    return _row_to_shareholder(out)


@app.delete("/api/shareholders/{shareholder_id}")
async def deactivate_shareholder(shareholder_id: str, authorization: str = Header(None)):
    """Soft-delete. Keeps historical event attribution intact."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        "SELECT id FROM shareholders WHERE id = ? AND org_id = ?",
        (shareholder_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found")
    db.execute(
        "UPDATE shareholders SET active = 0, updated_at = datetime('now') WHERE id = ?",
        (shareholder_id,),
    )
    db.commit()
    db.close()
    return {"id": shareholder_id, "active": False}


@app.get("/api/shareholders/{shareholder_id}/account-links")
async def list_account_links(shareholder_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    rows = db.execute(
        """SELECT sal.*, c.name AS company_name
           FROM shareholder_account_links sal
           LEFT JOIN companies c ON c.id = sal.company_id
           WHERE sal.shareholder_id = ? AND sal.org_id = ?
           ORDER BY c.name, sal.qbo_account_name""",
        (shareholder_id, org_id),
    ).fetchall()
    db.close()
    out = []
    for r in rows:
        link = _row_to_account_link(r)
        link["company_name"] = r["company_name"]
        out.append(link)
    return out


@app.post("/api/shareholders/{shareholder_id}/account-links")
async def create_account_link(
    shareholder_id: str, req: AccountLinkIn, authorization: str = Header(None)
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    if req.account_kind not in ("drawing", "dividend_payable"):
        raise HTTPException(status_code=400, detail="account_kind must be 'drawing' or 'dividend_payable'")
    db = get_db()
    sh = db.execute(
        "SELECT id FROM shareholders WHERE id = ? AND org_id = ?",
        (shareholder_id, org_id),
    ).fetchone()
    if not sh:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found")
    co = db.execute(
        "SELECT id FROM companies WHERE id = ? AND org_id = ?",
        (req.company_id, org_id),
    ).fetchone()
    if not co:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found in this org")
    lid = str(uuid.uuid4())
    try:
        db.execute(
            """INSERT INTO shareholder_account_links
               (id, org_id, shareholder_id, company_id, qbo_account_id,
                qbo_account_name, account_kind, is_default_for_writes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (lid, org_id, shareholder_id, req.company_id, req.qbo_account_id,
             req.qbo_account_name, req.account_kind,
             1 if req.is_default_for_writes else 0),
        )
        db.commit()
    except sqlite3.IntegrityError:
        db.close()
        raise HTTPException(
            status_code=409,
            detail="This QBO account is already linked (possibly to another shareholder).",
        )
    row = db.execute(
        "SELECT * FROM shareholder_account_links WHERE id = ?", (lid,)
    ).fetchone()
    db.close()
    return _row_to_account_link(row)


@app.delete("/api/shareholders/{shareholder_id}/account-links/{link_id}")
async def delete_account_link(
    shareholder_id: str, link_id: str, authorization: str = Header(None)
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        "SELECT id FROM shareholder_account_links WHERE id = ? AND shareholder_id = ? AND org_id = ?",
        (link_id, shareholder_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Link not found")
    db.execute("DELETE FROM shareholder_account_links WHERE id = ?", (link_id,))
    db.commit()
    db.close()
    return {"id": link_id, "deleted": True}


@app.get("/api/shareholders/{shareholder_id}/balances")
async def get_shareholder_balances(shareholder_id: str, authorization: str = Header(None)):
    """Sum current_balance across every linked QBO account, grouped by company.

    Reads from the cached company_accounts table — run
    `POST /api/companies/{id}/sync` to refresh upstream data.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    sh = db.execute(
        "SELECT * FROM shareholders WHERE id = ? AND org_id = ?",
        (shareholder_id, org_id),
    ).fetchone()
    if not sh:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found")
    rows = db.execute(
        """SELECT sal.company_id, sal.qbo_account_id, sal.qbo_account_name, sal.account_kind,
                  c.name AS company_name,
                  ca.current_balance AS balance
           FROM shareholder_account_links sal
           LEFT JOIN companies c ON c.id = sal.company_id
           LEFT JOIN company_accounts ca
             ON ca.company_id = sal.company_id
            AND ca.qbo_account_id = sal.qbo_account_id
           WHERE sal.shareholder_id = ? AND sal.org_id = ?""",
        (shareholder_id, org_id),
    ).fetchall()
    by_company = {}
    total = 0.0
    for r in rows:
        cid = r["company_id"]
        bal = float(r["balance"] or 0)
        if cid not in by_company:
            by_company[cid] = {
                "company_id": cid,
                "company_name": r["company_name"],
                "total_balance": 0.0,
                "accounts": [],
            }
        by_company[cid]["total_balance"] += bal
        by_company[cid]["accounts"].append({
            "qbo_account_id": r["qbo_account_id"],
            "qbo_account_name": r["qbo_account_name"],
            "account_kind": r["account_kind"],
            "current_balance": bal,
        })
        total += bal
    db.close()
    return {
        "shareholder_id": shareholder_id,
        "shareholder_name": sh["display_name"],
        "total_balance": round(total, 2),
        "companies": list(by_company.values()),
    }


@app.get("/api/dividend-events")
async def list_dividend_events(
    shareholder_id: Optional[str] = None,
    company_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    clauses = ["e.org_id = ?"]
    params = [org_id]
    if shareholder_id:
        clauses.append("e.shareholder_id = ?"); params.append(shareholder_id)
    if company_id:
        clauses.append("e.company_id = ?"); params.append(company_id)
    if start_date:
        clauses.append("e.event_date >= ?"); params.append(start_date)
    if end_date:
        clauses.append("e.event_date <= ?"); params.append(end_date)
    if status:
        clauses.append("e.qbo_post_status = ?"); params.append(status)
    sql = (
        "SELECT e.*, s.display_name AS shareholder_name, c.name AS company_name "
        "FROM shareholder_dividend_events e "
        "LEFT JOIN shareholders s ON s.id = e.shareholder_id "
        "LEFT JOIN companies c ON c.id = e.company_id "
        "WHERE " + " AND ".join(clauses) + " ORDER BY e.event_date DESC, e.created_at DESC"
    )
    db = get_db()
    rows = db.execute(sql, tuple(params)).fetchall()
    db.close()
    out = []
    for r in rows:
        evt = _row_to_dividend_event(r)
        evt["shareholder_name"] = r["shareholder_name"]
        evt["company_name"] = r["company_name"]
        out.append(evt)
    return out


@app.post("/api/dividend-events")
async def create_dividend_event(req: DividendEventIn, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    db = get_db()
    sh = db.execute(
        "SELECT * FROM shareholders WHERE id = ? AND org_id = ? AND active = 1",
        (req.shareholder_id, org_id),
    ).fetchone()
    if not sh:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found or inactive")
    co = db.execute(
        "SELECT id, name FROM companies WHERE id = ? AND org_id = ?",
        (req.company_id, org_id),
    ).fetchone()
    if not co:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")

    drawing_name = req.drawing_account_name
    if not drawing_name:
        _aid, drawing_name = _resolve_default_drawing_account(db, req.shareholder_id, req.company_id)
    if req.post_to_qbo and not drawing_name:
        db.close()
        raise HTTPException(
            status_code=400,
            detail="No drawing account is linked for this shareholder at this company. "
                   "Add an account link or pass drawing_account_name explicitly.",
        )
    if req.post_to_qbo and not req.cash_account_name:
        db.close()
        raise HTTPException(status_code=400, detail="cash_account_name is required when post_to_qbo=true")

    eid = str(uuid.uuid4())
    default_source = "import" if not req.post_to_qbo and (req.source is None) else "manual"
    source_val = (req.source or default_source)
    if source_val not in ("manual", "import", "sync"):
        source_val = "manual"
    db.execute(
        """INSERT INTO shareholder_dividend_events
           (id, org_id, shareholder_id, company_id, event_date, amount, kind,
            drawing_account_name, cash_account_name, memo, source,
            qbo_post_status, created_by_user_id, created_by_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (eid, org_id, req.shareholder_id, req.company_id, req.event_date,
         float(req.amount), req.kind or "payment",
         drawing_name, req.cash_account_name, req.memo, source_val,
         "pending" if req.post_to_qbo else "skipped",
         user["id"], user.get("name") or user.get("email")),
    )
    db.commit()

    posted = False
    if req.post_to_qbo:
        je_req = JournalEntryRequest(
            date=req.event_date,
            private_note=(req.memo or f"Dividend to {sh['display_name']}"),
            lines=[
                JournalEntryLine(posting_type="Debit",  account_name=drawing_name,          amount=float(req.amount)),
                JournalEntryLine(posting_type="Credit", account_name=req.cash_account_name, amount=float(req.amount)),
            ],
        )
        try:
            payload, total_debits, line_count = _build_je_payload(db, req.company_id, co["name"], je_req)
            je = await _post_je_to_qbo(db, req.company_id, payload)
            db.execute(
                """UPDATE shareholder_dividend_events
                   SET qbo_je_id = ?, qbo_post_status = 'posted', posted_at = datetime('now'),
                       qbo_response_json = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (je.get("Id"), json.dumps(je)[:20000], eid),
            )
            db.commit()
            posted = True
        except HTTPException as he:
            db.execute(
                """UPDATE shareholder_dividend_events
                   SET qbo_post_status = 'failed', qbo_post_error = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (str(he.detail)[:2000], eid),
            )
            db.commit()
            db.close()
            raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
        except Exception as e:
            db.execute(
                """UPDATE shareholder_dividend_events
                   SET qbo_post_status = 'failed', qbo_post_error = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (str(e)[:2000], eid),
            )
            db.commit()
            db.close()
            raise HTTPException(status_code=500, detail=f"Failed to post JE: {e}")

    row = db.execute(
        "SELECT * FROM shareholder_dividend_events WHERE id = ?", (eid,)
    ).fetchone()
    db.close()
    evt = _row_to_dividend_event(row)
    evt["posted"] = posted
    return evt


@app.post("/api/dividend-events/{event_id}/void")
async def void_dividend_event(event_id: str, authorization: str = Header(None)):
    """Reverse a posted (or imported) dividend event.

    For `posted` events this creates a reversing JE in QBO (debit Cash /
    credit Drawing). For `skipped` / `failed` / `pending` events it
    simply flips status to `voided` without a QBO call.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        """SELECT e.*, c.name AS company_name
           FROM shareholder_dividend_events e
           LEFT JOIN companies c ON c.id = e.company_id
           WHERE e.id = ? AND e.org_id = ?""",
        (event_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Event not found")
    if row["qbo_post_status"] == "voided":
        db.close()
        raise HTTPException(status_code=409, detail="Event is already voided")

    if row["qbo_post_status"] == "posted":
        if not row["drawing_account_name"] or not row["cash_account_name"]:
            db.close()
            raise HTTPException(
                status_code=400,
                detail="Event missing account names; cannot auto-reverse.",
            )
        je_req = JournalEntryRequest(
            date=datetime.now().strftime("%Y-%m-%d"),
            private_note=f"Reverse dividend {event_id} (original JE {row['qbo_je_id']})",
            lines=[
                JournalEntryLine(posting_type="Debit",  account_name=row["cash_account_name"],    amount=float(row["amount"])),
                JournalEntryLine(posting_type="Credit", account_name=row["drawing_account_name"], amount=float(row["amount"])),
            ],
        )
        try:
            payload, _td, _lc = _build_je_payload(db, row["company_id"], row["company_name"] or "", je_req)
            je = await _post_je_to_qbo(db, row["company_id"], payload)
        except HTTPException as he:
            db.close()
            raise HTTPException(status_code=502, detail=f"QBO error: {he.detail}")
        db.execute(
            """UPDATE shareholder_dividend_events
               SET qbo_post_status = 'voided', qbo_post_error = NULL,
                   qbo_response_json = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (json.dumps({"reversed_by": je.get("Id")})[:20000], event_id),
        )
    else:
        db.execute(
            """UPDATE shareholder_dividend_events
               SET qbo_post_status = 'voided', updated_at = datetime('now')
               WHERE id = ?""",
            (event_id,),
        )
    db.commit()
    out = db.execute("SELECT * FROM shareholder_dividend_events WHERE id = ?", (event_id,)).fetchone()
    db.close()
    return _row_to_dividend_event(out)


@app.post("/api/dividend-events/import")
async def import_dividend_events(req: DividendImportRequest, authorization: str = Header(None)):
    """Dry-run (default) or commit a bulk import of historical dividend rows.

    Rows are matched by shareholder display_name / short_name and by
    company name / id. Imported rows are flagged `source='import'`,
    `qbo_post_status='skipped'` (they're already in QBO — this only
    seeds this app's event log for reconciliation).
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    shareholders = db.execute(
        "SELECT id, display_name, short_name FROM shareholders WHERE org_id = ?",
        (org_id,),
    ).fetchall()
    companies = db.execute(
        "SELECT id, name FROM companies WHERE org_id = ?",
        (org_id,),
    ).fetchall()

    def _match_shareholder(val: str):
        v = (val or "").strip().lower()
        if not v:
            return None
        for s in shareholders:
            if (s["display_name"] or "").lower().strip() == v:
                return s
        for s in shareholders:
            if (s["short_name"] or "").lower().strip() == v:
                return s
        return None

    def _match_company(val: str):
        v = (val or "").strip().lower()
        if not v:
            return None
        for c in companies:
            if c["id"].lower() == v or (c["name"] or "").lower().strip() == v:
                return c
        return None

    preview = []
    for i, raw in enumerate(req.rows or []):
        row = raw if isinstance(raw, dict) else raw.dict()
        sh = _match_shareholder(row.get("shareholder_match", ""))
        co = _match_company(row.get("company_match", ""))
        try:
            amt = float(row.get("amount", 0) or 0)
        except (TypeError, ValueError):
            amt = 0.0
        errors = []
        if not sh: errors.append("unknown shareholder")
        if not co: errors.append("unknown company")
        if amt <= 0: errors.append("amount must be > 0")
        if not row.get("event_date"): errors.append("event_date required")
        preview.append({
            "index": i,
            "event_date": row.get("event_date"),
            "shareholder_id": sh["id"] if sh else None,
            "shareholder_name": sh["display_name"] if sh else None,
            "company_id": co["id"] if co else None,
            "company_name": co["name"] if co else None,
            "amount": amt,
            "memo": row.get("memo") or None,
            "errors": errors,
        })

    writable = [p for p in preview if not p["errors"]]
    if not req.commit:
        db.close()
        return {"dry_run": True, "preview": preview, "writable_count": len(writable)}

    # Commit path
    written = 0
    for p in writable:
        db.execute(
            """INSERT INTO shareholder_dividend_events
               (id, org_id, shareholder_id, company_id, event_date, amount, kind,
                drawing_account_name, cash_account_name, memo, source,
                qbo_post_status, created_by_user_id, created_by_name)
               VALUES (?, ?, ?, ?, ?, ?, 'payment', NULL, NULL, ?, 'import',
                       'skipped', ?, ?)""",
            (str(uuid.uuid4()), org_id, p["shareholder_id"], p["company_id"],
             p["event_date"], float(p["amount"]), p["memo"],
             user["id"], user.get("name") or user.get("email")),
        )
        written += 1
    db.commit()
    db.close()
    return {"dry_run": False, "preview": preview, "written": written}


@app.get("/api/dividend-events/reconcile")
async def reconcile_dividend_events(
    company_id: str,
    start_date: str,
    end_date: str,
    authorization: str = Header(None),
):
    """Reconcile local events against QBO transaction history for each
    drawing account linked to any shareholder in this company. Joins on
    QBO journal-entry id; returns three buckets: matched, qbo_only,
    app_only.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    links = db.execute(
        """SELECT DISTINCT qbo_account_name, shareholder_id
           FROM shareholder_account_links
           WHERE company_id = ? AND org_id = ? AND account_kind = 'drawing'""",
        (company_id, org_id),
    ).fetchall()
    events = db.execute(
        """SELECT * FROM shareholder_dividend_events
           WHERE company_id = ? AND org_id = ?
             AND event_date BETWEEN ? AND ?
             AND qbo_post_status IN ('posted','skipped','voided')""",
        (company_id, org_id, start_date, end_date),
    ).fetchall()
    db.close()
    by_je = {e["qbo_je_id"]: e for e in events if e["qbo_je_id"]}

    # Pull QBO transactions per unique account name (best-effort — don't
    # let a single failure kill the whole reconcile).
    matched = []
    qbo_only = []
    seen_je_ids = set()
    for link in links:
        try:
            params = TransactionDetailParams(
                account_name=link["qbo_account_name"],
                start_date=start_date,
                end_date=end_date,
                company_id=company_id,
            )
            resp = await get_transaction_detail(params, authorization)
        except Exception:
            continue
        txns = (resp or {}).get("transactions") or (resp or {}).get("current", {}).get("transactions") or []
        for t in txns:
            je_id = t.get("txn_id") or t.get("id") or t.get("TxnId")
            if not je_id:
                continue
            if je_id in seen_je_ids:
                continue
            seen_je_ids.add(je_id)
            local = by_je.get(str(je_id))
            if local:
                matched.append({"qbo_je_id": je_id, "qbo_txn": t, "event": _row_to_dividend_event(local)})
            else:
                qbo_only.append({"qbo_je_id": je_id, "qbo_txn": t, "account_name": link["qbo_account_name"]})

    app_only = []
    for e in events:
        if not e["qbo_je_id"] or e["qbo_je_id"] not in seen_je_ids:
            if e["source"] != "import":
                # imports are by definition app-only unless they have a JE id
                app_only.append(_row_to_dividend_event(e))
            elif e["qbo_je_id"]:
                app_only.append(_row_to_dividend_event(e))

    return {
        "company_id": company_id,
        "start_date": start_date,
        "end_date": end_date,
        "matched": matched,
        "qbo_only": qbo_only,
        "app_only": app_only,
    }


@app.post("/api/reconciliation/profit-dividend-cash")
async def profit_dividend_cash(req: ReconcileQuery, authorization: str = Header(None)):
    """Per-company: Net Income (P&L) ↔ Dividends Issued (this app) ↔ Cash Flow.

    Returns one row per company plus a total row, with derived fields
    `distributable_remainder` (= NI − dividends) and
    `financing_variance` (= financing_cash − (−dividends), i.e. the
    portion of financing activity that isn't dividend outflow).
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    db = get_db()
    co_rows = db.execute(
        "SELECT id, name FROM companies WHERE org_id = ? AND status IN ('connected','synced')",
        (org_id,),
    ).fetchall()
    if req.company_ids:
        wanted = set(req.company_ids)
        co_rows = [c for c in co_rows if c["id"] in wanted]

    # Cash on hand snapshot for the reconcile response
    _recon_cash_by_co = _cash_on_hand_by_company(db, [c["id"] for c in co_rows])

    # Pull dividend totals once for the period
    evt_clause = [
        "org_id = ?", "kind IN ('payment','managing_bonus')",
        "qbo_post_status IN ('posted','skipped')",
    ]
    evt_params = [org_id]
    if req.start_date:
        evt_clause.append("event_date >= ?"); evt_params.append(req.start_date)
    if req.end_date:
        evt_clause.append("event_date <= ?"); evt_params.append(req.end_date)
    if req.company_ids:
        placeholders = ",".join("?" for _ in req.company_ids)
        evt_clause.append(f"company_id IN ({placeholders})"); evt_params.extend(req.company_ids)
    div_rows = db.execute(
        "SELECT company_id, SUM(amount) AS total FROM shareholder_dividend_events "
        "WHERE " + " AND ".join(evt_clause) + " GROUP BY company_id",
        tuple(evt_params),
    ).fetchall()
    dividends_by_company = {r["company_id"]: float(r["total"] or 0) for r in div_rows}
    db.close()

    out_rows = []
    totals = {
        "net_income": 0.0, "dividends_paid": 0.0,
        "operating_cash": 0.0, "investing_cash": 0.0,
        "financing_cash": 0.0, "net_cash_change": 0.0,
    }
    for co in co_rows:
        try:
            pl_params = ReportParams(
                company_id=co["id"],
                start_date=req.start_date, end_date=req.end_date,
                date_macro=req.date_macro,
            )
            cf_params = ReportParams(
                company_id=co["id"],
                start_date=req.start_date, end_date=req.end_date,
                date_macro=req.date_macro,
            )
            pl = await get_profit_loss(pl_params, authorization)
            cf = await get_cash_flow(cf_params, authorization)
        except HTTPException:
            continue
        except Exception:
            continue

        def _extract_net_income(report):
            if not report: return 0.0
            cur = report.get("current") or report
            totals_obj = cur.get("totals") if isinstance(cur, dict) else None
            if isinstance(totals_obj, dict):
                for k in ("net_income", "NetIncome", "net_operating_income", "net_income_loss"):
                    if k in totals_obj:
                        try: return float(totals_obj[k])
                        except Exception: pass
            if isinstance(cur, dict) and "net_income" in cur:
                try: return float(cur["net_income"])
                except Exception: pass
            return 0.0

        def _extract_cash_block(report, key):
            if not report: return 0.0
            cur = report.get("current") or report
            totals_obj = cur.get("totals") if isinstance(cur, dict) else None
            if isinstance(totals_obj, dict) and key in totals_obj:
                try: return float(totals_obj[key])
                except Exception: pass
            if isinstance(cur, dict) and key in cur:
                try: return float(cur[key])
                except Exception: pass
            return 0.0

        ni = _extract_net_income(pl)
        op = _extract_cash_block(cf, "operating_activities")
        iv = _extract_cash_block(cf, "investing_activities")
        fn = _extract_cash_block(cf, "financing_activities")
        nc = _extract_cash_block(cf, "net_cash_change") or (op + iv + fn)
        div = dividends_by_company.get(co["id"], 0.0)

        notes = []
        if div > ni:       notes.append("dividends_exceed_net_income")
        if div > op:       notes.append("dividends_exceed_operating_cash")
        financing_variance = fn - (-div)
        if abs(financing_variance) > 1:
            notes.append("financing_variance_nonzero")

        out_rows.append({
            "company_id": co["id"], "company_name": co["name"],
            "cash_on_hand": round(_recon_cash_by_co.get(co["id"], 0.0), 2),
            "net_income": round(ni, 2), "dividends_paid": round(div, 2),
            "payout_ratio": round(div / ni, 4) if ni > 0 else None,
            "distributable_remainder": round(ni - div, 2),
            "operating_cash": round(op, 2), "investing_cash": round(iv, 2),
            "financing_cash": round(fn, 2), "net_cash_change": round(nc, 2),
            "expected_financing_out": round(-div, 2),
            "financing_variance": round(financing_variance, 2),
            "variance_notes": notes,
        })
        totals["net_income"]       += ni
        totals["dividends_paid"]   += div
        totals["operating_cash"]   += op
        totals["investing_cash"]   += iv
        totals["financing_cash"]   += fn
        totals["net_cash_change"]  += nc

    return {
        "start_date": req.start_date, "end_date": req.end_date,
        "date_macro": req.date_macro,
        "rows": out_rows,
        "totals": {
            **{k: round(v, 2) for k, v in totals.items()},
            "distributable_remainder": round(totals["net_income"] - totals["dividends_paid"], 2),
            "expected_financing_out": round(-totals["dividends_paid"], 2),
            "financing_variance": round(totals["financing_cash"] - (-totals["dividends_paid"]), 2),
        },
    }


# =====================================================================
#  SHARE ALLOCATIONS + ALIAS MAPS
# =====================================================================
#
# Ownership snapshots (shares_held, DPS, MB) per shareholder per
# effective-date. Used to compute expected pro-rata dividends and
# per-shareholder valuation. Also stores a free-form alias map so the
# xlsx importer (and ad-hoc admin input) can match messy source
# labels to our canonical records.

class ShareAllocationIn(BaseModel):
    shareholder_id: str
    effective_date: str        # YYYY-MM-DD
    shares_held: float
    ownership_pct: Optional[float] = None
    dividend_per_share: Optional[float] = None
    mb_amount: Optional[float] = 0
    notes: Optional[str] = None


def _row_to_allocation(row) -> dict:
    return {
        "id": row["id"],
        "shareholder_id": row["shareholder_id"],
        "effective_date": row["effective_date"],
        "shares_held": row["shares_held"],
        "ownership_pct": row["ownership_pct"],
        "dividend_per_share": row["dividend_per_share"],
        "mb_amount": row["mb_amount"],
        "notes": row["notes"],
        "created_at": row["created_at"],
    }


@app.get("/api/share-allocations")
async def list_share_allocations(
    shareholder_id: Optional[str] = None,
    as_of: Optional[str] = None,
    authorization: str = Header(None),
):
    """List share allocations for the org. Optional filters:
    - shareholder_id: limit to one person
    - as_of: YYYY-MM-DD → for each shareholder return the most recent
      allocation with effective_date <= as_of (their 'active' allocation).
    Without as_of, returns every snapshot.
    """
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    if as_of:
        rows = db.execute(
            """SELECT a.*, s.display_name AS shareholder_name
               FROM shareholder_share_allocations a
               LEFT JOIN shareholders s ON s.id = a.shareholder_id
               WHERE a.org_id = ? AND a.effective_date <= ?
               ORDER BY a.shareholder_id, a.effective_date DESC""",
            (org_id, as_of),
        ).fetchall()
        # Reduce to latest per shareholder
        seen = {}
        for r in rows:
            sid = r["shareholder_id"]
            if sid in seen: continue
            if shareholder_id and sid != shareholder_id: continue
            seen[sid] = r
        out = []
        for r in seen.values():
            a = _row_to_allocation(r)
            a["shareholder_name"] = r["shareholder_name"]
            out.append(a)
        db.close()
        return out
    # Full history
    clauses = ["a.org_id = ?"]
    params = [org_id]
    if shareholder_id:
        clauses.append("a.shareholder_id = ?"); params.append(shareholder_id)
    rows = db.execute(
        f"""SELECT a.*, s.display_name AS shareholder_name
            FROM shareholder_share_allocations a
            LEFT JOIN shareholders s ON s.id = a.shareholder_id
            WHERE {' AND '.join(clauses)}
            ORDER BY a.effective_date DESC, s.display_name""",
        tuple(params),
    ).fetchall()
    db.close()
    out = []
    for r in rows:
        a = _row_to_allocation(r)
        a["shareholder_name"] = r["shareholder_name"]
        out.append(a)
    return out


@app.post("/api/share-allocations")
async def create_share_allocation(req: ShareAllocationIn, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    sh = db.execute(
        "SELECT id FROM shareholders WHERE id = ? AND org_id = ?",
        (req.shareholder_id, org_id),
    ).fetchone()
    if not sh:
        db.close()
        raise HTTPException(status_code=404, detail="Shareholder not found")
    aid = str(uuid.uuid4())
    try:
        db.execute(
            """INSERT INTO shareholder_share_allocations
               (id, org_id, shareholder_id, effective_date, shares_held,
                ownership_pct, dividend_per_share, mb_amount, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (aid, org_id, req.shareholder_id, req.effective_date,
             float(req.shares_held or 0),
             req.ownership_pct, req.dividend_per_share,
             float(req.mb_amount or 0), req.notes),
        )
        db.commit()
    except sqlite3.IntegrityError:
        # Upsert on (shareholder_id, effective_date)
        db.execute(
            """UPDATE shareholder_share_allocations
               SET shares_held = ?, ownership_pct = ?, dividend_per_share = ?,
                   mb_amount = ?, notes = ?
               WHERE shareholder_id = ? AND effective_date = ?""",
            (float(req.shares_held or 0), req.ownership_pct, req.dividend_per_share,
             float(req.mb_amount or 0), req.notes,
             req.shareholder_id, req.effective_date),
        )
        db.commit()
    row = db.execute(
        """SELECT a.*, s.display_name AS shareholder_name
           FROM shareholder_share_allocations a
           LEFT JOIN shareholders s ON s.id = a.shareholder_id
           WHERE a.shareholder_id = ? AND a.effective_date = ?""",
        (req.shareholder_id, req.effective_date),
    ).fetchone()
    db.close()
    out = _row_to_allocation(row)
    out["shareholder_name"] = row["shareholder_name"]
    return out


@app.delete("/api/share-allocations/{allocation_id}")
async def delete_share_allocation(allocation_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        "SELECT id FROM shareholder_share_allocations WHERE id = ? AND org_id = ?",
        (allocation_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Allocation not found")
    db.execute("DELETE FROM shareholder_share_allocations WHERE id = ?", (allocation_id,))
    db.commit()
    db.close()
    return {"id": allocation_id, "deleted": True}


class AliasIn(BaseModel):
    alias_kind: str   # 'shareholder' | 'company'
    source_label: str
    target_id: str


@app.get("/api/alias-maps")
async def list_aliases(
    alias_kind: Optional[str] = None,
    authorization: str = Header(None),
):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    clauses = ["org_id = ?"]
    params = [org_id]
    if alias_kind:
        clauses.append("alias_kind = ?"); params.append(alias_kind)
    rows = db.execute(
        f"SELECT * FROM shareholder_alias_map WHERE {' AND '.join(clauses)} ORDER BY alias_kind, source_label",
        tuple(params),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@app.post("/api/alias-maps")
async def create_alias(req: AliasIn, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    if req.alias_kind not in ("shareholder", "company"):
        raise HTTPException(status_code=400, detail="alias_kind must be 'shareholder' or 'company'")
    aid = str(uuid.uuid4())
    db = get_db()
    try:
        db.execute(
            """INSERT INTO shareholder_alias_map (id, org_id, alias_kind, source_label, target_id)
               VALUES (?, ?, ?, ?, ?)""",
            (aid, org_id, req.alias_kind, req.source_label, req.target_id),
        )
        db.commit()
    except sqlite3.IntegrityError:
        db.execute(
            "UPDATE shareholder_alias_map SET target_id = ? WHERE org_id = ? AND alias_kind = ? AND source_label = ?",
            (req.target_id, org_id, req.alias_kind, req.source_label),
        )
        db.commit()
    row = db.execute(
        "SELECT * FROM shareholder_alias_map WHERE org_id = ? AND alias_kind = ? AND source_label = ?",
        (org_id, req.alias_kind, req.source_label),
    ).fetchone()
    db.close()
    return dict(row)


@app.delete("/api/alias-maps/{alias_id}")
async def delete_alias(alias_id: str, authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)
    db = get_db()
    row = db.execute(
        "SELECT id FROM shareholder_alias_map WHERE id = ? AND org_id = ?",
        (alias_id, org_id),
    ).fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Alias not found")
    db.execute("DELETE FROM shareholder_alias_map WHERE id = ?", (alias_id,))
    db.commit()
    db.close()
    return {"id": alias_id, "deleted": True}


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
#  DIVIDEND ANALYTICS DASHBOARD
# =====================================================================
#
# One endpoint bundles everything the dashboard needs: KPIs, flags,
# per-company breakdown, per-shareholder concentration, sourcing
# matrix, and a 12-month trend. Reuses P&L + Cash Flow handlers for
# per-company numbers, `shareholder_dividend_events` for distribution
# totals, and the cached `company_accounts` table for point-in-time
# cash on hand (no extra QBO calls).

class DividendDashboardQuery(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    date_macro: Optional[str] = None
    company_ids: Optional[list] = None
    trend_months: Optional[int] = 12
    compare_prior_period: Optional[bool] = True


def _resolve_period(start_date: Optional[str], end_date: Optional[str], macro: Optional[str]):
    """Return (start_date, end_date, label) tuple. If both explicit dates
    are provided they win; otherwise `macro` is interpreted as one of the
    common QBO-style values. Defaults to YTD."""
    if start_date and end_date:
        return start_date, end_date, f"{start_date} to {end_date}"
    now = datetime.now()
    m = (macro or "this-year-to-date").lower()
    if m in ("this-year-to-date", "ytd"):
        return f"{now.year}-01-01", now.strftime("%Y-%m-%d"), f"YTD {now.year}"
    if m in ("this-quarter", "qtd"):
        q_start_month = ((now.month - 1) // 3) * 3 + 1
        return f"{now.year}-{q_start_month:02d}-01", now.strftime("%Y-%m-%d"), f"Q{(q_start_month-1)//3+1} {now.year}"
    if m in ("this-month", "mtd"):
        return f"{now.year}-{now.month:02d}-01", now.strftime("%Y-%m-%d"), f"{calendar.month_name[now.month]} {now.year}"
    if m == "last-year":
        y = now.year - 1
        return f"{y}-01-01", f"{y}-12-31", f"FY {y}"
    if m == "last-month":
        y = now.year if now.month > 1 else now.year - 1
        mo = now.month - 1 or 12
        last = calendar.monthrange(y, mo)[1]
        return f"{y}-{mo:02d}-01", f"{y}-{mo:02d}-{last:02d}", f"{calendar.month_name[mo]} {y}"
    # Fallback YTD
    return f"{now.year}-01-01", now.strftime("%Y-%m-%d"), f"YTD {now.year}"


def _prior_period(start_date: str, end_date: str):
    """Shift (start, end) back by the same span so KPIs can show deltas."""
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return start_date, end_date, f"{start_date} to {end_date}"
    span_days = (e - s).days + 1
    prior_end = s - timedelta(days=1)
    prior_start = prior_end - timedelta(days=span_days - 1)
    ss = prior_start.strftime("%Y-%m-%d")
    ee = prior_end.strftime("%Y-%m-%d")
    return ss, ee, f"{ss} to {ee}"


def _cash_on_hand_by_company(db, company_ids: list):
    """Sum of Bank + cash-like Other-Current-Asset balances from the
    cached company_accounts table, per company. company_accounts has
    no org_id column; callers must pre-filter company_ids by org."""
    if not company_ids:
        return {}
    placeholders = ",".join("?" for _ in company_ids)
    rows = db.execute(
        f"""SELECT company_id, SUM(current_balance) AS cash
            FROM company_accounts
            WHERE active = 1
              AND company_id IN ({placeholders})
              AND ( account_type = 'Bank'
                    OR ( account_type = 'Other Current Asset'
                         AND ( account_sub_type LIKE '%Cash%'
                               OR account_sub_type = 'UndepositedFunds' ) ) )
            GROUP BY company_id""",
        tuple(company_ids),
    ).fetchall()
    return {r["company_id"]: float(r["cash"] or 0) for r in rows}


def _extract_pl_net_income(report: dict) -> float:
    if not report: return 0.0
    cur = report.get("current") or report
    totals_obj = cur.get("totals") if isinstance(cur, dict) else None
    if isinstance(totals_obj, dict):
        for k in ("net_income", "NetIncome", "net_operating_income", "net_income_loss"):
            if k in totals_obj:
                try: return float(totals_obj[k])
                except Exception: pass
    if isinstance(cur, dict) and "net_income" in cur:
        try: return float(cur["net_income"])
        except Exception: pass
    return 0.0


def _extract_cf_block(report: dict, key: str) -> float:
    if not report: return 0.0
    cur = report.get("current") or report
    totals_obj = cur.get("totals") if isinstance(cur, dict) else None
    if isinstance(totals_obj, dict) and key in totals_obj:
        try: return float(totals_obj[key])
        except Exception: pass
    if isinstance(cur, dict) and key in cur:
        try: return float(cur[key])
        except Exception: pass
    return 0.0


@app.post("/api/dashboard/dividends")
async def dividend_dashboard(req: DividendDashboardQuery, authorization: str = Header(None)):
    """One-shot dashboard payload: KPIs, flags, per-company, per-
    shareholder, monthly trend. Safe to call with no connected
    companies — returns empty arrays rather than 500."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    require_admin(user)
    org_id = get_org_id(user)

    period_start, period_end, period_label = _resolve_period(
        req.start_date, req.end_date, req.date_macro
    )

    db = get_db()
    co_rows = db.execute(
        "SELECT id, name FROM companies WHERE org_id = ? AND status IN ('connected','synced')",
        (org_id,),
    ).fetchall()
    if req.company_ids:
        wanted = set(req.company_ids)
        co_rows = [c for c in co_rows if c["id"] in wanted]
    company_ids = [c["id"] for c in co_rows]

    # Dividend totals per company for current period
    def _div_totals(s: str, e: str) -> dict:
        if not company_ids:
            return {}
        ph = ",".join("?" for _ in company_ids)
        rows = db.execute(
            f"""SELECT company_id, SUM(amount) AS total
                FROM shareholder_dividend_events
                WHERE org_id = ? AND kind IN ('payment','managing_bonus')
                  AND qbo_post_status IN ('posted','skipped')
                  AND event_date BETWEEN ? AND ?
                  AND company_id IN ({ph})
                GROUP BY company_id""",
            (org_id, s, e, *company_ids),
        ).fetchall()
        return {r["company_id"]: float(r["total"] or 0) for r in rows}

    current_div_by_co = _div_totals(period_start, period_end)
    prior_start = prior_end = prior_label = None
    prior_div_by_co = {}
    if req.compare_prior_period:
        prior_start, prior_end, prior_label = _prior_period(period_start, period_end)
        prior_div_by_co = _div_totals(prior_start, prior_end)

    # Per-kind breakdown (pro-rata Dividend vs Managing Bonus) for the
    # current period, so the dashboard can show the split.
    def _kind_totals(s: str, e: str) -> dict:
        if not company_ids:
            return {}
        ph = ",".join("?" for _ in company_ids)
        rows = db.execute(
            f"""SELECT company_id, kind, SUM(amount) AS total
                FROM shareholder_dividend_events
                WHERE org_id = ? AND kind IN ('payment','managing_bonus')
                  AND qbo_post_status IN ('posted','skipped')
                  AND event_date BETWEEN ? AND ?
                  AND company_id IN ({ph})
                GROUP BY company_id, kind""",
            (org_id, s, e, *company_ids),
        ).fetchall()
        out = {}
        for r in rows:
            out.setdefault(r["company_id"], {"payment": 0.0, "managing_bonus": 0.0})
            out[r["company_id"]][r["kind"]] = float(r["total"] or 0)
        return out

    kind_by_co = _kind_totals(period_start, period_end)

    # Per-shareholder aggregation for current period
    sh_rows = db.execute(
        f"""SELECT e.shareholder_id, s.display_name AS name, e.company_id, c.name AS company_name,
                   SUM(e.amount) AS amount
            FROM shareholder_dividend_events e
            LEFT JOIN shareholders s ON s.id = e.shareholder_id
            LEFT JOIN companies c    ON c.id = e.company_id
            WHERE e.org_id = ? AND e.kind IN ('payment','managing_bonus')
              AND e.qbo_post_status IN ('posted','skipped')
              AND e.event_date BETWEEN ? AND ?
              {('AND e.company_id IN (' + ','.join('?' for _ in company_ids) + ')') if company_ids else ''}
            GROUP BY e.shareholder_id, e.company_id
            ORDER BY s.display_name""",
        (org_id, period_start, period_end, *(company_ids if company_ids else ())),
    ).fetchall()

    by_shareholder_map = {}
    for r in sh_rows:
        sid = r["shareholder_id"]
        if sid not in by_shareholder_map:
            by_shareholder_map[sid] = {
                "shareholder_id": sid,
                "shareholder_name": r["name"] or "(unknown)",
                "total_paid": 0.0,
                "share_of_total": 0.0,
                "by_company": [],
            }
        amt = float(r["amount"] or 0)
        by_shareholder_map[sid]["total_paid"] += amt
        by_shareholder_map[sid]["by_company"].append({
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "amount": round(amt, 2),
        })

    # Cash on hand (current snapshot from cached accounts)
    cash_by_co = _cash_on_hand_by_company(db, company_ids)
    db.close()

    # Per-company: P&L + Cash Flow (live via existing handlers)
    by_company = []
    total_ni = 0.0
    total_div = 0.0
    total_operating = 0.0
    total_investing = 0.0
    total_financing = 0.0
    flags: list = []
    for co in co_rows:
        try:
            pl_params = ReportParams(
                company_id=co["id"],
                start_date=period_start, end_date=period_end,
            )
            cf_params = ReportParams(
                company_id=co["id"],
                start_date=period_start, end_date=period_end,
            )
            pl = await get_profit_loss(pl_params, authorization)
            cf = await get_cash_flow(cf_params, authorization)
        except HTTPException:
            pl = cf = None
        except Exception:
            pl = cf = None

        ni = _extract_pl_net_income(pl)
        op = _extract_cf_block(cf, "operating_activities")
        iv = _extract_cf_block(cf, "investing_activities")
        fn = _extract_cf_block(cf, "financing_activities")
        div = current_div_by_co.get(co["id"], 0.0)
        cash = cash_by_co.get(co["id"], 0.0)
        payout = (div / ni) if ni > 0 else None
        distributable = ni - div
        financing_variance = fn - (-div)

        row_flags = []
        if ni > 0 and div > ni:
            row_flags.append("over_distribution")
            flags.append({
                "kind": "over_distribution", "scope": "company",
                "id": co["id"], "label": co["name"],
                "message": f"{co['name']}: dividends (${div:,.0f}) exceed net income (${ni:,.0f}).",
            })
        if div > op and op > 0:
            row_flags.append("dividends_exceed_operating_cash")
            flags.append({
                "kind": "dividends_exceed_operating_cash", "scope": "company",
                "id": co["id"], "label": co["name"],
                "message": f"{co['name']}: dividends (${div:,.0f}) exceed operating cash (${op:,.0f}).",
            })
        # Thin cash cushion: cash < 3x monthly avg dividend for this company over the period
        # Approximate monthly avg by assuming the period days map to months proportionally.
        try:
            period_days = (datetime.strptime(period_end, "%Y-%m-%d") - datetime.strptime(period_start, "%Y-%m-%d")).days + 1
        except Exception:
            period_days = 30
        monthly_avg_div = (div / period_days * 30) if period_days else 0
        if monthly_avg_div > 0 and cash < 3 * monthly_avg_div:
            row_flags.append("thin_cash_cushion")
            flags.append({
                "kind": "thin_cash_cushion", "scope": "company",
                "id": co["id"], "label": co["name"],
                "message": f"{co['name']}: cash on hand (${cash:,.0f}) < 3 × monthly avg dividend (${monthly_avg_div:,.0f}).",
            })
        if abs(financing_variance) > 1:
            row_flags.append("financing_variance")

        kinds = kind_by_co.get(co["id"], {"payment": 0.0, "managing_bonus": 0.0})
        by_company.append({
            "company_id": co["id"], "company_name": co["name"],
            "cash_on_hand": round(cash, 2),
            "net_income": round(ni, 2),
            "dividends_paid": round(div, 2),
            "dividends_pro_rata": round(kinds.get("payment", 0.0), 2),
            "managing_bonus_paid": round(kinds.get("managing_bonus", 0.0), 2),
            "payout_ratio": None if payout is None else round(payout, 4),
            "distributable_remainder": round(distributable, 2),
            "operating_cash": round(op, 2),
            "investing_cash": round(iv, 2),
            "financing_cash": round(fn, 2),
            "financing_variance": round(financing_variance, 2),
            "flags": row_flags,
        })
        total_ni += ni
        total_div += div
        total_operating += op
        total_investing += iv
        total_financing += fn

    total_cash = sum(cash_by_co.values())
    prior_total_div = sum(prior_div_by_co.values())

    # Prior-period net income (for KPI delta) — single consolidated call
    prior_total_ni = 0.0
    if req.compare_prior_period and company_ids:
        for co in co_rows:
            try:
                pl_prior = await get_profit_loss(
                    ReportParams(company_id=co["id"], start_date=prior_start, end_date=prior_end),
                    authorization,
                )
                prior_total_ni += _extract_pl_net_income(pl_prior)
            except Exception:
                pass

    # Per-shareholder pro-rata ($ payment kind only) + MB actual totals
    pro_rata_by_sh = {}
    mb_by_sh = {}
    _sh_kind_rows = db_sh_kind = None
    try:
        db_sh_kind = get_db()
        _sh_kind_rows = db_sh_kind.execute(
            f"""SELECT shareholder_id, kind, SUM(amount) AS total
                FROM shareholder_dividend_events
                WHERE org_id = ? AND kind IN ('payment','managing_bonus')
                  AND qbo_post_status IN ('posted','skipped')
                  AND event_date BETWEEN ? AND ?
                  {('AND company_id IN (' + ','.join('?' for _ in company_ids) + ')') if company_ids else ''}
                GROUP BY shareholder_id, kind""",
            (org_id, period_start, period_end, *(company_ids if company_ids else ())),
        ).fetchall()
    finally:
        if db_sh_kind: db_sh_kind.close()
    for r in (_sh_kind_rows or []):
        if r["kind"] == "payment":
            pro_rata_by_sh[r["shareholder_id"]] = float(r["total"] or 0)
        else:
            mb_by_sh[r["shareholder_id"]] = float(r["total"] or 0)

    # Active share allocation as of period_end — used for expected pro-rata
    db_alloc = get_db()
    alloc_rows = db_alloc.execute(
        """SELECT shareholder_id, MAX(effective_date) AS eff, shares_held, ownership_pct, dividend_per_share, mb_amount
           FROM shareholder_share_allocations
           WHERE org_id = ? AND effective_date <= ?
           GROUP BY shareholder_id""",
        (org_id, period_end),
    ).fetchall()
    db_alloc.close()
    alloc_by_sh = {}
    for r in alloc_rows:
        alloc_by_sh[r["shareholder_id"]] = {
            "shares_held": float(r["shares_held"] or 0),
            "ownership_pct": float(r["ownership_pct"]) if r["ownership_pct"] is not None else None,
            "dividend_per_share": float(r["dividend_per_share"]) if r["dividend_per_share"] is not None else None,
            "mb_amount": float(r["mb_amount"] or 0),
        }

    # Shareholder share_of_total + concentration flag + pro-rata variance
    for sid, s in by_shareholder_map.items():
        s["total_paid"] = round(s["total_paid"], 2)
        s["share_of_total"] = round((s["total_paid"] / total_div) if total_div > 0 else 0.0, 4)
        s["pro_rata_paid"] = round(pro_rata_by_sh.get(sid, 0.0), 2)
        s["managing_bonus_paid"] = round(mb_by_sh.get(sid, 0.0), 2)
        alloc = alloc_by_sh.get(sid)
        s["allocation"] = alloc
        # Expected pro-rata = shares × DPS (from most recent allocation <= period_end)
        if alloc and alloc.get("shares_held") and alloc.get("dividend_per_share") is not None:
            expected = alloc["shares_held"] * alloc["dividend_per_share"]
        elif alloc and alloc.get("ownership_pct") is not None and total_pro_rata > 0:
            expected = alloc["ownership_pct"] * total_pro_rata
        else:
            expected = None
        s["expected_pro_rata"] = None if expected is None else round(expected, 2)
        s["pro_rata_variance"] = None if expected is None else round(s["pro_rata_paid"] - expected, 2)

        if s["share_of_total"] > 0.5 and total_div > 0:
            flags.append({
                "kind": "concentration", "scope": "shareholder",
                "id": sid, "label": s["shareholder_name"],
                "message": f"{s['shareholder_name']} accounts for {s['share_of_total']*100:.0f}% of distributions this period.",
            })
        if s["pro_rata_variance"] is not None and abs(s["pro_rata_variance"]) >= 1:
            flags.append({
                "kind": "pro_rata_variance", "scope": "shareholder",
                "id": sid, "label": s["shareholder_name"],
                "message": (
                    f"{s['shareholder_name']}: pro-rata variance "
                    f"${s['pro_rata_variance']:+,.0f} vs expected ${expected:,.0f} "
                    f"(paid ${s['pro_rata_paid']:,.0f})."
                ),
            })

    # KPIs
    payout_ratio = (total_div / total_ni) if total_ni > 0 else None
    # Cash runway: cash_on_hand / trailing-3-month avg dividends (use period-scaled)
    monthly_avg_all = (total_div / max(period_days, 1)) * 30 if total_div else 0
    cash_runway = (total_cash / monthly_avg_all) if monthly_avg_all > 0 else None
    cash_conversion = (total_operating / total_ni) if total_ni > 0 else None
    if cash_conversion is not None and (cash_conversion < 0.5 or cash_conversion > 1.5):
        flags.append({
            "kind": "cash_conversion_off", "scope": "org",
            "id": None, "label": "Cash Conversion",
            "message": f"Operating cash / net income = {cash_conversion:.2f} — expected 0.5–1.5.",
        })

    total_pro_rata = sum((k.get("payment", 0.0) for k in kind_by_co.values()), 0.0)
    total_mb = sum((k.get("managing_bonus", 0.0) for k in kind_by_co.values()), 0.0)
    kpis = {
        "cash_on_hand": {"current": round(total_cash, 2), "delta_vs_period_start": None},
        "net_income": {"current": round(total_ni, 2), "prior": round(prior_total_ni, 2) if req.compare_prior_period else None},
        "dividends_paid": {"current": round(total_div, 2), "prior": round(prior_total_div, 2) if req.compare_prior_period else None},
        "dividends_pro_rata": {"current": round(total_pro_rata, 2)},
        "managing_bonus_paid": {"current": round(total_mb, 2)},
        "payout_ratio": None if payout_ratio is None else round(payout_ratio, 4),
        "distributable_remainder": round(total_ni - total_div, 2),
        "cash_runway_months": None if cash_runway is None else round(cash_runway, 1),
        "cash_conversion": None if cash_conversion is None else round(cash_conversion, 4),
    }

    # Monthly trend: SQL for dividends; delegate to revenue-trend logic for NI
    trend_months = max(1, min(int(req.trend_months or 12), 24))
    # Build [(year, month, label), ...] for the last N months ending last complete month
    now = datetime.now()
    lm_month = (now.month - 1) or 12
    lm_year = now.year if now.month > 1 else now.year - 1
    months = []
    for i in range(trend_months - 1, -1, -1):
        total_m = lm_year * 12 + lm_month - i
        y = (total_m - 1) // 12
        m = ((total_m - 1) % 12) + 1
        months.append((y, m))

    # Monthly dividends from SQL
    monthly_div = {}
    db = get_db()
    ph = ",".join("?" for _ in company_ids) if company_ids else ""
    q = (
        "SELECT substr(event_date,1,7) AS ym, SUM(amount) AS total "
        "FROM shareholder_dividend_events "
        "WHERE org_id = ? AND kind IN ('payment','managing_bonus') "
        "AND qbo_post_status IN ('posted','skipped') "
        + (f"AND company_id IN ({ph}) " if company_ids else "")
        + "GROUP BY substr(event_date,1,7)"
    )
    params = (org_id, *(company_ids if company_ids else ()))
    for r in db.execute(q, params).fetchall():
        monthly_div[r["ym"]] = float(r["total"] or 0)
    db.close()

    # Monthly NI: call P&L per company per month. Cheap when few companies
    # in the local preview DB; for larger orgs this is where caching
    # (company_reports) already helps on the QBO side.
    trend = []
    for (y, m) in months:
        last_day = calendar.monthrange(y, m)[1]
        s = f"{y}-{m:02d}-01"
        e = f"{y}-{m:02d}-{last_day:02d}"
        ym = f"{y}-{m:02d}"
        ni_month = 0.0
        for co in co_rows:
            try:
                pl_m = await get_profit_loss(
                    ReportParams(company_id=co["id"], start_date=s, end_date=e),
                    authorization,
                )
                ni_month += _extract_pl_net_income(pl_m)
            except Exception:
                pass
        div_month = monthly_div.get(ym, 0.0)
        trend.append({
            "month": ym,
            "label": f"{calendar.month_abbr[m]} {y}",
            "net_income": round(ni_month, 2),
            "dividends_paid": round(div_month, 2),
            "payout_ratio": round(div_month / ni_month, 4) if ni_month > 0 else None,
        })

    return {
        "period": {"start_date": period_start, "end_date": period_end, "macro": req.date_macro, "label": period_label},
        "compare_period": (
            {"start_date": prior_start, "end_date": prior_end, "label": prior_label}
            if req.compare_prior_period else None
        ),
        "kpis": kpis,
        "flags": flags,
        "by_company": by_company,
        "by_shareholder": list(by_shareholder_map.values()),
        "trend_monthly": trend,
        "totals": {
            "cash_on_hand": round(total_cash, 2),
            "net_income": round(total_ni, 2),
            "dividends_paid": round(total_div, 2),
            "operating_cash": round(total_operating, 2),
            "investing_cash": round(total_investing, 2),
            "financing_cash": round(total_financing, 2),
        },
    }


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
        "SELECT id, name, status FROM companies WHERE org_id = ? ORDER BY name", (org_id,)
    ).fetchall()
    db.close()
    if not companies:
        return "No companies connected yet."
    lines = []
    for c in companies:
        lines.append(f"- {c['name']} (id: {c['id']}, status: {c['status']})")
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
