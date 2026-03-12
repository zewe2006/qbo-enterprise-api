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
import hashlib
import json
import logging
import os
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
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional

# ---------- AI Chat Config ----------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
AI_MODEL = os.environ.get("AI_MODEL", "gemini-2.0-flash")

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


@app.get("/api/companies/{company_id}/customers")
async def get_company_customers(company_id: str, authorization: str = Header(None)):
    """Fetch active customers from QBO for a specific company."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        result = await qbo_api_call(
            db, company_id, "query", method="GET",
            params={"query": "SELECT * FROM Customer WHERE Active = true", "minorversion": "65"}
        )
        customers = result.get("QueryResponse", {}).get("Customer", [])
        return [{"id": c["Id"], "name": c.get("DisplayName", c.get("CompanyName", "")), "type": "Customer"} for c in customers]
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"Error fetching customers: {str(ex)}")
    finally:
        db.close()


@app.get("/api/companies/{company_id}/vendors")
async def get_company_vendors(company_id: str, authorization: str = Header(None)):
    """Fetch active vendors from QBO for a specific company."""
    token = _extract_token(authorization)
    user = get_current_user(token)
    org_id = get_org_id(user)
    db = get_db()
    company = db.execute("SELECT id FROM companies WHERE id = ? AND org_id = ?", (company_id, org_id)).fetchone()
    if not company:
        db.close()
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        result = await qbo_api_call(
            db, company_id, "query", method="GET",
            params={"query": "SELECT * FROM Vendor WHERE Active = true", "minorversion": "65"}
        )
        vendors = result.get("QueryResponse", {}).get("Vendor", [])
        return [{"id": v["Id"], "name": v.get("DisplayName", v.get("CompanyName", "")), "type": "Vendor"} for v in vendors]
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
            if result.get("prior_month"):
                prior_month_reports.append(result["prior_month"])
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
2. **Pulling financial reports** — P&L, Balance Sheet, Cash Flow for specific companies or consolidated across all.
3. **Analyzing financial data** — Compare companies, identify trends, answer questions about revenue, expenses, etc.
4. **App navigation and help** — Guide users on how to use features.

When the user asks to create a journal entry, gather the necessary info and respond with a special JSON block that the frontend will parse:
```action:create_je
{{"source_company_id": "...", "dest_company_id": "...", "entry_type": "...", "description": "...", "date": "YYYY-MM-DD", "amount": 0, "lines": [{{"side": "source", "posting_type": "Debit", "account_name": "...", "amount": 0}}, ...]}}
```

When the user asks for a report, respond with:
```action:show_report
{{"report_type": "profit-loss|balance-sheet|cash-flow", "company_id": "all|<specific-id>", "period": "last_month|ytd|custom", "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}}
```

When the user asks to navigate somewhere:
```action:navigate
{{"page": "dashboard|companies|intercompany|account-mapping|users|billing"}}
```

Always be concise and helpful. Use the company and account context below to resolve company names to IDs.
For journal entries, each side (source and dest) must balance: total debits = total credits on that side.
Common entry types: Management Fee, Loan, Expense Reimbursement, Revenue Transfer, Cost Allocation.
Today's date: {today}.

{company_context}

{accounts_context}"""


@app.post("/api/chat")
async def chat(req: ChatMessage, authorization: str = Header(None)):
    """AI chat endpoint — processes user messages with Google Gemini."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="AI chat is not configured. Set GEMINI_API_KEY on Railway.")

    try:
        token = _extract_token(authorization)
        user = get_current_user(token)
        org_id = get_org_id(user)

        # Build context
        company_context = _build_company_context(org_id)
        accounts_context = _build_accounts_context(org_id)
        today = datetime.now().strftime("%Y-%m-%d")

        system_msg = CHAT_SYSTEM_PROMPT.format(
            today=today,
            company_context=company_context,
            accounts_context=accounts_context,
        )

        # Build Gemini contents array
        contents = []
        if req.conversation:
            for msg in req.conversation[-10:]:
                role = "model" if msg.get("role") == "assistant" else "user"
                contents.append({"role": role, "parts": [{"text": msg.get("content", "")}]})
        contents.append({"role": "user", "parts": [{"text": req.message}]})

        # Call Google Gemini API
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{AI_MODEL}:generateContent?key={GEMINI_API_KEY}"

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                gemini_url,
                headers={"Content-Type": "application/json"},
                json={
                    "system_instruction": {"parts": [{"text": system_msg}]},
                    "contents": contents,
                    "generationConfig": {
                        "temperature": 0.3,
                        "maxOutputTokens": 2000,
                    },
                },
            )

        if resp.status_code != 200:
            logger.error("Gemini API error: %s %s", resp.status_code, resp.text[:300])
            raise HTTPException(status_code=502, detail="AI service error. Please try again.")

        data = resp.json()
        try:
            reply = data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            logger.error("Gemini unexpected response: %s", json.dumps(data)[:500])
            raise HTTPException(status_code=502, detail="AI returned an unexpected response.")

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
