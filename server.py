#!/usr/bin/env python3
"""Enterprise QBO Consolidated Dashboard — Backend API Server.

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
import hashlib
import json
import os
import secrets
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional

# ---------- QBO Config ----------

QBO_CLIENT_ID = os.environ.get(
    "QBO_CLIENT_ID", "ABNtRaTG8srlHkmlrGKJd9lbhsGSB0es8t9mu2SY30v5Mekk0w"
)
QBO_CLIENT_SECRET = os.environ.get(
    "QBO_CLIENT_SECRET", "1q5IQ9fIJ4CE2kG6dF37oeN7HX0LQ4PoAnUbITte"
)
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
    "FRONTEND_ORIGIN", "https://consolidatedreport.netlify.app"
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
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT,
            role TEXT DEFAULT 'user',
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

    # Default admin user
    existing = db.execute("SELECT id FROM users LIMIT 1").fetchone()
    if not existing:
        admin_id = str(uuid.uuid4())
        pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
        db.execute(
            "INSERT INTO users (id, email, password_hash, name, role) VALUES (?, ?, ?, ?, ?)",
            (admin_id, "admin@enterprise.local", pw_hash, "Admin", "admin"),
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

    _upsert_company_from_info(db, cid, info, "connected")

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
                                token_expires_at=None):
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
        sql += " WHERE id=?"
        params.append(cid)
        db.execute(sql, params)
    else:
        db.execute(
            """INSERT INTO companies
               (id, name, legal_name, qbo_company_id, qbo_realm_id,
                access_token, refresh_token, token_expires_at,
                address, phone, email, industry, qbo_plan, status, last_synced)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (cid, name, legal, qbo_id, realm_id or "",
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

    if resp.status_code != 200:
        # Mark company as needing re-auth
        db.execute("UPDATE companies SET status='auth_expired' WHERE id=?", (company_id,))
        db.commit()
        raise HTTPException(
            status_code=401,
            detail=f"Token refresh failed. Please re-authorize this company. QBO response: {resp.text[:200]}"
        )

    tokens = resp.json()
    new_access = tokens["access_token"]
    new_refresh = tokens.get("refresh_token", refresh_token)
    expires_in = tokens.get("expires_in", 3600)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

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

    if resp.status_code == 401:
        # Token might be stale despite our check — try one refresh
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

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"QBO API error: {resp.text[:300]}"
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
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text[:300])
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
    yield

app = FastAPI(lifespan=lifespan, title="QBO Enterprise Dashboard API")
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
    db.close()
    u = dict(user)
    u["company_ids"] = [r["company_id"] for r in access_rows]
    return u


def require_admin(user: dict):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")


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
    db.close()
    return {
        "token": token,
        "user": {
            "id": user["id"], "email": user["email"], "name": user["name"],
            "role": user["role"],
            "company_ids": [r["company_id"] for r in access_rows],
        },
    }

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    # Self-registration disabled — admin-only
    raise HTTPException(status_code=403, detail="Self-registration is disabled. Contact an admin.")

@app.get("/api/auth/me")
async def get_me(authorization: str = Header(None)):
    token = _extract_token(authorization)
    user = get_current_user(token)
    return {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "role": user["role"], "company_ids": user.get("company_ids", []),
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
    db = get_db()
    rows = db.execute("SELECT id, email, name, role, created_at FROM users ORDER BY created_at").fetchall()
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
        "INSERT INTO users (id, email, password_hash, name, role) VALUES (?, ?, ?, ?, ?)",
        (user_id, req.email, pw_hash, req.name, req.role),
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
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
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
    if user_id == admin["id"]:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    db = get_db()
    db.execute("DELETE FROM user_company_access WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    db.close()
    return {"deleted": user_id}


# =====================================================================
#  QBO OAuth 2.0 — Direct Integration
# =====================================================================

class AuthorizeRequest(BaseModel):
    frontend_origin: Optional[str] = None


@app.post("/api/qbo/authorize")
async def qbo_authorize(request: Request, body: AuthorizeRequest = None):
    """Generate a QBO OAuth authorization URL.

    The frontend opens this URL in a popup window. After the user signs in
    and selects a company, QBO redirects to our callback endpoint.
    """
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

    # Store state + redirect_uri for validation in the callback
    db = get_db()
    db.execute(
        "INSERT INTO oauth_states (state, redirect_uri) VALUES (?, ?)",
        (state, redirect_uri),
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
        "SELECT state, redirect_uri FROM oauth_states WHERE state = ?", (state,)
    ).fetchone()
    if not state_row:
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

    if token_resp.status_code != 200:
        err_detail = token_resp.text[:300]
        db.close()
        return HTMLResponse(content=f"""<html><body>
            <h2>Token Exchange Failed</h2><p>{err_detail}</p>
            <script>
                if (window.opener) {{
                    window.opener.postMessage({{type:'qbo_auth_error', error:'Token exchange failed'}}, '*');
                    window.close();
                }}
            </script></body></html>""")

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

    if info_resp.status_code != 200:
        db.close()
        return HTMLResponse(content=f"""<html><body>
            <h2>Connected but could not fetch company info</h2>
            <script>
                if (window.opener) {{
                    window.opener.postMessage({{type:'qbo_auth_error', error:'Could not fetch company info'}}, '*');
                    window.close();
                }}
            </script></body></html>""")

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
        cid = str(uuid.uuid4())

    # Save company with tokens
    _upsert_company_from_info(
        db, cid, info, "connected",
        realm_id=realmId,
        access_token=access_token,
        refresh_token=refresh_token_val,
        token_expires_at=expires_at,
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
    db = get_db()
    rows = db.execute(
        """SELECT id, name, legal_name, qbo_company_id, qbo_realm_id,
                  status, last_synced, created_at, address, phone, email,
                  industry, qbo_plan,
                  CASE WHEN access_token IS NOT NULL AND access_token != '' THEN 1 ELSE 0 END as has_token
           FROM companies ORDER BY name"""
    ).fetchall()
    db.close()
    companies = [dict(r) for r in rows]
    # Non-admin users only see companies they have access to
    if user["role"] != "admin":
        allowed = set(user.get("company_ids", []))
        companies = [c for c in companies if c["id"] in allowed]
    return companies


@app.get("/api/companies/connected")
async def get_connected_company():
    """Get the first connected company with valid tokens for the header badge."""
    db = get_db()
    row = db.execute(
        """SELECT id, name, qbo_realm_id, access_token, refresh_token
           FROM companies WHERE status='connected' AND qbo_realm_id IS NOT NULL
           AND qbo_realm_id != '' LIMIT 1"""
    ).fetchone()
    db.close()

    if not row:
        raise HTTPException(status_code=404, detail="No connected company")

    return {
        "company": {"CompanyName": row["name"]},
        "company_db_id": row["id"],
    }


@app.post("/api/companies/{company_id}/sync")
async def sync_company(company_id: str):
    """Pull ALL data from a specific company using its stored tokens."""
    db = get_db()

    company = db.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
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
async def delete_company(company_id: str):
    db = get_db()
    db.execute("DELETE FROM company_reports WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM company_accounts WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM account_mappings WHERE company_id = ?", (company_id,))
    db.execute("DELETE FROM companies WHERE id = ?", (company_id,))
    db.commit()
    db.close()
    return {"deleted": company_id}


@app.get("/api/companies/{company_id}/accounts")
async def get_company_accounts(company_id: str):
    db = get_db()
    rows = db.execute(
        """SELECT * FROM company_accounts WHERE company_id = ? AND active = 1
           ORDER BY classification, account_type, name""",
        (company_id,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


@app.get("/api/companies/{company_id}/customers")
async def get_company_customers(company_id: str):
    """Fetch active customers from QBO for a specific company."""
    db = get_db()
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
async def get_company_vendors(company_id: str):
    """Fetch active vendors from QBO for a specific company."""
    db = get_db()
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


@app.post("/api/reports/profit-loss")
async def get_profit_loss(params: ReportParams):
    if params.company_id == "all":
        result = _get_cached_report(params, "profit_loss")
        if result.get("current") is None:
            result = await _get_live_consolidated(params, "ProfitAndLoss", "profit_loss")
        return result
    if params.company_id:
        return await _get_live_report_for_company(params, "ProfitAndLoss", "profit_loss")
    return {"current": None, "message": "Select a company"}


async def _get_live_consolidated(params, qbo_report_name, report_type):
    """Pull live reports from selected (or all) connected companies and merge them."""
    db = get_db()
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
    company_names = []
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
            if result.get("current"):
                reports.append(result["current"])
                company_names.append({"name": company["name"], "company_id": company["id"]})
        except Exception:
            pass

    if not reports:
        return {"current": None, "consolidated": True, "companies": [], "message": "Could not pull live data from any company."}

    merged = _merge_reports(reports)
    return {"current": merged, "consolidated": True, "companies": company_names}


@app.post("/api/reports/balance-sheet")
async def get_balance_sheet(params: ReportParams):
    if params.company_id == "all":
        result = _get_cached_report(params, "balance_sheet")
        if result.get("current") is None:
            result = await _get_live_consolidated(params, "BalanceSheet", "balance_sheet")
        return result
    if params.company_id:
        return await _get_live_report_for_company(params, "BalanceSheet", "balance_sheet")
    return {"current": None, "message": "Select a company"}


@app.post("/api/reports/cash-flow")
async def get_cash_flow(params: ReportParams):
    if params.company_id == "all":
        result = _get_cached_report(params, "cash_flow")
        if result.get("current") is None:
            result = await _get_live_consolidated(params, "CashFlow", "cash_flow")
        return result
    if params.company_id:
        return await _get_live_report_for_company(params, "CashFlow", "cash_flow")
    return {"current": None, "message": "Select a company"}


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

        if params.compare_prior_year and params.start_date and params.end_date:
            prior_params = dict(qbo_params)
            start = datetime.strptime(params.start_date, "%Y-%m-%d")
            end = datetime.strptime(params.end_date, "%Y-%m-%d")
            prior_params["start_date"] = start.replace(year=start.year - 1).strftime("%Y-%m-%d")
            prior_params["end_date"] = end.replace(year=end.year - 1).strftime("%Y-%m-%d")
            if "date_macro" in prior_params:
                del prior_params["date_macro"]
            try:
                result["prior_year"] = await qbo_get_report(
                    db, params.company_id, qbo_report_name, prior_params
                )
            except Exception:
                result["prior_year"] = None

        if params.compare_prior_month and params.start_date and params.end_date:
            prior_params = dict(qbo_params)
            start = datetime.strptime(params.start_date, "%Y-%m-%d")
            end = datetime.strptime(params.end_date, "%Y-%m-%d")
            m = start.month - 1 or 12
            y = start.year if start.month > 1 else start.year - 1
            pm_start = start.replace(year=y, month=m)
            m2 = end.month - 1 or 12
            y2 = end.year if end.month > 1 else end.year - 1
            pm_end = end.replace(year=y2, month=m2)
            prior_params["start_date"] = pm_start.strftime("%Y-%m-%d")
            prior_params["end_date"] = pm_end.strftime("%Y-%m-%d")
            if "date_macro" in prior_params:
                del prior_params["date_macro"]
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


def _get_cached_report(params, report_type):
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


def _merge_reports(reports):
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
async def list_cached_accounts(company_id: str = None):
    db = get_db()
    if company_id:
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
               WHERE ca.active = 1
               ORDER BY c.name, ca.classification, ca.account_type, ca.name"""
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
async def list_account_mappings(company_id: str = None):
    db = get_db()
    if company_id:
        rows = db.execute(
            """SELECT am.*, c.name AS company_name FROM account_mappings am
               LEFT JOIN companies c ON am.company_id = c.id
               WHERE am.company_id = ?
               ORDER BY am.consolidated_category""",
            (company_id,),
        ).fetchall()
    else:
        rows = db.execute(
            """SELECT am.*, c.name AS company_name FROM account_mappings am
               LEFT JOIN companies c ON am.company_id = c.id
               ORDER BY c.name, am.consolidated_category"""
        ).fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/api/account-mappings")
async def create_account_mapping(req: AccountMappingRequest):
    db = get_db()
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
async def delete_account_mapping(mapping_id: str):
    db = get_db()
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
async def list_ic_entries():
    db = get_db()
    rows = db.execute(
        """SELECT ie.*,
           sc.name AS source_company_name, dc.name AS dest_company_name
           FROM intercompany_entries ie
           LEFT JOIN companies sc ON ie.source_company_id = sc.id
           LEFT JOIN companies dc ON ie.dest_company_id = dc.id
           ORDER BY ie.created_at DESC"""
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
async def create_ic_entry(req: ICEntryRequest):
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
           (id, source_company_id, dest_company_id, entry_type, amount, description, date, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
        (entry_id, req.source_company_id, req.dest_company_id, req.entry_type,
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
async def post_ic_entry(entry_id: str):
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ?", (entry_id,)).fetchone()
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
async def update_ic_entry(entry_id: str, req: ICEntryRequest):
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ?", (entry_id,)).fetchone()
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
async def delete_ic_entry(entry_id: str):
    db = get_db()
    entry = db.execute("SELECT * FROM intercompany_entries WHERE id = ?", (entry_id,)).fetchone()
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
async def list_ic_templates():
    db = get_db()
    rows = db.execute("SELECT * FROM ic_templates ORDER BY name").fetchall()
    db.close()
    return [dict(r) for r in rows]

@app.post("/api/intercompany/templates")
async def create_ic_template(req: ICTemplateRequest):
    db = get_db()
    tid = str(uuid.uuid4())
    db.execute(
        """INSERT INTO ic_templates
           (id, name, source_company_id, dest_company_id, entry_type,
            source_debit_account, source_credit_account, dest_debit_account, dest_credit_account, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (tid, req.name, req.source_company_id, req.dest_company_id, req.entry_type,
         req.source_debit_account, req.source_credit_account,
         req.dest_debit_account, req.dest_credit_account, req.description),
    )
    db.commit()
    db.close()
    return {"id": tid}


@app.delete("/api/intercompany/templates/{template_id}")
async def delete_ic_template(template_id: str):
    db = get_db()
    db.execute("DELETE FROM ic_templates WHERE id = ?", (template_id,))
    db.commit()
    db.close()
    return {"ok": True}


# =====================================================================
#  DASHBOARD SUMMARY
# =====================================================================

@app.get("/api/dashboard/summary")
async def dashboard_summary():
    """KPI data for home page. Uses cached data from all companies."""
    db = get_db()
    company_count = db.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

    # Gather cached reports for KPIs
    year = datetime.now().year
    live_data = {"company_count": company_count}

    for key, rtype, period in [
        ("current_month_pl", "profit_loss", f"{year}-mtd"),
        ("last_month_pl", "profit_loss", f"{year}-last-month"),
        ("balance_sheet", "balance_sheet", f"{year}-ytd"),
        ("cash_flow", "cash_flow", f"{year}-mtd"),
    ]:
        rows = db.execute(
            "SELECT data_json FROM company_reports WHERE report_type=? AND period_key=?",
            (rtype, period)
        ).fetchall()
        if rows:
            reports = [json.loads(r["data_json"]) for r in rows]
            live_data[key] = _merge_reports(reports) if len(reports) > 1 else reports[0]
        else:
            # Try ytd fallback
            rows = db.execute(
                "SELECT data_json FROM company_reports WHERE report_type=? AND period_key=?",
                (rtype, f"{year}-ytd")
            ).fetchall()
            if rows:
                reports = [json.loads(r["data_json"]) for r in rows]
                live_data[key] = _merge_reports(reports) if len(reports) > 1 else reports[0]
            else:
                live_data[key] = None

    db.close()
    return live_data


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
