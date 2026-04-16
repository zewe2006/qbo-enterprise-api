# Consolidated Report API Documentation

External API reference for third-party integrations.

---

## Base URL

```
https://overflowing-ambition-production-4b7e.up.railway.app
```

---

## Authentication

All endpoints (except `/api/auth/login`, `/api/auth/register`, `/api/billing/plans`, `/api/health`, and Stripe/QBO webhooks) require a Bearer token in the `Authorization` header:

```
Authorization: Bearer <your_token>
```

### Get a Token

**POST** `/api/auth/login`

```json
{
  "email": "you@example.com",
  "password": "your_password"
}
```

**Response:**
```json
{
  "token": "uuid-token-string",
  "user": {
    "id": "...",
    "email": "...",
    "name": "...",
    "role": "admin",
    "org_id": "..."
  },
  "org": {
    "plan": "business",
    "max_companies": 50,
    "trial_active": false
  }
}
```

Tokens do not expire automatically but can be invalidated via `POST /api/auth/logout`.

---

## Companies

### List Companies
**GET** `/api/companies`

Returns all companies the authenticated user has access to.

**Response:**
```json
[
  {
    "id": "company-uuid",
    "name": "Sweet Hut Doraville",
    "legal_name": "Sweet Hut LLC",
    "status": "connected",
    "last_synced": "2026-04-15T10:00:00",
    "qbo_realm_id": "9341452..."
  }
]
```

### Get Company Chart of Accounts
**GET** `/api/companies/{company_id}/accounts`

### Create Account
**POST** `/api/companies/{company_id}/accounts` — **admin only**

Creates a new account in the company's QBO chart of accounts. The account is also immediately added to the local cache, so you can reference it by name in JE endpoints right away (no sync needed).

**Request:**
```json
{
  "name": "Marketing — Social Media",
  "account_type": "Expense",
  "account_sub_type": "AdvertisingPromotional",
  "description": "Paid social campaigns",
  "parent_name": "Marketing",
  "acct_num": "6120"
}
```

| Field | Required | Notes |
|---|---|---|
| `name` | ✅ | Account name (must be unique in the company) |
| `account_type` | ✅ | One of: `Bank`, `Other Current Asset`, `Fixed Asset`, `Other Asset`, `Accounts Receivable`, `Equity`, `Expense`, `Other Expense`, `Cost of Goods Sold`, `Accounts Payable`, `Credit Card`, `Long Term Liability`, `Other Current Liability`, `Income`, `Other Income` |
| `account_sub_type` | optional | QBO-specific sub-type (e.g. `Checking`, `Rent`, `AdvertisingPromotional`) |
| `description` | optional | Account description |
| `parent_name` | optional | Create as a sub-account of this existing account (by name) |
| `parent_qbo_id` | optional | Or provide the QBO account ID directly |
| `acct_num` | optional | Account number |
| `currency` | optional | Currency code (defaults to company's home currency) |

**Response (200):**
```json
{
  "status": "created",
  "company_id": "...",
  "qbo_account_id": "85",
  "name": "Marketing — Social Media",
  "fully_qualified_name": "Marketing:Marketing — Social Media",
  "account_type": "Expense",
  "account_sub_type": "AdvertisingPromotional",
  "classification": "Expense",
  "active": true
}
```

**Error responses:**
- `400` — Invalid `account_type` or parent account not found
- `409` — Account with that name already exists in the company
- `502` — QBO rejected the request

### Update Account
**PATCH** `/api/companies/{company_id}/accounts/{qbo_account_id}` — **admin only**

Sparse update — only the fields you provide are changed. Cannot change `account_type` (QBO limitation — you'd have to create a new account and move transactions).

**Request:**
```json
{
  "name": "Marketing — Paid Social",
  "description": "Updated description",
  "account_sub_type": "AdvertisingPromotional",
  "acct_num": "6121"
}
```

All fields are optional, but at least one must be provided.

**Response:**
```json
{
  "status": "updated",
  "qbo_account_id": "85",
  "name": "Marketing — Paid Social",
  "fully_qualified_name": "Marketing:Marketing — Paid Social",
  "account_type": "Expense",
  "account_sub_type": "AdvertisingPromotional",
  "active": true
}
```

### Deactivate Account
**DELETE** `/api/companies/{company_id}/accounts/{qbo_account_id}` — **admin only**

Sets `Active=false` in QBO. Historical transactions remain intact, but the account disappears from active lists and dropdowns.

**⚠️ QBO does not allow true deletion of accounts** — this is a soft delete. To reactivate, use PATCH (currently requires re-activating via QBO UI; future enhancement).

**Response:**
```json
{
  "status": "deactivated",
  "qbo_account_id": "85",
  "name": "Marketing — Paid Social",
  "active": false
}
```

If already inactive:
```json
{
  "status": "already_inactive",
  "qbo_account_id": "85",
  "name": "Marketing — Paid Social"
}
```



### Sync Company from QuickBooks
**POST** `/api/companies/{company_id}/sync`

Pulls latest reports and accounts from QBO.

### Get Customers / Vendors
- **GET** `/api/companies/{company_id}/customers`
- **GET** `/api/companies/{company_id}/vendors`

---

## Financial Reports

All report endpoints accept the same `ReportParams` body.

### Report Parameters

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-03-31",
  "date_macro": "This Month",
  "accounting_method": "Accrual",
  "compare_prior_year": false,
  "compare_prior_month": false,
  "company_id": "company-uuid",
  "company_ids": ["uuid1", "uuid2"],
  "by_company": false
}
```

**Notes:**
- `company_id: "all"` → consolidated across all user's companies
- `company_ids` → consolidated across the provided list
- `by_company: true` → includes per-company breakdown in response
- `accounting_method` → `"Accrual"` or `"Cash"`
- `date_macro` → QBO date macros like `"This Month"`, `"Last Month"`, `"This Year-to-date"` (overrides start/end dates)

### Profit & Loss
**POST** `/api/reports/profit-loss`

### Balance Sheet
**POST** `/api/reports/balance-sheet`

### Cash Flow
**POST** `/api/reports/cash-flow`

### Response Format

Returns QuickBooks native report structure:

```json
{
  "current": {
    "Header": {...},
    "Rows": {
      "Row": [
        {
          "type": "Section",
          "Header": {"ColData": [{"value": "Income"}]},
          "Rows": {"Row": [...]},
          "Summary": {"ColData": [{"value": "Total Income"}, {"value": "12345.67"}]}
        }
      ]
    }
  },
  "consolidated": true,
  "companies": [{"name": "...", "company_id": "..."}],
  "company_breakdowns": {
    "Sweet Hut Doraville": {"Food": 12345.67, "Total Income": 50000}
  },
  "prior_year": {...}
}
```

### Transaction Detail (Drill-down)
**POST** `/api/reports/transaction-detail`

```json
{
  "account_name": "Food",
  "start_date": "2026-01-01",
  "end_date": "2026-03-31",
  "accounting_method": "Accrual",
  "company_ids": ["uuid1", "uuid2"]
}
```

**Response:**
```json
{
  "account_name": "Food",
  "count": 42,
  "transactions": [
    {
      "date": "2026-01-15",
      "txn_type": "Sales Receipt",
      "doc_num": "1001",
      "name": "Customer Name",
      "memo": "...",
      "debit": 0,
      "credit": 125.00,
      "balance": 125.00,
      "company": "Sweet Hut Doraville"
    }
  ]
}
```

---

## Dashboard

### Summary KPIs
**GET** `/api/dashboard/summary`

Query parameters:
- `period` — `last_month` (default), `ytd_last_month`, `custom`
- `start_date`, `end_date` — required when `period=custom`
- `company_ids` — comma-separated list

### Revenue Trend
**GET** `/api/dashboard/revenue-trend?months=12&company_ids=uuid1,uuid2`

---

## Account Mappings

For consolidation category mapping (e.g. map "Food" → "Revenue").

### List Mappings
**GET** `/api/account-mappings?company_id={id}`

### Create Mapping
**POST** `/api/account-mappings`

```json
{
  "company_id": "...",
  "qbo_account_id": "...",
  "qbo_account_name": "Food",
  "consolidated_category": "Revenue",
  "consolidated_subcategory": "Food Revenue"
}
```

### Delete Mapping
**DELETE** `/api/account-mappings/{mapping_id}`

---

## Journal Entries (single company)

Create or retrieve a journal entry directly in a company's QuickBooks book.

**⚠️ Requires admin role.** The authenticated user must have admin access in the org.

### Create Journal Entry
**POST** `/api/companies/{company_id}/journal-entries`

**Request:**
```json
{
  "date": "2026-04-15",
  "doc_number": "JE-2026-0042",
  "private_note": "Month-end adjustment",
  "lines": [
    {
      "posting_type": "Debit",
      "account_name": "Rent Expense",
      "amount": 5000.00,
      "description": "April rent"
    },
    {
      "posting_type": "Credit",
      "account_name": "Checking",
      "amount": 5000.00,
      "description": "April rent"
    }
  ]
}
```

**Line fields:**
| Field | Type | Required | Notes |
|---|---|---|---|
| `posting_type` | string | ✅ | `"Debit"` or `"Credit"` |
| `account_name` | string | ✅ | Must match name or fully-qualified name in QBO chart of accounts |
| `amount` | number | ✅ | Always positive. `posting_type` determines debit/credit. |
| `entity_id` | string | ⚠️ | Required for Accounts Receivable (Customer) or Accounts Payable (Vendor) lines |
| `entity_type` | string | optional | `"Customer"` or `"Vendor"` — auto-detected if omitted |
| `class_id` | string | optional | QBO class reference |
| `description` | string | optional | Line memo |

**Top-level fields:**
| Field | Type | Required | Notes |
|---|---|---|---|
| `date` | string | ✅ | YYYY-MM-DD |
| `lines` | array | ✅ | Min 2 lines; debits must equal credits |
| `doc_number` | string | optional | Custom journal number |
| `private_note` | string | optional | Journal-level memo |
| `currency` | string | optional | Three-letter currency code (defaults to company's home currency) |

**Validation:**
- Lines must **balance** — total debits must equal total credits (within $0.01)
- All `account_name` values must exist in the cached chart of accounts — run a company sync first if a newly created account isn't found
- Accounts of type `Accounts Receivable` / `Accounts Payable` require an `entity_id`

**Response (201):**
```json
{
  "status": "posted",
  "company_id": "company-uuid",
  "company_name": "Sweet Hut Doraville",
  "journal_entry_id": "1234",
  "doc_number": "JE-2026-0042",
  "date": "2026-04-15",
  "total": 5000.00,
  "line_count": 2
}
```

**Error responses:**
- `400` — lines don't balance, invalid posting type, or accounts not found
- `401` — invalid/missing token
- `403` — not an admin user
- `404` — company not found in your org
- `502` — QBO API rejected the entry (invalid account combination, closed period, etc.)

### Bulk Create Journal Entries
**POST** `/api/companies/{company_id}/journal-entries/bulk`

Create multiple journal entries in a single request. Ideal for monthly adjusting entries, payroll imports, or bulk reclassifications.

**Request:**
```json
{
  "stop_on_error": false,
  "entries": [
    {
      "date": "2026-04-15",
      "doc_number": "ADJ-001",
      "private_note": "Rent accrual",
      "lines": [
        {"posting_type": "Debit",  "account_name": "Rent Expense", "amount": 5000},
        {"posting_type": "Credit", "account_name": "Accrued Liabilities", "amount": 5000}
      ]
    },
    {
      "date": "2026-04-15",
      "doc_number": "ADJ-002",
      "private_note": "Depreciation",
      "lines": [
        {"posting_type": "Debit",  "account_name": "Depreciation Expense", "amount": 1200},
        {"posting_type": "Credit", "account_name": "Accumulated Depreciation", "amount": 1200}
      ]
    }
  ]
}
```

**Top-level fields:**
| Field | Type | Required | Notes |
|---|---|---|---|
| `entries` | array | ✅ | Array of journal entries (same schema as single-entry endpoint). Max 100 per request. |
| `stop_on_error` | boolean | optional | If `true`, abort batch on first failure. Default `false` (best-effort — attempts every entry). |

Each entry follows the same schema as the single `POST /journal-entries` endpoint — `date`, `doc_number`, `private_note`, `currency`, `lines`.

**Response:**
```json
{
  "status": "partial",
  "company_id": "...",
  "company_name": "Sweet Hut Doraville",
  "total": 3,
  "posted": 2,
  "failed": 1,
  "aborted": 0,
  "results": [
    {
      "index": 0,
      "status": "posted",
      "journal_entry_id": "1234",
      "doc_number": "ADJ-001",
      "date": "2026-04-15",
      "total": 5000.00,
      "line_count": 2
    },
    {
      "index": 1,
      "status": "validation_error",
      "error": "Account(s) not found in Sweet Hut Doraville: Bogus Account. Sync the company to refresh the chart of accounts.",
      "doc_number": "ADJ-002",
      "date": "2026-04-15"
    },
    {
      "index": 2,
      "status": "posted",
      "journal_entry_id": "1235",
      "doc_number": "ADJ-003",
      "date": "2026-04-15",
      "total": 1200.00,
      "line_count": 2
    }
  ]
}
```

**Top-level `status`:**
- `"success"` — all entries posted
- `"partial"` — some posted, some failed
- `"failed"` — zero entries posted

**Per-entry `status`:**
| Status | Meaning |
|---|---|
| `posted` | Successfully created in QBO |
| `validation_error` | Rejected before QBO call (unbalanced, missing account, bad posting_type) |
| `qbo_error` | QBO API rejected the entry (closed period, invalid combination, etc.) |
| `aborted` | Skipped because a prior entry failed AND `stop_on_error: true` |

**Important notes:**
- **Not atomic.** Each entry is posted independently. If entry #5 fails after #1–4 succeeded, entries #1–4 remain posted in QBO. The response tells you exactly which ones made it.
- **Order preserved.** Results always include an `index` matching the request position, so you can correlate failures with your input.
- **Best-effort default.** Unless you set `stop_on_error: true`, the endpoint keeps going past failures and returns detailed per-entry results — usually more useful than aborting.
- **Batch limit.** 100 entries per request. For larger imports, split into multiple calls.

### Get Journal Entry
**GET** `/api/companies/{company_id}/journal-entries/{je_id}`

Returns the raw QBO `JournalEntry` object. Useful to verify an entry was posted correctly.

---

## Example: Complete JE flow (curl)

```bash
# 1. Look up the company IDs
curl https://overflowing-ambition-production-4b7e.up.railway.app/api/companies \
  -H "Authorization: Bearer $TOKEN"

# 2. Look up accounts for that company
curl "https://overflowing-ambition-production-4b7e.up.railway.app/api/companies/$COMPANY_ID/accounts" \
  -H "Authorization: Bearer $TOKEN"

# 3. Post a single JE
curl -X POST https://overflowing-ambition-production-4b7e.up.railway.app/api/companies/$COMPANY_ID/journal-entries \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "date": "2026-04-15",
    "doc_number": "ADJ-001",
    "private_note": "Correcting entry",
    "lines": [
      {"posting_type": "Debit",  "account_name": "Rent Expense", "amount": 5000},
      {"posting_type": "Credit", "account_name": "Checking",     "amount": 5000}
    ]
  }'

# 4. Or post multiple entries in one call
curl -X POST https://overflowing-ambition-production-4b7e.up.railway.app/api/companies/$COMPANY_ID/journal-entries/bulk \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {
        "date": "2026-04-15", "doc_number": "ADJ-001",
        "lines": [
          {"posting_type": "Debit",  "account_name": "Rent Expense", "amount": 5000},
          {"posting_type": "Credit", "account_name": "Checking",     "amount": 5000}
        ]
      },
      {
        "date": "2026-04-15", "doc_number": "ADJ-002",
        "lines": [
          {"posting_type": "Debit",  "account_name": "Utilities", "amount": 450},
          {"posting_type": "Credit", "account_name": "Checking",  "amount": 450}
        ]
      }
    ]
  }'
```

---

## Intercompany Journal Entries

### List Entries
**GET** `/api/intercompany`

### Create Entry
**POST** `/api/intercompany`

```json
{
  "source_company_id": "uuid-a",
  "dest_company_id": "uuid-b",
  "entry_type": "transfer",
  "description": "Intercompany loan",
  "date": "2026-04-15",
  "lines": [
    {
      "side": "source",
      "posting_type": "Credit",
      "account_name": "Cash",
      "amount": 10000.00,
      "description": "Loan disbursement"
    },
    {
      "side": "source",
      "posting_type": "Debit",
      "account_name": "Due from Company B",
      "amount": 10000.00
    },
    {
      "side": "dest",
      "posting_type": "Debit",
      "account_name": "Cash",
      "amount": 10000.00
    },
    {
      "side": "dest",
      "posting_type": "Credit",
      "account_name": "Due to Company A",
      "amount": 10000.00
    }
  ]
}
```

### Post to QuickBooks
**POST** `/api/intercompany/{entry_id}/post`

Pushes journal entries to BOTH companies in QBO.

### Update / Delete
- **PUT** `/api/intercompany/{entry_id}`
- **DELETE** `/api/intercompany/{entry_id}`

---

## Delivery Import (Uber Eats / DoorDash)

### Parse PDF Statement
**POST** `/api/delivery-import/parse`

Content-Type: `multipart/form-data`, field name: `file` (PDF)

**Response:**
```json
{
  "platform": "ubereats",
  "store_name": "Sweet Hut Doraville",
  "statement_period": "Apr 1 - Apr 7, 2026",
  "payouts": [
    {
      "date": "2026-04-07",
      "amount": 2500.00,
      "fees": 450.00,
      "marketing": 50.00,
      "chargeback": 0,
      "adjustments": 0,
      "bank_deposit": 2000.00
    }
  ]
}
```

### Get / Save Account Mapping
- **GET** `/api/delivery-import/mapping?company_id={id}&platform=ubereats`
- **POST** `/api/delivery-import/mapping`

```json
{
  "company_id": "...",
  "platform": "ubereats",
  "mapping": {
    "bank": "Checking",
    "income": "Uber Eats Sales",
    "fees": "Platform Fees",
    "marketing": "Marketing",
    "chargeback": "Chargebacks",
    "adjustments": "Other Income"
  }
}
```

### Generate Journal CSV
**POST** `/api/delivery-import/csv`

```json
{
  "parsed": { /* output from /parse */ },
  "mapping": { /* account mapping */ },
  "prefix": "UBER",
  "company_id": "..."
}
```

### Export Directly to QuickBooks
**POST** `/api/delivery-import/export-qbo`

Same body as `/csv`. Creates journal entries in QBO.

**Response:**
```json
{
  "status": "exported",
  "posted_count": 5,
  "total_count": 5,
  "errors": []
}
```

### Import History
- **GET** `/api/delivery-import/history?company_id={id}`
- **GET** `/api/delivery-import/history/{history_id}/csv`

---

## Rate Limiting

No hard rate limits are enforced, but please be considerate:
- Avoid polling more than once per minute
- Batch multi-company queries using `company_ids` instead of making one call per company
- Report endpoints cache data when possible — back-to-back identical requests are cheap

---

## Error Responses

All errors return JSON with a `detail` field:

```json
{
  "detail": "Error message here"
}
```

Common status codes:
- `400` — bad request / validation error
- `401` — missing or invalid token
- `403` — insufficient permissions (e.g. non-admin calling admin endpoint)
- `404` — resource not found
- `500` — server error

---

## CORS

Allowed origin: `https://consolidatedreport.app`

If your colleague's app runs on a different domain, contact us to add it to the allowlist, or make API calls from their backend instead of browser.

---

## Support

Email: support@sweethut.com
