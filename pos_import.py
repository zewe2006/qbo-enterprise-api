"""
Ordyx / Tonic POS daily-sales importer (ported from the Financials lib/pos/*).

Pulls a closed business-day batch and posts it as one balanced summary journal
entry into a manual company's Supabase GL:

  Dr  settlement by channel (cards / cash / A/R per delivery platform / gift card)
        Cr  revenue by sales-group parent (Food / Drink / Retail / Other)
        Cr  Sales Tax Payable
        Cr  Tips Payable

The batch reports $0 line items, so prices come from the Orders API `selections`
and the sales group comes from the batch `items` (index-aligned within an order).
Revenue is booked NET of comps by allocating each order's netAmount across its
groups (largest-remainder), so Σ revenue = payments − tax and the entry balances.

Supabase access is injected (the `sb` argument) so this module has no hard
dependency on server.py and can be tested standalone. `sb` must provide async
callables: select(table, params), insert(table, row|rows), delete(table, params),
rpc(fn, args).
"""

import asyncio
import httpx

DEFAULT_BATCH_BASE = "https://integration.ordyx.com"
DEFAULT_ORDERS_BASE = "https://api.tonicpos.com"
MEMO_PREFIX = "pos:ordyx:batch:"
OTHER_REVENUE = "Other"

# ---------------------------------------------------------------------------
# Ordyx / Tonic API client
# ---------------------------------------------------------------------------


class OrdyxError(Exception):
    def __init__(self, message, status, path):
        super().__init__(message)
        self.status = status
        self.path = path


async def _get_batch(client, batch_base, api_key, path):
    # integration.ordyx.com can be slow to first-byte; retry once on timeout.
    last_exc = None
    for _ in range(2):
        try:
            r = await client.get(f"{batch_base}{path}", headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"}, timeout=120)
            break
        except httpx.TimeoutException as e:
            last_exc = e
    else:
        raise OrdyxError(f"Ordyx batch fetch timed out: {last_exc}", 504, path)
    if r.status_code == 404:
        return None  # not closed yet / nothing pending
    if r.status_code >= 300:
        raise OrdyxError(f"Ordyx batch {r.status_code}: {r.text[:300]}", r.status_code, path)
    return r.json()


async def fetch_next_batch(client, batch_base, api_key):
    return await _get_batch(client, batch_base, api_key, "/batch")


async def fetch_batch(client, batch_base, api_key, batch_number):
    return await _get_batch(client, batch_base, api_key, f"/batch?batch={batch_number}")


async def acknowledge_batch(client, batch_base, api_key, batch_id):
    r = await client.post(f"{batch_base}/batch/{batch_id}", headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"})
    if r.status_code >= 300:
        raise OrdyxError(f"Ordyx acknowledge {r.status_code}: {r.text[:300]}", r.status_code, f"/batch/{batch_id}")


async def fetch_order_detail(client, orders_base, api_key, store_id, order_id):
    r = await client.get(
        f"{orders_base}/activity/order/{order_id}",
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json", "StoreId": str(store_id)},
    )
    if r.status_code >= 300:
        raise OrdyxError(f"Ordyx order {r.status_code}: {r.text[:300]}", r.status_code, f"/activity/order/{order_id}")
    o = r.json()
    total_amount = int(o.get("totalAmount") or 0)
    total_tax = int(o.get("totalTax") or 0)
    sels = o.get("selections") or []
    return {
        "id": int(o.get("id")),
        "totalAmount": total_amount,
        "totalTax": total_tax,
        "tipAmount": int(o.get("tipAmount") or 0),
        "netAmount": int(o["netAmount"]) if o.get("netAmount") is not None else total_amount - total_tax,
        "selections": [{"name": s.get("name"), "totalAmount": int(s.get("totalAmount") or 0)} for s in sels],
    }


async def fetch_order_details(client, orders_base, api_key, store_id, order_ids, concurrency=8):
    """Enrich every order with bounded concurrency + one retry. Returns
    (details_by_id, missing_ids)."""
    details = {}
    missing = []
    sem = asyncio.Semaphore(concurrency)

    async def one(oid):
        async with sem:
            for _ in range(2):
                try:
                    d = await fetch_order_detail(client, orders_base, api_key, store_id, oid)
                    details[oid] = d
                    return
                except Exception:
                    continue
            missing.append(oid)

    await asyncio.gather(*[one(o) for o in order_ids])
    return details, missing


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------

DEFAULT_PAYMENT_TYPE_TO_BUCKET = {
    "Credit": "cards",
    "Online Credit": "cards",
    "Cash": "cash",
    "UberEats": "ar_ubereats",
    "DoorDash": "ar_doordash",
    "GrubHub": "ar_grubhub",
    "Bill": "ar_house",
}

SETTLEMENT_BUCKETS = {
    "cards": {"key": "cards", "name": "Undeposited Funds – Cards", "type": "asset", "side": "debit", "code": "1205",
              "match": ["Undeposited Funds", "Undeposited Funds – Cards", "Credit Card Clearing", "Merchant Clearing"]},
    "cash": {"key": "cash", "name": "Cash on Hand", "type": "asset", "side": "debit", "code": "1010",
             "match": ["Cash on Hand", "Cash Drawer", "Petty Cash"]},
    "ar_ubereats": {"key": "ar_ubereats", "name": "A/R – UberEats", "type": "asset", "side": "debit", "code": "1210",
                    "match": ["A/R – UberEats", "UberEats Receivable", "Uber Eats", "UberEats"]},
    "ar_doordash": {"key": "ar_doordash", "name": "A/R – DoorDash", "type": "asset", "side": "debit", "code": "1211",
                    "match": ["A/R – DoorDash", "DoorDash Receivable", "DoorDash"]},
    "ar_grubhub": {"key": "ar_grubhub", "name": "A/R – GrubHub", "type": "asset", "side": "debit", "code": "1212",
                   "match": ["A/R – GrubHub", "GrubHub Receivable", "GrubHub", "Grubhub"]},
    "ar_house": {"key": "ar_house", "name": "A/R – House Accounts", "type": "asset", "side": "debit", "code": "1213",
                 "match": ["A/R – House Accounts", "House Accounts", "Accounts Receivable", "House Account"]},
    "gift_card": {"key": "gift_card", "name": "Gift Card Liability", "type": "liability", "side": "debit", "code": "2300",
                  "match": ["Gift Card Liability", "Gift Cards Outstanding", "Gift Cards", "Gift Card", "Deferred Revenue – Gift Cards"]},
    "other_settlement": {"key": "other_settlement", "name": "Undeposited Funds – Other", "type": "asset", "side": "debit", "code": "1209",
                         "match": ["Undeposited Funds – Other", "Other Clearing"]},
    "sales_tax_payable": {"key": "sales_tax_payable", "name": "Sales Tax Payable", "type": "liability", "side": "credit", "code": "2200",
                          "match": ["Sales Tax Payable", "Sales Tax", "Tax Payable"]},
    "tips_payable": {"key": "tips_payable", "name": "Tips Payable", "type": "liability", "side": "credit", "code": "2205",
                     "match": ["Tips Payable", "Gratuities Payable", "Tips", "Gratuity Payable"]},
}


def _slug(s):
    out = "".join(c if c.isalnum() else "_" for c in (s or "").lower()).strip("_")
    return out or "other"


def revenue_bucket(parent):
    return {
        "key": f"rev_{_slug(parent)}",
        "name": f"Sales – {parent}",
        "type": "income",
        "side": "credit",
        "code": "4010",
        "match": [f"Sales – {parent}", f"Sales - {parent}", f"{parent} Sales", f"Sales {parent}"],
    }


# ---------------------------------------------------------------------------
# Aggregation (pure)
# ---------------------------------------------------------------------------


def _dollars_to_cents(s):
    if not s:
        return 0
    s = str(s).strip()
    neg = s.startswith("-")
    s = s.lstrip("-")
    whole, _, frac = s.partition(".")
    cents = int(whole or 0) * 100 + int((frac + "00")[:2] or 0)
    return -cents if neg else cents


def cents_to_dollars(cents):
    neg = cents < 0
    a = abs(cents)
    return f"{'-' if neg else ''}{a // 100}.{a % 100:02d}"


def _parent_of(item):
    if not item:
        return OTHER_REVENUE
    return item.get("salesGroupParent") or item.get("salesGroup") or OTHER_REVENUE


def _allocate_net(net_cents, gross_by_group):
    """Allocate net_cents across groups in proportion to gross, largest-remainder
    so the parts sum to net_cents exactly."""
    out = {}
    total_gross = sum(gross_by_group.values())
    if total_gross <= 0:
        out[OTHER_REVENUE] = net_cents
        return out
    raw = []
    for g, gross in gross_by_group.items():
        exact = (net_cents * gross) / total_gross
        floor = int(exact // 1)
        raw.append([g, floor, exact - floor])
    assigned = sum(r[1] for r in raw)
    remainder = net_cents - assigned
    raw.sort(key=lambda r: r[2], reverse=True)
    for r in raw:
        v = r[1]
        if remainder > 0:
            v += 1
            remainder -= 1
        out[r[0]] = out.get(r[0], 0) + v
    return out


def aggregate_batch(batch, order_details, payment_type_map=None, missing_order_ids=None):
    type_map = dict(DEFAULT_PAYMENT_TYPE_TO_BUCKET)
    if payment_type_map:
        type_map.update(payment_type_map)
    warnings = []
    orders = batch.get("orders") or []

    # learn name -> parent from index-aligned orders
    name_to_parent = {}
    for o in orders:
        d = order_details.get(o["id"])
        if not d:
            continue
        items = o.get("items") or []
        if len(items) == len(d["selections"]):
            for i, sel in enumerate(d["selections"]):
                nm = sel.get("name")
                if nm and nm not in name_to_parent:
                    name_to_parent[nm] = _parent_of(items[i])

    # revenue by parent (net of comps), tax, tips
    revenue_by_parent = {}
    gross_sales = sales_tax = tips = unattributed = 0
    for o in orders:
        d = order_details.get(o["id"])
        if not d:
            continue
        gross_sales += d["totalAmount"]
        sales_tax += d["totalTax"]
        tips += d["tipAmount"]
        items = o.get("items") or []
        aligned = len(items) == len(d["selections"]) and len(items) > 0
        gross_by_group = {}
        for i, sel in enumerate(d["selections"]):
            parent = _parent_of(items[i]) if aligned else name_to_parent.get(sel.get("name") or "", OTHER_REVENUE)
            gross_by_group[parent] = gross_by_group.get(parent, 0) + sel["totalAmount"]
        for parent, c in _allocate_net(d["netAmount"], gross_by_group).items():
            revenue_by_parent[parent] = revenue_by_parent.get(parent, 0) + c
            if parent == OTHER_REVENUE:
                unattributed += c

    # settlement by channel
    debits = {}
    payments_cents = payment_tips_cents = 0
    unknown_types = set()
    for o in orders:
        for p in (o.get("payments") or []):
            amt = _dollars_to_cents(p.get("amount"))
            tip = _dollars_to_cents(p.get("tip"))
            payments_cents += amt
            payment_tips_cents += tip
            bucket = type_map.get(p.get("type"))
            if not bucket:
                bucket = "other_settlement"
                unknown_types.add(p.get("type"))
            debits[bucket] = debits.get(bucket, 0) + amt + tip

    if unknown_types:
        warnings.append("Unmapped payment type(s) routed to Undeposited Funds – Other: " + ", ".join(str(t) for t in unknown_types))
    if unattributed > 0:
        warnings.append(f"{cents_to_dollars(unattributed)} of net revenue could not be attributed to a sales group (booked to Sales – Other).")
    if missing_order_ids:
        warnings.append(f"{len(missing_order_ids)} order(s) could not be enriched with totals — day is under-counted; refusing to post until resolved.")

    # assemble lines (drop zero buckets)
    lines = []
    for key, c in debits.items():
        if c == 0:
            continue
        lines.append({"bucket": SETTLEMENT_BUCKETS[key], "debit": c / 100.0, "credit": 0})
    for parent, c in sorted(revenue_by_parent.items(), key=lambda kv: -kv[1]):
        if c == 0:
            continue
        lines.append({"bucket": revenue_bucket(parent), "debit": 0, "credit": c / 100.0})
    if sales_tax != 0:
        lines.append({"bucket": SETTLEMENT_BUCKETS["sales_tax_payable"], "debit": 0, "credit": sales_tax / 100.0})
    if tips != 0:
        lines.append({"bucket": SETTLEMENT_BUCKETS["tips_payable"], "debit": 0, "credit": tips / 100.0})

    total_debits = sum(debits.values())
    total_revenue = sum(revenue_by_parent.values())
    balance_delta = total_debits - (total_revenue + sales_tax + tips)

    return {
        "batchId": batch.get("id"),
        "storeId": batch.get("storeId"),
        "businessDate": (batch.get("opened") or "")[:10],
        "lines": lines,
        "totals": {
            "grossSalesCents": gross_sales,
            "netSalesCents": gross_sales - sales_tax,
            "salesTaxCents": sales_tax,
            "tipsCents": tips,
            "paymentsCents": payments_cents,
            "paymentTipsCents": payment_tips_cents,
        },
        "reconciliation": {
            "grossVsPaymentsDeltaCents": gross_sales - payments_cents,
            "tipsDeltaCents": tips - payment_tips_cents,
            "balanceDeltaCents": balance_delta,
            "balanced": balance_delta == 0,
        },
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# COA resolution (pure)
# ---------------------------------------------------------------------------


def _norm(s):
    out = []
    for ch in (s or "").lower():
        out.append(ch if ch.isalnum() else " ")
    return " ".join("".join(out).split())


def _free_code(preferred, used):
    try:
        n = int(preferred)
    except (TypeError, ValueError):
        n = 9000
    for i in range(10000):
        code = str(n + i)
        if code not in used:
            return code
    return f"{preferred}-{len(used)}"


def resolve_buckets(buckets, existing, overrides=None):
    overrides = overrides or {}
    by_norm = {}
    by_code = {}
    for a in existing:
        by_code[a["code"]] = a
        k = _norm(a["name"])
        by_norm.setdefault(k, a)
    used = set(a["code"] for a in existing)
    out = []
    seen = set()
    for b in buckets:
        if b["key"] in seen:
            continue
        seen.add(b["key"])
        ov = overrides.get(b["key"])
        if ov:
            acct = next((a for a in existing if a["id"] == ov), None)
            out.append({"bucketKey": b["key"], "accountId": ov,
                        "matchedCode": acct["code"] if acct else None,
                        "matchedName": acct["name"] if acct else None, "create": None})
            continue
        match = None
        for cand in b["match"]:
            match = by_norm.get(_norm(cand)) or by_code.get(cand)
            if match:
                break
        if match:
            out.append({"bucketKey": b["key"], "accountId": match["id"],
                        "matchedCode": match["code"], "matchedName": match["name"], "create": None})
            continue
        code = _free_code(b["code"], used)
        used.add(code)
        out.append({"bucketKey": b["key"], "accountId": None, "matchedCode": None, "matchedName": None,
                    "create": {"code": code, "name": b["name"], "type": b["type"]}})
    return out


# ---------------------------------------------------------------------------
# Orchestration — `sb` provides async select/insert/delete/rpc
# ---------------------------------------------------------------------------


async def _load_connection(sb, company_id):
    rows = await sb.select("pos_connections", {
        "company_id": f"eq.{company_id}", "provider": "eq.ordyx",
        "select": "id,store_id,payment_type_map,bucket_account_map,last_acked_batch,status,batch_base_url,orders_base_url",
        "limit": "1",
    })
    return rows[0] if rows else None


async def _load_coa(sb, company_id):
    return await sb.select("chart_of_accounts", {
        "company_id": f"eq.{company_id}", "is_active": "eq.true",
        "select": "id,code,name,type", "limit": "5000",
    })


async def _ensure_accounts(sb, company_id, resolved):
    ids = {}
    for r in resolved:
        if r["accountId"]:
            ids[r["bucketKey"]] = r["accountId"]
    to_create = [r for r in resolved if not r["accountId"] and r["create"]]
    if not to_create:
        return ids
    codes = [r["create"]["code"] for r in to_create]
    clash = await sb.select("chart_of_accounts", {
        "company_id": f"eq.{company_id}", "code": f"in.({','.join(codes)})", "select": "id,code",
    })
    by_code = {a["code"]: a["id"] for a in clash}
    fresh = [r for r in to_create if r["create"]["code"] not in by_code]
    if fresh:
        inserted = await sb.insert("chart_of_accounts", [
            {"company_id": company_id, "code": r["create"]["code"], "name": r["create"]["name"],
             "type": r["create"]["type"], "is_active": True}
            for r in fresh
        ])
        if isinstance(inserted, dict):
            inserted = [inserted]
        for a in inserted:
            by_code[a["code"]] = a["id"]
    for r in to_create:
        ids[r["bucketKey"]] = by_code[r["create"]["code"]]
    return ids


def _view_lines(lines, resolved):
    rmap = {r["bucketKey"]: r for r in resolved}
    view = []
    for l in lines:
        r = rmap[l["bucket"]["key"]]
        acct = f'{r["matchedCode"]} {r["matchedName"]}' if r["accountId"] else f'{r["create"]["code"]} {r["create"]["name"]} (new)'
        view.append({"bucketKey": l["bucket"]["key"], "bucketLabel": l["bucket"]["name"],
                     "debit": l["debit"], "credit": l["credit"], "account": acct, "isNew": not r["accountId"]})
    return view


async def _process_batch(sb, client, conn, batch, *, dry_run, acknowledge, created_by, company_id, system_user_id):
    api_key = conn["_api_key"]
    batch_base = conn.get("batch_base_url") or DEFAULT_BATCH_BASE
    orders_base = conn.get("orders_base_url") or DEFAULT_ORDERS_BASE
    order_ids = [o["id"] for o in (batch.get("orders") or [])]
    details, missing = await fetch_order_details(client, orders_base, api_key, conn["store_id"], order_ids)
    summary = aggregate_batch(batch, details, conn.get("payment_type_map"), missing)
    lines = summary["lines"]
    coa = await _load_coa(sb, company_id)
    resolved = resolve_buckets([l["bucket"] for l in lines], coa, conn.get("bucket_account_map"))

    base = {
        "batchId": batch.get("id"),
        "businessDate": summary["businessDate"],
        "balanced": summary["reconciliation"]["balanced"],
        "reconciliation": summary["reconciliation"],
        "totals": summary["totals"],
        "lines": _view_lines(lines, resolved),
        "createdAccounts": [],
        "posted": False,
        "acknowledged": False,
        "warnings": summary["warnings"],
    }

    if not summary["reconciliation"]["balanced"] or missing:
        base["error"] = (f"{len(missing)} order(s) missing totals; not posting" if missing
                         else f"unbalanced by {cents_to_dollars(summary['reconciliation']['balanceDeltaCents'])}; not posting")
        return base

    if dry_run:
        return base

    account_ids = await _ensure_accounts(sb, company_id, resolved)
    base["createdAccounts"] = [r["create"]["code"] for r in resolved if not r["accountId"]]

    memo = f"{MEMO_PREFIX}{batch.get('id')}"
    # idempotent: clear any prior posting of this batch (lines cascade)
    await sb.delete("journal_entries", {"company_id": f"eq.{company_id}", "memo": f"eq.{memo}", "source": "eq.auto"})
    entry = await sb.insert("journal_entries", {
        "company_id": company_id, "date": summary["businessDate"], "memo": memo,
        "source": "auto", "created_by": (created_by or system_user_id or None),
    })
    je_lines = [{
        "journal_entry_id": entry["id"],
        "coa_account_id": account_ids[l["bucket"]["key"]],
        "debit": l["debit"], "credit": l["credit"],
        "description": l["bucket"]["name"],
    } for l in lines]
    await sb.insert("journal_lines", je_lines)
    base["posted"] = True

    if acknowledge:
        await acknowledge_batch(client, batch_base, api_key, batch.get("id"))
        await sb.update("pos_connections", {"id": f"eq.{conn['id']}"},
                        {"last_acked_batch": batch.get("id")})
        base["acknowledged"] = True
    return base


async def run_pos_sync(sb, params, created_by, system_user_id=None):
    """params: {companyId, mode, batchNumber?, maxBatches?, fromDate?, dryRun?, acknowledge?}.
    companyId is the SUPABASE company UUID."""
    company_id = params["companyId"]
    mode = params.get("mode", "next")
    dry_run = params.get("dryRun", True) is not False
    acknowledge = params.get("acknowledge") is True and not dry_run

    conn = await _load_connection(sb, company_id)
    if not conn:
        raise ValueError(f"no Ordyx connection for company {company_id}")
    api_key = await sb.rpc("pos_api_key", {"p_connection_id": conn["id"]})
    if not api_key or not isinstance(api_key, str):
        raise ValueError("could not decrypt POS api key")
    conn["_api_key"] = api_key
    batch_base = conn.get("batch_base_url") or DEFAULT_BATCH_BASE

    result = {"companyId": company_id, "storeId": conn["store_id"], "dryRun": dry_run, "processed": []}
    limit = max(1, min(params.get("maxBatches") or 20, 60)) if mode == "backlog" else 1

    async with httpx.AsyncClient(timeout=60) as client:
        for _ in range(limit):
            if mode == "specific":
                if not params.get("batchNumber"):
                    raise ValueError("batchNumber required for mode=specific")
                batch = await fetch_batch(client, batch_base, api_key, params["batchNumber"])
            else:
                batch = await fetch_next_batch(client, batch_base, api_key)
            if not batch:
                break

            if mode == "from-date" and params.get("fromDate"):
                day = (batch.get("opened") or "")[:10]
                if day < params["fromDate"]:
                    if not dry_run:
                        await acknowledge_batch(client, batch_base, api_key, batch.get("id"))
                        await sb.update("pos_connections", {"id": f"eq.{conn['id']}"}, {"last_acked_batch": batch.get("id")})
                    result["processed"].append({
                        "batchId": batch.get("id"), "businessDate": day, "balanced": True,
                        "reconciliation": {"balanceDeltaCents": 0, "balanced": True},
                        "totals": {"netSalesCents": 0, "salesTaxCents": 0, "tipsCents": 0},
                        "lines": [], "createdAccounts": [], "posted": False,
                        "acknowledged": not dry_run, "warnings": [f"before fromDate {params['fromDate']} — {'would skip' if dry_run else 'skipped'}"],
                    })
                    continue

            br = await _process_batch(sb, client, conn, batch, dry_run=dry_run, acknowledge=acknowledge,
                                      created_by=created_by, company_id=company_id, system_user_id=system_user_id)
            result["processed"].append(br)

            if mode == "specific":
                break
            if mode in ("backlog", "from-date") and not br.get("acknowledged"):
                break

    return result
