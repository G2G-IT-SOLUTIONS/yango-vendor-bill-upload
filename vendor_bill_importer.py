"""
Yango Driver Bonus Vendor Bill Importer - Odoo 17

Workflow:
    Create -> Post -> Register full payment -> In Payment -> STOP

Hard rules:
    - No bank statement creation.
    - No bank reconciliation.
    - No reconciliation/matching operations.
    - Duplicate-safe and resumable.
    - One output CSV per run: same filename as the input CSV, containing
      every original column plus "status" (completed/dry_run/duplicate/
      failed) and "reason" (populated for failed and duplicate rows).
    - DRY_RUN=True and MAX_RECORDS=5 are recommended for first test.

Required .env:
    ODOO_URL=https://production.g2gitsolutions.com
    ODOO_DB=production
    ODOO_USERNAME=...
    ODOO_PASSWORD=...
    INPUT_FILE=...

Optional .env:
    PAYMENT_JOURNAL_ID=123
    PAYMENT_JOURNAL_NAME=Telebirr
    ACCOUNTING_DATE_MODE=month_end   # month_end | bill_date
    DRY_RUN=True
    MAX_RECORDS=5                    # 0 = no limit
    BATCH_SIZE=100
    LOG_DIR=logs
    RPC_MAX_RETRIES=3
    RPC_RETRY_DELAY_SECONDS=3

CSV columns expected:
    Partner
    Vendor ID
    Reference
    Invoice/Bill Date
    Payment Reference
    Due Date
    Invoice lines/Label
    Invoice lines/Account
    Invoice lines/Analytic Distribution
    Invoice lines/Unit Price
    Invoice lines/Taxes

Driver identification:
    - Vendor ID present: fetched by Vendor ID (the trusted primary key).
      The Partner name is cross-checked (case-insensitive, whitespace-
      normalized) purely for visibility — a mismatch is logged as a
      warning but does NOT fail the row. Vendor ID always wins.
    - Vendor ID empty: matched on Partner name alone, same normalized
      comparison.
    - Both empty: row fails.

Per-row financial fields (no fallback to any hardcoded config):
    - Invoice lines/Account   -> REQUIRED. Row fails if empty. Matched
                                 by exact code.
    - Invoice lines/Taxes     -> OPTIONAL. Empty means NO withholding tax
                                 is applied to that line (not an error).
                                 When present, matched case-insensitively
                                 and whitespace-normalized.
    - Invoice lines/Analytic Distribution -> REQUIRED. Row fails if empty.

Network resilience:
    - Every Odoo RPC call is retried automatically on transient network
      errors (DNS resolution failures, connection resets, timeouts) up
      to RPC_MAX_RETRIES times with a delay between attempts, so a
      momentary internet blip mid-run doesn't fail a row unnecessarily.
"""

import csv
import json
import logging
import os
import socket
import sys
import time
import traceback
import xmlrpc.client
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import calendar

from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

ODOO_URL = os.getenv("ODOO_URL", "https://production.g2gitsolutions.com").rstrip("/")
ODOO_DB = os.getenv("ODOO_DB", "production")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")

INPUT_FILE = Path(os.getenv("INPUT_FILE", "input.csv"))
if not INPUT_FILE.is_absolute():
    INPUT_FILE = BASE_DIR / INPUT_FILE

DRY_RUN = os.getenv("DRY_RUN", "True").strip().lower() in ("1", "true", "yes", "y")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

ACCOUNTING_DATE_MODE = os.getenv("ACCOUNTING_DATE_MODE", "month_end").strip().lower()

PAYMENT_JOURNAL_ID = os.getenv("PAYMENT_JOURNAL_ID", "").strip()
PAYMENT_JOURNAL_NAME = os.getenv("PAYMENT_JOURNAL_NAME", "").strip()

RPC_MAX_RETRIES = int(os.getenv("RPC_MAX_RETRIES", "3"))
RPC_RETRY_DELAY_SECONDS = float(os.getenv("RPC_RETRY_DELAY_SECONDS", "3"))

LOG_ROOT = Path(os.getenv("LOG_DIR", "logs"))
if not LOG_ROOT.is_absolute():
    LOG_ROOT = BASE_DIR / LOG_ROOT
LOG_ROOT.mkdir(parents=True, exist_ok=True)

# No timestamped subfolder. Both the text log and the output CSV are
# named after the input file, e.g. "july.csv" -> "july_logs.log" and
# "july_logs.csv". Re-running against the same input overwrites the
# previous run's log/output rather than accumulating one folder per run.
LOG_STEM = f"{INPUT_FILE.stem}_logs"

IMPORT_LOG = LOG_ROOT / f"{LOG_STEM}.log"

# Output CSV: every original column plus "status" (and "reason" for
# failed/duplicate rows).
OUTPUT_CSV = LOG_ROOT / f"{LOG_STEM}.csv"


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger("vendor_bill_importer")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

# mode="w": each run starts a fresh log file rather than appending
# to whatever was left from the previous run against this input file.
file_handler = logging.FileHandler(
    IMPORT_LOG,
    mode="w",
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def write_csv(path: Path, row: dict, fieldnames):
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore"
        )
        if not exists:
            writer.writeheader()
        writer.writerow(row)


# ============================================================
# XML-RPC
# ============================================================

common = None
models = None
uid = None

# Transient network errors worth retrying automatically. socket.gaierror
# (DNS resolution failures like "Temporary failure in name resolution"),
# TimeoutError, and ConnectionError/OSError cover the common cases of a
# momentary internet blip.
_TRANSIENT_NETWORK_ERRORS = (
    socket.gaierror,
    ConnectionError,
    TimeoutError,
    OSError,
    xmlrpc.client.ProtocolError,
)


def rpc(model, method, args=None, kwargs=None):
    last_exc = None

    for attempt in range(1, RPC_MAX_RETRIES + 1):
        try:
            return models.execute_kw(
                ODOO_DB,
                uid,
                ODOO_PASSWORD,
                model,
                method,
                args or [],
                kwargs or {},
            )
        except _TRANSIENT_NETWORK_ERRORS as exc:
            last_exc = exc
            logger.warning(
                "RPC transient network error (attempt %s/%s) on %s.%s: %s",
                attempt,
                RPC_MAX_RETRIES,
                model,
                method,
                exc,
            )
            if attempt < RPC_MAX_RETRIES:
                time.sleep(RPC_RETRY_DELAY_SECONDS)

    raise RuntimeError(
        f"RPC call {model}.{method} failed after {RPC_MAX_RETRIES} "
        f"attempts due to a network error: {last_exc}"
    )


def connect():
    global common, models, uid

    if not ODOO_PASSWORD:
        raise RuntimeError(
            "ODOO_PASSWORD is not set. Check your .env file."
        )

    common = xmlrpc.client.ServerProxy(
        f"{ODOO_URL}/xmlrpc/2/common",
        allow_none=True,
    )
    models = xmlrpc.client.ServerProxy(
        f"{ODOO_URL}/xmlrpc/2/object",
        allow_none=True,
    )

    uid = common.authenticate(
        ODOO_DB,
        ODOO_USERNAME,
        ODOO_PASSWORD,
        {},
    )

    if not uid:
        raise RuntimeError(
            "Odoo authentication failed."
        )

    version = common.version()
    logger.info(
        "Connected to Odoo %s | DB=%s | UID=%s | Version=%s",
        ODOO_URL,
        ODOO_DB,
        uid,
        version.get("server_version"),
    )


# ============================================================
# CONFIG DISCOVERY (per-row, cached)
# ============================================================

_account_cache = {}
_tax_cache = {}


def find_account_by_code(code):
    """
    Resolve an account.account by its code, as given per-row in the CSV.
    No hardcoded fallback. Cached so repeated codes across rows don't
    trigger repeated RPC calls.
    """
    code = clean(code)

    if code in _account_cache:
        return _account_cache[code]

    rows = rpc(
        "account.account",
        "search_read",
        [[
            ["code", "=", code],
            ["deprecated", "=", False],
        ]],
        {
            "fields": ["id", "code", "name", "account_type"],
            "limit": 10,
        },
    )

    if len(rows) != 1:
        raise ValueError(
            f"Expected exactly one active account with code "
            f"{code!r}, found {len(rows)}: {rows}"
        )

    _account_cache[code] = rows[0]
    return rows[0]


def find_tax_by_name(name):
    """
    Resolve an account.tax by its name, as given per-row in the CSV.
    Only called when the CSV's tax cell is non-empty; an empty cell
    means no withholding tax and this function is not invoked.

    Exact match only, deliberately — no normalization/guessing here.
    If a tax name from the CSV doesn't match, run list_taxes.py to see
    exactly how taxes are configured in Odoo (name, id, amount) and
    either fix the CSV value or confirm the correct exact string,
    rather than the script silently widening what counts as a match.
    """
    name = clean(name)

    if name in _tax_cache:
        return _tax_cache[name]

    rows = rpc(
        "account.tax",
        "search_read",
        [[
            ["name", "=", name],
            ["active", "=", True],
            ["type_tax_use", "=", "purchase"],
        ]],
        {
            "fields": [
                "id",
                "name",
                "amount",
                "amount_type",
                "type_tax_use",
                "price_include",
            ],
            "limit": 10,
        },
    )

    if len(rows) != 1:
        raise ValueError(
            f"Expected exactly one active purchase tax named "
            f"{name!r}, found {len(rows)}: {rows}. "
            "Run list_taxes.py to see the exact tax names configured "
            "in Odoo."
        )

    _tax_cache[name] = rows[0]
    return rows[0]


def list_payment_journals():
    return rpc(
        "account.journal",
        "search_read",
        [[
            ["type", "in", ["bank", "cash"]],
            ["active", "=", True],
        ]],
        {
            "fields": [
                "id",
                "name",
                "code",
                "type",
                "company_id",
                "currency_id",
            ],
            "order": "id",
        },
    )


def find_payment_journal():
    journals = list_payment_journals()

    if PAYMENT_JOURNAL_ID:
        matches = [
            j for j in journals
            if str(j["id"]) == PAYMENT_JOURNAL_ID
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"PAYMENT_JOURNAL_ID={PAYMENT_JOURNAL_ID} was not found "
                f"among active bank/cash journals. Available: {journals}"
            )
        return matches[0]

    if PAYMENT_JOURNAL_NAME:
        matches = [
            j for j in journals
            if j["name"].strip().lower() == PAYMENT_JOURNAL_NAME.lower()
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"PAYMENT_JOURNAL_NAME={PAYMENT_JOURNAL_NAME!r} matched "
                f"{len(matches)} journals. Available: {journals}"
            )
        return matches[0]

    if len(journals) == 1:
        logger.warning(
            "No payment journal configured. Exactly one active bank/cash "
            "journal exists, so it will be used: %s",
            journals[0],
        )
        return journals[0]

    raise RuntimeError(
        "Payment journal is not configured. Set PAYMENT_JOURNAL_ID or "
        "PAYMENT_JOURNAL_NAME in .env.\n"
        f"Available bank/cash journals:\n{json.dumps(journals, indent=2, default=str)}"
    )


# ============================================================
# DATA HELPERS
# ============================================================

def clean(value):
    if value is None:
        return ""
    return str(value).strip()


def _normalize_name(value):
    """
    Case-insensitive, whitespace-collapsed form of a name/label, used
    for tolerant matching (partner names, tax names) so that ALL CAPS,
    extra spaces, or minor formatting differences don't cause a real
    match to be missed.
    """
    return " ".join(clean(value).split()).casefold()


def parse_decimal(value, field_name):
    text = clean(value).replace(",", "")
    if not text:
        raise ValueError(f"{field_name} is empty.")

    try:
        return Decimal(text)
    except InvalidOperation:
        raise ValueError(
            f"Invalid decimal in {field_name}: {value!r}"
        )


def decimal_to_float(value):
    return float(
        value.quantize(
            Decimal("0.00000001"),
            rounding=ROUND_HALF_UP
        )
    )


def parse_date(value, field_name):
    """
    Dates in this CSV are MM/DD/YYYY only. No other format is accepted,
    so a wrongly-formatted date fails loudly instead of being silently
    misread (e.g. day/month swapped).
    """
    text = clean(value)
    if not text:
        raise ValueError(f"{field_name} is empty.")

    try:
        return datetime.strptime(text, "%m/%d/%Y").date()
    except ValueError:
        raise ValueError(
            f"{field_name} must be in MM/DD/YYYY format, got: {value!r}"
        )


def accounting_date_for(bill_date):
    if ACCOUNTING_DATE_MODE == "bill_date":
        return bill_date

    if ACCOUNTING_DATE_MODE == "month_end":
        last_day = calendar.monthrange(
            bill_date.year,
            bill_date.month
        )[1]
        return date(
            bill_date.year,
            bill_date.month,
            last_day
        )

    raise ValueError(
        "ACCOUNTING_DATE_MODE must be 'month_end' or 'bill_date'."
    )


def parse_analytic_distribution(raw):
    """
    Analytic Distribution is REQUIRED per row. No fallback of any kind.
    An empty cell is a row failure.
    """
    text = clean(raw)

    if not text:
        raise ValueError(
            "Invoice lines/Analytic Distribution is empty. "
            "No fallback is used; every row must specify its own "
            "analytic distribution."
        )

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError(
            f"Invalid analytic distribution JSON: {text!r}"
        )

    if not isinstance(parsed, dict):
        raise ValueError(
            "Analytic Distribution must be a JSON object."
        )

    if not parsed:
        raise ValueError(
            "Analytic Distribution JSON object is empty."
        )

    return {
        str(k): float(v)
        for k, v in parsed.items()
    }


# ============================================================
# PARTNER / DUPLICATE LOOKUP
# ============================================================

def find_partner(vendor_id, partner_name):
    """
    Identify the driver for this row.

    - Vendor ID present, exactly one match: fetched by Vendor ID (the
      trusted primary key). Partner name is cross-checked purely for
      visibility — a mismatch is logged as a warning, not raised.
      Vendor ID always wins.
    - Vendor ID present, MULTIPLE matches (duplicate data in Odoo):
      disambiguated using the CSV Partner name (normalized). If that
      narrows it to exactly one candidate, that one is used (with a
      warning logged, since this indicates Odoo has duplicate partner
      records for this Vendor ID and should be cleaned up). If it
      doesn't narrow to exactly one, the row fails with the full
      candidate list rather than guessing.
    - Vendor ID empty: matched on Partner name alone, using a
      case-insensitive, whitespace-normalized comparison so formatting
      differences (ALL CAPS, extra spaces) don't prevent a real match.
    - Both empty: error, nothing to identify the driver by.

    Note: the is_driver=True filter is intentionally NOT applied here.
    """
    vendor_id = clean(vendor_id)
    partner_name = clean(partner_name)

    if not vendor_id and not partner_name:
        raise ValueError(
            "Both Vendor ID and Partner are empty; cannot identify the driver."
        )

    if vendor_id:
        rows = rpc(
            "res.partner",
            "search_read",
            [[
                ["yango_contractor_id", "=", vendor_id],
                ["active", "=", True],
            ]],
            {
                "fields": [
                    "id",
                    "name",
                    "yango_contractor_id",
                    "is_driver",
                    "supplier_rank",
                ],
                "limit": 10,
            },
        )

        if len(rows) == 0:
            raise ValueError(
                f"Expected exactly one driver for Vendor ID={vendor_id!r}, "
                f"found 0."
            )

        if len(rows) > 1:
            # Duplicate data in Odoo: the same Vendor ID is assigned to
            # more than one partner record. Don't guess — try to
            # disambiguate using the CSV's Partner name (normalized).
            # Only proceed if that narrows it to exactly one candidate;
            # otherwise fail with the full candidate list for review.
            if partner_name:
                name_matches = [
                    r for r in rows
                    if _normalize_name(r.get("name", "")) == _normalize_name(partner_name)
                ]
            else:
                name_matches = []

            if len(name_matches) == 1:
                logger.warning(
                    "Vendor ID=%r is assigned to %s partner records in "
                    "Odoo (duplicate data). Disambiguated using Partner "
                    "name match -> partner id=%s (%r). This should be "
                    "cleaned up in Odoo.",
                    vendor_id,
                    len(rows),
                    name_matches[0]["id"],
                    name_matches[0]["name"],
                )
                partner = name_matches[0]
                return partner

            raise ValueError(
                f"Vendor ID={vendor_id!r} is assigned to {len(rows)} "
                f"partner records in Odoo, and the CSV Partner name "
                f"{partner_name!r} did not uniquely match one of them. "
                f"Candidates: {rows}. This Vendor ID has duplicate "
                "partner records in Odoo and needs manual cleanup."
            )

        partner = rows[0]

        if partner_name:
            if _normalize_name(partner_name) != _normalize_name(partner.get("name", "")):
                # Vendor ID is the trusted key. A name mismatch is logged
                # so it's visible for review, but does NOT fail the row —
                # the bill still goes to the partner identified by Vendor ID.
                logger.warning(
                    "Name mismatch for Vendor ID=%r: Odoo partner name is "
                    "%r, CSV Partner is %r. Proceeding using Vendor ID "
                    "match (Odoo name wins).",
                    vendor_id,
                    partner.get("name"),
                    partner_name,
                )

        return partner

    # No Vendor ID: match on Partner name alone.
    target = _normalize_name(partner_name)

    candidates = rpc(
        "res.partner",
        "search_read",
        [[
            ["name", "ilike", partner_name],
            ["active", "=", True],
        ]],
        {
            "fields": [
                "id",
                "name",
                "yango_contractor_id",
                "is_driver",
                "supplier_rank",
            ],
            "limit": 50,
        },
    )

    matches = [
        c for c in candidates
        if _normalize_name(c.get("name", "")) == target
    ]

    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one driver for Partner={partner_name!r}, "
            f"found {len(matches)}: {matches}"
        )

    return matches[0]


def find_existing_bill(partner_id, reference):
    reference = clean(reference)

    if not reference:
        return []

    return rpc(
        "account.move",
        "search_read",
        [[
            ["move_type", "=", "in_invoice"],
            ["partner_id", "=", partner_id],
            ["ref", "=", reference],
        ]],
        {
            "fields": [
                "id",
                "name",
                "state",
                "payment_state",
                "partner_id",
                "ref",
                "invoice_date",
                "amount_total",
                "amount_residual",
            ],
            "limit": 10,
            "order": "id desc",
        },
    )


def find_existing_bill_by_reference(reference):
    """
    Fallback duplicate check across all vendors.

    This prevents duplicate creation if the same reference already exists
    for another partner. We do NOT modify that existing bill.
    """
    reference = clean(reference)

    if not reference:
        return []

    return rpc(
        "account.move",
        "search_read",
        [[
            ["move_type", "=", "in_invoice"],
            ["ref", "=", reference],
        ]],
        {
            "fields": [
                "id",
                "name",
                "state",
                "payment_state",
                "partner_id",
                "ref",
                "amount_total",
                "amount_residual",
            ],
            "limit": 10,
            "order": "id desc",
        },
    )


# ============================================================
# BILL CREATION
# ============================================================

def create_bill(row, partner):
    reference = clean(row.get("Reference"))
    payment_reference = clean(row.get("Payment Reference"))
    label = clean(row.get("Invoice lines/Label")) or "Driver Bonus"

    bill_date = parse_date(
        row.get("Invoice/Bill Date"),
        "Invoice/Bill Date"
    )

    due_date_raw = clean(row.get("Due Date"))
    due_date = (
        parse_date(due_date_raw, "Due Date")
        if due_date_raw
        else bill_date
    )

    accounting_date = accounting_date_for(bill_date)

    unit_price = parse_decimal(
        row.get("Invoice lines/Unit Price"),
        "Invoice lines/Unit Price"
    )

    # --- Account: REQUIRED, from CSV, no fallback ---
    account_code = clean(row.get("Invoice lines/Account"))
    if not account_code:
        raise ValueError("Invoice lines/Account is empty.")
    account = find_account_by_code(account_code)

    # --- Tax: OPTIONAL, from CSV. Empty = no withholding, not an error ---
    tax_name = clean(row.get("Invoice lines/Taxes"))
    if tax_name:
        tax = find_tax_by_name(tax_name)
        tax_ids = [(6, 0, [tax["id"]])]
        tax_label = tax["name"]
    else:
        tax = None
        tax_ids = []
        tax_label = "NONE (no withholding)"

    # --- Analytic distribution: REQUIRED, from CSV, no fallback ---
    analytic_distribution = parse_analytic_distribution(
        row.get("Invoice lines/Analytic Distribution")
    )

    line = {
        "name": label,
        "account_id": account["id"],
        "quantity": 1.0,
        "price_unit": decimal_to_float(unit_price),
        "tax_ids": tax_ids,
        "analytic_distribution": analytic_distribution,
    }

    values = {
        "move_type": "in_invoice",
        "partner_id": partner["id"],
        "ref": reference,
        "invoice_date": bill_date.isoformat(),
        "date": accounting_date.isoformat(),
        "invoice_date_due": due_date.isoformat(),
        "payment_reference": payment_reference or reference,
        "invoice_line_ids": [
            (0, 0, line)
        ],
    }

    logger.info(
        "Creating bill | vendor=%s | partner=%s | ref=%s | "
        "bill_date=%s | accounting_date=%s | unit_price=%s | "
        "account=%s (%s) | tax=%s",
        partner["yango_contractor_id"],
        partner["name"],
        reference,
        bill_date,
        accounting_date,
        unit_price,
        account["code"],
        account["name"],
        tax_label,
    )

    bill_id = rpc(
        "account.move",
        "create",
        [values],
    )

    return bill_id


# ============================================================
# POST BILL
# ============================================================

def get_bill(bill_id):
    rows = rpc(
        "account.move",
        "read",
        [[bill_id]],
        {
            "fields": [
                "id",
                "name",
                "state",
                "payment_state",
                "partner_id",
                "ref",
                "invoice_date",
                "date",
                "invoice_date_due",
                "payment_reference",
                "amount_untaxed",
                "amount_tax",
                "amount_total",
                "amount_residual",
                "currency_id",
                "journal_id",
            ]
        },
    )

    if not rows:
        raise RuntimeError(
            f"Bill {bill_id} could not be read after creation."
        )

    return rows[0]


def post_bill(bill_id):
    bill = get_bill(bill_id)

    if bill["state"] == "posted":
        return bill

    if bill["state"] != "draft":
        raise RuntimeError(
            f"Bill {bill_id} is in unexpected state "
            f"{bill['state']!r}."
        )

    logger.info(
        "Posting bill | id=%s | ref=%s",
        bill_id,
        bill.get("ref"),
    )

    rpc(
        "account.move",
        "action_post",
        [[bill_id]],
    )

    bill = get_bill(bill_id)

    if bill["state"] != "posted":
        raise RuntimeError(
            f"Bill {bill_id} did not become posted. "
            f"Current state={bill['state']!r}"
        )

    return bill


# ============================================================
# REGISTER PAYMENT
# ============================================================

def register_full_payment(bill_id, payment_journal):
    bill = get_bill(bill_id)

    if bill["state"] != "posted":
        raise RuntimeError(
            f"Cannot register payment for bill {bill_id}: "
            f"state={bill['state']!r}"
        )

    payment_state = bill.get("payment_state")

    if payment_state == "in_payment":
        logger.info(
            "Bill already In Payment | id=%s | ref=%s",
            bill_id,
            bill.get("ref"),
        )
        return get_bill(bill_id)

    if payment_state == "paid":
        raise RuntimeError(
            f"Bill {bill_id} is already PAID. "
            "Importer will not perform any further payment/reconciliation."
        )

    amount_residual = bill.get("amount_residual")

    if amount_residual is None:
        raise RuntimeError(
            f"Bill {bill_id} has no amount_residual."
        )

    if float(amount_residual) <= 0:
        raise RuntimeError(
            f"Bill {bill_id} residual is {amount_residual}; "
            "cannot register a full payment."
        )

    payment_date = bill.get("invoice_date")

    if not payment_date:
        raise RuntimeError(
            f"Bill {bill_id} has no invoice_date; cannot determine "
            "payment date."
        )

    logger.info(
        "Registering full payment | bill=%s | ref=%s | "
        "residual=%s | journal=%s (%s) | payment_date=%s",
        bill_id,
        bill.get("ref"),
        amount_residual,
        payment_journal["name"],
        payment_journal["id"],
        payment_date,
    )

    context = {
        "active_model": "account.move",
        "active_ids": [bill_id],
        "active_id": bill_id,
    }

    wizard_values = {
        "journal_id": payment_journal["id"],
        "amount": amount_residual,
        "payment_date": payment_date,
    }

    wizard_id = rpc(
        "account.payment.register",
        "create",
        [wizard_values],
        {
            "context": context
        },
    )

    rpc(
        "account.payment.register",
        "action_create_payments",
        [[wizard_id]],
        {
            "context": context
        },
    )

    result = get_bill(bill_id)

    if result.get("payment_state") != "in_payment":
        raise RuntimeError(
            f"Payment was registered but bill did NOT reach "
            f"'in_payment'. "
            f"Bill={bill_id}, state={result.get('state')!r}, "
            f"payment_state={result.get('payment_state')!r}, "
            f"residual={result.get('amount_residual')!r}. "
            "No reconciliation will be attempted."
        )

    return result


# ============================================================
# PROCESS ONE ROW
# ============================================================

def process_row(row_number, row, payment_journal):
    vendor_id = clean(row.get("Vendor ID"))
    partner_name = clean(row.get("Partner"))
    reference = clean(row.get("Reference"))

    if not vendor_id and not partner_name:
        raise ValueError("Both Vendor ID and Partner are empty.")

    if not reference:
        raise ValueError("Reference is empty.")

    partner = find_partner(vendor_id, partner_name)

    # --------------------------------------------------------
    # DUPLICATE CHECK #1:
    # same partner + same reference
    # --------------------------------------------------------

    existing = find_existing_bill(
        partner["id"],
        reference,
    )

    if existing:
        bill = existing[0]

        logger.warning(
            "DUPLICATE | row=%s | ref=%s | existing_bill=%s | "
            "state=%s | payment_state=%s",
            row_number,
            reference,
            bill["id"],
            bill.get("state"),
            bill.get("payment_state"),
        )

        return {
            "status": "duplicate",
            "row": row_number,
            "reference": reference,
            "vendor_id": vendor_id,
            "partner": partner["name"],
            "existing_bill_id": bill["id"],
            "existing_bill_name": bill["name"],
            "existing_state": bill.get("state"),
            "existing_payment_state": bill.get("payment_state"),
        }

    # --------------------------------------------------------
    # DUPLICATE CHECK #2:
    # reference exists for another partner
    # --------------------------------------------------------

    same_reference = find_existing_bill_by_reference(reference)

    if same_reference:
        details = json.dumps(
            same_reference,
            default=str
        )

        raise ValueError(
            f"Reference {reference!r} already exists on another "
            f"vendor bill. Refusing to create a possible duplicate. "
            f"Existing: {details}"
        )

    # --------------------------------------------------------
    # DRY RUN
    # --------------------------------------------------------

    if DRY_RUN:
        bill_date = parse_date(
            row.get("Invoice/Bill Date"),
            "Invoice/Bill Date"
        )
        accounting_date = accounting_date_for(bill_date)

        unit_price = parse_decimal(
            row.get("Invoice lines/Unit Price"),
            "Invoice lines/Unit Price"
        )

        # Validate the per-row account/tax/analytic fields even in
        # dry run, so config problems surface before a real run.
        account_code = clean(row.get("Invoice lines/Account"))
        if not account_code:
            raise ValueError("Invoice lines/Account is empty.")
        account = find_account_by_code(account_code)

        tax_name = clean(row.get("Invoice lines/Taxes"))
        if tax_name:
            tax = find_tax_by_name(tax_name)
            tax_label = tax["name"]
        else:
            tax_label = "NONE (no withholding)"

        parse_analytic_distribution(
            row.get("Invoice lines/Analytic Distribution")
        )

        logger.info(
            "DRY RUN | row=%s | vendor=%s | partner=%s | "
            "ref=%s | amount=%s | bill_date=%s | accounting_date=%s | "
            "account=%s (%s) | tax=%s",
            row_number,
            vendor_id,
            partner["name"],
            reference,
            unit_price,
            bill_date,
            accounting_date,
            account["code"],
            account["name"],
            tax_label,
        )

        return {
            "status": "dry_run",
            "row": row_number,
            "reference": reference,
            "vendor_id": vendor_id,
            "partner": partner["name"],
            "amount": str(unit_price),
            "bill_date": bill_date.isoformat(),
            "accounting_date": accounting_date.isoformat(),
            "account": account["code"],
            "tax": tax_label,
        }

    # --------------------------------------------------------
    # CREATE
    # --------------------------------------------------------

    bill_id = create_bill(row, partner)

    logger.info(
        "CREATED | row=%s | bill_id=%s | ref=%s",
        row_number,
        bill_id,
        reference,
    )

    # --------------------------------------------------------
    # POST
    # --------------------------------------------------------

    bill = post_bill(bill_id)

    logger.info(
        "POSTED | bill_id=%s | name=%s | ref=%s | total=%s | "
        "residual=%s",
        bill_id,
        bill["name"],
        reference,
        bill["amount_total"],
        bill["amount_residual"],
    )

    # --------------------------------------------------------
    # REGISTER FULL PAYMENT
    # --------------------------------------------------------

    bill = register_full_payment(
        bill_id,
        payment_journal,
    )

    logger.info(
        "COMPLETED | bill_id=%s | name=%s | ref=%s | "
        "payment_state=%s | residual=%s",
        bill_id,
        bill["name"],
        reference,
        bill["payment_state"],
        bill["amount_residual"],
    )

    return {
        "status": "completed",
        "row": row_number,
        "reference": reference,
        "vendor_id": vendor_id,
        "partner": partner["name"],
        "bill_id": bill_id,
        "bill_name": bill["name"],
        "bill_state": bill["state"],
        "payment_state": bill["payment_state"],
        "amount_total": bill["amount_total"],
        "amount_residual": bill["amount_residual"],
    }


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("=" * 80)
    logger.info("YANGO VENDOR BILL IMPORT START")
    logger.info("Input: %s", INPUT_FILE)
    logger.info("Output CSV: %s", OUTPUT_CSV)
    logger.info("DRY_RUN: %s", DRY_RUN)
    logger.info("MAX_RECORDS: %s", MAX_RECORDS)
    logger.info("ACCOUNTING_DATE_MODE: %s", ACCOUNTING_DATE_MODE)
    logger.info(
        "Account / Tax / Analytic Distribution: all read per-row "
        "from the CSV. No hardcoded fallback for any of them. "
        "An empty Tax cell means no withholding."
    )
    logger.info(
        "RPC_MAX_RETRIES=%s | RPC_RETRY_DELAY_SECONDS=%s",
        RPC_MAX_RETRIES,
        RPC_RETRY_DELAY_SECONDS,
    )
    logger.info("=" * 80)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input CSV not found: {INPUT_FILE}"
        )

    connect()

    payment_journal = None

    if not DRY_RUN:
        payment_journal = find_payment_journal()

        logger.info(
            "Payment journal: %s | ID=%s | type=%s",
            payment_journal["name"],
            payment_journal["id"],
            payment_journal["type"],
        )

    # --------------------------------------------------------
    # Read CSV
    # --------------------------------------------------------

    with INPUT_FILE.open(
        "r",
        newline="",
        encoding="utf-8-sig"
    ) as f:

        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise RuntimeError(
                "CSV has no header."
            )

        required_columns = [
            "Partner",
            "Vendor ID",
            "Reference",
            "Invoice/Bill Date",
            "Payment Reference",
            "Due Date",
            "Invoice lines/Label",
            "Invoice lines/Account",
            "Invoice lines/Analytic Distribution",
            "Invoice lines/Unit Price",
            "Invoice lines/Taxes",
        ]

        missing = [
            c for c in required_columns
            if c not in reader.fieldnames
        ]

        if missing:
            raise RuntimeError(
                "Missing required CSV columns: "
                + ", ".join(missing)
            )

        # Single output CSV: every original column, plus "status" and
        # "reason" (reason is only populated for failed/duplicate rows).
        output_fieldnames = list(reader.fieldnames) + ["status", "reason"]

        # Start each run with a clean output CSV rather than appending
        # to whatever is left from a previous run against this input file.
        if OUTPUT_CSV.exists():
            try:
                OUTPUT_CSV.unlink()
            except PermissionError:
                raise RuntimeError(
                    f"Cannot overwrite {OUTPUT_CSV} because it is open "
                    "in another program (e.g. Excel). Close the file "
                    "and run the script again."
                )

        stats = {
            "read": 0,
            "completed": 0,
            "dry_run": 0,
            "duplicates": 0,
            "failed": 0,
        }

        processed = 0

        for row_number, row in enumerate(reader, start=2):

            if MAX_RECORDS > 0 and processed >= MAX_RECORDS:
                break

            processed += 1
            stats["read"] += 1

            reference = clean(row.get("Reference"))
            vendor_id = clean(row.get("Vendor ID"))

            output_row = dict(row)
            output_row["status"] = ""
            output_row["reason"] = ""

            try:

                result = process_row(
                    row_number,
                    row,
                    payment_journal,
                )

                status = result["status"]
                output_row["status"] = status

                if status == "completed":
                    stats["completed"] += 1

                elif status == "duplicate":
                    stats["duplicates"] += 1
                    output_row["reason"] = (
                        f"duplicate of bill {result.get('existing_bill_name')} "
                        f"(id={result.get('existing_bill_id')}, "
                        f"state={result.get('existing_state')}, "
                        f"payment_state={result.get('existing_payment_state')})"
                    )

                elif status == "dry_run":
                    stats["dry_run"] += 1

            except Exception as exc:

                stats["failed"] += 1

                error_message = str(exc)

                logger.error(
                    "FAILED | row=%s | vendor=%s | ref=%s | error=%s",
                    row_number,
                    vendor_id,
                    reference,
                    error_message,
                )

                logger.error(
                    traceback.format_exc()
                )

                output_row["status"] = "failed"
                output_row["reason"] = error_message

            write_csv(
                OUTPUT_CSV,
                output_row,
                output_fieldnames,
            )

            # Progress
            if processed % BATCH_SIZE == 0:
                logger.info(
                    "PROGRESS | processed=%s | completed=%s | "
                    "duplicates=%s | failed=%s | dry_run=%s",
                    processed,
                    stats["completed"],
                    stats["duplicates"],
                    stats["failed"],
                    stats["dry_run"],
                )

    logger.info("=" * 80)
    logger.info("IMPORT FINISHED")
    logger.info("Read: %s", stats["read"])
    logger.info("Completed: %s", stats["completed"])
    logger.info("Duplicates: %s", stats["duplicates"])
    logger.info("Failed: %s", stats["failed"])
    logger.info("Dry run: %s", stats["dry_run"])
    logger.info("Output CSV: %s", OUTPUT_CSV)
    logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("FATAL ERROR: %s", exc)
        logger.error(traceback.format_exc())
        print(f"\nFATAL ERROR: {exc}")
        print(f"Log: {IMPORT_LOG}")
        sys.exit(1)
