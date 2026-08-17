"""
Diagnostic: list every active purchase tax exactly as it's configured
in Odoo — name, id, amount, amount_type. Use this to find the exact
tax name string to put in the CSV's "Invoice lines/Taxes" column,
instead of guessing.

Reads the same .env as vendor_bill_importer.py (ODOO_URL, ODOO_DB,
ODOO_USERNAME, ODOO_PASSWORD). Read-only — makes no changes in Odoo.

Usage:
    python list_taxes.py            # list all active purchase taxes
    python list_taxes.py 30         # filter to names containing "30"
    python list_taxes.py WH         # filter to names containing "WH"
"""

import os
import sys
import xmlrpc.client
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

ODOO_URL = os.getenv("ODOO_URL", "https://production.g2gitsolutions.com").rstrip("/")
ODOO_DB = os.getenv("ODOO_DB", "production")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")


def main():
    filter_text = sys.argv[1] if len(sys.argv) > 1 else ""

    if not ODOO_PASSWORD:
        print("ODOO_PASSWORD is not set. Check your .env file.")
        sys.exit(1)

    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common", allow_none=True)
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object", allow_none=True)

    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})
    if not uid:
        print("Odoo authentication failed.")
        sys.exit(1)

    domain = [
        ["active", "=", True],
        ["type_tax_use", "=", "purchase"],
    ]

    if filter_text:
        domain.append(["name", "ilike", filter_text])

    taxes = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "account.tax", "search_read",
        [domain],
        {
            "fields": ["id", "name", "amount", "amount_type", "price_include"],
            "order": "name",
        },
    )

    if not taxes:
        print(f"No active purchase taxes found" + (f" matching {filter_text!r}." if filter_text else "."))
        return

    print(f"{'ID':<8}{'Name (exact, use this in the CSV)':<40}{'Amount':<12}{'Type':<15}{'Price incl.'}")
    print("-" * 100)
    for t in taxes:
        print(f"{t['id']:<8}{t['name']!r:<40}{t['amount']:<12}{t['amount_type']:<15}{t['price_include']}")


if __name__ == "__main__":
    main()
