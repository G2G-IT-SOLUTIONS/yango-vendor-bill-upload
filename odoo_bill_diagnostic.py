import os
import json
import xmlrpc.client
from datetime import datetime

from dotenv import load_dotenv

# Load .env from the same directory as this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(BASE_DIR, ".env")

load_dotenv(ENV_FILE)

# ============================================================
# CONFIGURATION
# ============================================================

ODOO_URL = os.getenv(
    "ODOO_URL",
    "https://production.g2gitsolutions.com"
)

ODOO_DB = os.getenv(
    "ODOO_DB",
    "production"
)

ODOO_USERNAME = os.getenv(
    "ODOO_USERNAME",
    "sajjat.sheikh@g2gitsolutions.com"
)

ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

SAMPLE_VENDOR_ID = os.getenv(
    "SAMPLE_VENDOR_ID",
    "eaaa7a82010f4d1fa1d61c60ecb05a19"
)

SAMPLE_BILL_ID = int(
    os.getenv("SAMPLE_BILL_ID", "0")
)

OUTPUT_FILE = "odoo_bill_diagnostic_report.json"

print("Environment loaded:", ENV_FILE)
print("Password loaded:", bool(ODOO_PASSWORD))

if not ODOO_PASSWORD:
    raise Exception(
        "ODOO_PASSWORD environment variable is not set."
    )


# ============================================================
# HELPERS
# ============================================================

def separator(title):
    print("\n")
    print("=" * 70)
    print(title)
    print("=" * 70)


def rpc(model, method, args=None, kwargs=None):
    """
    Helper for Odoo XML-RPC calls.
    """
    return models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        model,
        method,
        args or [],
        kwargs or {}
    )


# ============================================================
# PASSWORD CHECK
# ============================================================

if not ODOO_PASSWORD:
    raise Exception(
        "\nODOO_PASSWORD environment variable is not set.\n"
        "Set it before running this script.\n"
    )


# ============================================================
# CONNECT TO ODOO
# ============================================================

separator("1. ODOO CONNECTION")

print("URL:", ODOO_URL)
print("Database:", ODOO_DB)
print("Username:", ODOO_USERNAME)

common = xmlrpc.client.ServerProxy(
    f"{ODOO_URL}/xmlrpc/2/common"
)

models = xmlrpc.client.ServerProxy(
    f"{ODOO_URL}/xmlrpc/2/object"
)

uid = common.authenticate(
    ODOO_DB,
    ODOO_USERNAME,
    ODOO_PASSWORD,
    {}
)

if not uid:
    raise Exception("Authentication failed.")

print("Connected successfully.")
print("UID:", uid)


# ============================================================
# GET ODOO VERSION
# ============================================================

separator("2. ODOO VERSION")

version_info = common.version()

print(json.dumps(version_info, indent=4, default=str))


# ============================================================
# REPORT STRUCTURE
# ============================================================

report = {
    "generated_at": datetime.now().isoformat(),
    "odoo": {},
    "partner_lookup": {},
    "account_2203": {},
    "tax_3_wh": {},
    "analytic_accounts": {},
    "vendor_bill": {},
}


report["odoo"] = {
    "url": ODOO_URL,
    "database": ODOO_DB,
    "uid": uid,
    "version": version_info,
}


# ============================================================
# 3. CHECK PARTNER USING CONTRACTOR ID
# ============================================================

separator("3. PARTNER / CONTRACTOR ID")

print("Searching Vendor ID:")
print(SAMPLE_VENDOR_ID)

partners = rpc(
    "res.partner",
    "search_read",
    [[
        ["yango_contractor_id", "=", SAMPLE_VENDOR_ID]
    ]],
    {
        "fields": [
            "id",
            "name",
            "yango_contractor_id",
            "is_driver",
            "supplier_rank",
            "customer_rank",
            "active",
        ],
        "limit": 10,
    }
)

print(json.dumps(partners, indent=4, default=str))

report["partner_lookup"] = {
    "search_field": "yango_contractor_id",
    "searched_value": SAMPLE_VENDOR_ID,
    "results": partners,
}


# ============================================================
# 4. CHECK ACCOUNT 2203
# ============================================================

separator("4. ACCOUNT 2203")

accounts = rpc(
    "account.account",
    "search_read",
    [[
        ["code", "=", "2203"]
    ]],
    {
        "fields": [
            "id",
            "code",
            "name",
            "account_type",
            "reconcile",
            "deprecated",
        ],
        "limit": 10,
    }
)

print(json.dumps(accounts, indent=4, default=str))

report["account_2203"] = {
    "searched_code": "2203",
    "results": accounts,
}


# ============================================================
# 5. CHECK TAX 3% WH
# ============================================================

separator("5. TAX 3% WH")

taxes = rpc(
    "account.tax",
    "search_read",
    [[
        ["name", "=", "3% WH"]
    ]],
    {
        "fields": [
            "id",
            "name",
            "amount",
            "amount_type",
            "type_tax_use",
            "price_include",
            "active",
            "description",
            "tax_group_id",
            "sequence",
        ],
        "limit": 20,
    }
)

print(json.dumps(taxes, indent=4, default=str))

report["tax_3_wh"] = {
    "searched_name": "3% WH",
    "results": taxes,
}


# ============================================================
# 6. GET ANALYTIC ACCOUNTS
# ============================================================

separator("6. ANALYTIC ACCOUNTS")

try:

    analytic_accounts = rpc(
        "account.analytic.account",
        "search_read",
        [[]],
        {
            "fields": [
                "id",
                "name",
                "code",
                "active",
                "plan_id",
            ],
            "limit": 200,
        }
    )

    print(json.dumps(
        analytic_accounts,
        indent=4,
        default=str
    ))

    report["analytic_accounts"] = {
        "results": analytic_accounts
    }

except Exception as e:

    print("Could not read analytic accounts.")
    print(str(e))

    report["analytic_accounts"] = {
        "error": str(e)
    }


# ============================================================
# 7. FIND EXISTING VENDOR BILL
# ============================================================

separator("7. EXISTING VENDOR BILL")

if SAMPLE_BILL_ID:

    print("Using specified bill ID:", SAMPLE_BILL_ID)

    bill_ids = [SAMPLE_BILL_ID]

else:

    print("No bill ID specified.")
    print("Finding a recent vendor bill...")

    bill_ids = rpc(
        "account.move",
        "search",
        [[
            ["move_type", "=", "in_invoice"]
        ]],
        {
            "limit": 1,
            "order": "id desc",
        }
    )

print("Bill IDs:", bill_ids)

if not bill_ids:

    print("No vendor bills found.")

    report["vendor_bill"] = {
        "error": "No vendor bills found."
    }

else:

    bill_id = bill_ids[0]

    print("Inspecting bill:", bill_id)

    # --------------------------------------------------------
    # BILL HEADER
    # --------------------------------------------------------

    bill = rpc(
        "account.move",
        "read",
        [[bill_id]],
        {
            "fields": [
                "id",
                "name",
                "move_type",
                "state",
                "partner_id",
                "ref",
                "invoice_date",
                "date",
                "invoice_date_due",
                "payment_reference",
                "currency_id",
                "journal_id",
                "company_id",
                "amount_untaxed",
                "amount_tax",
                "amount_total",
                "invoice_line_ids",
            ]
        }
    )

    print("\nBILL HEADER:")
    print(json.dumps(
        bill,
        indent=4,
        default=str
    ))

    # --------------------------------------------------------
    # INVOICE LINES
    # --------------------------------------------------------

    line_ids = bill[0].get("invoice_line_ids", [])

    print("\nInvoice line IDs:")
    print(line_ids)

    lines = []

    if line_ids:

        lines = rpc(
            "account.move.line",
            "read",
            [line_ids],
            {
                "fields": [
                    "id",
                    "move_id",
                    "name",
                    "account_id",
                    "quantity",
                    "price_unit",
                    "price_subtotal",
                    "price_total",
                    "tax_ids",
                    "analytic_distribution",
                    "product_id",
                    "partner_id",
                    "currency_id",
                ]
            }
        )

    print("\nINVOICE LINES:")
    print(json.dumps(
        lines,
        indent=4,
        default=str
    ))

    # --------------------------------------------------------
    # TAX DETAILS
    # --------------------------------------------------------

    tax_ids = []

    for line in lines:
        tax_ids.extend(
            line.get("tax_ids", [])
        )

    tax_ids = list(set(tax_ids))

    bill_tax_details = []

    if tax_ids:

        bill_tax_details = rpc(
            "account.tax",
            "read",
            [tax_ids],
            {
                "fields": [
                    "id",
                    "name",
                    "amount",
                    "amount_type",
                    "type_tax_use",
                    "price_include",
                    "description",
                    "tax_group_id",
                    "sequence",
                ]
            }
        )

    print("\nTAXES USED BY BILL:")
    print(json.dumps(
        bill_tax_details,
        indent=4,
        default=str
    ))

    # --------------------------------------------------------
    # PARTNER DETAILS
    # --------------------------------------------------------

    partner_id = bill[0].get("partner_id")

    partner_details = []

    if partner_id:

        partner_db_id = partner_id[0]

        partner_details = rpc(
            "res.partner",
            "read",
            [[partner_db_id]],
            {
                "fields": [
                    "id",
                    "name",
                    "yango_contractor_id",
                    "is_driver",
                    "supplier_rank",
                ]
            }
        )

    print("\nBILL PARTNER:")
    print(json.dumps(
        partner_details,
        indent=4,
        default=str
    ))

    # --------------------------------------------------------
    # COMPLETE BILL REPORT
    # --------------------------------------------------------

    report["vendor_bill"] = {
        "bill_header": bill,
        "invoice_lines": lines,
        "tax_details": bill_tax_details,
        "partner_details": partner_details,
    }


# ============================================================
# 8. FIELD INFORMATION
# ============================================================

separator("8. FIELD INFORMATION")

fields_to_check = [
    ("res.partner", "yango_contractor_id"),
    ("res.partner", "is_driver"),
    ("account.move", "payment_reference"),
    ("account.move", "invoice_date_due"),
    ("account.move.line", "analytic_distribution"),
]

field_report = {}

for model_name, field_name in fields_to_check:

    try:

        result = rpc(
            model_name,
            "fields_get",
            [[field_name]],
            {
                "attributes": [
                    "string",
                    "type",
                    "required",
                    "readonly",
                    "relation",
                ]
            }
        )

        field_report[
            f"{model_name}.{field_name}"
        ] = result

        print(
            f"\n{model_name}.{field_name}"
        )

        print(json.dumps(
            result,
            indent=4,
            default=str
        ))

    except Exception as e:

        field_report[
            f"{model_name}.{field_name}"
        ] = {
            "error": str(e)
        }

        print(
            f"\nCould not inspect "
            f"{model_name}.{field_name}:"
        )

        print(str(e))


report["field_information"] = field_report


# ============================================================
# 9. CHECK REQUIRED MODELS
# ============================================================

separator("9. REQUIRED MODELS")

models_to_check = [
    "res.partner",
    "account.move",
    "account.move.line",
    "account.account",
    "account.tax",
    "account.analytic.account",
]

model_report = {}

for model_name in models_to_check:

    try:

        result = rpc(
            model_name,
            "check_access_rights",
            ["read"],
            {
                "raise_exception": False
            }
        )

        model_report[model_name] = {
            "read_access": result
        }

        print(
            f"{model_name}: "
            f"read access = {result}"
        )

    except Exception as e:

        model_report[model_name] = {
            "error": str(e)
        }

        print(
            f"{model_name}: ERROR"
        )
        print(str(e))


report["models"] = model_report


# ============================================================
# SAVE REPORT
# ============================================================

separator("10. SAVING REPORT")

with open(
    OUTPUT_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        report,
        f,
        indent=4,
        ensure_ascii=False,
        default=str
    )

print(
    f"Report saved to: {OUTPUT_FILE}"
)

print("\nDONE.")
print(
    "Send me the generated "
    f"{OUTPUT_FILE} file."
)