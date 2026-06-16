"""
generate_invoices.py
--------------------
Generates synthetic invoice data mimicking the anonymised export
for a generic debt collection client.

Scenarios injected:
  - Duplicate invoice_id (same invoice imported twice — the core bug)
  - Duplicate invoice_id with different amounts (split responsibility)
  - Mismatched case_id (invoice linked to wrong debtor)
  - Correct records (majority)

Run: python scripts/generate_invoices.py
Output: data/raw/invoices_client_xyz.csv
"""

import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw" / "invoices_client_xyz.csv"
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

CLIENT_ID = 1
CLIENT_NAME = "CLIENTXYZ"

STATUSES = ["IMPORTED", "PROCESSED", "REJECTED", "PENDING"]


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def random_amount() -> float:
    return round(random.uniform(150.0, 5000.0), 2)


def generate_case_id() -> str:
    return f"CASE-{random.randint(10000, 99999)}"


def generate_invoice_id() -> str:
    return f"INV-{uuid.uuid4().hex[:10].upper()}"


records = []
start_date = date(2023, 1, 1)
end_date = date(2024, 6, 30)

# --- Normal records (700)
normal_invoice_ids = [generate_invoice_id() for _ in range(700)]
for inv_id in normal_invoice_ids:
    records.append({
        "invoice_id": inv_id,
        "case_id": generate_case_id(),
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "amount": random_amount(),
        "import_date": random_date(start_date, end_date),
        "status": random.choice(STATUSES),
        "source_system": "ANT",
        "batch_id": f"BATCH-{random.randint(1, 50):04d}",
    })

# --- Scenario 1: Exact duplicate (same invoice_id, same amount, same case)
# 40 duplicates — imported twice by ANT without deduplication check
duplicate_pool = random.sample(normal_invoice_ids, 40)
for inv_id in duplicate_pool:
    original = next(r for r in records if r["invoice_id"] == inv_id)
    dup = original.copy()
    dup["import_date"] = random_date(start_date, end_date)
    dup["batch_id"] = f"BATCH-{random.randint(51, 99):04d}"
    records.append(dup)

# --- Scenario 2: Duplicate invoice_id with different amount (shared responsibility)
# ANT imported with one amount, Dental Par sent correction with different amount
for _ in range(20):
    inv_id = generate_invoice_id()
    case_id = generate_case_id()
    base_amount = random_amount()
    records.append({
        "invoice_id": inv_id,
        "case_id": case_id,
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "amount": base_amount,
        "import_date": random_date(start_date, end_date),
        "status": "IMPORTED",
        "source_system": "ANT",
        "batch_id": f"BATCH-{random.randint(1, 50):04d}",
    })
    records.append({
        "invoice_id": inv_id,
        "case_id": case_id,
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "amount": round(base_amount * random.uniform(0.8, 1.2), 2),
        "import_date": random_date(start_date, end_date),
        "status": "IMPORTED",
        "source_system": "DENTAL_PAR",
        "batch_id": f"BATCH-{random.randint(51, 99):04d}",
    })

# --- Scenario 3: Mismatched case_id (invoice linked to wrong debtor)
for _ in range(15):
    inv_id = generate_invoice_id()
    records.append({
        "invoice_id": inv_id,
        "case_id": generate_case_id(),
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "amount": random_amount(),
        "import_date": random_date(start_date, end_date),
        "status": "PROCESSED",
        "source_system": "ANT",
        "batch_id": f"BATCH-MISMATCH-{random.randint(1, 10):04d}",
    })
    # Same invoice re-imported with different case_id
    records.append({
        "invoice_id": inv_id,
        "case_id": generate_case_id(),
        "client_id": CLIENT_ID,
        "client_name": CLIENT_NAME,
        "amount": random_amount(),
        "import_date": random_date(start_date, end_date),
        "status": "IMPORTED",
        "source_system": "ANT",
        "batch_id": f"BATCH-MISMATCH-{random.randint(1, 10):04d}",
    })

random.shuffle(records)

fieldnames = [
    "invoice_id", "case_id", "client_id", "client_name",
    "amount", "import_date", "status", "source_system", "batch_id"
]

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)

print(f"Generated {len(records)} records → {OUTPUT_PATH}")

# Summary
exact_dups = len(duplicate_pool)
amount_dups = 20
mismatch = 15
print(f"  Normal records  : 700")
print(f"  Exact duplicates: {exact_dups}")
print(f"  Amount mismatch : {amount_dups} pairs")
print(f"  Case mismatch   : {mismatch} pairs")
print(f"  Total rows      : {len(records)}")
