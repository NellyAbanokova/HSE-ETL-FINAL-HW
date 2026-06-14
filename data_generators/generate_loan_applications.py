import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_FILE = PROJECT_DIR / "data" / "loan_applications.csv"

regions = ["DE-HE", "DE-BE", "DE-BY", "FR-IDF", "ES-MD", "IT-LAZ", "PL-MZ", "NL-NH"]
products = ["cash_loan", "credit_card", "mortgage", "car_loan", "business_loan"]
risk_levels = ["low", "medium", "high"]
decisions = ["approved", "rejected", "manual_review"]
channels = ["mobile", "web", "branch", "call_center", "partner"]

target_rows = 700_000

start = datetime(2026, 5, 1, 0, 0, 0)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    writer.writerow([
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec"
    ])

    for i in range(target_rows):
        requested_amount = random.randint(500, 50000)

        credit_score = random.randint(300, 850)

        if credit_score >= 720:
            risk_level = "low"
            decision_status = random.choices(decisions, weights=[0.75, 0.10, 0.15], k=1)[0]
        elif credit_score >= 580:
            risk_level = "medium"
            decision_status = random.choices(decisions, weights=[0.45, 0.25, 0.30], k=1)[0]
        else:
            risk_level = "high"
            decision_status = random.choices(decisions, weights=[0.15, 0.55, 0.30], k=1)[0]

        if decision_status == "approved":
            approved_amount = requested_amount
        else:
            approved_amount = 0

        event_time = start + timedelta(seconds=random.randint(0, 2_500_000))

        writer.writerow([
            f"app_20260501_{i:08d}",
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"cust_{random.randint(10000, 999999)}",
            random.choice(regions),
            random.choice(products),
            requested_amount,
            random.choice([6, 12, 24, 36, 48, 60]),
            credit_score,
            risk_level,
            decision_status,
            approved_amount,
            random.choice(channels),
            random.choice(["true", "false"]),
            random.randint(1, 180)
        ])

print(f"Файл создан: {OUTPUT_FILE}")
print("Готово.")