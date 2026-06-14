import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]

OUTPUT_FILE = PROJECT_DIR / "data" / "transactions_v2.csv"

regions = ["DE-HE", "DE-BE", "DE-BY", "FR-IDF", "ES-MD", "IT-LAZ"]
campaigns = [
    "credit_card_offer",
    "cash_loan",
    "mortgage",
    "insurance",
    "deposit"
]
statuses = ["answered", "missed", "busy", "failed"]
responses = ["interested", "not_interested", "callback", "no_response"]

target_rows = 400_000

start = datetime(2026, 5, 1, 8, 0, 0)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    writer.writerow([
        "call_id",
        "call_time",
        "client_id",
        "region_code",
        "campaign_type",
        "call_status",
        "client_response",
        "duration_sec",
        "follow_up_required"
    ])

    for i in range(target_rows):
        call_time = start + timedelta(seconds=random.randint(0, 2_500_000))
        status = random.choice(statuses)

        if status == "answered":
            response = random.choice(responses)
        else:
            response = "no_response"

        writer.writerow([
            f"call_20260501_{i:08d}",
            call_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"client_{random.randint(1000, 999999)}",
            random.choice(regions),
            random.choice(campaigns),
            status,
            response,
            random.randint(5, 900),
            random.choice(["true", "false"])
        ])

print(f"Файл создан: {OUTPUT_FILE}")
print("Готово.")