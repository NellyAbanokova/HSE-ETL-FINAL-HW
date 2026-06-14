import json
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row

KAFKA_BOOTSTRAP = "rc1a-jvot9avldu1a61ir.mdb.yandexcloud.net:9091"
TOPIC = "loan-applications-json"
USERNAME = "etl_user"
PASSWORD = "<KAFKA_PASSWORD>"

spark = SparkSession.builder.appName("kafka-producer-loan-events").getOrCreate()

regions = ["DE-HE", "DE-BE", "DE-BY", "FR-IDF", "ES-MD", "IT-LAZ"]
risk_levels = ["low", "medium", "high"]
decisions = ["approved", "rejected", "manual_review"]
doc_statuses = ["verified", "pending", "rejected"]

rows = []
start = datetime(2026, 5, 1, 0, 0, 0)

for i in range(250_000):
    event = {
        "application_id": f"loan_{i:08d}",
        "customer": {
            "customer_id": f"cust_{random.randint(1000, 999999)}",
            "region": random.choice(regions)
        },
        "loan": {
            "amount": random.randint(500, 50000),
            "term_months": random.choice([6, 12, 24, 36, 48, 60])
        },
        "scoring": {
            "score": random.randint(300, 850),
            "risk_level": random.choice(risk_levels)
        },
        "documents": [
            {
                "type": "passport",
                "status": random.choice(doc_statuses)
            }
        ],
        "decision_status": random.choice(decisions),
        "submitted_at": (
            start + timedelta(seconds=random.randint(0, 2_000_000))
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    rows.append(Row(value=json.dumps(event, ensure_ascii=False)))

df = spark.createDataFrame(rows)

(
    df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";'
    )
    .save()
)

spark.stop()