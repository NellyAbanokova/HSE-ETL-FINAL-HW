from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

KAFKA_BOOTSTRAP = "rc1a-jvot9avldu1a61ir.mdb.yandexcloud.net:9091"
TOPIC = "loan-applications-json"
USERNAME = "etl_user"
PASSWORD = "<KAFKA_PASSWORD>"

BUCKET = "nelly-etl-final-2026"
OUTPUT_PATH = f"s3a://{BUCKET}/processed/kafka_flat/"

spark = SparkSession.builder.appName("kafka-consumer-flatten-json").getOrCreate()

schema = StructType([
    StructField("application_id", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType()),
        StructField("term_months", IntegerType())
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType()),
        StructField("risk_level", StringType())
    ])),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("status", StringType())
    ]))),
    StructField("decision_status", StringType()),
    StructField("submitted_at", StringType())
])

raw = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";'
    )
    .load()
)

json_df = raw.selectExpr("CAST(value AS STRING) AS json_value")

parsed = json_df.select(from_json(col("json_value"), schema).alias("data"))

flat = (
    parsed
    .withColumn("document", explode(col("data.documents")))
    .select(
        col("data.application_id").alias("application_id"),
        col("data.customer.customer_id").alias("customer_id"),
        col("data.customer.region").alias("region_code"),
        col("data.loan.amount").alias("amount"),
        col("data.loan.term_months").alias("term_months"),
        col("data.scoring.score").alias("score"),
        col("data.scoring.risk_level").alias("risk_level"),
        col("document.type").alias("document_type"),
        col("document.status").alias("document_status"),
        col("data.decision_status").alias("decision_status"),
        col("data.submitted_at").alias("submitted_at")
    )
)

flat.write.mode("overwrite").parquet(OUTPUT_PATH)

spark.stop()