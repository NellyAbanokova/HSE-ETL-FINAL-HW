from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    when,
    to_timestamp,
    round as spark_round,
    lit
)

BUCKET = "nelly-etl-final-2026"

INPUT_PATH = f"s3a://{BUCKET}/raw/applications/loan_applications.csv"

OUTPUT_CLEANED = f"s3a://{BUCKET}/processed/applications/cleaned/"
OUTPUT_DM_REGION = f"s3a://{BUCKET}/processed/applications/dm_region/"
OUTPUT_DM_PRODUCT_RISK = f"s3a://{BUCKET}/processed/applications/dm_product_risk/"
OUTPUT_DM_DECISION_CHANNEL = f"s3a://{BUCKET}/processed/applications/dm_decision_channel/"
OUTPUT_QUALITY = f"s3a://{BUCKET}/processed/applications/quality_report/"

spark = (
    SparkSession.builder
    .appName("etl-final-applications-processing-max")
    .getOrCreate()
)

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

input_rows = df_raw.count()

df = (
    df_raw
    .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("requested_amount", col("requested_amount").cast("double"))
    .withColumn("approved_amount", col("approved_amount").cast("double"))
    .withColumn("term_months", col("term_months").cast("int"))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .withColumn("processing_time_sec", col("processing_time_sec").cast("int"))
    .withColumn(
        "is_approved",
        when(col("decision_status") == "approved", lit(1)).otherwise(lit(0))
    )
    .withColumn(
        "is_manual_review",
        when(col("decision_status") == "manual_review", lit(1)).otherwise(lit(0))
    )
    .withColumn(
        "is_rejected",
        when(col("decision_status") == "rejected", lit(1)).otherwise(lit(0))
    )
)

# 1. Очищенный слой
(
    df.write
    .mode("overwrite")
    .parquet(OUTPUT_CLEANED)
)

# 2. Витрина по регионам
dm_region = (
    df.groupBy("region_code")
    .agg(
        count("*").alias("applications_count"),
        spark_sum("requested_amount").alias("total_requested_amount"),
        spark_sum("approved_amount").alias("total_approved_amount"),
        spark_round(avg("credit_score"), 2).alias("avg_credit_score"),
        spark_round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
        spark_round(avg("is_approved") * 100, 2).alias("approval_rate_percent"),
        spark_round(avg("is_manual_review") * 100, 2).alias("manual_review_rate_percent")
    )
)

(
    dm_region.write
    .mode("overwrite")
    .parquet(OUTPUT_DM_REGION)
)

# 3. Витрина по продукту и уровню риска
dm_product_risk = (
    df.groupBy("product_type", "risk_level")
    .agg(
        count("*").alias("applications_count"),
        spark_sum("requested_amount").alias("total_requested_amount"),
        spark_sum("approved_amount").alias("total_approved_amount"),
        spark_round(avg("credit_score"), 2).alias("avg_credit_score"),
        spark_round(avg("is_approved") * 100, 2).alias("approval_rate_percent"),
        spark_round(avg("is_rejected") * 100, 2).alias("rejection_rate_percent")
    )
)

(
    dm_product_risk.write
    .mode("overwrite")
    .parquet(OUTPUT_DM_PRODUCT_RISK)
)

# 4. Витрина по каналу и решению
dm_decision_channel = (
    df.groupBy("channel", "decision_status")
    .agg(
        count("*").alias("applications_count"),
        spark_round(avg("requested_amount"), 2).alias("avg_requested_amount"),
        spark_round(avg("approved_amount"), 2).alias("avg_approved_amount"),
        spark_round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec")
    )
)

(
    dm_decision_channel.write
    .mode("overwrite")
    .parquet(OUTPUT_DM_DECISION_CHANNEL)
)

# 5. Quality report
null_application_id = df.filter(col("application_id").isNull()).count()
null_customer_id = df.filter(col("customer_id").isNull()).count()
negative_requested_amount = df.filter(col("requested_amount") < 0).count()
negative_approved_amount = df.filter(col("approved_amount") < 0).count()
invalid_credit_score = df.filter((col("credit_score") < 300) | (col("credit_score") > 850)).count()
duplicate_application_id = input_rows - df.select("application_id").distinct().count()

min_event_time = df.agg(spark_min("event_ts")).collect()[0][0]
max_event_time = df.agg(spark_max("event_ts")).collect()[0][0]

quality_rows = [
    ("input_rows", str(input_rows)),
    ("null_application_id", str(null_application_id)),
    ("null_customer_id", str(null_customer_id)),
    ("negative_requested_amount", str(negative_requested_amount)),
    ("negative_approved_amount", str(negative_approved_amount)),
    ("invalid_credit_score", str(invalid_credit_score)),
    ("duplicate_application_id", str(duplicate_application_id)),
    ("min_event_time", str(min_event_time)),
    ("max_event_time", str(max_event_time)),
]

quality_df = spark.createDataFrame(quality_rows, ["metric_name", "metric_value"])

(
    quality_df.write
    .mode("overwrite")
    .option("header", True)
    .csv(OUTPUT_QUALITY)
)

spark.stop()