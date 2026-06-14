from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum as spark_sum, round as spark_round

BUCKET = "nelly-etl-final-2026"

INPUT_PATH = f"s3a://{BUCKET}/processed/kafka_flat/"
OUTPUT_ANALYTICS = f"s3a://{BUCKET}/processed/kafka_analytics/"
OUTPUT_QUALITY = f"s3a://{BUCKET}/processed/kafka_quality_report/"

spark = SparkSession.builder.appName("kafka-flat-analytics").getOrCreate()

df = spark.read.parquet(INPUT_PATH)

analytics = (
    df.groupBy("region_code", "risk_level", "decision_status")
    .agg(
        count("*").alias("applications_count"),
        spark_round(avg("amount"), 2).alias("avg_amount"),
        spark_sum("amount").alias("total_amount"),
        spark_round(avg("score"), 2).alias("avg_score"),
        spark_round(avg("term_months"), 2).alias("avg_term_months")
    )
)

analytics.write.mode("overwrite").parquet(OUTPUT_ANALYTICS)

quality_rows = [
    ("flat_rows_count", str(df.count())),
    ("unique_applications_count", str(df.select("application_id").distinct().count())),
    ("unique_customers_count", str(df.select("customer_id").distinct().count())),
    ("regions_count", str(df.select("region_code").distinct().count())),
    ("risk_levels_count", str(df.select("risk_level").distinct().count())),
]

quality_df = spark.createDataFrame(quality_rows, ["metric_name", "metric_value"])
quality_df.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_QUALITY)

spark.stop()