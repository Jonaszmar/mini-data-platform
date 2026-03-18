import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType
from delta import configure_spark_with_delta_pip

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

builder = (
    SparkSession.builder
    .appName("KafkaToDeltaLake")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Debezium JsonConverter output structure varies slightly by version/config.
# This schema safely captures top-level fields as strings/maps.
json_schema = StructType([
    StructField("schema", MapType(StringType(), StringType()), True),
    StructField("payload", MapType(StringType(), StringType()), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribePattern", "dbserver1.public.*")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(topic AS STRING) as topic", "CAST(value AS STRING) as json_str")
    .withColumn("parsed", from_json(col("json_str"), json_schema))
    .select(
        col("topic"),
        col("json_str"),
        col("parsed.payload").alias("payload"),
        current_timestamp().alias("processed_at")
    )
    .withColumn("source_table", col("topic"))
)

# Simple transformation: keep topic/table + raw message + processing timestamp
output_path = "s3a://datalake/delta/business_events"
checkpoint_path = "s3a://datalake/checkpoints/business_events"

query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)

query.awaitTermination()






