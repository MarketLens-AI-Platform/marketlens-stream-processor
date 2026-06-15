from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("MarketLensStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    # Define schema based on marketlens-ingestion/scraping/schemas.py
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("url", StringType(), True),
        StructField("store_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("images", ArrayType(StringType()), True),
        StructField("category", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("review_count", DoubleType(), True),
        StructField("stock_status", StringType(), True),
    ])

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "marketlens-kafka:9092")
    s3_bucket = os.getenv("S3_BUCKET", "marketlens-raw-ingestion-v110")
    checkpoint_location = f"s3://{s3_bucket}/checkpoints/stream-processor"
    output_path = f"s3://{s3_bucket}/raw-data/parquet"

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "raw-market-data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Write to S3 as Parquet
    query = parsed_df \
        .writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
