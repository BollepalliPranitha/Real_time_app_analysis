import logging
import configparser
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, sum as sum_, initcap, lower, from_json
)
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType
)
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/stream_live_streaming.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.info("Starting stream_live_streaming.py")

try:
    # Load AWS configurations
    config = configparser.ConfigParser()
    config.read('/app/config/aws_config.ini')
    aws_access_key = config['aws']['access_key_id']
    aws_secret_key = config['aws']['secret_access_key']
    aws_region = config['aws']['region']
    logger.info("AWS configurations loaded")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("StreamLiveStreamingClean") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    logger.info("Spark session initialized")

    # Schema for live streaming data
    live_streaming_schema = StructType([
        StructField("EventID", LongType(), True),
        StructField("EventType", StringType(), True),
        StructField("UserID", LongType(), True),
        StructField("Platform", StringType(), True),
        StructField("LiveEngagement", LongType(), True),
        StructField("ViewerCount", LongType(), True),
        StructField("StreamDuration", LongType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("WatchTime", StringType(), True),
        StructField("AddictionLevel", LongType(), True)
    ])
    logger.info("Schema defined")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "live_streaming") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info("Kafka stream initialized")

    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), live_streaming_schema).alias("data")
    ).select("data.*").withColumn("IngestionTimestamp", current_timestamp())
    logger.info("JSON parsing completed")

    # Cleaning: Step 1 - Handle null values
    critical_columns = ["EventID", "UserID", "LiveEngagement"]
    cleaned_df = parsed_df.dropna(subset=critical_columns)
    fill_values = {
        "EventType": "Unknown",
        "Platform": "Unknown",
        "ViewerCount": 0,
        "StreamDuration": 0,
        "DeviceType": "Unknown",
        "WatchTime": "00:00:00",
        "AddictionLevel": 0
    }
    cleaned_df = cleaned_df.fillna(fill_values)

    # Cleaning: Step 2 - Validate and correct data
    valid_condition = (
        (col("EventID") > 0) &
        (col("UserID") > 0) &
        (col("LiveEngagement") >= 0) &
        (col("ViewerCount") >= 0) &
        (col("StreamDuration") >= 0) &
        (col("AddictionLevel") >= 0)
    )
    cleaned_df = cleaned_df.filter(valid_condition)

    # Cleaning: Step 3 - Standardize EventType
    valid_event_types = ["LiveStream", "Broadcast", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "EventType",
        when(col("EventType").isNull(), "Unknown").otherwise(
            when(col("EventType").isin(valid_event_types), col("EventType")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 4 - Standardize Platform
    valid_platforms = ["Instagram", "YouTube", "Facebook", "TikTok", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "Platform",
        when(col("Platform").isNull(), "Unknown").otherwise(
            when(col("Platform").isin(valid_platforms), col("Platform")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 5 - Standardize DeviceType
    valid_device_types = ["Mobile", "Desktop", "Tablet", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "DeviceType",
        when(col("DeviceType").isNull(), "Unknown").otherwise(
            when(col("DeviceType").isin(valid_device_types), col("DeviceType")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 6 - Standardize WatchTime format
    cleaned_df = cleaned_df.withColumn(
        "WatchTime",
        when(col("WatchTime").isNull(), "00:00:00").otherwise(
            when(col("WatchTime").cast("timestamp").isNotNull(), col("WatchTime")).otherwise("00:00:00")
        )
    )

    # Cleaning: Step 7 - Standardize formats
    string_columns = ["EventType", "Platform", "DeviceType", "WatchTime"]
    for column in string_columns:
        cleaned_df = cleaned_df.withColumn(column, trim(col(column)))
        if column in ["EventType", "Platform", "DeviceType"]:
            cleaned_df = cleaned_df.withColumn(column, initcap(col(column)))

    # Cleaning: Step 8 - Remove duplicates using watermark
    cleaned_df = cleaned_df \
        .withWatermark("IngestionTimestamp", "10 minutes") \
        .dropDuplicates(["EventID", "UserID"])

    # Write cleaned data to S3 staging
    staging_s3_path = "s3a://datastreaming-analytics-1/staging/live_streaming"
    logger.info(f"Writing cleaned data to {staging_s3_path}")
    query = cleaned_df.writeStream \
        .option("checkpointLocation", "/app/checkpoints/stream_live_streaming_clean") \
        .format("parquet") \
        .option("path", staging_s3_path) \
        .option("compression", "snappy") \
        .partitionBy("IngestionTimestamp") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Log query status
    logger.info(f"Streaming query started: {query.name}, ID: {query.id}")
    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in stream_live_streaming.py: {str(e)}", exc_info=True)
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped")