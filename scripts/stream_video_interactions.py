import logging
import configparser
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, sum as sum_, initcap, lower, from_json
)
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType, BooleanType
)
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/stream_video_interactions.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.info("Starting stream_video_interactions.py")

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
        .appName("StreamVideoInteractionsClean") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    logger.info("Spark session initialized")

    # Schema for video interactions data
    video_interactions_schema = StructType([
        StructField("UserID", LongType(), True),
        StructField("Age", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Income", LongType(), True),
        StructField("Debt", BooleanType(), True),
        StructField("OwnsProperty", BooleanType(), True),
        StructField("Profession", StringType(), True),
        StructField("Demographics", StringType(), True),
        StructField("Platform", StringType(), True),
        StructField("TotalTimeSpent", LongType(), True),
        StructField("NumberOfSessions", LongType(), True),
        StructField("VideoID", LongType(), True),
        StructField("VideoCategory", StringType(), True),
        StructField("VideoLength", LongType(), True),
        StructField("Engagement", LongType(), True),
        StructField("ImportanceScore", LongType(), True),
        StructField("TimeSpentOnVideo", LongType(), True),
        StructField("NumberOfVideosWatched", LongType(), True),
        StructField("ScrollRate", LongType(), True),
        StructField("Frequency", StringType(), True),
        StructField("ProductivityLoss", LongType(), True),
        StructField("Satisfaction", LongType(), True),
        StructField("WatchReason", StringType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("OS", StringType(), True),
        StructField("WatchTime", StringType(), True),
        StructField("SelfControl", LongType(), True),
        StructField("AddictionLevel", LongType(), True),
        StructField("CurrentActivity", StringType(), True),
        StructField("ConnectionType", StringType(), True)
    ])
    logger.info("Schema defined")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "video_interactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    logger.info("Kafka stream initialized")

    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), video_interactions_schema).alias("data")
    ).select("data.*").withColumn("IngestionTimestamp", current_timestamp()).filter(col("data").isNotNull())
    logger.info("JSON parsing completed")

    # Cleaning: Step 1 - Handle null values
    critical_columns = ["UserID", "VideoID", "TimeSpentOnVideo"]
    cleaned_df = parsed_df.dropna(subset=critical_columns)
    median_age = 30  # Default median for streaming
    fill_values = {
        "Age": median_age,
        "Gender": "Unknown",
        "Location": "Unknown",
        "Income": 0,
        "Debt": False,
        "OwnsProperty": False,
        "Profession": "Unknown",
        "Demographics": "Unknown",
        "Platform": "Unknown",
        "TotalTimeSpent": 0,
        "NumberOfSessions": 0,
        "VideoCategory": "Unknown",
        "VideoLength": 0,
        "Engagement": 0,
        "ImportanceScore": 0,
        "NumberOfVideosWatched": 0,
        "ScrollRate": 0,
        "Frequency": "Unknown",
        "ProductivityLoss": 0,
        "Satisfaction": 0,
        "WatchReason": "Unknown",
        "DeviceType": "Unknown",
        "OS": "Unknown",
        "WatchTime": "00:00:00",
        "SelfControl": 0,
        "AddictionLevel": 0,
        "CurrentActivity": "Unknown",
        "ConnectionType": "Unknown"
    }
    cleaned_df = cleaned_df.fillna(fill_values)

    # Cleaning: Step 2 - Validate and correct data
    valid_condition = (
        (col("UserID") > 0) &
        (col("VideoID") > 0) &
        (col("Age").between(13, 100)) &
        (col("TimeSpentOnVideo") >= 0) &
        (col("TotalTimeSpent") >= 0) &
        (col("NumberOfSessions") >= 0) &
        (col("VideoLength") >= 0) &
        (col("Engagement") >= 0) &
        (col("ImportanceScore") >= 0) &
        (col("NumberOfVideosWatched") >= 0) &
        (col("ScrollRate") >= 0) &
        (col("ProductivityLoss") >= 0) &
        (col("Satisfaction") >= 0) &
        (col("SelfControl") >= 0) &
        (col("AddictionLevel") >= 0)
    )
    cleaned_df = cleaned_df.filter(valid_condition)

    # Cleaning: Step 3 - Standardize Gender
    cleaned_df = cleaned_df.withColumn(
        "Gender",
        when(col("Gender").isNull(), "Unknown").otherwise(
            when(col("Gender").isin("Male", "M", "male"), "Male").otherwise(
                when(col("Gender").isin("Female", "F", "female"), "Female").otherwise(
                    when(col("Gender").isin("Other", "Non-binary", "non-binary"), "Other").otherwise("Unknown")
                )
            )
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

    # Cleaning: Step 6 - Standardize OS
    valid_os = ["iOS", "Android", "Windows", "MacOS", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "OS",
        when(col("OS").isNull(), "Unknown").otherwise(
            when(col("OS").isin(valid_os), col("OS")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 7 - Standardize ConnectionType
    valid_connection_types = ["WiFi", "Cellular", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "ConnectionType",
        when(col("ConnectionType").isNull(), "Unknown").otherwise(
            when(col("ConnectionType").isin(valid_connection_types), col("ConnectionType")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 8 - Standardize VideoCategory
    valid_categories = ["Entertainment", "Education", "Gaming", "Vlog", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "VideoCategory",
        when(col("VideoCategory").isNull(), "Unknown").otherwise(
            when(col("VideoCategory").isin(valid_categories), col("VideoCategory")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 9 - Standardize WatchReason
    valid_reasons = ["Entertainment", "Education", "Relaxation", "Unknown"]
    cleaned_df = cleaned_df.withColumn(
        "WatchReason",
        when(col("WatchReason").isNull(), "Unknown").otherwise(
            when(col("WatchReason").isin(valid_reasons), col("WatchReason")).otherwise("Unknown")
        )
    )

    # Cleaning: Step 10 - Standardize WatchTime format
    cleaned_df = cleaned_df.withColumn(
        "WatchTime",
        when(col("WatchTime").isNull(), "00:00:00").otherwise(
            when(col("WatchTime").cast("timestamp").isNotNull(), col("WatchTime")).otherwise("00:00:00")
        )
    )

    # Cleaning: Step 11 - Standardize formats
    string_columns = [
        "Gender", "Location", "Profession", "Demographics", "Platform",
        "VideoCategory", "Frequency", "WatchReason", "DeviceType", "OS",
        "CurrentActivity", "ConnectionType"
    ]
    for column in string_columns:
        cleaned_df = cleaned_df.withColumn(column, trim(lower(col(column))))
        if column in ["Location", "Profession", "Platform", "VideoCategory", "DeviceType", "OS", "CurrentActivity"]:
            cleaned_df = cleaned_df.withColumn(column, initcap(col(column)))

    # Cleaning: Step 12 - Add derived column (AgeGroup)
    cleaned_df = cleaned_df.withColumn(
        "AgeGroup",
        when(col("Age").between(13, 17), "13-17").otherwise(
            when(col("Age").between(18, 24), "18-24").otherwise(
                when(col("Age").between(25, 34), "25-34").otherwise(
                    when(col("Age").between(35, 44), "35-44").otherwise(
                        when(col("Age").between(45, 54), "45-54").otherwise(
                            when(col("Age").between(55, 64), "55-64").otherwise("65+")
                        )
                    )
                )
            )
        )
    )

    # Cleaning: Step 13 - Remove duplicates using watermark
    cleaned_df = cleaned_df \
        .withWatermark("IngestionTimestamp", "10 minutes") \
        .dropDuplicates(["UserID", "VideoID"])

    # Write cleaned data to S3 staging
    staging_s3_path = "s3a://datastreaming-analytics-1/staging/video_interactions"
    logger.info(f"Writing cleaned data to {staging_s3_path}")
    query = cleaned_df.writeStream \
        .option("checkpointLocation", "/app/checkpoints/stream_video_clean") \
        .format("parquet") \
        .option("path", staging_s3_path) \
        .option("compression", "snappy") \
        .partitionBy("IngestionTimestamp") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Log query status
    logger.info(f"Streaming query started: {query.name}, ID: {query.id}")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming query stopped by user")
        query.stop()

except Exception as e:
    logger.error(f"Error in stream_video_interactions.py: {str(e)}", exc_info=True)
    raise
finally:
    spark.stop()
    logger.info("Spark session stopped")