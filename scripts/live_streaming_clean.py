import logging
import configparser
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit, current_timestamp, sum as sum_, initcap
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/live_streaming_clean.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load AWS configuration
config = configparser.ConfigParser()
config.read('/app/config/aws_config.ini')
aws_access_key = config['aws']['access_key_id']
aws_secret_key = config['aws']['secret_access_key']
aws_region = config['aws']['region']

try:
    # Initialize Spark session
    logger.info("Initializing Spark session")
    spark = SparkSession.builder \
        .appName("LiveStreamingClean") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.sql.parquet.outputLegacyFormat", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    # Live Streaming Schema
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
        StructField("AddictionLevel", LongType(), True),
        StructField("IngestionTimestamp", TimestampType(), True)
    ])

    # Helper function to check if S3 path is accessible
    def check_s3_path(spark, s3_path, bucket="datastreaming-analytics-1"):
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI.create(s3_path.split("://")[0] + "://"),
                hadoop_conf
            )
            path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
            exists = fs.exists(path)
            files = fs.listFiles(path, False) if exists else []
            file_list = [(f.getPath().getName(), f.getLen()) for f in files]
            file_count = len(file_list)
            logger.info(f"S3 path {s3_path} exists: {exists}, file count: {file_count}, files: {file_list}")
            return exists and file_count > 0
        except Exception as e:
            logger.warning(f"Hadoop check failed for S3 path {s3_path}: {str(e)}")
            try:
                s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
                prefix = s3_path.split(f"s3a://{bucket}/")[1]
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                files = response.get('Contents', []) if 'Contents' in response else []
                file_list = [(obj['Key'], obj['Size']) for obj in files]
                file_count = len(file_list)
                logger.info(f"AWS SDK check for {s3_path}: file count: {file_count}, files: {file_list}")
                return file_count > 0
            except Exception as sdk_e:
                logger.error(f"AWS SDK check failed for {s3_path}: {str(sdk_e)}")
                return False

    # Helper function to validate nulls in critical columns
    def validate_nulls(df, critical_columns, file_name):
        try:
            null_counts_expr = [sum_(col(c).isNull().cast("int")).alias(c) for c in df.columns]
            null_counts = df.select(null_counts_expr).collect()[0].asDict()
            logger.info(f"Null counts for {file_name}: {null_counts}")
            invalid_rows = df.filter(reduce(lambda x, y: x | y, [col(c).isNull() for c in critical_columns]))
            invalid_count = invalid_rows.count()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} rows with nulls in critical columns for {file_name}")
                for row in invalid_rows.limit(10).collect():
                    logger.info(f"Rejected row (Nulls): EventID={row['EventID']}, UserID={row['UserID']}, "
                                f"Reason='Null in critical columns: {', '.join(critical_columns)}', Row={row}")
            return invalid_count, invalid_rows
        except Exception as e:
            logger.error(f"Error validating nulls for {file_name}: {str(e)}")
            return -1, None

    # Clean live_streaming data
    raw_s3_path = "s3a://datastreaming-analytics-1/raw/live_streaming"
    staging_s3_path = "s3a://datastreaming-analytics-1/staging/live_streaming"

    if check_s3_path(spark, raw_s3_path):
        # Read raw data
        logger.info(f"Reading raw data from {raw_s3_path}")
        raw_df = spark.read.parquet(raw_s3_path)
        logger.info(f"Raw schema: {raw_df.schema}")

        # Apply schema
        live_df = spark.createDataFrame(raw_df.rdd, live_streaming_schema)
        logger.info(f"Schema applied for live_streaming: {live_df.schema}")
        logger.info(f"Raw row count: {live_df.count()}")

        # Step 1: Handle null values
        critical_columns = ["EventID", "UserID", "LiveEngagement"]
        null_count, null_rows = validate_nulls(live_df, critical_columns, "live_streaming_before_null_drop")
        live_df = live_df.dropna(subset=critical_columns)
        logger.info(f"Row count after dropping nulls in critical columns: {live_df.count()}")

        # Fill nulls in non-critical columns
        median_addiction = live_df.selectExpr("percentile_approx(AddictionLevel, 0.5)").collect()[0][0] or 0
        fill_values = {
            "EventType": "Unknown",
            "Platform": "Unknown",
            "ViewerCount": 0,
            "StreamDuration": 0,
            "DeviceType": "Unknown",
            "WatchTime": "Unknown",
            "AddictionLevel": median_addiction
        }
        for column, fill_value in fill_values.items():
            null_rows_before = live_df.filter(col(column).isNull())
            if null_rows_before.count() > 0:
                for row in null_rows_before.limit(10).collect():
                    logger.info(f"Updated row: EventID={row['EventID']}, UserID={row['UserID']}, "
                                f"Field={column}, OldValue=None, NewValue={fill_value}, "
                                f"Reason='Filled null in {column}'")
        live_df = live_df.fillna(fill_values)

        # Step 2: Validate and correct data
        valid_condition = (
            (col("EventID") > 0) &
            (col("UserID") > 0) &
            (col("LiveEngagement") >= 0) &
            (col("ViewerCount") >= 0) &
            (col("StreamDuration") >= 0) &
            (col("AddictionLevel").between(0, 10))
        )
        invalid_rows = live_df.filter(~valid_condition)
        if invalid_rows.count() > 0:
            invalid_rows = invalid_rows.withColumn(
                "Reason",
                when(col("EventID") <= 0, "Invalid EventID").otherwise(
                    when(col("UserID") <= 0, "Invalid UserID").otherwise(
                        when(col("LiveEngagement") < 0, "Negative LiveEngagement").otherwise(
                            when(col("ViewerCount") < 0, "Negative ViewerCount").otherwise(
                                when(col("StreamDuration") < 0, "Negative StreamDuration").otherwise(
                                    "Invalid AddictionLevel"
                                )
                            )
                        )
                    )
                )
            )
            for row in invalid_rows.limit(10).collect():
                logger.info(f"Rejected row (Validation): EventID={row['EventID']}, UserID={row['UserID']}, "
                            f"Reason={row['Reason']}, Row={row}")
        live_df = live_df.filter(valid_condition)
        logger.info(f"Row count after validation filters: {live_df.count()}")

        # Step 3: Standardize Platform
        original_platform = live_df.select("EventID", "UserID", col("Platform").alias("OldPlatform"))
        valid_platforms = ["Instagram", "YouTube", "Facebook", "TikTok", "Unknown"]
        live_df = live_df.withColumn(
            "Platform",
            when(col("Platform").isNull(), "Unknown").otherwise(
                when(col("Platform").isin(valid_platforms), col("Platform")).otherwise("Unknown")
            )
        )
        platform_updates = live_df.join(original_platform, ["EventID", "UserID"]).filter(
            col("Platform") != col("OldPlatform")
        )
        for row in platform_updates.limit(10).collect():
            logger.info(f"Updated row: EventID={row['EventID']}, UserID={row['UserID']}, "
                        f"Field=Platform, OldValue={row['OldPlatform']}, NewValue={row['Platform']}, "
                        f"Reason='Standardized Platform'")

        # Step 4: Standardize DeviceType
        original_device = live_df.select("EventID", "UserID", col("DeviceType").alias("OldDeviceType"))
        valid_devices = ["Smartphone", "Tablet", "Desktop", "Unknown"]
        live_df = live_df.withColumn(
            "DeviceType",
            when(col("DeviceType").isNull(), "Unknown").otherwise(
                when(col("DeviceType").isin("Computer", "PC", "Laptop"), "Desktop").otherwise(
                    when(col("DeviceType").isin(valid_devices), col("DeviceType")).otherwise("Unknown")
                )
            )
        )
        device_updates = live_df.join(original_device, ["EventID", "UserID"]).filter(
            col("DeviceType") != col("OldDeviceType")
        )
        for row in device_updates.limit(10).collect():
            logger.info(f"Updated row: EventID={row['EventID']}, UserID={row['UserID']}, "
                        f"Field=DeviceType, OldValue={row['OldDeviceType']}, NewValue={row['DeviceType']}, "
                        f"Reason='Standardized DeviceType'")

        # Step 5: Standardize EventType
        original_event_type = live_df.select("EventID", "UserID", col("EventType").alias("OldEventType"))
        valid_event_types = ["LiveStream", "Broadcast", "Unknown"]
        live_df = live_df.withColumn(
            "EventType",
            when(col("EventType").isNull(), "Unknown").otherwise(
                when(col("EventType").isin("Sports", "Concert", "Gaming", "Q&A"), "LiveStream").otherwise(
                    when(col("EventType").isin(valid_event_types), col("EventType")).otherwise("Unknown")
                )
            )
        )
        event_type_updates = live_df.join(original_event_type, ["EventID", "UserID"]).filter(
            col("EventType") != col("OldEventType")
        )
        for row in event_type_updates.limit(10).collect():
            logger.info(f"Updated row: EventID={row['EventID']}, UserID={row['UserID']}, "
                        f"Field=EventType, OldValue={row['OldEventType']}, NewValue={row['EventType']}, "
                        f"Reason='Standardized EventType'")

        # Step 6: Remove duplicates
        window_spec = Window.partitionBy("EventID", "UserID").orderBy(col("IngestionTimestamp").desc())
        live_df = live_df.withColumn("row_num", row_number().over(window_spec))
        duplicate_rows = live_df.filter(col("row_num") > 1)
        if duplicate_rows.count() > 0:
            for row in duplicate_rows.limit(10).collect():
                logger.info(f"Rejected row (Duplicate): EventID={row['EventID']}, UserID={row['UserID']}, "
                            f"Reason='Duplicate row (not most recent)', Row={row}")
        live_df = live_df.filter(col("row_num") == 1).drop("row_num")
        logger.info(f"Row count after deduplication: {live_df.count()}")

        # Step 7: Standardize formats
        string_columns = ["EventType", "Platform", "DeviceType", "WatchTime"]
        for column in string_columns:
            original_values = live_df.select("EventID", "UserID", col(column).alias(f"Old{column}"))
            live_df = live_df.withColumn(column, trim(col(column)))
            if column in ["EventType", "Platform", "DeviceType"]:
                live_df = live_df.withColumn(column, initcap(col(column)))
            updates = live_df.join(original_values, ["EventID", "UserID"]).filter(
                col(column) != col(f"Old{column}")
            )
            for row in updates.limit(10).collect():
                logger.info(f"Updated row: EventID={row['EventID']}, UserID={row['UserID']}, "
                            f"Field={column}, OldValue={row[f'Old{column}']}, NewValue={row[column]}, "
                            f"Reason='Trimmed and formatted {column}'")

        # Step 8: Validate nulls after cleaning
        null_count, _ = validate_nulls(live_df, critical_columns, "live_streaming_cleaned")
        if null_count > 0:
            logger.warning("Null values found in critical columns after cleaning")

        # Step 9: Write cleaned data to staging
        cleaned_count = live_df.count()
        if cleaned_count > 0:
            live_df.coalesce(1).write \
                .option("compression", "snappy") \
                .mode("overwrite") \
                .parquet(staging_s3_path)
            logger.info(f"Successfully wrote {cleaned_count} rows to {staging_s3_path}")
            logger.info(f"Written schema: {live_df.schema}")
        else:
            logger.info("No data to write to staging for live_streaming")

    else:
        logger.error(f"No data found in {raw_s3_path}")

    spark.stop()
except Exception as e:
    logger.error(f"Unexpected error in live_streaming_clean.py: {str(e)}")
    raise