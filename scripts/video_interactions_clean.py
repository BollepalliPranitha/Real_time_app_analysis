import logging
import configparser
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit, current_timestamp, sum as sum_, initcap
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, BooleanType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/video_interactions_clean.log',
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
        .appName("VideoInteractionsClean") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.sql.parquet.outputLegacyFormat", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

    # Video Interactions Schema
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
        StructField("ConnectionType", StringType(), True),
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
                    logger.info(f"Rejected row (Nulls): UserID={row['UserID']}, VideoID={row['VideoID']}, "
                                f"Reason='Null in critical columns: {', '.join(critical_columns)}', Row={row}")
            return invalid_count, invalid_rows
        except Exception as e:
            logger.error(f"Error validating nulls for {file_name}: {str(e)}")
            return -1, None

    # Clean video_interactions data
    raw_s3_path = "s3a://datastreaming-analytics-1/raw/video_interactions"
    staging_s3_path = "s3a://datastreaming-analytics-1/staging/video_interactions"

    if check_s3_path(spark, raw_s3_path):
        # Read raw data
        logger.info(f"Reading raw data from {raw_s3_path}")
        raw_df = spark.read.parquet(raw_s3_path)
        logger.info(f"Raw schema: {raw_df.schema}")

        # Apply schema
        video_df = spark.createDataFrame(raw_df.rdd, video_interactions_schema)
        logger.info(f"Schema applied for video_interactions: {video_df.schema}")
        logger.info(f"Raw row count: {video_df.count()}")

        # Step 1: Handle null values
        critical_columns = ["UserID", "VideoID", "TotalTimeSpent"]
        null_count, null_rows = validate_nulls(video_df, critical_columns, "video_interactions_before_null_drop")
        video_df = video_df.dropna(subset=critical_columns)
        logger.info(f"Row count after dropping nulls in critical columns: {video_df.count()}")

        # Fill nulls in non-critical columns
        median_age = video_df.selectExpr("percentile_approx(Age, 0.5)").collect()[0][0] or 30
        median_income = video_df.selectExpr("percentile_approx(Income, 0.5)").collect()[0][0] or 50000
        median_addiction = video_df.selectExpr("percentile_approx(AddictionLevel, 0.5)").collect()[0][0] or 0
        fill_values = {
            "Age": median_age,
            "Gender": "Unknown",
            "Location": "Unknown",
            "Income": median_income,
            "Debt": False,
            "OwnsProperty": False,
            "Profession": "Unknown",
            "Demographics": "Unknown",
            "Platform": "Unknown",
            "NumberOfSessions": 0,
            "VideoCategory": "Unknown",
            "VideoLength": 0,
            "Engagement": 0,
            "ImportanceScore": 0,
            "TimeSpentOnVideo": 0,
            "NumberOfVideosWatched": 0,
            "ScrollRate": 0,
            "Frequency": "Unknown",
            "ProductivityLoss": 0,
            "Satisfaction": 0,
            "WatchReason": "Unknown",
            "DeviceType": "Unknown",
            "OS": "Unknown",
            "WatchTime": "Unknown",
            "SelfControl": 0,
            "AddictionLevel": median_addiction,
            "CurrentActivity": "Unknown",
            "ConnectionType": "Unknown"
        }
        for column, fill_value in fill_values.items():
            null_rows_before = video_df.filter(col(column).isNull())
            if null_rows_before.count() > 0:
                for row in null_rows_before.limit(10).collect():
                    logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                                f"Field={column}, OldValue=None, NewValue={fill_value}, "
                                f"Reason='Filled null in {column}'")
        video_df = video_df.fillna(fill_values)

        # Step 2: Validate and correct data
        valid_condition = (
            (col("UserID") > 0) &
            (col("VideoID") > 0) &
            (col("Age").between(13, 100)) &
            (col("TotalTimeSpent") >= 0) &
            (col("NumberOfSessions") >= 0) &
            (col("VideoLength") >= 0) &
            (col("Engagement") >= 0) &
            (col("ImportanceScore") >= 0) &
            (col("TimeSpentOnVideo") >= 0) &
            (col("NumberOfVideosWatched") >= 0) &
            (col("ScrollRate") >= 0) &
            (col("ProductivityLoss") >= 0) &
            (col("Satisfaction").between(0, 10)) &
            (col("SelfControl").between(0, 10)) &
            (col("AddictionLevel").between(0, 10))
        )
        invalid_rows = video_df.filter(~valid_condition)
        if invalid_rows.count() > 0:
            invalid_rows = invalid_rows.withColumn(
                "Reason",
                when(col("UserID") <= 0, "Invalid UserID").otherwise(
                    when(col("VideoID") <= 0, "Invalid VideoID").otherwise(
                        when(~col("Age").between(13, 100), "Invalid Age").otherwise(
                            when(col("TotalTimeSpent") < 0, "Negative TotalTimeSpent").otherwise(
                                when(col("NumberOfSessions") < 0, "Negative NumberOfSessions").otherwise(
                                    when(col("VideoLength") < 0, "Negative VideoLength").otherwise(
                                        when(col("Engagement") < 0, "Negative Engagement").otherwise(
                                            when(col("ImportanceScore") < 0, "Negative ImportanceScore").otherwise(
                                                when(col("TimeSpentOnVideo") < 0, "Negative TimeSpentOnVideo").otherwise(
                                                    when(col("NumberOfVideosWatched") < 0, "Negative NumberOfVideosWatched").otherwise(
                                                        when(col("ScrollRate") < 0, "Negative ScrollRate").otherwise(
                                                            when(col("ProductivityLoss") < 0, "Negative ProductivityLoss").otherwise(
                                                                when(~col("Satisfaction").between(0, 10), "Invalid Satisfaction").otherwise(
                                                                    when(~col("SelfControl").between(0, 10), "Invalid SelfControl").otherwise(
                                                                        "Invalid AddictionLevel"
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            for row in invalid_rows.limit(10).collect():
                logger.info(f"Rejected row (Validation): UserID={row['UserID']}, VideoID={row['VideoID']}, "
                            f"Reason={row['Reason']}, Row={row}")
        video_df = video_df.filter(valid_condition)
        logger.info(f"Row count after validation filters: {video_df.count()}")

        # Step 3: Standardize Gender
        original_gender = video_df.select("UserID", "VideoID", col("Gender").alias("OldGender"))
        video_df = video_df.withColumn(
            "Gender",
            when(col("Gender").isNull(), "Unknown").otherwise(
                when(col("Gender").isin("Male", "M", "male"), "Male").otherwise(
                    when(col("Gender").isin("Female", "F", "female"), "Female").otherwise(
                        when(col("Gender").isin("Other", "Non-binary", "non-binary"), "Other").otherwise("Unknown")
                    )
                )
            )
        )
        gender_updates = video_df.join(original_gender, ["UserID", "VideoID"]).filter(
            col("Gender") != col("OldGender")
        )
        for row in gender_updates.limit(10).collect():
            logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                        f"Field=Gender, OldValue={row['OldGender']}, NewValue={row['Gender']}, "
                        f"Reason='Standardized Gender'")

        # Step 4: Standardize Platform
        original_platform = video_df.select("UserID", "VideoID", col("Platform").alias("OldPlatform"))
        valid_platforms = ["Instagram", "YouTube", "Facebook", "TikTok", "Unknown"]
        video_df = video_df.withColumn(
            "Platform",
            when(col("Platform").isNull(), "Unknown").otherwise(
                when(col("Platform").isin(valid_platforms), col("Platform")).otherwise("Unknown")
            )
        )
        platform_updates = video_df.join(original_platform, ["UserID", "VideoID"]).filter(
            col("Platform") != col("OldPlatform")
        )
        for row in platform_updates.limit(10).collect():
            logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                        f"Field=Platform, OldValue={row['OldPlatform']}, NewValue={row['Platform']}, "
                        f"Reason='Standardized Platform'")

        # Step 5: Standardize DeviceType
        original_device = video_df.select("UserID", "VideoID", col("DeviceType").alias("OldDeviceType"))
        valid_devices = ["Smartphone", "Tablet", "Desktop", "Unknown"]
        video_df = video_df.withColumn(
            "DeviceType",
            when(col("DeviceType").isNull(), "Unknown").otherwise(
                when(col("DeviceType").isin("Computer", "PC", "Laptop"), "Desktop").otherwise(
                    when(col("DeviceType").isin(valid_devices), col("DeviceType")).otherwise("Unknown")
                )
            )
        )
        device_updates = video_df.join(original_device, ["UserID", "VideoID"]).filter(
            col("DeviceType") != col("OldDeviceType")
        )
        for row in device_updates.limit(10).collect():
            logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                        f"Field=DeviceType, OldValue={row['OldDeviceType']}, NewValue={row['DeviceType']}, "
                        f"Reason='Standardized DeviceType'")

        # Step 6: Standardize OS
        original_os = video_df.select("UserID", "VideoID", col("OS").alias("OldOS"))
        valid_os = ["Android", "iOS", "Windows", "MacOS", "Unknown"]
        video_df = video_df.withColumn(
            "OS",
            when(col("OS").isNull(), "Unknown").otherwise(
                when(col("OS").isin(valid_os), col("OS")).otherwise("Unknown")
            )
        )
        os_updates = video_df.join(original_os, ["UserID", "VideoID"]).filter(
            col("OS") != col("OldOS")
        )
        for row in os_updates.limit(10).collect():
            logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                        f"Field=OS, OldValue={row['OldOS']}, NewValue={row['OS']}, "
                        f"Reason='Standardized OS'")

        # Step 7: Standardize ConnectionType
        original_connection = video_df.select("UserID", "VideoID", col("ConnectionType").alias("OldConnectionType"))
        valid_connections = ["Wi-Fi", "Cellular", "Unknown"]
        video_df = video_df.withColumn(
            "ConnectionType",
            when(col("ConnectionType").isNull(), "Unknown").otherwise(
                when(col("ConnectionType").isin("Mobile Data", "4G", "5G"), "Cellular").otherwise(
                    when(col("ConnectionType").isin(valid_connections), col("ConnectionType")).otherwise("Unknown")
                )
            )
        )
        connection_updates = video_df.join(original_connection, ["UserID", "VideoID"]).filter(
            col("ConnectionType") != col("OldConnectionType")
        )
        for row in connection_updates.limit(10).collect():
            logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                        f"Field=ConnectionType, OldValue={row['OldConnectionType']}, NewValue={row['ConnectionType']}, "
                        f"Reason='Standardized ConnectionType'")

        # Step 8: Remove duplicates
        window_spec = Window.partitionBy("UserID", "VideoID").orderBy(col("IngestionTimestamp").desc())
        video_df = video_df.withColumn("row_num", row_number().over(window_spec))
        duplicate_rows = video_df.filter(col("row_num") > 1)
        if duplicate_rows.count() > 0:
            for row in duplicate_rows.limit(10).collect():
                logger.info(f"Rejected row (Duplicate): UserID={row['UserID']}, VideoID={row['VideoID']}, "
                            f"Reason='Duplicate row (not most recent)', Row={row}")
        video_df = video_df.filter(col("row_num") == 1).drop("row_num")
        logger.info(f"Row count after deduplication: {video_df.count()}")

        # Step 9: Standardize formats
        string_columns = ["Gender", "Location", "Profession", "Demographics", "Platform", "VideoCategory",
                          "Frequency", "WatchReason", "DeviceType", "OS", "WatchTime", "CurrentActivity", "ConnectionType"]
        for column in string_columns:
            original_values = video_df.select("UserID", "VideoID", col(column).alias(f"Old{column}"))
            video_df = video_df.withColumn(column, trim(col(column)))
            if column in ["Location", "Profession", "Platform", "VideoCategory", "DeviceType", "OS", "CurrentActivity"]:
                video_df = video_df.withColumn(column, initcap(col(column)))
            updates = video_df.join(original_values, ["UserID", "VideoID"]).filter(
                col(column) != col(f"Old{column}")
            )
            for row in updates.limit(10).collect():
                logger.info(f"Updated row: UserID={row['UserID']}, VideoID={row['VideoID']}, "
                            f"Field={column}, OldValue={row[f'Old{column}']}, NewValue={row[column]}, "
                            f"Reason='Trimmed and formatted {column}'")

        # Step 10: Add derived column (AgeGroup)
        video_df = video_df.withColumn(
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

        # Step 11: Validate nulls after cleaning
        null_count, _ = validate_nulls(video_df, critical_columns, "video_interactions_cleaned")
        if null_count > 0:
            logger.warning("Null values found in critical columns after cleaning")

        # Step 12: Write cleaned data to staging
        cleaned_count = video_df.count()
        if cleaned_count > 0:
            video_df.coalesce(1).write \
                .option("compression", "snappy") \
                .mode("overwrite") \
                .parquet(staging_s3_path)
            logger.info(f"Successfully wrote {cleaned_count} rows to {staging_s3_path}")
            logger.info(f"Written schema: {video_df.schema}")
        else:
            logger.info("No data to write to staging for video_interactions")

    else:
        logger.error(f"No data found in {raw_s3_path}")

    spark.stop()
except Exception as e:
    logger.error(f"Unexpected error in video_interactions_clean.py: {str(e)}")
    raise