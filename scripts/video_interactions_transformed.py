import logging
import configparser
import boto3
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, sum as sum_, initcap,
    to_timestamp, date_format, coalesce, lower, row_number, hour, md5, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType, BooleanType, FloatType, IntegerType
)
from pyspark.sql.window import Window
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/video_interactions_transformed.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)
logger = logging.getLogger(__name__)

try:
    # Load AWS and Snowflake configuration
    config = configparser.ConfigParser()
    config.read('/app/config/aws_config.ini')
    aws_access_key = config['aws']['access_key_id']
    aws_secret_key = config['aws']['secret_access_key']
    aws_region = config['aws']['region']
    config.read('/app/config/snowflake_config.ini')
    snowflake_account = config['snowflake']['account']
    snowflake_user = config['snowflake']['user']
    snowflake_password = config['snowflake']['password']
    snowflake_database = config['snowflake']['database']
    snowflake_schema = config['snowflake']['schema']
    snowflake_warehouse = config['snowflake']['warehouse']
    snowflake_role = config['snowflake']['role']

    # Initialize Spark session
    logger.info("Initializing Spark session")
    spark = SparkSession.builder \
        .appName("VideoInteractionsTransformed") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.sql.parquet.outputLegacyFormat", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.16.1,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.4") \
        .getOrCreate()

    # Snowflake connection options
    snowflake_options = {
        "sfURL": f"{snowflake_account}.snowflakecomputing.com",
        "sfUser": snowflake_user,
        "sfPassword": snowflake_password,
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": snowflake_warehouse,
        "sfRole": snowflake_role
    }
    logger.info(f"Snowflake connection options: {snowflake_options}")

    # Helper function to check if S3 path is accessible
    def check_s3_path(spark, s3_path, bucket="datastreaming-analytics-1"):
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI.create(f"s3a://{bucket}"),
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
            logger.warning(f"Hadoop check failed for S3 path {s3_path}: {str(e)}\n{traceback.format_exc()}")
            try:
                s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
                prefix = s3_path.split(f"s3a://{bucket}/")[1]
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                files = response.get('Contents', []) if 'Contents' in response else []
                file_list = [(obj['Key'], obj['Size']) for obj in files]
                file_count = len(file_list)
                logger.info(f"AWS SDK check for {s3_path}: file count: {file_count}, files: {file_list}")
                return file_count > 0
            except Exception as sdk_e:
                logger.error(f"AWS SDK check failed for {s3_path}: {str(sdk_e)}\n{traceback.format_exc()}")
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
                    logger.info(f"Rejected row (Nulls): VideoID={row['VideoID']}, UserID={row['UserID']}, "
                                f"Reason='Null in critical columns: {', '.join(critical_columns)}', Row={row}")
            return invalid_count, invalid_rows
        except Exception as e:
            logger.error(f"Error validating nulls for {file_name}: {str(e)}\n{traceback.format_exc()}")
            return -1, None

    # Define expected schema for video interactions
    video_interactions_schema = StructType([
        StructField("VideoID", LongType(), True),
        StructField("UserID", LongType(), True),
        StructField("Platform", StringType(), True),
        StructField("WatchTime", StringType(), True),
        StructField("IngestionTimestamp", TimestampType(), True),
        StructField("Age", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Income", LongType(), True),
        StructField("Debt", BooleanType(), True),
        StructField("OwnsProperty", BooleanType(), True),
        StructField("Profession", StringType(), True),
        StructField("Demographics", StringType(), True),
        StructField("CurrentActivity", StringType(), True),
        StructField("AgeGroup", StringType(), True),
        StructField("VideoCategory", StringType(), True),
        StructField("VideoLength", LongType(), True),
        StructField("TimeSpentOnVideo", LongType(), True),
        StructField("TotalTimeSpent", LongType(), True),
        StructField("Engagement", LongType(), True),
        StructField("NumberOfSessions", LongType(), True),
        StructField("NumberOfVideosWatched", LongType(), True),
        StructField("ScrollRate", LongType(), True),
        StructField("ImportanceScore", LongType(), True),
        StructField("ProductivityLoss", LongType(), True),
        StructField("Satisfaction", LongType(), True),
        StructField("SelfControl", LongType(), True),
        StructField("AddictionLevel", LongType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("OS", StringType(), True),
        StructField("ConnectionType", StringType(), True),
        StructField("WatchReason", StringType(), True)
    ])

    # S3 paths
    staging_path = "s3a://datastreaming-analytics-1/staging/video_interactions"

    # Read staging data with schema
    logger.info(f"Reading staging data from {staging_path}")
    if check_s3_path(spark, staging_path):
        try:
            temp_df = spark.read.parquet(staging_path)
            logger.info(f"Actual Parquet schema: {temp_df.schema}")
            video_df = spark.read.schema(video_interactions_schema).parquet(staging_path)
            # Normalize string columns for deduplication
            video_df = video_df.withColumn("Platform", trim(lower(col("Platform")))) \
                              .withColumn("DeviceType", trim(lower(col("DeviceType")))) \
                              .withColumn("OS", trim(lower(col("OS")))) \
                              .withColumn("ConnectionType", trim(lower(col("ConnectionType")))) \
                              .withColumn("WatchReason", trim(lower(col("WatchReason")))) \
                              .withColumn("WatchTime", trim(lower(col("WatchTime"))))
            # Deduplicate input data
            logger.info("Checking duplicates in video_df")
            duplicate_count = video_df.groupBy("UserID", "VideoID", "IngestionTimestamp").count().filter(col("count") > 1).count()
            logger.info(f"Found {duplicate_count} duplicate combinations in video_df")
            video_df.groupBy("UserID", "VideoID").count().orderBy(col("count").desc()).show(10, truncate=False)
            video_df = video_df.dropDuplicates(["UserID", "VideoID", "IngestionTimestamp"]).cache()
            row_count = video_df.count()
            logger.info(f"Read and deduplicated video_interactions: {row_count} rows")
            logger.info(f"Input schema: {video_df.schema}")
            video_df.show(5, truncate=False)
        except Exception as e:
            logger.error(f"Failed to read staging data: {str(e)}\n{traceback.format_exc()}")
            raise
    else:
        logger.error(f"No data found in {staging_path}")
        raise Exception(f"No data found in {staging_path}")

    # Test access to Snowflake tables
    try:
        logger.info("Testing access to Snowflake tables")
        test_tables = ["DIM_TIME", "DIM_USER", "DIM_VIDEO", "DIM_PLATFORM", "DIM_DEVICE_TYPE", "DIM_OS", "DIM_CONNECTION_TYPE", "DIM_WATCH_REASON"]
        for table in test_tables:
            test_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", f"PUBLIC.{table}").load()
            logger.info(f"Successfully accessed PUBLIC.{table}: {test_df.count()} rows")
            test_df.show(5, truncate=False)
    except Exception as e:
        logger.error(f"Failed to access Snowflake tables: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_time
    try:
        logger.info("Creating dim_time")
        new_time_df = video_df.select(
            coalesce(col("WatchTime"), lit("00:00:00")).alias("WatchTime")
        ).distinct()
        logger.info(f"New WatchTime values: {new_time_df.count()} rows")
        new_time_df.show(5, truncate=False)
        # Check for existing times in Snowflake
        existing_time_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_TIME").load().select("WatchTime").cache()
        missing_time_df = new_time_df.join(existing_time_df, "WatchTime", "left_anti")
        missing_time_df = missing_time_df.withColumn(
            "TimeID",
            md5(col("WatchTime").cast("string"))
        ).withColumn(
            "Hour",
            hour(to_timestamp(col("WatchTime"), "HH:mm:ss"))
        ).select(
            col("TimeID").cast("string"),
            col("WatchTime").cast("string"),
            col("Hour").cast("long")
        )

        missing_time_count = missing_time_df.count()
        logger.info(f"Found {missing_time_count} new times to add to dim_time")
        logger.info(f"missing_time_df schema: {missing_time_df.schema}")
        logger.info("Sample missing_time_df rows:")
        missing_time_df.show(5, truncate=False)
        if missing_time_count > 0:
            try:
                missing_time_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_TIME") \
                    .save()
                logger.info(f"Successfully appended to dim_time: {missing_time_df.count()} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_time: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new times to append to dim_time")
        existing_time_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_time: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_user
    try:
        logger.info("Creating dim_user")
        new_user_df = video_df.select(
            col("UserID").cast(StringType()).alias("UserID"),
            col("Age").cast("long"),
            col("Gender").cast("string"),
            col("Location").cast("string"),
            col("Income").cast("long"),
            col("Debt").cast("boolean"),
            col("OwnsProperty").cast("boolean"),
            col("Profession").cast("string"),
            col("Demographics").cast("string"),
            col("CurrentActivity").cast("string"),
            col("AgeGroup").cast("string")
        ).distinct()
        # Check for existing users in Snowflake
        existing_user_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_USER").load().select("UserID").cache()
        missing_user_df = new_user_df.join(existing_user_df, "UserID", "left_anti")
        missing_user_df = missing_user_df.withColumn(
            "User_S_ID",
            md5(col("UserID").cast("string"))
        ).select(
            col("User_S_ID").cast("string"),
            col("UserID").cast("string"),
            col("Age").cast("long"),
            col("Gender").cast("string"),
            col("Location").cast("string"),
            col("Income").cast("long"),
            col("Debt").cast("boolean"),
            col("OwnsProperty").cast("boolean"),
            col("Profession").cast("string"),
            col("Demographics").cast("string"),
            col("CurrentActivity").cast("string"),
            col("AgeGroup").cast("string")
        )

        missing_user_count = missing_user_df.count()
        logger.info(f"Found {missing_user_count} new users to add to dim_user")
        logger.info(f"missing_user_df schema: {missing_user_df.schema}")
        logger.info("Sample missing_user_df rows:")
        missing_user_df.show(5, truncate=False)
        if missing_user_count > 0:
            try:
                missing_user_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_USER") \
                    .save()
                logger.info(f"Successfully appended to dim_user: {missing_user_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_user: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new users to append to dim_user")
        existing_user_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_user: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_video
    try:
        logger.info("Creating dim_video")
        video_dim_df = video_df.select(
            col("VideoID").cast("long"),
            col("VideoCategory").cast("string"),
            col("VideoLength").cast("long"),
            when(col("TotalTimeSpent") < 300, "Short")
            .when(col("TotalTimeSpent").between(300, 1800), "Medium")
            .otherwise("Long").alias("TimeSpentCategory")
        ).distinct()
        video_dim_df = video_dim_df.withColumn(
            "Video_S_ID",
            md5(col("VideoID").cast("string"))
        ).select(
            col("Video_S_ID").cast("string"),
            col("VideoID").cast("long"),
            col("VideoCategory").cast("string"),
            col("VideoLength").cast("long"),
            col("TimeSpentCategory").cast("string")
        )

        video_dim_count = video_dim_df.count()
        logger.info(f"Prepared dim_video: {video_dim_count} rows")
        logger.info(f"video_dim_df schema: {video_dim_df.schema}")
        logger.info("Sample video_dim_df rows:")
        video_dim_df.show(5, truncate=False)
        try:
            video_dim_df.write \
                .format("snowflake") \
                .mode("overwrite") \
                .options(**snowflake_options) \
                .option("dbtable", "PUBLIC.DIM_VIDEO") \
                .save()
            logger.info(f"Successfully wrote dim_video: {video_dim_count} rows")
        except Exception as write_e:
            logger.error(f"Failed to write to dim_video: {str(write_e)}\n{traceback.format_exc()}")
            raise
    except Exception as e:
        logger.error(f"Error creating dim_video: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_platform
    try:
        logger.info("Creating dim_platform")
        new_platform_df = video_df.select(col("Platform").cast("string")).distinct()
        # Check for existing platforms in Snowflake
        existing_platform_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_PLATFORM").load().select("Platform").cache()
        missing_platform_df = new_platform_df.join(existing_platform_df, "Platform", "left_anti")
        missing_platform_df = missing_platform_df.withColumn(
            "PlatformID",
            md5(col("Platform").cast("string"))
        ).select(
            col("PlatformID").cast("string"),
            col("Platform").cast("string")
        )

        missing_platform_count = missing_platform_df.count()
        logger.info(f"Found {missing_platform_count} new platforms to add to dim_platform")
        logger.info(f"missing_platform_df schema: {missing_platform_df.schema}")
        logger.info("Sample missing_platform_df rows:")
        missing_platform_df.show(5, truncate=False)
        if missing_platform_count > 0:
            try:
                missing_platform_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_PLATFORM") \
                    .save()
                logger.info(f"Successfully appended to dim_platform: {missing_platform_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_platform: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new platforms to append to dim_platform")
        existing_platform_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_platform: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_device_type
    try:
        logger.info("Creating dim_device_type")
        new_device_type_df = video_df.select(col("DeviceType").cast("string")).distinct()
        # Check for existing device types in Snowflake
        existing_device_type_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_DEVICE_TYPE").load().select("DeviceType").cache()
        missing_device_type_df = new_device_type_df.join(existing_device_type_df, "DeviceType", "left_anti")
        missing_device_type_df = missing_device_type_df.withColumn(
            "DeviceTypeID",
            md5(col("DeviceType").cast("string"))
        ).select(
            col("DeviceTypeID").cast("string"),
            col("DeviceType").cast("string")
        )

        missing_device_type_count = missing_device_type_df.count()
        logger.info(f"Found {missing_device_type_count} new device types to add to dim_device_type")
        logger.info(f"missing_device_type_df schema: {missing_device_type_df.schema}")
        logger.info("Sample missing_device_type_df rows:")
        missing_device_type_df.show(5, truncate=False)
        if missing_device_type_count > 0:
            try:
                missing_device_type_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_DEVICE_TYPE") \
                    .save()
                logger.info(f"Successfully appended to dim_device_type: {missing_device_type_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_device_type: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new device types to append to dim_device_type")
        existing_device_type_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_device_type: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_os
    try:
        logger.info("Creating dim_os")
        new_os_df = video_df.select(col("OS").cast("string")).distinct()
        # Check for existing OS in Snowflake
        existing_os_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_OS").load().select("OS").cache()
        missing_os_df = new_os_df.join(existing_os_df, "OS", "left_anti")
        missing_os_df = missing_os_df.withColumn(
            "OS_ID",
            md5(col("OS").cast("string"))
        ).select(
            col("OS_ID").cast("string"),
            col("OS").cast("string")
        )

        missing_os_count = missing_os_df.count()
        logger.info(f"Found {missing_os_count} new OS to add to dim_os")
        logger.info(f"missing_os_df schema: {missing_os_df.schema}")
        logger.info("Sample missing_os_df rows:")
        missing_os_df.show(5, truncate=False)
        if missing_os_count > 0:
            try:
                missing_os_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_OS") \
                    .save()
                logger.info(f"Successfully appended to dim_os: {missing_os_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_os: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new OS to append to dim_os")
        existing_os_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_os: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_connection_type
    try:
        logger.info("Creating dim_connection_type")
        new_connection_type_df = video_df.select(col("ConnectionType").cast("string")).distinct()
        # Check for existing connection types in Snowflake
        existing_connection_type_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_CONNECTION_TYPE").load().select("ConnectionType").cache()
        missing_connection_type_df = new_connection_type_df.join(existing_connection_type_df, "ConnectionType", "left_anti")
        missing_connection_type_df = missing_connection_type_df.withColumn(
            "ConnectionTypeID",
            md5(col("ConnectionType").cast("string"))
        ).select(
            col("ConnectionTypeID").cast("string"),
            col("ConnectionType").cast("string")
        )

        missing_connection_type_count = missing_connection_type_df.count()
        logger.info(f"Found {missing_connection_type_count} new connection types to add to dim_connection_type")
        logger.info(f"missing_connection_type_df schema: {missing_connection_type_df.schema}")
        logger.info("Sample missing_connection_type_df rows:")
        missing_connection_type_df.show(5, truncate=False)
        if missing_connection_type_count > 0:
            try:
                missing_connection_type_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_CONNECTION_TYPE") \
                    .save()
                logger.info(f"Successfully appended to dim_connection_type: {missing_connection_type_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_connection_type: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new connection types to append to dim_connection_type")
        existing_connection_type_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_connection_type: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_watch_reason
    try:
        logger.info("Creating dim_watch_reason")
        new_watch_reason_df = video_df.select(col("WatchReason").cast("string")).distinct()
        # Check for existing watch reasons in Snowflake
        existing_watch_reason_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_WATCH_REASON").load().select("WatchReason").cache()
        missing_watch_reason_df = new_watch_reason_df.join(existing_watch_reason_df, "WatchReason", "left_anti")
        missing_watch_reason_df = missing_watch_reason_df.withColumn(
            "WatchReasonID",
            md5(col("WatchReason").cast("string"))
        ).select(
            col("WatchReasonID").cast("string"),
            col("WatchReason").cast("string")
        )

        watch_reason_count = missing_watch_reason_df.count()
        logger.info(f"Found {watch_reason_count} new watch reasons to add to dim_watch_reason")
        logger.info(f"missing_watch_reason_df schema: {missing_watch_reason_df.schema}")
        logger.info("Sample missing_watch_reason_df rows:")
        missing_watch_reason_df.show(5, truncate=False)
        if watch_reason_count > 0:
            try:
                missing_watch_reason_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_WATCH_REASON") \
                    .save()
                logger.info(f"Successfully appended to dim_watch_reason: {watch_reason_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_watch_reason: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new watch reasons to append to dim_watch_reason")
        existing_watch_reason_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_watch_reason: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create fact_video_interactions
    try:
        logger.info("Creating fact_video_interactions")
        fact_df = video_df.withColumn(
            "InteractionID",
            md5(concat_ws("_", col("UserID").cast("string"), col("VideoID").cast("string"), col("IngestionTimestamp").cast("string")))
        ).cache()

        logger.info("Checking duplicates in fact_df before joins")
        duplicate_count = fact_df.groupBy("InteractionID").count().filter(col("count") > 1).count()
        logger.info(f"Found {duplicate_count} duplicate InteractionIDs in fact_df")
        fact_df.groupBy("UserID", "VideoID").count().orderBy(col("count").desc()).show(10, truncate=False)

        # Join with dim_user
        dim_user_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_USER").load().select("User_S_ID", "UserID").cache()
        logger.info(f"dim_user_df schema: {dim_user_df.schema}")
        dim_user_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_user_df, fact_df["UserID"].cast("string") == dim_user_df["UserID"], "inner").withColumnRenamed("User_S_ID", "UserID_Surrogate")
        logger.info(f"After joining with dim_user: {fact_df.count()} rows")
        dim_user_df.unpersist()

        # Join with dim_video
        dim_video_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_VIDEO").load().select("Video_S_ID", "VideoID").cache()
        logger.info(f"dim_video_df schema: {dim_video_df.schema}")
        dim_video_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_video_df, fact_df["VideoID"] == dim_video_df["VideoID"], "inner").withColumnRenamed("Video_S_ID", "VideoID_Surrogate")
        logger.info(f"After joining with dim_video: {fact_df.count()} rows")
        dim_video_df.unpersist()

        # Join with dim_platform
        dim_platform_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_PLATFORM").load().select("PlatformID", "Platform").cache()
        logger.info(f"dim_platform_df schema: {dim_platform_df.schema}")
        dim_platform_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_platform_df, fact_df["Platform"] == dim_platform_df["Platform"], "inner")
        logger.info(f"After joining with dim_platform: {fact_df.count()} rows")
        dim_platform_df.unpersist()

        # Join with dim_device_type
        dim_device_type_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_DEVICE_TYPE").load().select("DeviceTypeID", "DeviceType").cache()
        logger.info(f"dim_device_type_df schema: {dim_device_type_df.schema}")
        dim_device_type_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_device_type_df, fact_df["DeviceType"] == dim_device_type_df["DeviceType"], "inner")
        logger.info(f"After joining with dim_device_type: {fact_df.count()} rows")
        dim_device_type_df.unpersist()

        # Join with dim_os
        dim_os_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_OS").load().select("OS_ID", "OS").cache()
        logger.info(f"dim_os_df schema: {dim_os_df.schema}")
        dim_os_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_os_df, fact_df["OS"] == dim_os_df["OS"], "inner")
        logger.info(f"After joining with dim_os: {fact_df.count()} rows")
        dim_os_df.unpersist()

        # Join with dim_connection_type
        dim_connection_type_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_CONNECTION_TYPE").load().select("ConnectionTypeID", "ConnectionType").cache()
        logger.info(f"dim_connection_type_df schema: {dim_connection_type_df.schema}")
        dim_connection_type_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_connection_type_df, fact_df["ConnectionType"] == dim_connection_type_df["ConnectionType"], "inner")
        logger.info(f"After joining with dim_connection_type: {fact_df.count()} rows")
        dim_connection_type_df.unpersist()

        # Join with dim_time
        fact_df = fact_df.withColumn(
            "WatchTime",
            coalesce(col("WatchTime"), lit("00:00:00"))
        )
        dim_time_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_TIME").load().select("TimeID", "WatchTime").cache()
        logger.info(f"dim_time_df schema: {dim_time_df.schema}")
        dim_time_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_time_df, fact_df["WatchTime"] == dim_time_df["WatchTime"], "inner")
        logger.info(f"After joining with dim_time: {fact_df.count()} rows")
        dim_time_df.unpersist()

        # Join with dim_watch_reason
        dim_watch_reason_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_WATCH_REASON").load().select("WatchReasonID", "WatchReason").cache()
        logger.info(f"dim_watch_reason_df schema: {dim_watch_reason_df.schema}")
        dim_watch_reason_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_watch_reason_df, fact_df["WatchReason"] == dim_watch_reason_df["WatchReason"], "inner")
        logger.info(f"After joining with dim_watch_reason: {fact_df.count()} rows")
        dim_watch_reason_df.unpersist()

        # Deduplicate fact_df
        logger.info("Deduplicating fact_df")
        fact_df = fact_df.dropDuplicates(["InteractionID"])
        logger.info(f"After deduplication: {fact_df.count()} rows")

        # Select final columns with explicit casts
        fact_df = fact_df.select(
            col("InteractionID").cast("string"),
            col("UserID_Surrogate").cast("string"),
            col("VideoID_Surrogate").cast("string"),
            col("PlatformID").cast("string"),
            col("DeviceTypeID").cast("string"),
            col("OS_ID").cast("string"),
            col("ConnectionTypeID").cast("string"),
            col("TimeID").cast("string"),
            col("WatchReasonID").cast("string"),
            col("TimeSpentOnVideo").cast("long"),
            col("TotalTimeSpent").cast("long"),
            col("Engagement").cast("float"),
            col("NumberOfSessions").cast("long"),
            col("NumberOfVideosWatched").cast("long"),
            col("ScrollRate").cast("float"),
            col("ImportanceScore").cast("float"),
            col("ProductivityLoss").cast("float"),
            col("Satisfaction").cast("float"),
            col("SelfControl").cast("float"),
            col("AddictionLevel").cast("float"),
            col("IngestionTimestamp").cast("timestamp")
        )

        # Validate nulls
        critical_columns = ["InteractionID", "UserID_Surrogate", "VideoID_Surrogate", "TimeSpentOnVideo", "TotalTimeSpent"]
        null_count, _ = validate_nulls(fact_df, critical_columns, "fact_video_interactions")
        if null_count > 0:
            logger.warning("Null values found in critical columns for fact_video_interactions")

        # Write to Snowflake
        fact_row_count = fact_df.count()
        logger.info(f"Prepared fact_video_interactions: {fact_row_count} rows")
        logger.info(f"fact_df schema: {fact_df.schema}")
        fact_df.show(5, truncate=False)
        if fact_row_count > 0:
            try:
                logger.info(f"Writing to PUBLIC.FACT_VIDEO_INTERACTIONS with options: {snowflake_options}")
                fact_df.write \
                    .format("snowflake") \
                    .mode("overwrite") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.FACT_VIDEO_INTERACTIONS") \
                    .save()
                logger.info(f"Successfully wrote fact_video_interactions: {fact_df.count()} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to fact_video_interactions: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No data to write for fact_video_interactions")

        # Unpersist cached DataFrames
        video_df.unpersist()
        fact_df.unpersist()

    except Exception as e:
        logger.error(f"Error creating fact_video_interactions: {str(e)}\n{traceback.format_exc()}")
        raise

    spark.stop()
    logger.info("Spark session stopped")

except Exception as e:
    logger.error(f"Unexpected error in video_interactions_transformed.py: {str(e)}\n{traceback.format_exc()}")
    raise