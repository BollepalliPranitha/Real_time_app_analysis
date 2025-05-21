import logging
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, date_trunc, md5, concat_ws, sum as sum_,
    count, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType, DateType
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/user_activity_transformed.log',
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
        .appName("UserActivityTransformed") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "2") \
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

    # Define explicit schema for DataFrames
    unified_schema = StructType([
        StructField("UserID_Surrogate", StringType(), True),
        StructField("PlatformID", StringType(), True),
        StructField("TimeID", StringType(), True),
        StructField("Engagement", FloatType(), True),
        StructField("TotalTimeSpent", FloatType(), True),
        StructField("NumberOfSessions", LongType(), True),
        StructField("IngestionTimestamp", TimestampType(), True)
    ])

    # Helper function to validate nulls
    def validate_nulls(df, critical_columns, file_name):
        null_counts_expr = [sum_(col(c).isNull().cast("int")).alias(c) for c in df.columns]
        null_counts = df.select(null_counts_expr).collect()[0].asDict()
        logger.info(f"Null counts for {file_name}: {null_counts}")
        invalid_rows = df.filter(reduce(lambda x, y: x | y, [col(c).isNull() for c in critical_columns]))
        invalid_count = invalid_rows.count()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} rows with nulls in critical columns for {file_name}")
        return invalid_count

    # Read fact tables from Snowflake
    logger.info("Reading fact tables from Snowflake")
    community_df = spark.read.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "PUBLIC.FACT_COMMUNITY_INTERACTIONS") \
        .load() \
        .select(
            coalesce(col("UserID_Surrogate"), lit("UNKNOWN")).cast("string").alias("UserID_Surrogate"),
            coalesce(col("PlatformID"), lit("UNKNOWN")).cast("string").alias("PlatformID"),
            lit(None).cast("string").alias("TimeID"),
            coalesce(col("CommunityEngagement"), lit(0)).cast("float").alias("Engagement"),
            coalesce(col("TotalTimeSpent"), lit(0)).cast("float").alias("TotalTimeSpent"),
            lit(0).cast("long").alias("NumberOfSessions"),
            coalesce(col("IngestionTimestamp"), current_timestamp()).cast("timestamp").alias("IngestionTimestamp")
        )

    live_df = spark.read.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "PUBLIC.FACT_LIVE_STREAMING") \
        .load() \
        .select(
            coalesce(col("UserID_Surrogate"), lit("UNKNOWN")).cast("string").alias("UserID_Surrogate"),
            coalesce(col("PlatformID"), lit("UNKNOWN")).cast("string").alias("PlatformID"),
            coalesce(col("TimeID"), lit(None)).cast("string").alias("TimeID"),
            coalesce(col("LiveEngagement"), lit(0)).cast("float").alias("Engagement"),
            lit(0.0).cast("float").alias("TotalTimeSpent"),
            lit(0).cast("long").alias("NumberOfSessions"),
            coalesce(col("IngestionTimestamp"), current_timestamp()).cast("timestamp").alias("IngestionTimestamp")
        )

    video_df = spark.read.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "PUBLIC.FACT_VIDEO_INTERACTIONS") \
        .load() \
        .select(
            coalesce(col("UserID_Surrogate"), lit("UNKNOWN")).cast("string").alias("UserID_Surrogate"),
            coalesce(col("PlatformID"), lit("UNKNOWN")).cast("string").alias("PlatformID"),
            coalesce(col("TimeID"), lit(None)).cast("string").alias("TimeID"),
            coalesce(col("Engagement"), lit(0)).cast("float").alias("Engagement"),
            coalesce(col("TotalTimeSpent"), lit(0)).cast("float").alias("TotalTimeSpent"),
            coalesce(col("NumberOfSessions"), lit(0)).cast("long").alias("NumberOfSessions"),
            coalesce(col("IngestionTimestamp"), current_timestamp()).cast("timestamp").alias("IngestionTimestamp")
        )

    # Debug schemas and sample data
    logger.info("Community DataFrame schema:")
    community_df.printSchema()
    logger.info("Community DataFrame sample:")
    community_df.show(5, truncate=False)
    logger.info("Live DataFrame schema:")
    live_df.printSchema()
    logger.info("Live DataFrame sample:")
    live_df.show(5, truncate=False)
    logger.info("Video DataFrame schema:")
    video_df.printSchema()
    logger.info("Video DataFrame sample:")
    video_df.show(5, truncate=False)

    # Union fact tables with explicit schema
    logger.info("Unioning fact tables")
    community_df = spark.createDataFrame(community_df.rdd, unified_schema)
    live_df = spark.createDataFrame(live_df.rdd, unified_schema)
    video_df = spark.createDataFrame(video_df.rdd, unified_schema)
    unified_df = community_df.unionByName(live_df).unionByName(video_df)

    # Debug unified schema
    logger.info("Unified DataFrame schema:")
    unified_df.printSchema()
    logger.info("Unified DataFrame sample:")
    unified_df.show(5, truncate=False)

    # Aggregate by UserID, Platform, ActivityDate
    logger.info("Aggregating data")
    aggregated_df = unified_df.withColumn(
        "ActivityDate",
        date_trunc("day", col("IngestionTimestamp"))
    ).groupBy(
        col("UserID_Surrogate"),
        col("PlatformID"),
        col("TimeID"),
        col("ActivityDate")
    ).agg(
        sum_("Engagement").alias("TotalEngagement"),
        sum_("TotalTimeSpent").alias("TotalTimeSpent"),
        sum_("NumberOfSessions").alias("SessionCount"),
        count("*").alias("InteractionCount")
    )

    # Generate InteractionID and add timestamps
    fact_df = aggregated_df.withColumn(
        "InteractionID",
        md5(concat_ws("_", col("UserID_Surrogate"), col("ActivityDate").cast("string"), current_timestamp().cast("string")))
    ).withColumn(
        "IngestionTimestamp",
        current_timestamp()
    ).withColumn(
        "LoadTimestamp",
        current_timestamp()
    )

    # Select final columns
    fact_df = fact_df.select(
        col("InteractionID").cast("string"),
        col("UserID_Surrogate").cast("string"),
        col("PlatformID").cast("string"),
        col("TimeID").cast("string"),
        col("ActivityDate").cast("date"),
        col("TotalEngagement").cast("float"),
        col("TotalTimeSpent").cast("float"),
        col("SessionCount").cast("long"),
        col("InteractionCount").cast("long"),
        col("IngestionTimestamp").cast("timestamp"),
        col("LoadTimestamp").cast("timestamp")
    )

    # Validate nulls
    critical_columns = ["InteractionID", "UserID_Surrogate", "ActivityDate"]
    null_count = validate_nulls(fact_df, critical_columns, "fact_user_activity")
    if null_count > 0:
        logger.warning("Null values found in critical columns")

    # Write to Snowflake
    fact_row_count = fact_df.count()
    logger.info(f"Prepared fact_user_activity: {fact_row_count} rows")
    fact_df.show(5, truncate=False)
    if fact_row_count > 0:
        try:
            fact_df.write \
                .format("snowflake") \
                .mode("append") \
                .options(**snowflake_options) \
                .option("dbtable", "PUBLIC.FACT_USER_ACTIVITY") \
                .save()
            logger.info(f"Successfully appended fact_user_activity: {fact_row_count} rows")
        except Exception as e:
            logger.error(f"Failed to write to fact_user_activity: {str(e)}")
            raise
    else:
        logger.info("No data to write for fact_user_activity")

    spark.stop()
    logger.info("Spark session stopped")

except Exception as e:
    logger.error(f"Unexpected error in user_activity_transformed.py: {str(e)}")
    raise