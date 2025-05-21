import logging
import configparser
import boto3
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, sum as sum_, initcap,
    md5, concat_ws, lower, row_number
)
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, TimestampType, FloatType
)
from pyspark.sql.window import Window
from functools import reduce

# Configure logging
logging.basicConfig(
    filename='/app/logs/community_interactions_transformed.log',
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
        .appName("CommunityInteractionsTransformed") \
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
                    logger.info(f"Rejected row (Nulls): CommunityID={row['CommunityID']}, UserID={row['UserID']}, "
                                f"Reason='Null in critical columns: {', '.join(critical_columns)}', Row={row}")
            return invalid_count, invalid_rows
        except Exception as e:
            logger.error(f"Error validating nulls for {file_name}: {str(e)}\n{traceback.format_exc()}")
            return -1, None

    # Define expected schema for community interactions
    community_interactions_schema = StructType([
        StructField("CommunityID", LongType(), True),
        StructField("CommunityName", StringType(), True),
        StructField("UserID", LongType(), True),
        StructField("Platform", StringType(), True),
        StructField("Age", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("CommunityEngagement", LongType(), True),
        StructField("MembershipStatus", StringType(), True),
        StructField("TotalTimeSpent", LongType(), True),
        StructField("IngestionTimestamp", TimestampType(), True),
        StructField("AgeGroup", StringType(), True)
    ])

    # S3 paths
    staging_path = "s3a://datastreaming-analytics-1/staging/community_interactions"

    # Read staging data with schema
    logger.info(f"Reading staging data from {staging_path}")
    if check_s3_path(spark, staging_path):
        try:
            temp_df = spark.read.parquet(staging_path)
            logger.info(f"Actual Parquet schema: {temp_df.schema}")
            community_df = spark.read.schema(community_interactions_schema).parquet(staging_path)
            # Normalize string columns for deduplication
            community_df = community_df.withColumn("Platform", trim(lower(col("Platform")))) \
                                      .withColumn("MembershipStatus", trim(lower(col("MembershipStatus")))) \
                                      .withColumn("Gender", trim(lower(col("Gender")))) \
                                      .withColumn("CommunityName", trim(lower(col("CommunityName"))))
            # Deduplicate input data
            logger.info("Checking duplicates in community_df")
            duplicate_count = community_df.groupBy("CommunityID", "UserID", "IngestionTimestamp").count().filter(col("count") > 1).count()
            logger.info(f"Found {duplicate_count} duplicate combinations in community_df")
            community_df.groupBy("CommunityID", "UserID").count().orderBy(col("count").desc()).show(10, truncate=False)
            community_df = community_df.dropDuplicates(["CommunityID", "UserID", "IngestionTimestamp"]).cache()
            row_count = community_df.count()
            logger.info(f"Read and deduplicated community_interactions: {row_count} rows")
            logger.info(f"Input schema: {community_df.schema}")
            community_df.show(5, truncate=False)
        except Exception as e:
            logger.error(f"Failed to read staging data: {str(e)}\n{traceback.format_exc()}")
            raise
    else:
        logger.error(f"No data found in {staging_path}")
        raise Exception(f"No data found in {staging_path}")

    # Test access to Snowflake tables
    try:
        logger.info("Testing access to Snowflake tables")
        test_tables = ["DIM_USER", "DIM_COMMUNITY", "DIM_PLATFORM", "DIM_MEMBERSHIP_STATUS"]
        for table in test_tables:
            test_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", f"PUBLIC.{table}").load()
            logger.info(f"Successfully accessed PUBLIC.{table}: {test_df.count()} rows")
            test_df.show(5, truncate=False)
    except Exception as e:
        logger.error(f"Failed to access Snowflake tables: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_user
    try:
        logger.info("Creating dim_user")
        new_user_df = community_df.select(
            col("UserID").cast(StringType()).alias("UserID"),
            col("Age").cast("long"),
            col("Gender").cast("string"),
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
            col("AgeGroup").cast("string"),
            lit(None).cast("string").alias("Location"),
            lit(None).cast("long").alias("Income"),
            lit(None).cast("boolean").alias("Debt"),
            lit(None).cast("boolean").alias("OwnsProperty"),
            lit(None).cast("string").alias("Profession"),
            lit(None).cast("string").alias("Demographics"),
            lit(None).cast("string").alias("CurrentActivity")
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

    # Create dim_community
    try:
        logger.info("Creating dim_community")
        community_dim_df = community_df.select(
            col("CommunityID").cast("long"),
            col("CommunityName").cast("string")
        ).distinct()
        community_dim_df = community_dim_df.withColumn(
            "Community_S_ID",
            md5(col("CommunityID").cast("string"))
        ).select(
            col("Community_S_ID").cast("string"),
            col("CommunityID").cast("long"),
            col("CommunityName").cast("string")
        )

        community_dim_count = community_dim_df.count()
        logger.info(f"Prepared dim_community: {community_dim_count} rows")
        logger.info(f"community_dim_df schema: {community_dim_df.schema}")
        logger.info("Sample community_dim_df rows:")
        community_dim_df.show(5, truncate=False)
        try:
            community_dim_df.write \
                .format("snowflake") \
                .mode("overwrite") \
                .options(**snowflake_options) \
                .option("dbtable", "PUBLIC.DIM_COMMUNITY") \
                .save()
            logger.info(f"Successfully wrote dim_community: {community_dim_count} rows")
        except Exception as write_e:
            logger.error(f"Failed to write to dim_community: {str(write_e)}\n{traceback.format_exc()}")
            raise
    except Exception as e:
        logger.error(f"Error creating dim_community: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create dim_platform
    try:
        logger.info("Creating dim_platform")
        new_platform_df = community_df.select(col("Platform").cast("string")).distinct()
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

    # Create dim_membership_status
    try:
        logger.info("Creating dim_membership_status")
        new_membership_status_df = community_df.select(col("MembershipStatus").cast("string")).distinct()
        # Check for existing membership statuses in Snowflake
        existing_membership_status_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_MEMBERSHIP_STATUS").load().select("MembershipStatus").cache()
        missing_membership_status_df = new_membership_status_df.join(existing_membership_status_df, "MembershipStatus", "left_anti")
        missing_membership_status_df = missing_membership_status_df.withColumn(
            "MembershipStatusID",
            md5(col("MembershipStatus").cast("string"))
        ).select(
            col("MembershipStatusID").cast("string"),
            col("MembershipStatus").cast("string")
        )

        missing_membership_status_count = missing_membership_status_df.count()
        logger.info(f"Found {missing_membership_status_count} new membership statuses to add to dim_membership_status")
        logger.info(f"missing_membership_status_df schema: {missing_membership_status_df.schema}")
        logger.info("Sample missing_membership_status_df rows:")
        missing_membership_status_df.show(5, truncate=False)
        if missing_membership_status_count > 0:
            try:
                missing_membership_status_df.write \
                    .format("snowflake") \
                    .mode("append") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.DIM_MEMBERSHIP_STATUS") \
                    .save()
                logger.info(f"Successfully appended to dim_membership_status: {missing_membership_status_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to dim_membership_status: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No new membership statuses to append to dim_membership_status")
        existing_membership_status_df.unpersist()
    except Exception as e:
        logger.error(f"Error creating dim_membership_status: {str(e)}\n{traceback.format_exc()}")
        raise

    # Create факт_community_interactions
    try:
        logger.info("Creating fact_community_interactions")
        fact_df = community_df.withColumn(
            "InteractionID",
            md5(concat_ws("_", col("CommunityID").cast("string"), col("UserID").cast("string"), col("IngestionTimestamp").cast("string")))
        ).cache()

        logger.info("Checking duplicates in fact_df before joins")
        duplicate_count = fact_df.groupBy("InteractionID").count().filter(col("count") > 1).count()
        logger.info(f"Found {duplicate_count} duplicate InteractionIDs in fact_df")
        fact_df.groupBy("CommunityID", "UserID").count().orderBy(col("count").desc()).show(10, truncate=False)

        # Join with dim_user
        dim_user_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_USER").load().select("User_S_ID", "UserID").cache()
        logger.info(f"dim_user_df schema: {dim_user_df.schema}")
        dim_user_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_user_df, fact_df["UserID"].cast("string") == dim_user_df["UserID"], "inner").withColumnRenamed("User_S_ID", "UserID_Surrogate")
        logger.info(f"After joining with dim_user: {fact_df.count()} rows")
        dim_user_df.unpersist()

        # Join with dim_community
        dim_community_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_COMMUNITY").load().select("Community_S_ID", "CommunityID").cache()
        logger.info(f"dim_community_df schema: {dim_community_df.schema}")
        dim_community_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_community_df, fact_df["CommunityID"] == dim_community_df["CommunityID"], "inner").withColumnRenamed("Community_S_ID", "CommunityID_Surrogate")
        logger.info(f"After joining with dim_community: {fact_df.count()} rows")
        dim_community_df.unpersist()

        # Join with dim_platform
        dim_platform_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_PLATFORM").load().select("PlatformID", "Platform").cache()
        logger.info(f"dim_platform_df schema: {dim_platform_df.schema}")
        dim_platform_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_platform_df, fact_df["Platform"] == dim_platform_df["Platform"], "inner")
        logger.info(f"After joining with dim_platform: {fact_df.count()} rows")
        dim_platform_df.unpersist()

        # Join with dim_membership_status
        dim_membership_status_df = spark.read.format("snowflake").options(**snowflake_options).option("dbtable", "PUBLIC.DIM_MEMBERSHIP_STATUS").load().select("MembershipStatusID", "MembershipStatus").cache()
        logger.info(f"dim_membership_status_df schema: {dim_membership_status_df.schema}")
        dim_membership_status_df.show(5, truncate=False)
        fact_df = fact_df.join(dim_membership_status_df, fact_df["MembershipStatus"] == dim_membership_status_df["MembershipStatus"], "inner")
        logger.info(f"After joining with dim_membership_status: {fact_df.count()} rows")
        dim_membership_status_df.unpersist()

        # Deduplicate fact_df
        logger.info("Deduplicating fact_df")
        fact_df = fact_df.dropDuplicates(["InteractionID"])
        logger.info(f"After deduplication: {fact_df.count()} rows")

        # Select final columns with explicit casts
        fact_df = fact_df.select(
            col("InteractionID").cast("string"),
            col("UserID_Surrogate").cast("string"),
            col("CommunityID_Surrogate").cast("string"),
            col("PlatformID").cast("string"),
            col("MembershipStatusID").cast("string"),
            col("CommunityEngagement").cast("float"),
            col("TotalTimeSpent").cast("float"),
            col("IngestionTimestamp").cast("timestamp")
        )

        # Validate nulls
        critical_columns = ["InteractionID", "UserID_Surrogate", "CommunityID_Surrogate", "TotalTimeSpent"]
        null_count, _ = validate_nulls(fact_df, critical_columns, "fact_community_interactions")
        if null_count > 0:
            logger.warning("Null values found in critical columns for fact_community_interactions")

        # Write to Snowflake
        fact_row_count = fact_df.count()
        logger.info(f"Prepared fact_community_interactions: {fact_row_count} rows")
        logger.info(f"fact_df schema: {fact_df.schema}")
        fact_df.show(5, truncate=False)
        if fact_row_count > 0:
            try:
                logger.info(f"Writing to PUBLIC.FACT_COMMUNITY_INTERACTIONS with options: {snowflake_options}")
                fact_df.write \
                    .format("snowflake") \
                    .mode("overwrite") \
                    .options(**snowflake_options) \
                    .option("dbtable", "PUBLIC.FACT_COMMUNITY_INTERACTIONS") \
                    .save()
                logger.info(f"Successfully wrote fact_community_interactions: {fact_row_count} rows")
            except Exception as write_e:
                logger.error(f"Failed to write to fact_community_interactions: {str(write_e)}\n{traceback.format_exc()}")
                raise
        else:
            logger.info("No data to write for fact_community_interactions")

        # Unpersist cached DataFrames
        community_df.unpersist()
        fact_df.unpersist()

    except Exception as e:
        logger.error(f"Error creating fact_community_interactions: {str(e)}\n{traceback.format_exc()}")
        raise

    spark.stop()
    logger.info("Spark session stopped")

except Exception as e:
    logger.error(f"Unexpected error in community_interactions_transformed.py: {str(e)}\n{traceback.format_exc()}")
    raise