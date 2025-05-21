import logging
import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, sum as sum_
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, IntegerType
from functools import reduce

# Configure logging
logging.basicConfig(filename='/app/logs/batch_ingest_to_raw.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load AWS configuration
config = configparser.ConfigParser()
config.read('/app/config/aws_config.ini')
aws_access_key = config['aws']['access_key_id']
aws_secret_key = config['aws']['secret_access_key']
aws_region = config['aws']['region']

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BatchIngestToRaw") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    # Community Interactions Schema
    community_interactions_schema = StructType([
        StructField("CommunityID", LongType(), True),
        StructField("CommunityName", StringType(), True),
        StructField("UserID", LongType(), True),
        StructField("Platform", StringType(), True),
        StructField("Age", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("CommunityEngagement", LongType(), True),
        StructField("MembershipStatus", StringType(), True),
        StructField("Total Time Spent", LongType(), True)
    ])

    # Live Streaming Schema (removed Timestamp)
    live_streaming_schema = StructType([
        StructField("EventID", LongType(), True),
        StructField("EventType", StringType(), True),
        StructField("UserID", LongType(), True),
        StructField("Platform", StringType(), True),
        StructField("LiveEngagement", LongType(), True),
        StructField("ViewerCount", LongType(), True),
        StructField("StreamDuration", LongType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("Watch Time", StringType(), True),
        StructField("Addiction Level", LongType(), True)
    ])

    # Video Interactions Schema for CSV (removed Timestamp)
    video_interactions_schema = StructType([
        StructField("UserID", IntegerType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Income", IntegerType(), True),
        StructField("Debt", BooleanType(), True),
        StructField("Owns Property", BooleanType(), True),
        StructField("Profession", StringType(), True),
        StructField("Demographics", StringType(), True),
        StructField("Platform", StringType(), True),
        StructField("Total Time Spent", IntegerType(), True),
        StructField("Number of Sessions", IntegerType(), True),
        StructField("Video ID", IntegerType(), True),
        StructField("Video Category", StringType(), True),
        StructField("Video Length", IntegerType(), True),
        StructField("Engagement", IntegerType(), True),
        StructField("Importance Score", IntegerType(), True),
        StructField("Time Spent On Video", IntegerType(), True),
        StructField("Number of Videos Watched", IntegerType(), True),
        StructField("Scroll Rate", IntegerType(), True),
        StructField("Frequency", StringType(), True),
        StructField("ProductivityLoss", IntegerType(), True),
        StructField("Satisfaction", IntegerType(), True),
        StructField("Watch Reason", StringType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("OS", StringType(), True),
        StructField("Watch Time", StringType(), True),
        StructField("Self Control", IntegerType(), True),
        StructField("Addiction Level", IntegerType(), True),
        StructField("CurrentActivity", StringType(), True),
        StructField("ConnectionType", StringType(), True)
    ])

    # Video Interactions Schema for NDJSON (removed Timestamp)
    video_interactions_ndjson_schema = StructType([
        StructField("UserID", LongType(), True),
        StructField("Age", LongType(), True),
        StructField("Gender", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Income", LongType(), True),
        StructField("Debt", BooleanType(), True),
        StructField("Owns Property", BooleanType(), True),
        StructField("Profession", StringType(), True),
        StructField("Demographics", StringType(), True),
        StructField("Platform", StringType(), True),
        StructField("Total Time Spent", LongType(), True),
        StructField("Number of Sessions", LongType(), True),
        StructField("Video ID", LongType(), True),
        StructField("Video Category", StringType(), True),
        StructField("Video Length", LongType(), True),
        StructField("Engagement", LongType(), True),
        StructField("Importance Score", LongType(), True),
        StructField("Time Spent On Video", LongType(), True),
        StructField("Number of Videos Watched", LongType(), True),
        StructField("Scroll Rate", LongType(), True),
        StructField("Frequency", StringType(), True),
        StructField("Productivity Loss", LongType(), True),
        StructField("Satisfaction", LongType(), True),
        StructField("Watch Reason", StringType(), True),
        StructField("DeviceType", StringType(), True),
        StructField("OS", StringType(), True),
        StructField("Watch Time", StringType(), True),
        StructField("Self Control", LongType(), True),
        StructField("Addiction Level", LongType(), True),
        StructField("CurrentActivity", StringType(), True),
        StructField("ConnectionType", StringType(), True)
    ])

    # Helper function to check if file exists and is non-empty
    def check_file(file_path):
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        if os.path.getsize(file_path) == 0:
            logger.error(f"File is empty: {file_path}")
            return False
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:5]
                logger.info(f"First 5 lines of {file_path}:\n{lines}")
        except UnicodeDecodeError:
            logger.warning(f"Unable to read {file_path} with UTF-8 encoding, skipping content preview")
        return True

    # Helper function to validate Parquet file
    def is_valid_parquet(file_path):
        try:
            spark.read.parquet(file_path).limit(1).collect()
            return True
        except Exception as e:
            logger.error(f"Invalid Parquet file {file_path}: {str(e)}")
            return False

    # Helper function to check if S3 path is accessible
    def check_s3_path(spark, s3_path):
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI.create(s3_path.split("://")[0] + "://"),
                hadoop_conf
            )
            path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
            exists = fs.exists(path)
            files = fs.listFiles(path, False) if exists else []
            file_count = sum(1 for _ in files) if exists else 0
            logger.info(f"S3 path {s3_path} exists: {exists}, file count: {file_count}")
            return exists and file_count > 0
        except Exception as e:
            logger.warning(f"Unable to check S3 path {s3_path}: {str(e)}")
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
                invalid_rows.show(truncate=False)
        except Exception as e:
            logger.error(f"Error validating nulls for {file_name}: {str(e)}")

    # List to store video_interactions DataFrames
    video_interactions_dfs = []

    # Ingest community_interactions_data.parquet
    community_file_path = 'data/community_interactions_data.parquet'
    try:
        if check_file(community_file_path):
            if is_valid_parquet(community_file_path):
                community_df = spark.read.schema(community_interactions_schema).parquet(community_file_path)
                logger.info(f"Schema applied for community_interactions_data.parquet: {community_df.schema}")
                #community_df.show(5, truncate=False)
                validate_nulls(community_df, ["CommunityID", "UserID", "Total Time Spent"], "community_interactions_data.parquet")
                community_df = community_df.withColumn("IngestionTimestamp", current_timestamp())
                s3_path = "s3a://datastreaming-analytics-1/raw/community_interactions"
                if check_s3_path(spark, s3_path):
                    logger.info(f"Existing data found in {s3_path}, performing deduplication")
                    existing_community_df = spark.read.schema(community_interactions_schema).parquet(s3_path)
                    logger.info(f"Rows before deduplication: {community_df.count()}")
                    new_community_df = community_df.join(existing_community_df, ["CommunityID", "UserID", "IngestionTimestamp"], "left_anti")
                    logger.info(f"Rows after deduplication: {new_community_df.count()}")
                else:
                    logger.info(f"No existing data in {s3_path}, writing all data as new")
                    new_community_df = community_df
                if new_community_df.count() > 0:
                    new_community_df.write.option("compression", "snappy").mode("append").parquet(s3_path)
                    logger.info("Successfully ingested community_interactions_data.parquet to S3 raw layer")
                    logger.info(f"Written schema to S3: {new_community_df.schema}")
                else:
                    logger.info("No new data to ingest from community_interactions_data.parquet")
            else:
                logger.error(f"Skipping ingestion of {community_file_path} due to invalid Parquet format")
        else:
            logger.error(f"Cannot ingest {community_file_path}: file check failed")
    except Exception as e:
        logger.error("Error ingesting community_interactions_data.parquet to S3 raw layer - %s", str(e))

    # Ingest live_streaming_data.ndjson
    live_streaming_file_path = 'data/live_streaming_data.ndjson'
    try:
        if check_file(live_streaming_file_path):
            live_df = spark.read.schema(live_streaming_schema).json(live_streaming_file_path)
            logger.info(f"Schema applied for live_streaming_data.ndjson: {live_df.schema}")
            #live_df.show(5, truncate=False)
            validate_nulls(live_df, ["EventID", "UserID", "Watch Time"], "live_streaming_data.ndjson")
            live_df = live_df.withColumn("IngestionTimestamp", current_timestamp())
            s3_path = "s3a://datastreaming-analytics-1/raw/live_streaming"
            if check_s3_path(spark, s3_path):
                logger.info(f"Existing data found in {s3_path}, performing deduplication")
                existing_live_df = spark.read.schema(live_streaming_schema).parquet(s3_path)
                logger.info(f"Rows before deduplication: {live_df.count()}")
                new_live_df = live_df.join(existing_live_df, ["EventID", "IngestionTimestamp"], "left_anti")
                logger.info(f"Rows after deduplication: {new_live_df.count()}")
            else:
                logger.info(f"No existing data in {s3_path}, writing all data as new")
                new_live_df = live_df
            if new_live_df.count() > 0:
                new_live_df.write.option("compression", "snappy").mode("append").parquet(s3_path)
                logger.info("Successfully ingested live_streaming_data.ndjson to S3 raw layer")
                logger.info(f"Written schema to S3: {new_live_df.schema}")
            else:
                logger.info("No new data to ingest from live_streaming_data.ndjson")
    except Exception as e:
        logger.error("Error ingesting live_streaming_data.ndjson to S3 raw layer - %s", str(e))

    # Ingest original_data.csv
    original_data_file_path = 'data/original_data.csv'
    try:
        if check_file(original_data_file_path):
            video_df = spark.read.option("header", "true").option("nullValue", "N/A").schema(video_interactions_schema).csv(original_data_file_path)
            logger.info(f"Schema applied for original_data.csv: {video_df.schema}")
            #video_df.show(5, truncate=False)
            validate_nulls(video_df, ["UserID", "Video ID", "Watch Time", "Total Time Spent"], "original_data.csv")
            video_df = video_df.withColumn("IngestionTimestamp", current_timestamp())
            s3_path = "s3a://datastreaming-analytics-1/raw/video_interactions"
            if check_s3_path(spark, s3_path):
                logger.info(f"Existing data found in {s3_path}, performing deduplication")
                existing_video_df = spark.read.schema(video_interactions_schema).parquet(s3_path)
                logger.info(f"Rows before deduplication: {video_df.count()}")
                new_video_df = video_df.join(existing_video_df, ["UserID", "Video ID", "IngestionTimestamp"], "left_anti")
                logger.info(f"Rows after deduplication: {new_video_df.count()}")
            else:
                logger.info(f"No existing data in {s3_path}, writing all data as new")
                new_video_df = video_df
            if new_video_df.count() > 0:
                video_interactions_dfs.append(new_video_df)
                logger.info("Collected original_data.csv for video_interactions")
            else:
                logger.info("No new data to ingest from original_data.csv")
    except Exception as e:
        logger.error("Error ingesting original_data.csv to S3 raw layer - %s", str(e))

    # Ingest synthetic_data.ndjson
    synthetic_ndjson_file_path = 'data/synthetic_data.ndjson'
    try:
        if check_file(synthetic_ndjson_file_path):
            synth_ndjson_df = spark.read.option("mode", "PERMISSIVE").schema(video_interactions_ndjson_schema).json(synthetic_ndjson_file_path)
            logger.info(f"Schema applied for synthetic_data.ndjson: {synth_ndjson_df.schema}")
            #synth_ndjson_df.show(5, truncate=False)
            validate_nulls(synth_ndjson_df, ["UserID", "Video ID", "Watch Time", "Total Time Spent"], "synthetic_data.ndjson")
            synth_ndjson_df = synth_ndjson_df.withColumn("IngestionTimestamp", current_timestamp())
            s3_path = "s3a://datastreaming-analytics-1/raw/video_interactions"
            if check_s3_path(spark, s3_path):
                logger.info(f"Existing data found in {s3_path}, performing deduplication")
                existing_synth_df = spark.read.schema(video_interactions_ndjson_schema).parquet(s3_path)
                logger.info(f"Rows before deduplication: {synth_ndjson_df.count()}")
                new_synth_df = synth_ndjson_df.join(existing_synth_df, ["UserID", "Video ID", "IngestionTimestamp"], "left_anti")
                logger.info(f"Rows after deduplication: {new_synth_df.count()}")
            else:
                logger.info(f"No existing data in {s3_path}, writing all data as new")
                new_synth_df = synth_ndjson_df
            if new_synth_df.count() > 0:
                video_interactions_dfs.append(new_synth_df)
                logger.info("Collected synthetic_data.ndjson for video_interactions")
            else:
                logger.info("No new data to ingest from synthetic_data.ndjson")
    except Exception as e:
        logger.error("Error ingesting synthetic_data.ndjson to S3 raw layer - %s", str(e))

    # Ingest synthetic_data_fixed.ndjson
    synthetic_fixed_ndjson_file_path = 'data/synthetic_data_fixed.ndjson'
    try:
        if check_file(synthetic_fixed_ndjson_file_path):
            synth_data_fixed_df = spark.read.option("mode", "PERMISSIVE").schema(video_interactions_ndjson_schema).json(synthetic_fixed_ndjson_file_path)
            logger.info(f"Schema applied for synthetic_data_fixed.ndjson: {synth_data_fixed_df.schema}")
            #synth_data_fixed_df.show(5, truncate=False)
            validate_nulls(synth_data_fixed_df, ["UserID", "Video ID", "Watch Time", "Total Time Spent"], "synthetic_data_fixed.ndjson")
            synth_data_fixed_df = synth_data_fixed_df.withColumn("IngestionTimestamp", current_timestamp())
            s3_path = "s3a://datastreaming-analytics-1/raw/video_interactions"
            if check_s3_path(spark, s3_path):
                logger.info(f"Existing data found in {s3_path}, performing deduplication")
                existing_synth_fixed_df = spark.read.schema(video_interactions_ndjson_schema).parquet(s3_path)
                logger.info(f"Rows before deduplication: {synth_data_fixed_df.count()}")
                new_synth_fixed_df = synth_data_fixed_df.join(existing_synth_fixed_df, ["UserID", "Video ID", "IngestionTimestamp"], "left_anti")
                logger.info(f"Rows after deduplication: {new_synth_fixed_df.count()}")
            else:
                logger.info(f"No existing data in {s3_path}, writing all data as new")
                new_synth_fixed_df = synth_data_fixed_df
            if new_synth_fixed_df.count() > 0:
                video_interactions_dfs.append(new_synth_fixed_df)
                logger.info("Collected synthetic_data_fixed.ndjson for video_interactions")
            else:
                logger.info("No new data to ingest from synthetic_data_fixed.ndjson")
    except Exception as e:
        logger.error("Error ingesting synthetic_data_fixed.ndjson to S3 raw layer - %s", str(e))

    # Write all video_interactions data as a single Parquet file
    try:
        s3_path = "s3a://datastreaming-analytics-1/raw/video_interactions"
        if video_interactions_dfs:
            # Normalize schemas by casting CSV IntegerType to LongType and renaming Productivity Loss
            normalized_dfs = []
            for df in video_interactions_dfs:
                if "ProductivityLoss" in df.columns:
                    df = df.withColumnRenamed("ProductivityLoss", "Productivity Loss")
                    df = df.select([col(c).cast(LongType()) if video_interactions_ndjson_schema[c].dataType == LongType() else col(c) for c in video_interactions_ndjson_schema.names] + [col("IngestionTimestamp")])
                normalized_dfs.append(df)
            
            # Union all DataFrames
            final_video_df = normalized_dfs[0]
            for df in normalized_dfs[1:]:
                final_video_df = final_video_df.unionByName(df, allowMissingColumns=True)
            
            logger.info(f"Total rows in combined video_interactions: {final_video_df.count()}")
            final_video_df.coalesce(1).write.option("compression", "snappy").mode("overwrite").parquet(s3_path)
            logger.info("Successfully wrote all video_interactions data to S3 as a single Parquet file")
            logger.info(f"Written schema to S3: {final_video_df.schema}")
        else:
            logger.info("No video_interactions data to write to S3")
    except Exception as e:
        logger.error("Error writing combined video_interactions to S3 - %s", str(e))

    # Validate S3 output
    try:
        s3_paths = [
            "s3a://datastreaming-analytics-1/raw/community_interactions",
            "s3a://datastreaming-analytics-1/raw/live_streaming",
            "s3a://datastreaming-analytics-1/raw/video_interactions"
        ]
        for s3_path in s3_paths:
            if check_s3_path(spark, s3_path):
                s3_df = spark.read.parquet(s3_path)
                logger.info(f"Sample data from {s3_path}:")
                #s3_df.show(5, truncate=False)
                null_counts = s3_df.select([sum_(col(c).isNull().cast("int")).alias(c) for c in s3_df.columns]).collect()[0].asDict()
                logger.info(f"Null counts in {s3_path}: {null_counts}")
    except Exception as e:
        logger.error("Error validating S3 output - %s", str(e))

    spark.stop()
except Exception as e:
    logger.error("Unexpected error in batch_ingest_to_raw.py - %s", str(e))