from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    minute,
    unix_timestamp,
    when
)


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Processing")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Input / Output paths
# --------------------------------------------------

INPUT_PATH = (
    "hdfs://namenode:8020/"
    "data/raw/yellow_taxi/2015-01/"
    "yellow_tripdata_2015-01.parquet"
)

OUTPUT_PATH = (
    "hdfs://namenode:8020/"
    "data/processed/yellow_taxi/2015-01"
)


# --------------------------------------------------
# Read raw data
# --------------------------------------------------

print("Reading raw taxi data from HDFS...")

df = spark.read.parquet(INPUT_PATH)

print(f"Raw record count: {df.count()}")


# --------------------------------------------------
# Data cleaning
# --------------------------------------------------

print("Cleaning data...")

clean_df = (
    df
    .filter(col("tpep_pickup_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    .filter(col("trip_distance") >= 0)
    .filter(
        col("passenger_count").isNull()
        | (col("passenger_count") > 0)
    )
)


# --------------------------------------------------
# Timestamp processing
# --------------------------------------------------

print("Processing timestamps...")

clean_df = (
    clean_df
    .withColumn(
        "pickup_date",
        to_date(col("tpep_pickup_datetime"))
    )
    .withColumn(
        "pickup_hour",
        hour(col("tpep_pickup_datetime"))
    )
    .withColumn(
        "pickup_minute",
        minute(col("tpep_pickup_datetime"))
    )
)


# --------------------------------------------------
# Feature transformation
# --------------------------------------------------

print("Creating derived features...")

clean_df = (
    clean_df
    .withColumn(
        "trip_duration_minutes",
        (
            unix_timestamp(col("tpep_dropoff_datetime"))
            - unix_timestamp(col("tpep_pickup_datetime"))
        ) / 60
    )
    .withColumn(
        "average_speed_mph",
        when(
            col("trip_distance") > 0,
            col("trip_distance")
            / (col("trip_duration_minutes") / 60)
        )
    )
)


# --------------------------------------------------
# Remove obviously invalid derived values
# --------------------------------------------------

clean_df = clean_df.filter(
    (col("trip_duration_minutes") >= 0)
    & (
        col("trip_duration_minutes").isNull()
        | (col("trip_duration_minutes") <= 24 * 60)
    )
)


# --------------------------------------------------
# Final record count
# --------------------------------------------------

print("Calculating processed record count...")

processed_count = clean_df.count()

print(f"Processed record count: {processed_count}")


# --------------------------------------------------
# Write processed data
# --------------------------------------------------

print("Writing processed data to HDFS...")

(
    clean_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print("Processing completed successfully.")
print(f"Output: {OUTPUT_PATH}")


spark.stop()
