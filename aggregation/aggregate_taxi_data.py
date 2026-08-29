from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum,
    hour,
    to_date,
    round
)


# ==================================================
# Spark Session
# ==================================================

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Aggregation")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# Paths
# ==================================================

INPUT_PATH = (
    "hdfs://namenode:8020/"
    "data/processed/yellow_taxi/2015-01"
)

OUTPUT_BASE = (
    "hdfs://namenode:8020/"
    "data/aggregated/yellow_taxi/2015-01"
)


# ==================================================
# Read processed data
# ==================================================

print("Reading processed data from HDFS...")

df = spark.read.parquet(INPUT_PATH)

print(f"Processed records available: {df.count()}")


# ==================================================
# 1. Trips by Hour
# ==================================================

print("Creating trips-by-hour aggregation...")

trips_by_hour = (
    df
    .groupBy(
        hour(col("tpep_pickup_datetime")).alias("pickup_hour")
    )
    .agg(
        count("*").alias("trip_count")
    )
    .orderBy("pickup_hour")
)

trips_by_hour.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/trips_by_hour"
)


# ==================================================
# 2. Trips by Day
# ==================================================

print("Creating trips-by-day aggregation...")

trips_by_day = (
    df
    .groupBy(
        to_date(col("tpep_pickup_datetime")).alias("pickup_date")
    )
    .agg(
        count("*").alias("trip_count")
    )
    .orderBy("pickup_date")
)

trips_by_day.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/trips_by_day"
)


# ==================================================
# 3. Average Distance and Average Fare
# ==================================================

print("Creating distance and fare aggregation...")

distance_fare = (
    df
    .agg(
        round(avg("trip_distance"), 2).alias("average_trip_distance"),
        round(avg("fare_amount"), 2).alias("average_fare")
    )
)

distance_fare.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/distance_fare"
)


# ==================================================
# 4. Total Revenue
# ==================================================

print("Creating revenue aggregation...")

revenue = (
    df
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("total_trips")
    )
)

revenue.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/revenue"
)


# ==================================================
# 5. Location-Level Summary
# ==================================================

print("Creating location-level aggregation...")

location_summary = (
    df
    .groupBy(
        col("PULocationID").alias("pickup_location_id")
    )
    .agg(
        count("*").alias("trip_count"),
        round(avg("trip_distance"), 2).alias("average_distance"),
        round(avg("fare_amount"), 2).alias("average_fare"),
        round(sum("total_amount"), 2).alias("total_revenue")
    )
    .orderBy(col("trip_count").desc())
)

location_summary.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/location_summary"
)


# ==================================================
# Display results
# ==================================================

print("\n========== TRIPS BY HOUR ==========")
trips_by_hour.show(24, truncate=False)

print("\n========== TRIPS BY DAY ==========")
trips_by_day.show(31, truncate=False)

print("\n========== DISTANCE / FARE ==========")
distance_fare.show(truncate=False)

print("\n========== REVENUE ==========")
revenue.show(truncate=False)

print("\n========== TOP PICKUP LOCATIONS ==========")
location_summary.show(10, truncate=False)


print("\nAggregation completed successfully.")
print(f"Output base: {OUTPUT_BASE}")


spark.stop()
