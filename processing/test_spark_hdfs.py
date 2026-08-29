from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Task1-HDFS-Test")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

input_path = (
    "hdfs://namenode:8020/"
    "data/raw/yellow_taxi/2015-01/"
    "yellow_tripdata_2015-01.parquet"
)

print("\nReading data from HDFS...")

df = spark.read.parquet(input_path)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== RECORD COUNT =====")
print("Records:", df.count())

print("\n===== SAMPLE DATA =====")
df.show(5, truncate=False)

spark.stop()
