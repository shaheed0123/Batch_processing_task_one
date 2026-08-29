# Data Engineering Task 1 – Batch Processing

## 1. Project Overview

This project implements a batch-processing data architecture for the
January 2015 New York Yellow Taxi dataset.

The system uses:

- Python for data ingestion
- Hadoop HDFS for distributed storage
- Apache Spark for data processing and aggregation
- Docker for the Hadoop and Spark environment
- Parquet for processed and aggregated data
- GitHub for version control

The purpose of the project is to take a large taxi dataset, store it in
HDFS, clean and transform the data with Spark, and produce structured
aggregated data that can be used by a future machine-learning application.

---

## 2. Folder Structure

After opening the project folder, you will see:

├── aggregation/
│   └── aggregate_taxi_data.py
│
├── ingestion/
│   └── ingest_to_hdfs.py
│
├── processing/
│   ├── process_taxi_data.py
│   └── test_spark_hdfs.py
│
├── hadoop-config/
│   ├── core-site.xml
│   └── hdfs-site.xml
│
├── data/
│   ├── raw/
│   │   └── yellow_tripdata_2015-01.parquet
│   ├── processed/
│   └── aggregated/
│
├── docker-compose.yml
└── .gitignore

---

## 3. Where Should You Start?

Open the folder:

Then open a terminal in this folder.

The main file to start the infrastructure is:

docker-compose.yml

This file starts the Hadoop and Spark containers.

---

## 4. Software Required

The project was developed as a local Docker-based data engineering system.

The following software is required:

- Docker Desktop
- Docker Compose
- Python 3

The project uses the following container images:

- Apache Hadoop 3.4.1
- Apache Spark 3.5.7

Make sure Docker Desktop is running before starting the project.

---

## 5. Start the Hadoop and Spark Infrastructure

Open a terminal inside the `data-engineering-task1` folder.

Run:

docker compose up -d

This starts:

1. Hadoop NameNode
2. Hadoop DataNode
3. Spark Master
4. Spark Worker

To check the running containers, use:

docker ps

You should see the containers for the Hadoop and Spark services.

---

## 6. Start with the Data

The input dataset is located at:

data/raw/yellow_tripdata_2015-01.parquet

This is the January 2015 New York Yellow Taxi dataset.

The dataset contains approximately 12.7 million records.

The ingestion script expects the project folder to be located in the user's
home directory with the folder name.
This is because the ingestion script uses the user's home directory to
locate the project.

---

## 7. Ingestion

The ingestion script is located at:

ingestion/ingest_to_hdfs.py

The script:

1. Checks that the local dataset exists.
2. Checks the file size.
3. Copies the dataset into the Hadoop NameNode container.
4. Creates the HDFS directory.
5. Removes an existing copy if necessary.
6. Uploads the dataset into HDFS.
7. Lists the HDFS directory to verify the upload.

Run:

python ingestion/ingest_to_hdfs.py

The raw dataset is stored in HDFS at:

/data/raw/yellow_taxi/2015-01/

The final HDFS file is:

/data/raw/yellow_taxi/2015-01/yellow_tripdata_2015-01.parquet

---

## 8. Processing

The processing code is located at:

processing/process_taxi_data.py

This stage uses Apache Spark.

The script reads the raw Parquet data from HDFS and performs data cleaning
and transformation.

The processing includes:

- Removing records with missing pickup timestamps
- Removing records with missing drop-off timestamps
- Removing trips where the drop-off time is before the pickup time
- Removing negative trip distances
- Handling invalid passenger counts
- Creating pickup date
- Creating pickup hour
- Creating pickup minute
- Calculating trip duration
- Calculating average speed
- Removing obviously invalid trip durations

The processed data is written back to HDFS as Parquet.

The output location is:

/data/processed/yellow_taxi/2015-01

The Spark application connects to the Spark cluster using:

spark://spark-master:7077

and accesses HDFS using:

hdfs://namenode:8020

---

## 9. Aggregation

The aggregation code is located at:

aggregation/aggregate_taxi_data.py

This stage reads the processed Parquet data from:

/data/processed/yellow_taxi/2015-01

Apache Spark then creates five analytical outputs.

### Output 1 – Trips by Hour

Shows the number of trips for each pickup hour.

Location:

/data/aggregated/yellow_taxi/2015-01/trips_by_hour

### Output 2 – Trips by Day

Shows the number of trips for each pickup date.

Location:

/data/aggregated/yellow_taxi/2015-01/trips_by_day

### Output 3 – Distance and Fare

Provides:

- Average trip distance
- Average fare

Location:

/data/aggregated/yellow_taxi/2015-01/distance_fare

### Output 4 – Revenue

Provides:

- Total revenue
- Total number of trips

Location:

/data/aggregated/yellow_taxi/2015-01/revenue

### Output 5 – Location Summary

Provides pickup-location-level:

- Trip count
- Average distance
- Average fare
- Total revenue

Location:

/data/aggregated/yellow_taxi/2015-01/location_summary

All aggregation results are stored as Parquet data in HDFS.

---

## 10. Checking HDFS

You can check the HDFS directories by opening a terminal and using:

docker exec -u hadoop task1-namenode hdfs dfs -ls -R /data

This allows you to see the raw, processed and aggregated data stored in HDFS.

---

## 11. Hadoop Web Interface

The Hadoop NameNode web interface is available at:

http://localhost:9870

This can be used to view information about the HDFS system.

---

## 12. Spark Web Interface

The Spark Master interface is available at:

http://localhost:8085

This can be used to check the Spark master and worker.

The Spark worker interface is available at:

http://localhost:8081

---

## 13. Testing

The file:

processing/test_spark_hdfs.py

is included for testing the connection between Spark and HDFS.

It can be used to check that Spark can connect to HDFS and read data from
the Hadoop environment.

---

## 14. Stopping the System

When the processing is complete, the containers can be stopped with:

docker compose down

The Docker volumes are used to keep the Hadoop NameNode and DataNode
storage available between container runs.

---

## 15. Complete Data Flow

The complete project follows this flow:

New York Yellow Taxi Dataset-------Python Ingestion------Hadoop HDFS – Raw Data-------Apache Spark Processing
------Cleaned and Transformed Parquet Data-------Apache Spark Aggregation----------Aggregated Parquet Data
----------Future Machine Learning Application


## 16. Main Project Components

### Ingestion Service

Responsible for moving the local taxi dataset into HDFS.

### Storage Service

Hadoop HDFS stores the raw, processed and aggregated data.

### Processing Service

Apache Spark cleans and transforms the raw taxi data.

### Aggregation Service

Apache Spark creates the five analytical outputs.

### Docker

Docker provides the local containerised environment for Hadoop and Spark.

### GitHub

GitHub provides version control for the project code.

---

## 17. Important Note

This project was developed as a local batch-processing data architecture
for the Data Engineering course.

The implementation demonstrates the complete ingestion, storage,
processing and aggregation workflow.

The architecture can be extended in the future with additional Hadoop
nodes, Spark workers, stronger security controls, automated orchestration,
monitoring and a real-time streaming pipeline using technologies such as
Apache Kafka and Spark Structured Streaming.

---

## 18. Repository

GitHub:


https://github.com/shaheed0123/Batch_processing_task_one/tree/main
