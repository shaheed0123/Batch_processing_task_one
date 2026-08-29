import subprocess
import sys
from pathlib import Path


# ==================================================
# Configuration
# ==================================================

PROJECT_ROOT = Path.home() / "data-engineering-task1"

LOCAL_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "yellow_tripdata_2015-01.parquet"
)

HDFS_DIR = "/data/raw/yellow_taxi/2015-01"

HDFS_FILE = (
    f"{HDFS_DIR}/yellow_tripdata_2015-01.parquet"
)


# ==================================================
# Helper function
# ==================================================

def run_command(command):
    print(f"\nRunning: {' '.join(command)}")

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        sys.exit(result.returncode)

    if result.stdout:
        print(result.stdout)

    return result.stdout


# ==================================================
# Validate local file
# ==================================================

print("Checking local dataset...")

if not LOCAL_FILE.exists():
    print(f"ERROR: File not found: {LOCAL_FILE}")
    sys.exit(1)

file_size = LOCAL_FILE.stat().st_size

print(f"Local file: {LOCAL_FILE}")
print(f"Local size: {file_size / (1024 * 1024):.2f} MB")


# ==================================================
# Copy file into NameNode container
# ==================================================

print("\nCopying dataset into Hadoop container...")

run_command([
    "docker",
    "cp",
    str(LOCAL_FILE),
    "task1-namenode:/tmp/yellow_tripdata_2015-01.parquet"
])


# ==================================================
# Create HDFS directory
# ==================================================

print("\nCreating HDFS raw directory...")

run_command([
    "docker",
    "exec",
    "-u",
    "hadoop",
    "task1-namenode",
    "hdfs",
    "dfs",
    "-mkdir",
    "-p",
    HDFS_DIR
])


# ==================================================
# Remove existing file if present
# ==================================================

print("\nRemoving existing HDFS copy if present...")

run_command([
    "docker",
    "exec",
    "-u",
    "hadoop",
    "task1-namenode",
    "hdfs",
    "dfs",
    "-rm",
    "-f",
    HDFS_FILE
])


# ==================================================
# Upload into HDFS
# ==================================================

print("\nUploading dataset to HDFS...")

run_command([
    "docker",
    "exec",
    "-u",
    "hadoop",
    "task1-namenode",
    "hdfs",
    "dfs",
    "-put",
    "/tmp/yellow_tripdata_2015-01.parquet",
    HDFS_DIR
])


# ==================================================
# Verify upload
# ==================================================

print("\nVerifying HDFS upload...")

run_command([
    "docker",
    "exec",
    "-u",
    "hadoop",
    "task1-namenode",
    "hdfs",
    "dfs",
    "-ls",
    "-h",
    HDFS_DIR
])


print("\n========================================")
print("INGESTION COMPLETED SUCCESSFULLY")
print("========================================")
