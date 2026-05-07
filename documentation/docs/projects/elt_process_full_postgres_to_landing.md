# PostgreSQL to MinIO Landing Layer Ingestion

## Overview

This process is responsible for extracting data from the PostgreSQL AdventureWorks database and loading it into the Landing layer of the Data Lakehouse stored in MinIO (S3-compatible storage).

The pipeline was developed using Python with Apache Spark running inside Docker containers through Jupyter Notebook.


## Main Process Flow

### 1. Environment Configuration

The application loads environment variables using `python-dotenv` to securely access:

- MinIO credentials
- PostgreSQL credentials
- Container hostnames

This avoids hardcoded sensitive information inside the code.

---

### 2. Spark Session Configuration

A custom Spark configuration (`SparkConf`) is created to:

- Enable S3A connectivity for MinIO
- Configure PostgreSQL JDBC connection
- Enable Delta Lake support
- Connect to Hive Metastore
- Automatically download required dependencies (JARs)

### Main libraries configured

- Hadoop AWS
- AWS SDK Bundle
- PostgreSQL JDBC Driver
- Delta Spark

---

### 3. Data Extraction from PostgreSQL

For each table configured in the project:


spark.read.format("jdbc")
Spark connects to PostgreSQL using JDBC and loads the table into a Spark DataFrame.

---


### 4. Data Standardization

During processing:

Table names are converted to a standardized naming convention
A load timestamp column is added
A month_key column is created from the modifieddate field

The month_key column is used for partitioning the data in the Landing layer.


### 5. Data Loading into MinIO Landing Layer

The processed DataFrame is written into MinIO using the Parquet format:

.write.format("parquet")
.partitionBy("month_key")


### 6. Logging and Monitoring

The pipeline uses Python logging to monitor:

Process start
Table processing
Successful writes
Error handling