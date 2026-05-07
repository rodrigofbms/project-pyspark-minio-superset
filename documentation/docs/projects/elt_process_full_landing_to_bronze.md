# MinIO Landing to Bronze Layer Ingestion

## Overview

This process is responsible for extracting data stored in the **Landing Layer** inside MinIO in **Parquet** format and loading it into the **Bronze Layer** in **Delta Lake** format using **PySpark** running in a containerized environment with Jupyter Notebook and Apache Spark.

The main objective of this pipeline is to standardize the raw data structure, enable transactional storage with Delta Lake, and prepare the datasets for future transformations in the Silver and Gold layers.

---

## Data Flow Summary

```text
MinIO Landing Layer (Parquet)
            ↓
      Apache Spark
            ↓
 Metadata Enrichment
            ↓
Delta Lake Conversion
            ↓
MinIO Bronze Layer (Delta)
```

---

## Environment Variables

The application loads credentials and container information using environment variables through the `.env` file.

```python
load_dotenv()

MINIO_USER=os.getenv("MINIO_USER")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
MINIO_CONTAINER=os.getenv("MINIO_CONTAINER")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
POSTGRES_CONTAINER=os.getenv("POSTGRES_CONTAINER")
```

This approach improves security and flexibility by avoiding hardcoded credentials in the source code.

---

## Required Dependencies

```python
conf.set("spark.jars.packages", ...)
```

### Packages Used

| Package | Purpose |
|---|---|
| hadoop-aws | Enables S3A filesystem support |
| aws-java-sdk-bundle | AWS SDK required for S3 communication |
| postgresql JDBC | PostgreSQL database connectivity |
| delta-spark | Delta Lake support |

These dependencies are automatically downloaded by Spark during execution.

---

## MinIO Configuration

### S3 Endpoint

```python
conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_CONTAINER}:9000")
```

Defines the MinIO server endpoint.

---

### Authentication

```python
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_USER)
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
```

Used to authenticate Spark in MinIO.

---

### Path Style Access

```python
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
```

MinIO requires path-style URLs instead of the default AWS virtual-host style.

---

### S3A Filesystem

```python
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Enables Spark and Hadoop to communicate with MinIO using the S3A connector.

---

### Credentials Provider

```python
conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
```

Defines how credentials are provided to Spark.

---

## Delta Lake Configuration

### Enable Delta Extensions

```python
conf.set(
    "spark.sql.extensions",
    "io.delta.sql.DeltaSparkSessionExtension"
)
```

Activates Delta Lake features inside Spark.

---

### Delta Catalog

```python
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
```

Replaces Spark’s default catalog with the Delta catalog.

---

### Hive Metastore Integration

```python
conf.set("hive.metastore.uris", "thrift://metastore:9083")
```

Connects Spark to an external Hive Metastore service for metadata management.

---

### Spark Session Initialization

```python
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

Creates the Spark session using all previously defined configurations.

---

## Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

Enables execution monitoring and process tracking through logs.

---

## Ingestion Process

### Start Process Log

```python
logging.info(
    "Starting convertions from Minio landing (Parquet) to Minio Bronze (Delta)..."
)
```

Indicates the beginning of the ingestion pipeline.

---

### Table Iteration

```python
for table in config_file.tables_postgres_adventureworks.values():
```

The process iterates through all configured tables defined in the configuration file.

---

### Table Name Standardization

```python
table_name = func_file.convert_table_name(table)
```

Converts table names by replacing dots (`.`) with underscores (`_`) to create compatible folder names inside MinIO.

### Example

```text
sales.orderheader → sales_orderheader
```

---

## Layer Paths

### Landing Layer Path

```python
table_path = config_file.data_lakehouse_path['landing']
```

Source location of the Parquet files.

---

### Bronze Layer Path

```python
output_path = config_file.data_lakehouse_path['bronze']
```

Destination location where Delta tables will be stored.

---

## Reading Parquet Files

```python
dataframe = spark.read.format("parquet").load(f"{table_path}{table_name}")
```

Reads raw data from the Landing layer stored in Parquet format.

---

## Adding Metadata Column

```python
df_with_data = func_file.add_data_last_update(dataframe)
```

Adds a metadata column related to the ingestion/load timestamp.

---

## Writing Data in Delta Format

```python
df_with_data.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("month_key") \
    .save(f"{output_path}bronze_{table_name}")
```

## Process Description

The transformed dataset is written to the Bronze layer using Delta Lake format.

---

### Write Mode

```python
.mode("overwrite")
```

Replaces existing data in the target table.

---

### Partition Strategy

```python
.partitionBy("month_key")
```

Partitions the dataset by the `month_key` column.


---

## Error Handling

```python
except Exception as e:
```

Captures exceptions during processing and logs the error.

---