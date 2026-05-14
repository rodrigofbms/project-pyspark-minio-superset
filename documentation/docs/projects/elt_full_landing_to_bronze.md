# Landing Layer to Bronze Layer Full Load Ingestion

## Overview

The pipeline is responsible for reading raw datasets stored in the **Landing Layer** in **Parquet** format and loading them into the **Bronze Layer** in **Delta Lake** format inside MinIO.

This process represents the second stage of a Lakehouse architecture, where raw ingested data is standardized into a transactional and optimized storage format suitable for scalable downstream transformations.

The workflow also enriches the datasets with metadata columns and maintains partitioning strategies for performance optimization and efficient data organization.

---

## Full Data Flow

```text
MinIO Landing Layer (Parquet)
                ↓
         Apache Spark
                ↓
      Metadata Enrichment
                ↓
        Delta Conversion
                ↓
         Partitioning
                ↓
MinIO Bronze Layer (Delta)
```

---

# Main Process Flow

## 1.Environment Variables

The application uses environment variables loaded from a `.env` file to securely manage credentials and infrastructure configurations.

```python
load_dotenv()

MINIO_USER=os.getenv("MINIO_USER")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
MINIO_CONTAINER=os.getenv("MINIO_CONTAINER")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
POSTGRES_CONTAINER=os.getenv("POSTGRES_CONTAINER")
```

---

## 2.Spark Configuration

The Spark session is configured to support:

- Distributed processing
- MinIO integration through S3A
- Delta Lake operations
- Hive Metastore integration

---

### Required Dependencies

```python
conf.set("spark.jars.packages", ...)
```

### Packages Used

| Dependency |
|---|---|
| hadoop-aws | S3A filesystem support |
| aws-java-sdk-bundle | AWS SDK integration |
| postgresql JDBC | JDBC connectivity support |
| delta-spark | Delta Lake support |

These dependencies are automatically downloaded during Spark execution.

---

### MinIO Configuration

```python
conf.set(
    "spark.hadoop.fs.s3a.endpoint",
    f"http://{MINIO_CONTAINER}:9000"
)
```

Defines the MinIO object storage endpoint.

---

### Authentication

```python
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_USER)
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
```

Authenticates Spark against MinIO.

---

### Path Style Access

```python
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
```

Required because MinIO uses path-style bucket access instead of AWS virtual-host addressing.

---

### S3A Filesystem

```python
conf.set(
    "spark.hadoop.fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
```

Enables Spark and Hadoop to communicate with MinIO through the S3A connector.

---

### Delta Lake Configuration

```python
conf.set(
    "spark.sql.extensions",
    "io.delta.sql.DeltaSparkSessionExtension"
)
```

Activates Delta Lake functionality inside Spark.

---

### Delta Catalog

```python
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
```

Replaces Spark’s default catalog with Delta Lake support.

---

### Hive Metastore Integration

```python
conf.set(
    "hive.metastore.uris",
    "thrift://metastore:9083"
)
```

Connects Spark to an external Hive Metastore service for centralized metadata management.

---

## 3.Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

Provides monitoring, observability, and troubleshooting support during execution.

---

## 4.Full Ingestion Bronze Layer Pipeline

### Process Initialization

```python
logging.info(
    "Starting convertions from Minio landing (Parquet) to Minio Bronze (Delta)..."
)
```

Indicates the beginning of the full Bronze loading process.

---

### Iterating Through Source Tables

```python
for table in config_file.tables_postgres_adventureworks.values():
```

The pipeline iterates dynamically through all configured source tables.

---

### Table Name Standardization

```python
table_name = func_file.convert_table_name(table)
```

Converts source table names into compatible MinIO object names.

Example:

```
sales.orderheader → sales_orderheader
```

---

### Landing Layer Path

```python
table_path = config_file.data_lakehouse_path['landing']
```

Defines the source location of Landing layer datasets stored in Parquet format.

---

### Bronze Layer Path

```python
output_path = config_file.data_lakehouse_path['bronze']
```

Defines the destination location for Bronze layer datasets stored in Delta format.

---

### Logging Processing

```python
logging.info(f"processing table {table_name}")
```

Tracks the execution progress for each table.

---

### Reading Data from Landing Layer

```python
dataframe = spark.read \
    .format("parquet") \
    .load(f"{table_path}{table_name}")
```

Reads raw datasets stored in the Landing layer using Parquet format.

---

### Adding Metadata Column

```python
df_with_data = func_file.add_data_last_update(dataframe)
```

Adds a metadata column related to ingestion or processing timestamp.

### Writing Data to Bronze Layer

```python
df_with_data.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("month_key") \
    .save(f"{output_path}bronze_{table_name}")
```

Writes the dataset into the Bronze layer using Delta Lake format.

---

## 5.Partitioning Strategy

```python
.partitionBy("month_key")
```

Partitions datasets based on the `month_key` column.

Example:

```
2026-05
```

Benefits of Partitioning:

- Faster query performance
- Reduced file scanning
- Better storage organization
- Improved scalability
- Optimized analytical workloads

---

### Overwrite Mode

```python
.mode("overwrite")
```

Ensures that the complete Bronze dataset replaces any previous version.

This behavior is appropriate for full load processing scenarios.

---

### Success Logging

```python
logging.info(
    f"Write table {table_name} Completed and saved on: {output_path}bronze_{table_name}"
)
```

Tracks successful processing and storage operations.

---

## 6.Error Handling

```python
except Exception as e:
    logging.info(f"Error processing {table_name}: {str(e)}")
```

Captures and logs execution errors.

This improves observability and troubleshooting capabilities.

---

## 7.End of Pipeline

```python
logging.info("Ingestions to bronze layer Completed")
```

Indicates the successful completion of the full Bronze loading pipeline.

---
