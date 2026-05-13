# PostgreSQL to MinIO Landing Layer Full load Ingestion

## Overview

The pipeline is responsible for extracting complete datasets from a PostgreSQL database and loading them into the **Landing Layer** inside MinIO in **Parquet** format.

This process represents the initial raw data ingestion stage of a Lakehouse architecture, where source data is stored with minimal transformations and organized for future processing in Bronze, Silver, and Gold layers.

The ingestion workflow also enriches the datasets with metadata columns and partitioning strategies to improve scalability, query performance, and data organization.

---

## Full Data Flow

```text
PostgreSQL Database
          ↓
      Apache Spark
          ↓
 Metadata Enrichment
          ↓
Partition Generation
          ↓
MinIO Landing Layer (Parquet)
```

---

# Main Process Flow

## 1.Environment Variables

The application uses environment variables loaded from a `.env` file to securely manage credentials and infrastructure configurations.

```python
load_dotenv()

MINIO_CONTAINER=os.getenv("MINIO_CONTAINER")
MINIO_USER=os.getenv("MINIO_USER")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
POSTGRES_CONTAINER=os.getenv("POSTGRES_CONTAINER")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
```

---

## 2.Spark Configuration

The Spark session is configured to support:

- Distributed processing
- PostgreSQL JDBC connectivity
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
| postgresql JDBC | PostgreSQL JDBC connectivity |
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

Activates Delta Lake support inside Spark.

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
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

Provides monitoring, observability, and troubleshooting support during execution.

---

## 4.Full Ingestion Landing Layer Pipeline

### Process Initialization

```python
logging.info(
    "Starting ingestions from Postgres adventureworks to MinIO landing..."
)
```

Indicates the beginning of the full ingestion process.

---

### Iterating Through Source Tables

```python
for table_name in config_file.tables_postgres_adventureworks.values():
```

The pipeline iterates dynamically through all configured PostgreSQL source tables.

This design improves scalability and maintainability.

---

### Table Name Standardization

```python
table_name_converted_to_s3 = func_file.convert_table_name(table_name)
```

Converts PostgreSQL table names into compatible MinIO object names.

Example:

```
sales.orderheader → sales_orderheader
```

---

### Reading Data from PostgreSQL

```python
df_input_data = spark.read \
    .format("jdbc") \
    .option(
        "url",
        f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/Adventureworks"
    ) \
    .option("dbtable", table_name) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()
```

Reads complete datasets directly from PostgreSQL using JDBC connectivity.

---

### Landing Layer Path

```python
landing_path = config_file.data_lakehouse_path["landing"]
```

Defines the destination location for the Landing layer inside MinIO.

---

### Output Path

```python
output_table_path = f"{landing_path}{table_name_converted_to_s3}"
```

Defines the destination path where the dataset will be stored.

---

### Logging Processing

```python
logging.info(
    f"processing table {table_name_converted_to_s3}"
)
```

Tracks the execution progress for each table.

---

### Adding Metadata Column

```python
df_with_update_data = func_file.add_data_last_update(
    df_input_data
)
```

Adds a metadata column related to ingestion or processing timestamp.

---

## 5.Partition Generation

```python
df_with_month_partition = df_with_update_data.withColumn(
    "month_key",
    date_format(
        df_with_update_data["modifieddate"],
        "yyyy-MM"
    )
)
```

Creates a partition column based on the `modifieddate`.

Example:

```
2026-05
```

Benefits of Partitioning:

- Faster query performance
- Reduced file scanning
- Better storage organization
- Optimized analytical workloads

---

### Writing Data to MinIO Landing Layer

```python
df_with_month_partition.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("month_key") \
    .save(output_table_path)
```

Writes the complete dataset into the Landing layer in Parquet format.

---

### Overwrite Mode

```python
.mode("overwrite")
```

Ensures that the full dataset replaces any previous version stored in the Landing layer.

This behavior is appropriate for full ingestion scenarios.

---

### Success Logging

```python
logging.info(
    f"Table {table_name_converted_to_s3} Sucessfully processed and saved in MinIO landing on: {output_table_path}"
)
```

Tracks successful ingestion and storage operations.

---

## 7.Error Handling

```python
except Exception as e:
    logging.error(f"Error processing table {table_name}: {str(e)}")
```

Captures and logs execution errors.

This improves observability and troubleshooting capabilities.

---

## 8.End of Pipeline

```python
logging.info("Ingestions to landing completed!")
```

Indicates the successful completion of the full ingestion pipeline.

---