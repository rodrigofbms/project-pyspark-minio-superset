# PostgreSQL to MinIO Landing Layer Incremental Ingestion

## Overview

The pipeline is responsible for incrementally extracting new or updated records from a PostgreSQL database and loading them into the **Landing Layer** inside MinIO in **Parquet** format.

The ingestion strategy uses the `modifieddate` column to identify newly inserted or updated records, ensuring efficient incremental processing and avoiding full table reloads.

---

## Incremental Data Flow

```text
PostgreSQL Database
          ↓
Incremental Filter(modifieddate)
          ↓
      Apache Spark
          ↓
 Metadata Enrichment
          ↓
Partition Generation
          ↓
MinIO Landing Layer(Parquet)
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

## 2.Project Structure Configuration

```python
sys.path.append(os.path.abspath("../../"))
```

Reconfigures the absolute path to allow importing custom modules located outside the notebook directory structure.

---

## 3.Spark Configuration

The Spark session is configured to support:

- Distributed processing
- PostgreSQL JDBC connectivity
- MinIO integration using S3A
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

Used to authenticate Spark against MinIO.

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

Enables Spark and Hadoop to communicate with MinIO using the S3A connector.

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

Connects Spark to an external Hive Metastore service for metadata management.

---

## 4.Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

Provides execution monitoring, observability, and troubleshooting support.

---

## 5.Incremental Ingestion Pipeline

### Process Initialization

```python
logging.info(
    "Starting incremental extract from Postgres adventureworks to MinIO landing..."
)
```

Indicates the beginning of the incremental ingestion process.

---

### Iterating Through Tables

```python
for table_name in config_file.tables_postgres_adventureworks.values():
```

The pipeline iterates dynamically through all configured PostgreSQL tables.

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

### Landing Layer Path

```python
landing_path = config_file.data_lakehouse_path["landing"]
```

Defines the location of the Landing layer inside MinIO.

---

### Output Path

```python
output_table_path = f"{landing_path}{table_name_converted_to_s3}"
```

Defines the destination path where incremental data will be stored.

---

## 6.Incremental Strategy

### Getting the Latest Processed Timestamp

```python
max_modified_date_landing = spark.read.format("parquet") \
    .load(output_table_path) \
    .select(
        functions.max("modifieddate")
        .alias("max_modifieddate")
    ) \
    .limit(1) \
    .collect()[0]["max_modifieddate"]
```

Retrieves the most recent `modifieddate` value already stored in the Landing layer.

This timestamp is used as the incremental checkpoint.

---

### Incremental Query Generation

```python
query_filtered = f"""
    select * from {table_name}
    where modifieddate > '{max_modified_date_landing}'
"""
```

Builds a dynamic SQL query that extracts only records newer than the last processed timestamp.

---

### Reading Data from PostgreSQL

```python
df_input_data = spark.read \
    .format("jdbc") \
    .option(
        "url",
        f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/Adventureworks"
    ) \
    .option("dbtable", f"({query_filtered})") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()
```

Reads incremental data directly from PostgreSQL using JDBC connectivity.

---

### Empty Increment Validation

```python
if df_input_data.count() == 0:
```

Checks whether new records were found.

If no new records exist, the pipeline skips unnecessary writes.

---

### Logging New Rows

```python
logging.info(
    f"Number of new rows to update for table {table_name_converted_to_s3}: {df_input_data.count()}"
)
```
Tracks the number of incremental records processed.

---

### Adding Metadata Column

```python
df_with_update_data = func_file.add_data_last_update(df_input_data)
```

Adds a metadata column related to ingestion or processing timestamp.

---

## 7.Partition Creation

```python
df_with_month_partition = df_with_update_data.withColumn(
    "month_key",
    date_format(df_with_update_data["modifieddate"], "yyyy-MM")
)
```

Creates a partition column based on the `modifieddate`.

Example:

```
2026-05
```

---

Benefits of Partitioning:

- Faster query performance
- Reduced file scanning
- Better storage organization
- Optimized data retrieval

---

## 8.Writing Data to MinIO Landing Layer

```python
df_with_month_partition.write \
    .format("parquet") \
    .mode("append") \
    .partitionBy("month_key") \
    .save(output_table_path)
```

Appends new incremental records into the Landing layer in Parquet format.

---

### Append Mode

```python
.mode("append")
```

Ensures only new records are added without overwriting existing data.

---

### Success Logging

```python
logging.info(
    f"Table {table_name_converted_to_s3} Sucessfully updated and saved in MinIO landing on: {output_table_path}"
)
```

Tracks successful incremental ingestion.

---

## 9.Error Handling

```python
except Exception as e:
    logging.error(f"Error processing table {table_name}: {str(e)}")
```

Captures and logs processing errors.

This improves observability and troubleshooting capabilities.

---

## 10.End of Pipeline

```python
logging.info("Incremental ingestion to landing completed!")
```

Indicates the successful completion of the incremental ingestion process.

---