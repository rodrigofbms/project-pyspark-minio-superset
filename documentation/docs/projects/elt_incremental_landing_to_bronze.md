# MinIO Landing to Bronze Layer Incremental Ingestion

## Overview

The pipeline is responsible for incrementally loading data from the **Landing Layer** stored in **Parquet** format into the **Bronze Layer** stored in **Delta Lake** format inside a Lakehouse architecture.

The incremental strategy compares the latest processed `modifieddate` available in the Bronze layer against the data stored in the Landing layer, ensuring that only new or updated records are processed.

This approach minimizes unnecessary reprocessing and improves overall ingestion performance and scalability.

---

## Incremental Data Flow

```
MinIO Landing Layer(Parquet)
                ↓
      Incremental Comparison
                ↓
          Apache Spark
                ↓
      Metadata Enrichment
                ↓
        Partition Creation
                ↓
 MinIO Bronze Layer(Delta)
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

Reconfigures the absolute path to allow importing shared project modules outside the notebook directory.

---

## 3.Spark Configuration

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

Required because MinIO uses path-style bucket addressing instead of AWS virtual-host addressing.

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

Replaces the default Spark catalog with Delta Lake support.

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

Provides monitoring, observability, and troubleshooting support during execution.

---

## 5.Incremental Bronze Loading Pipeline

### Process Initialization

```python
logging.info(
    "Starting incrmental load from MinIO landing to MinIO bronze..."
)
```

Indicates the beginning of the incremental Bronze layer loading process.

---

### Iterating Through Tables

```python
for table_name in config_file.tables_postgres_adventureworks.values():
```

The pipeline iterates dynamically through all configured source tables.

This design improves scalability and maintainability.

---

### Table Name Standardization

```python
table_name_converted = func_file.convert_table_name(table_name)
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

Defines the source path for Landing layer datasets.

---

### Bronze Layer Path

```python
bronze_path = config_file.data_lakehouse_path["bronze"]
```

Defines the destination path for Bronze layer datasets.

---

### Landing Table Path

```python
landing_table_path = f"{landing_path}{table_name_converted}"
```

Path of the source Parquet dataset.

---

### Bronze Table Path

```python
bronze_table_path = f"{bronze_path}bronze_{table_name_converted}"
```

Destination path for the Bronze Delta table.

---

## 6.Incremental Strategy

### Getting the Latest Bronze Layer Timestamp

```python
df_max_modifieddate_bronze = spark.read \
    .format("delta") \
    .load(bronze_table_path) \
    .select(
        functions.max("modifieddate")
        .alias("max_modifieddate")
    ) \
    .limit(1) \
    .collect()[0]["max_modifieddate"]
```

Retrieves the latest `modifieddate` already processed in the Bronze layer.

This timestamp acts as the incremental checkpoint.

---

### Reading Incremental Data from Landing Layer

```python
df_update_data_from_landing = spark.read \
    .format("parquet") \
    .load(landing_table_path) \
    .filter(
        functions.col("modifieddate") >
        functions.lit(df_max_modifieddate_bronze)
    )
```

Reads only records from the Landing layer that are newer than the latest processed record in the Bronze layer.

This minimizes unnecessary processing.

---

### Empty Increment Validation

```python
if df_update_data_from_landing.count() == 0:
```

Checks whether new records exist for processing.

If no new records are found, the pipeline skips unnecessary writes.

---

### Logging New Rows

```python
logging.info(
    f"Number of new rows to update for table {table_name_converted}: {df_update_data_from_landing.count()}"
)
```

Tracks the number of incremental rows processed.

---

### Adding Metadata Column

```python
df_with_update_date = func_file.add_data_last_update(
    df_update_data_from_landing
)
```

---

## 7.Partition Generation

```python
df_with_month_partition = df_with_update_date.withColumn(
    "month_key",
    date_format(
        df_with_update_date["modifieddate"],
        "yyyy-MM"
    )
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
- Optimized analytical workloads

---

## 8.Writing Data to Bronze Layer

```python
df_with_month_partition.write \
    .format("parquet") \
    .mode("append") \
    .partitionBy("month_key") \
    .save(bronze_table_path)
```

Appends incremental records into the Bronze layer.

---

### Append Mode

```python
.mode("append")
```

Ensures only new records are added without overwriting historical data.

---

### Important Note About Storage Format

Although the pipeline configuration enables Delta Lake support, the current write operation is saving the Bronze layer using:

```python
.format("parquet")
```

If the intention is to store Bronze datasets in Delta Lake format, the correct implementation should be:

```python
.format("delta")
```

This would enable:

- ACID transactions
- Time travel
- Schema evolution
- Better reliability
- Optimized incremental processing

---

### Success Logging

```python
logging.info(
    f"Table {table_name_converted} Sucessfully updated and saved in MinIO bronze on: {bronze_table_path}"
)
```

Tracks successful incremental loading into the Bronze layer.

---

## 9.Error Handling

```python
except Exception as e:
    logging.error(f"Error processing table {table_name}: {str(e)}")
```

Captures and logs execution errors.

This improves observability and troubleshooting.

---

## 10.End of Pipeline

```python
logging.info(
    "Incremental ingestion to bronze layer completed!"
)
```

Indicates the successful completion of the Bronze incremental loading process.

---