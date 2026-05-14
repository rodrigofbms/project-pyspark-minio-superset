# Bronze Layer to Silver Layer Incremental Ingestion

## Overview

The pipeline is responsible for incrementally transforming curated raw datasets stored in the **Bronze Layer** into refined and business-ready datasets stored in the **Silver Layer** within a Lakehouse architecture.

The transformation process applies SQL-based business rules and processes only new or updated records based on the `modifieddate` column, ensuring efficient incremental processing and optimized resource utilization.

All transformed datasets are stored in **Delta Lake** format to provide transactional reliability, scalability, and improved analytical performance.

---

## Incremental Data Flow

```
MinIO Bronze Layer(Delta)
                ↓
    Incremental Comparison
                ↓
        SQL Transformations
                ↓
         Apache Spark
                ↓
      Metadata Enrichment
                ↓
        Partition Creation
                ↓
 MinIO Silver Layer(Delta)
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

## 4.Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

Provides monitoring, observability, and troubleshooting support during execution.

---

## 5.Incremental Silver Transformation Pipeline

### Process Initialization

```python
logging.info(
    "Starting incrmental transform from MinIO bronze to MinIO silver..."
)
```

Indicates the beginning of the incremental Silver transformation process.

---

### Iterating Through Transformation Queries

```python
for table_name in config_file.queries_silver.keys():
```

The pipeline iterates dynamically through all configured Silver layer transformation queries.

This design improves scalability and maintainability.

---

### Bronze Layer Path

```python
bronze_path = config_file.data_lakehouse_path["bronze"]
```

Defines the source location of Bronze layer datasets.

---

### Silver Layer Path

```python
silver_path = config_file.data_lakehouse_path["silver"]
```

Defines the destination location for Silver layer datasets.

---

### Bronze Table Path

```python
bronze_table_path = f"{bronze_path}bronze_{table_name}"
```

Path of the source Bronze Delta table.

---

### Silver Table Path

```python
silver_table_path = f"{silver_path}silver_{table_name}"
```

Destination path for the Silver Delta table.

---

## 6.Incremental Strategy

### Getting the Latest Silver Layer Timestamp

```python
df_max_modifieddate_silver = spark.read \
    .format("delta") \
    .load(silver_table_path) \
    .select(
        functions.max("modifieddate")
        .alias("max_modifieddate")
    ) \
    .limit(1) \
    .collect()[0]["max_modifieddate"]
```

Retrieves the latest `modifieddate` already processed in the Silver layer.

This timestamp acts as the incremental checkpoint.

---

### Dynamic Transformation Query Generation

```python
query = func_file.get_query(
    table_name,
    queries_tables,
    bronze_path
)
```

Generates SQL transformation queries dynamically based on:

- Table name
- Transformation rules
- Bronze layer paths

---

### Incremental Transformation Query

```python
query_update_data_from_silver = spark.sql(f"""
    select * from ({query}) as subquery
    where modifieddate > '{df_max_modifieddate_silver}'
""")
```

Executes transformations only for records newer than the latest processed timestamp available in the Silver layer.

This incremental strategy minimizes:

- Processing costs
- Resource consumption
- Data reprocessing
- Query execution time

---

### Counting Incremental Rows

```python
rows_to_update = query_update_data_from_silver.count()
```

Determines the number of incremental records returned by the transformation query.

---

### Empty Increment Validation

```python
if rows_to_update == 0:
```

Checks whether new transformed records exist.

If no new data exists, the pipeline skips unnecessary writes.

---

### Logging New Rows

```python
logging.info(
    f"Number of new rows to update for table {table_name}: {rows_to_update}"
)
```

Tracks the number of incremental transformed records processed.

---

### Adding Metadata Column

```python
df_with_update_date = func_file.add_data_last_update(
    query_update_data_from_silver
)
```

Adds a metadata column related to ingestion or processing timestamp.

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

### Benefits of Partitioning

- Faster query performance
- Reduced file scanning
- Better storage organization
- Optimized analytical workloads

---

## 8.Writing Data to Silver Layer

```python
df_with_month_partition.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("month_key") \
    .save(silver_table_path)
```

Appends transformed incremental records into the Silver layer using Delta Lake format.

---

### Append Mode

```python
.mode("append")
```

Ensures only new transformed records are added without overwriting historical data.

---

### Success Logging

```python
logging.info(
    f"Table {table_name} Sucessfully updated and saved in MinIO silver on: {silver_table_path}"
)
```

Tracks successful incremental transformation and loading.

---

## 9.Error Handling

```python
except Exception as e:
    logging.error(f"Error processing table {table_name}: {str(e)}")
```

Captures and logs execution errors.

This improves observability and troubleshooting capabilities.

---

## 10.End of Pipeline

```python
logging.info(
    "Incremental ingestion to silver layer completed!"
)
```

Indicates the successful completion of the incremental Silver transformation pipeline.

---