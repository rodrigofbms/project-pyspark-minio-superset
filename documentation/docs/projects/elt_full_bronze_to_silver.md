# Bronze Layer to Silver Layer Full Load Ingestion

## Overview

The pipeline is responsible for transforming standardized raw datasets stored in the **Bronze Layer** into curated and business-ready datasets stored in the **Silver Layer** inside MinIO.

The transformation process applies SQL-based business rules, joins, filters, and data standardization operations using Spark SQL queries dynamically defined in configuration files.

All transformed datasets are stored in **Delta Lake** format to provide transactional consistency, scalability, and optimized analytical performance.

---

## Full Data Flow

```text
MinIO Bronze Layer(Delta)
                ↓
         Apache Spark
                ↓
      SQL Transformations
                ↓
      Metadata Enrichment
                ↓
         Partitioning
                ↓
MinIO Silver Layer(Delta)
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

## 4.Full Silver Transformation Pipeline

### Process Initialization

```python
logging.info(
    "Starting transform tables from Minio Bronze layer to Minio Silver layer..."
)
```

Indicates the beginning of the full Silver transformation process.

---

### Iterating Through Transformation Queries

```python
for table_name in config_file.queries_silver.keys():
```

The pipeline iterates dynamically through all configured Silver layer transformation queries.

This design improves scalability and maintainability.

---

### Query Configuration

```python
queries_tables = config_file.queries_silver
```

Loads all transformation queries configured for the Silver layer.

---

### Bronze Layer Path

```python
bronze_path = config_file.data_lakehouse_path['bronze']
```

Defines the source location of Bronze layer datasets.

---

### Silver Layer Path

```python
silver_path = config_file.data_lakehouse_path['silver']
```

Defines the destination location for Silver layer datasets.

---

### Output Path

```python
output_path = f"{silver_path}silver_{table_name}"
```

Defines the destination path where transformed datasets will be stored.

---

### Logging Processing

```python
logging.info(f"processing table {table_name}")
```

Tracks the execution progress for each transformation process.

---

### Dynamic Query Generation

```python
query = func_file.get_query(
    table_name,
    queries_tables,
    bronze_path
)
```

Generates Spark SQL transformation queries dynamically based on:

- Table name
- Transformation rules
- Bronze layer paths

This design improves flexibility and modularity.

---

### Executing Transformations

```python
dataframe = spark.sql(query)
```

Executes SQL-based transformations directly on Bronze layer datasets.

---

### Adding Metadata Column

```python
dataframe_with_last_update = func_file.add_data_last_update(dataframe)
```

Adds a metadata column related to ingestion or processing timestamp.

---

### Writing Data to Silver Layer

```python
dataframe_with_last_update.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("month_key") \
    .save(output_path)
```

Writes transformed datasets into the Silver layer using Delta Lake format.

---

## 5.Partitioning Strategy

```python
.partitionBy("month_key")
```

Partitions datasets using the `month_key` column.

Example:

```
2026-05
```

---

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

Ensures that the complete Silver dataset replaces any previous version stored in the layer.

This behavior is appropriate for full transformation scenarios.

---

### Success Logging

```python
logging.info(
    f"Transform table {table_name} Completed and saved on: {output_path}"
)
```

Tracks successful transformation and storage operations.

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
logging.info("Tranforms to silver layer Completed")
```

Indicates the successful completion of the Silver transformation pipeline.

---