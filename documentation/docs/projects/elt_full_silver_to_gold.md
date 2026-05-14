# Silver Layer to Gold Layer Full Ingestion

## Overview

The pipeline is responsible for reading curated datasets from the **Silver Layer**, applying business aggregations and analytical transformations, and storing the final business-ready datasets in the **Gold Layer** inside MinIO.

The Gold layer represents the final analytical layer of the Lakehouse architecture, optimized for:

- Business Intelligence
- Dashboards
- Reporting
- KPIs
- Data Analytics
- Decision-Making

All aggregated datasets are stored using **Delta Lake** format to provide transactional consistency, schema management, scalability, and high-performance analytical queries.

---

## Lakehouse Layer Flow

```
Landing Layer
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Gold Layer
```

## Full Data Flow

```
MinIO Silver Layer(Delta)
                ↓
         Apache Spark
                ↓
      SQL Aggregations
                ↓
       KPI Calculations
                ↓
      Business Metrics
                ↓
      Metadata Enrichment
                ↓
MinIO Gold Layer(Delta)
```

---

# Main Process Flow

## 1.Environment Variables

The application uses environment variables loaded from a `.env` file to securely manage infrastructure credentials and runtime configurations.

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

- Distributed data processing
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

Required because MinIO uses path-style bucket addressing instead of AWS virtual-host style access.

---

### S3A Connector

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

Provides monitoring, observability, and troubleshooting capabilities during pipeline execution.

---

## 4.Full Gold Aggregation Pipeline

### Process Initialization

```python
logging.info(
    "Starting Agregations from MinIO Silver to MinIO Gold..."
)
```

Indicates the beginning of the Gold aggregation pipeline.

---

### Iterating Through Aggregation Queries

```python
for table_name in config_file.queries_gold.keys():
```

The pipeline dynamically iterates through all configured Gold layer aggregation queries.

This design improves scalability and maintainability.

---

### Loading Aggregation Queries

```python
queries_tables = config_file.queries_gold
```

Loads all aggregation queries configured for the Gold layer.

---

### Silver Layer Path

```python
silver_path = config_file.data_lakehouse_path['silver']
```

Defines the source location of curated Silver datasets.

---

### Gold Layer Path

```python
gold_path = config_file.data_lakehouse_path['gold']
```

Defines the destination location for Gold layer datasets.

---

### Output Path

```python
output_path = f"{gold_path}gold_{table_name}"
```

Defines the final destination path where Gold datasets will be stored.

---

### Logging Processing

```python
logging.info(f"processing table {table_name}")
```

Tracks the aggregation execution process for each dataset.

---

### Dynamic Query Generation

```python
query = func_file.get_query(
    table_name,
    queries_tables,
    silver_path
)
```

Generates aggregation queries dynamically based on:

- Table name
- Aggregation rules
- Silver layer datasets

---

### Executing Aggregation Queries

```python
dataframe = spark.sql(query)
```

Executes SQL-based aggregations directly on Silver layer datasets.

---

### Adding Metadata Column

```python
dataframe_with_last_update = func_file.add_data_last_update(dataframe)
```

Adds metadata related to processing or ingestion timestamps.

---

### Writing Data to Gold Layer

```python
dataframe_with_last_update.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(output_path)
```

Stores the aggregated datasets in the Gold layer using Delta Lake format.

---

### Overwrite Mode

```python
.mode("overwrite")
```

Ensures the Gold dataset is completely replaced during each full aggregation execution.

This behavior is appropriate for full-load aggregation pipelines.

---

### Schema Evolution Support

```python
.option("overwriteSchema", "true")
```

Allows schema updates during overwrite operations.

Benefits:

- Supports schema changes
- Enables flexible aggregation evolution
- Prevents schema conflicts during writes

This is especially useful when business requirements evolve over time.

---

### Success Logging

```python
logging.info(
    f"Agregation table {table_name} Completed and saved on: {output_path}"
)
```

Tracks successful aggregation execution and storage operations.

---

## 5.Error Handling

```python
except Exception as e:
    logging.info(f"Error processing {table_name}: {str(e)}")
```

Captures and logs execution errors.

This improves observability and troubleshooting.

---

## 6.End of Pipeline

```python
logging.info("Agreagations to Gold layer Completed")
```

Indicates the successful completion of the Gold aggregation pipeline.

---
