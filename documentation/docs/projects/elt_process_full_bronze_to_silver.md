# Bronze Layer to Silver Layer Transformation with PySpark and Delta Lake

## Overview

This project demonstrates a data transformation pipeline built with **PySpark**, **Apache Spark**, **Delta Lake**, and **MinIO** using a containerized architecture with Docker and Jupyter Notebook.

The pipeline is responsible for transforming curated datasets from the **Bronze Layer** into business-ready datasets in the **Silver Layer** inside a Lakehouse architecture.

The transformation process applies SQL-based business rules, enriches metadata, and stores the processed datasets in **Delta Lake** format for improved reliability, governance, and analytical performance.

---

## Data Flow

```text
MinIO Bronze Layer (Delta)
              ↓
       Apache Spark
              ↓
     SQL Transformations
              ↓
    Metadata Enrichment
              ↓
MinIO Silver Layer (Delta)
```

---


## Objective

The main goal of this pipeline is to:

- Read curated raw data from the Bronze layer
- Apply transformation rules using Spark SQL
- Standardize and enrich datasets
- Store optimized analytical tables in the Silver layer
- Maintain transactional consistency using Delta Lake

---

## Environment Variables

The application uses environment variables loaded from a `.env` file to securely manage credentials and infrastructure configuration.

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

## Spark Configuration

The Spark session is configured to support:

- MinIO integration using S3A
- Delta Lake operations
- Hive Metastore integration
- Distributed processing

---

## Required Dependencies

```python
conf.set("spark.jars.packages", ...)
```

### Packages

| Dependency | Purpose |
|---|---|
| hadoop-aws | S3A filesystem support |
| aws-java-sdk-bundle | AWS SDK integration |
| postgresql JDBC | PostgreSQL connectivity |
| delta-spark | Delta Lake support |

All dependencies are automatically downloaded by Spark during runtime.

---

## MinIO Configuration

### S3 Endpoint

```python
conf.set(
    "spark.hadoop.fs.s3a.endpoint",
    f"http://{MINIO_CONTAINER}:9000"
)
```

Defines the MinIO server endpoint.

---

### Authentication

```python
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_USER)
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
```

Responsible for authenticating Spark in MinIO.

---

### Path Style Access

```python
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
```

Required because MinIO uses path-style bucket access instead of the default AWS virtual-host format.

---

### S3A Filesystem

```python
conf.set(
    "spark.hadoop.fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
```

Enables Spark to communicate with MinIO through Hadoop’s S3A connector.

---

### Enable Delta Extensions

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

Replaces the default Spark catalog with the Delta catalog.

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

### Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

Provides execution monitoring and process traceability.

---

## Transformation Pipeline

### Iterating Through Transformation Queries

```python
for table_name in config_file.queries_silver.keys():
```

The pipeline iterates through all configured Silver layer transformation queries.

Each table transformation is dynamically controlled through the configuration file.

---

## Layer Paths

### Bronze Layer Path

```python
bronze_path = config_file.data_lakehouse_path['bronze']
```

Location of the source Delta tables.

---

### Silver Layer Path

```python
silver_path = config_file.data_lakehouse_path['silver']
```

Destination location for transformed analytical datasets.

---

### Getting SQL Queries

```python
query = func_file.get_query(
    table_name,
    queries_tables,
    bronze_path
)
```

Get transformation queries dynamically based on:

- Table name
- List of SQL transformation
- Bronze layer paths

---

### Executing Transformations

```python
dataframe = spark.sql(query)
```

Executes SQL transformations directly using Spark SQL.

---

### Adding Metadata Column

```python
dataframe_with_last_update = func_file.add_data_last_update(dataframe)
```

Adds a metadata column containing the ingestion or update timestamp.

---

### Writing Data to Silver Layer

```python
dataframe_with_last_update.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("month_key") \
    .save(output_path)
```

---

### Overwrite Mode

```python
.mode("overwrite")
```

Replaces existing data in the target table during full-load processing.

---

### Partitioning Strategy

```python
.partitionBy("month_key")
```

Partitions the dataset using the `month_key` column.

---

### Success Logging

```python
logging.info(
    f"Transform table {table_name} Completed and saved on: {output_path}"
)
```

Tracks successful execution for each transformed table.

---

### Error Handling

```python
except Exception as e:
    logging.info(f"Error processing {table_name}: {str(e)}")
```

Captures and logs exceptions during execution.

This improves observability and troubleshooting.

---

## Silver Layer Characteristics

The Silver layer contains:

- Cleaned datasets
- Standardized schemas
- Business-level transformations
- Optimized analytical structures
- Curated enterprise-ready data

---