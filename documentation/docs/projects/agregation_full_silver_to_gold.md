# Silver layer to Gold Layer Aggregation with PySpark and Delta Lake

## Overview

This project demonstrates a data aggregation pipeline built with **PySpark**, **Apache Spark**, **Delta Lake**, and **MinIO** using a containerized environment with Docker and Jupyter Notebook.

The pipeline is responsible for processing curated datasets from the **Silver Layer** and generating business-oriented aggregated datasets in the **Gold Layer** of a Lakehouse architecture.

The Gold layer represents the final analytical layer optimized for:

- Business Intelligence (BI)
- Dashboards
- Reporting
- KPIs
- Decision-making
- Advanced analytics

All datasets are stored in **Delta Lake** format to ensure transactional reliability, scalability, and optimized query performance.

---

## Data Flow

```text
MinIO Silver Layer (Delta)
              ↓
       Apache Spark
              ↓
    Business Aggregations
              ↓
     KPI Calculations
              ↓
MinIO Gold Layer (Delta)
```

---

## Objective

The main objective of this pipeline is to:

- Read curated data from the Silver layer
- Execute aggregation and business logic queries
- Generate analytical datasets and KPIs
- Store optimized tables in the Gold layer
- Deliver business-ready data for analytics and reporting

---

## Environment Variables

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

## Spark Configuration

The Spark session is configured to support:

- Distributed data processing
- MinIO integration through S3A
- Delta Lake operations
- Hive Metastore connectivity

---

## Required Dependencies

```python
conf.set("spark.jars.packages", ...)
```

### Packages Used

| Dependency | Purpose |
|---|---|
| hadoop-aws | S3A filesystem support |
| aws-java-sdk-bundle | AWS SDK integration |
| postgresql JDBC | PostgreSQL connectivity |
| delta-spark | Delta Lake support |

These packages are automatically downloaded during Spark execution.

---

## MinIO Configuration

### S3 Endpoint

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

Required because MinIO uses path-style bucket addressing instead of AWS virtual-host addressing.

---

### S3A Filesystem

```python
conf.set(
    "spark.hadoop.fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
```

Enables Hadoop and Spark to communicate with MinIO using the S3A connector.

---

## Delta Lake Configuration

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

Replaces the default Spark catalog with Delta Lake support.

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

### Logging Configuration

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

Provides execution monitoring, debugging, and process traceability.

---

## Aggregation Pipeline

### Process Initialization

```python
logging.info("Starting Agregations from MinIO Silver to MinIO Gold...")
```

Indicates the beginning of the Gold layer aggregation process.

---

### Iterating Through Aggregation Queries

```python
for table_name in config_file.queries_gold.keys():
```

The pipeline iterates dynamically through all configured Gold layer aggregation queries.

Each aggregation is managed through centralized configuration files.

---

## Layer Paths

### Silver Layer Path

```python
silver_path = config_file.data_lakehouse_path['silver']
```

Location of the curated Silver layer datasets.

---

### Gold Layer Path

```python
gold_path = config_file.data_lakehouse_path['gold']
```

Destination location for the final analytical datasets.

---

## Getting SQL Queries

```python
query = func_file.get_query(
    table_name,
    queries_tables,
    silver_path
)
```

Builds SQL aggregation queries dynamically based on:

- Table name
- Aggregation rules
- Silver layer paths

---

## Executing Aggregations

```python
dataframe = spark.sql(query)
```

Executes business aggregation queries using Spark SQL.

Typical operations may include:

- KPI calculations
- Aggregations
- Metrics generation
- Grouping operations
- Dimensional modeling
- Analytical summarization
- Business rule application

---

## Adding Metadata Column

```python
dataframe_with_last_update = func_file.add_data_last_update(dataframe)
```

Adds a metadata column related to the processing timestamp.

---

## Writing Data to Gold Layer

```python
dataframe_with_last_update.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)
```

---

## Overwrite Mode

```python
.mode("overwrite")
```

Replaces existing Gold layer data during the full-load process.

This guarantees that analytical datasets always reflect the latest processed version.

---

## Error Handling

```python
except Exception as e:
    logging.info(f"Error processing {table_name}: {str(e)}")
```

Captures and logs execution errors during processing.

This improves observability and simplifies troubleshooting.

---
