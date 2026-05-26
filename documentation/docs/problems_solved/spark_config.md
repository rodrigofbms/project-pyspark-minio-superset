# Spark Infrastructure Configuration: Command Line vs. Property Files

This document details the architectural strategy adopted in this project for managing dependencies, connectors (MinIO/PostgreSQL/Delta Lake), and connections in Apache Spark 3.5.0, explaining the benefits of migrating configurations from the command line to global property files.

---

## 1. The Traditional Approach: CLI Injection (`--packages`)


In the traditional development model, dependencies and environment variables are passed directly when the command is called 
`spark-submit`:

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2,io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  /app/seu_script.py
```

Why did we avoid that in this project?

- Pollution in Orchestration (Airflow): DAGs become lengthy, cluttered, and difficult to read due to the length of the command strings in the operators (DockerOperator, SparkSubmitOperator).

- Code/Configuration Duplication: If you have 10 ETL scripts, you’ll need to repeat the same long line of packages and credentials in every DAG task or in every Jupyter notebook call.

- Complexity in Python Code: Forces the developer to expand the `SparkSession.builder` function within the .py file with infrastructure rules that pertain to the environment, not the business logic.

---

## 2. The Approach Taken: Centralization using spark-defaults.conf and hive-site.xml

I decided to shift infrastructure management to Spark's native configuration files within the Docker ecosystem.

How it work?

When Spark starts up (whether locally in an isolated Airflow container or in a distributed manner within the cluster), it automatically reads the files in the directory: /opt/spark/conf/.

1. spark-defaults.conf (Package and Extension Manager)

This file tells the Ivy manager (package dependency manager) which packages to automatically download in the background before it starts processing the first line of the Python script:

```
# Automatic download of connectors (MinIO/S3 + Postgres + Delta Lake)
spark.jars.packages                  io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2

# Delta Lake Global Extensions
spark.sql.extensions                 io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog      org.apache.spark.sql.delta.catalog.DeltaCatalog
```

2. hive-site.xml (Metastore Storage Abstraction)

It centralizes communication with the external metadata catalog and inherits the connection properties from Object Storage (MinIO), handling s3a:// paths natively and transparently.

```
<configuration>

    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://metastore:9083</value>
        <description>Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore.</description>
    </property>

    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>

    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>true</value>
    </property>

    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>

    <property>
        <name>fs.s3a.access.key</name>
        <value>admin</value>
    </property>

    <property>
        <name>fs.s3a.secret.key</name>
        <value>@admin123</value>
    </property>

    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>

    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>

</configuration>
```

---

## 3. Matriz de comparação

| Feature | Via the Command Line (--packages) | Via Configuration Files (.conf / .xml) |
|     :---:      |             :---:                 |                  :---:                      |
|Readability of the DAG | Bad (long commands that are prone to syntax errors) | Excellent (just run `spark-submit script.py`) |
|Maintenance | Complex (changing a version requires modifying all tasks in the DAG) | Centralized (change a line in the .conf file) |
|Portability | Download (the code is linked to the parameters passed via the CLI) | Deployment (the same code runs in Jupyter, Airflow, or locally) |
|Business Isolation | Bad (the Python script needs to know the keys and endpoints) | Perfect (the Python code focuses solely on DataFrames and transformations)|

---


## 4. Lessons Learned & Pitfalls Overcome (Troubleshooting)

1. The Error -> java.lang.NoClassDefFoundError: org/apache/hadoop/fs/impl/prefetch/PrefetchingStatistics:

- Cause: Incompatibility between the Spark core version and the hadoop-aws package version. Spark 3.5.0 uses Hadoop 3.3.4 internally. When attempting to inject hadoop-aws:3.3.6, the connector looked for optimization classes that do not exist in the core.

Solução: Strict version alignment solved the problem:

hadoop-aws -> 3.3.4

aws-java-sdk-bundle -> 1.12.262

2. The Error -> java.lang.NoClassDefFoundError: com/amazonaws/AmazonClientException:

- Cause: Airflow's DockerOperator creates isolated containers that run Spark in local mode. If the spark-defaults.conf file is not copied into the image during the Docker build, the container is created without knowing where to find the AWS SDK classes.

Solution: Ensure that the configuration files are copied in the Dockerfile to the directory: /opt/spark/conf/.