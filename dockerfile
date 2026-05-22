# Creating a dockerfile when airflow execute the dag for ETL 
FROM apache/spark:3.5.0-python3

# Is necessary use the root user to setting all configurations on image
USER root

# Setting these environment variables is necessary to run “spark-submit” and “pyspark”
# from the command line without having to specify their respective directories
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Install the python-dotenv
RUN pip install --no-cache-dir python-dotenv

# Create a directory call "app"
RUN mkdir -p /app

# Copying the spark configurations files and Jars from Spark-master
COPY apps/spark_jupyter/config/util/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY apps/spark_jupyter/config/util/hive-site.xml /opt/spark/conf/hive-site.xml

# Copying the functions and configurations from jupyter notebooks
COPY src/notebooks/functions /app/functions/
COPY src/notebooks/configurations /app/configurations/

# Copying the notebooks from jupyter notebook to inside the directory  "/app"
COPY src/notebooks/.env /app/
COPY src/notebooks/elt/incremental_load/05_incremental_extract_postgresql_to_landing_minio_parquet.py /app/
COPY src/notebooks/elt/incremental_load/06_incremental_load_landing_to_bronze_delta.py /app/
COPY src/notebooks/elt/incremental_load/07_incremental_transform_bronze_to_silver.py /app/
COPY src/notebooks/elt/incremental_load/08_incremental_agregation_silver_to_gold.py /app/

WORKDIR /app