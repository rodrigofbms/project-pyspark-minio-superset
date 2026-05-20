# Creating a dockerfile when airflow execute the dag for ETL 
FROM apache/spark:3.5.0


# Use a root user to set up the environment
USER root

# Install the dotenv lib
RUN pip install --no-cache-dir python-dotenv

# Create a directory call "app"
RUN mkdir -p /app

# Copying the functions and configurations from jupyter notebooks
COPY src/notebooks/functions /app/functions/
COPY src/notebooks/configurations /app/configurations/

# Copying the notebooks from jupyter notebook to inside the directory  "/app"
COPY src/notebooks/.env /app/
COPY src/notebooks/elt/incremental_load/05_incremental_extract_postgresql_to_landing_minio_parquet.py /app/
COPY src/notebooks/elt/incremental_load/06_incremental_load_landing_to_bronze_delta.py /app/
COPY src/notebooks/elt/incremental_load/07_incremental_transform_bronze_to_silver.py /app/
COPY src/notebooks/elt/incremental_load/08_incremental_agregation_silver_to_gold.py /app/

# Spark configurations
COPY apps/spark_jupyter/config/env /env/
COPY apps/spark_jupyter/config/util /util/

WORKDIR /app
