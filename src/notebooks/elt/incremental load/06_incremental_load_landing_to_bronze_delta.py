from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import date_format
import logging
from datetime import datetime

# Import for get the environment variables 
from dotenv import load_dotenv
import os
import sys
sys.path.append(os.path.abspath("../../")) # Used to reconfigure the absolut path. In this case, setting the absolut path to 2 folders back (notebooks/...) 
from configurations import configurations as config_file # Import configurations.py from the configurations folder
from functions import functions as func_file # Import functions.py from the functions folder

load_dotenv()

MINIO_CONTAINER=os.getenv("MINIO_CONTAINER")
MINIO_USER=os.getenv("MINIO_USER")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
POSTGRES_CONTAINER=os.getenv("POSTGRES_CONTAINER")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")

conf = SparkConf()

conf.setAppName("Incremental load from MinIO landing to MinIO bronze") # Spark application name, Usefull for logs
# Add the jars from hadoop-aws and aws-java-sdk-bundle is necessary for org.apache.hadoop.fs.s3a.S3AFileSystem,
# add the Postgresql JDBC jar is necessary for connect on database. Add the delta-spark is necessary for delta catalog, all this Jars is auto-download from spark
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,"
         "com.amazonaws:aws-java-sdk-bundle:1.12.767,"
         "org.postgresql:postgresql:42.7.2,"
         "io.delta:delta-spark_2.12:3.2.0")
conf.set("spark.hadoop.fs.s3a.endpoint",f"http://{MINIO_CONTAINER}:9000") # Container and Port from MinIO
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_USER) # Login from MinIO
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) # Password from MinIO
conf.set("spark.hadoop.fs.s3a.path.style.access", True) # Enforces the use of URLs as the format. Without this, Spark attempts to use the AWS standard (bucket.endpoint), which fails in MinIO
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # Talk to Hadoop/Spark to use new conector S3A
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") # How to credentials are acess via config(access key + secret)
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # active extension from Delta Lake
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") # Change the standard catalog from spark to Delta 
conf.set("hive.metastore.uris", "thrift://metastore:9083") # Connect to Hive Metastore external

spark = SparkSession.builder.config(conf=conf).getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Logging the Start process from ingestion
logging.info("Starting incrmental load from MinIO landing to MinIO bronze...")

for table_name in config_file.tables_postgres_adventureworks.values():
    
    try:
        # convert the table name from postgres to name in minIO s3
        table_name_converted = func_file.convert_table_name(table_name)

        # landing path
        landing_path = config_file.data_lakehouse_path["landing"]
        # bronze path
        bronze_path = config_file.data_lakehouse_path["bronze"]
        
        # Landing table path
        landing_table_path = f"{landing_path}{table_name_converted}"
        # Output table path
        bronze_table_path = f"{bronze_path}bronze_{table_name_converted}"

        # Logging the processing
        logging.info(f"processing table {table_name_converted}")
        
        # Getting max date value from minIO bronze in the modifieddate column. limit at 1 result and get this result on 1º row at max_modifieddate column
        logging.info("Reading table from bronze layer")
        df_max_modifieddate_bronze = spark.read.format("delta").load(bronze_table_path) \
            .select(functions.max("modifieddate").alias("max_modifieddate")).limit(1).collect()[0]["max_modifieddate"]

        # Getting max date value from minIO landing on parquet format in the modifieddate column.
        logging.info("Reading table from landing layer and compare with new data")
        df_update_data_from_landing = spark.read.format("parquet").load(landing_table_path) \
        .filter(functions.col("modifieddate") > functions.lit(df_max_modifieddate_bronze))
        
        
        if df_update_data_from_landing.count() == 0:
            # Logging if get no rows to update in minio landing
            logging.info(f"No new data to process for table {table_name_converted}")

        else:
            # Logging number of rows to update
            logging.info(f"Number of new rows to update for table {table_name_converted}: {df_update_data_from_landing.count()}")
            
            # Adding a new column date related the load data
            df_with_update_date = func_file.add_data_last_update(df_update_data_from_landing)
    
            # modifing the postgres tables for add a new column "month_key" to create a partition on the minIO landing based on modifieddate column
            df_with_month_partition = df_with_update_date.withColumn("month_key", date_format(df_with_update_date["modifieddate"], "yyyy-MM"))
            
            # Updating the dataframe on minIO landing
            logging.info(f"Updating table {table_name_converted}...")
            df_with_month_partition.write.format("parquet").mode("append").partitionBy("month_key").save(bronze_table_path)
            
            # Logging the sucessfully process
            logging.info(f"Table {table_name_converted} Sucessfully updated and saved in MinIO bronze on: {bronze_table_path}")

    except Exception as e:
        # Logging the Error
         logging.error(f"Error processing table {table_name}: {str(e)}")

# Logging the Incremental ingestion
logging.info(f"Incremental ingestion to bronze layer completed!")


