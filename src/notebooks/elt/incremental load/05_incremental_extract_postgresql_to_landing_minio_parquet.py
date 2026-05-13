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

spark = SparkSession.builder \
.appName("Incremental extract from postgres adventureworks to minIO landing") \
.config("spark.master", "spark://spark-master:7077") \
.config("spark.hadoop.fs.s3a.endpoint",f"http://{MINIO_CONTAINER}:9000") \
.config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
.config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
.config("spark.hadoop.fs.s3a.path.style.access", True) \
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.config("hive.metastore.uris", "thrift://metastore:9083") \
.getOrCreate()



logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Logging the Start process from ingestion
logging.info("Starting incremental extract from Postgres adventureworks to MinIO landing...")

for table_name in config_file.tables_postgres_adventureworks.values():
    
    try:
        # convert the table name from postgres to name in minIO s3
        table_name_converted_to_s3 = func_file.convert_table_name(table_name)

        # Landing path
        landing_path = config_file.data_lakehouse_path["landing"]
        # Output path
        output_table_path = f"{landing_path}{table_name_converted_to_s3}"

        # Getting max date value from minIO Landing in the modifieddate column. limit at 1 result and get this result on 1º row at max_modifieddate column
        max_modified_date_landing = spark.read.format("parquet").load(output_table_path) \
            .select(functions.max("modifieddate").alias("max_modifieddate")).limit(1).collect()[0]["max_modifieddate"]

           
        query_filtered = f""" select * from {table_name} where modifieddate > '{max_modified_date_landing}' """
        
        # Logging the processing
        logging.info(f"processing table {table_name_converted_to_s3}")
        
        # Reading table in postgres via JDBC
        df_input_data = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_CONTAINER}:5432/Adventureworks") \
            .option("dbtable", f"({query_filtered})") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() 

        
        if df_input_data.count() == 0:
            # Logging if get no rows to update in minio landing
            logging.info(f"No new data to process for table {table_name_converted_to_s3}")

        else:
            # Logging number of rows to update
            logging.info(f"Number of new rows to update for table {table_name_converted_to_s3}: {df_input_data.count()}")
            
            # Adding a new column date related the load data
            df_with_update_data = func_file.add_data_last_update(df_input_data)
    
            # modifing the postgres tables for add a new column "month_key" to create a partition on the minIO landing based on modifieddate column
            df_with_month_partition = df_with_update_data.withColumn("month_key", date_format(df_with_update_data["modifieddate"], "yyyy-MM"))
            
            # Updating the dataframe on minIO landing
            logging.info(f"Updating table {table_name_converted_to_s3}...")
            df_with_month_partition.write.format("parquet").mode("append").partitionBy("month_key").save(output_table_path)
            
            # Logging the sucessfully process
            logging.info(f"Table {table_name_converted_to_s3} Sucessfully updated and saved in MinIO landing on: {output_table_path}")

    except Exception as e:
        # Logging the Error
         logging.error(f"Error processing table {table_name}: {str(e)}")

# Logging the Incremental ingestion
logging.info(f"Incremental ingestion to landing completed!")


