from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import date_format
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from delta.tables import DeltaTable

# Import for get the environment variables 
from dotenv import load_dotenv
import os
from configurations import configurations as config_file # Import configurations.py from the configurations folder
from functions import functions as func_file # Import functions.py from the functions folder

load_dotenv()

MINIO_CONTAINER=os.getenv("MINIO_CONTAINER")
MINIO_USER=os.getenv("MINIO_USER")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
POSTGRES_CONTAINER=os.getenv("POSTGRES_CONTAINER")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")

def configure_spark():
    conf = SparkConf()
    
    conf.setAppName("Incremental agregation from MinIO silver to MinIO gold") # Spark application name, Usefull for logs
    conf.set("spark.master", "spark://spark-master:7077") # set the Spark container to be distributed among the workers
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
    return spark



logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def process_table(spark, table_name, query, primary_key, output_table_path):

    try:
        
         # Logging the processing
        logging.info(f"processing table {table_name}")
        
        

        try:
            df_gold = spark.read.format("delta").load(output_table_path)

            if df_gold.count() > 0:

                # Getting max date value from minIO gold in the last_update column. limit at 1 result and get this result on 1º row
                # at max_modifieddate column. In this layer, it’s best to use the highest value from the “last_update” column, 
                # because if you use the highest value from the “modifieddate” column, it will return different dates for each row, 
                # rather than the same date for all the rows that were loaded. This directly affects the SQL query used to view the data 
                # — for example, when using “group by” — so it’s best to use the “last_update” column to present the data in the most effective way.
                
                df_max_last_update_gold = df_gold.select(functions.max("last_update").alias("max_last_update")) \
                .limit(1).collect()[0]["max_last_update"]

                if df_max_last_update_gold is None:
                    df_max_last_update_gold = "1900-01-01 00:00:00"

            else:
                df_max_last_update_gold = "1900-01-01 00:00:00"

        except Exception:
            df_max_last_update_gold = "1900-01-01 00:00:00"

        
         #Transforming data from the silver layer where the “modifieddate” column is more recent than the “modifieddate” column in the gold layer
        query_update_data_to_gold = spark.sql(f"""
            select * from ({query}) as subquery
            where last_update > '{df_max_last_update_gold}'
            """)
    
        # Number of rows returns from query to update, if exists
        rows_to_update = query_update_data_to_gold.count()
        
        if rows_to_update == 0:
            # Logging if get no rows to update in minio gold
            logging.info(f"No new data to process for table {table_name}")
    
        else:
            # Logging number of rows to update
            logging.info(f"Number of new rows to update for table {table_name}: {rows_to_update}")

            # Adding a new column date related the load data
            df_with_update_date = func_file.add_data_last_update(query_update_data_to_gold)

            # As primary_key is just a single column, example: “customer_id,” pass it directly to the col() function, 
            # using a cast() because the ‘sha2’ function does not accept numeric data types, and finally generate the hash
            gold_hash_expr = functions.sha2(functions.col(primary_key).cast("string"), 256)
        
            df_with_gold_hash =  df_with_update_date.withColumn("gold_row_hash", gold_hash_expr)

            if not DeltaTable.isDeltaTable(spark, output_table_path):
                # Updating the dataframe on minIO gold
                logging.info(f"First Load. Creating Gold table {table_name}...")
                
                df_with_gold_hash.write.format("delta").mode("overwrite").save(output_table_path)

            else:
                # Updating the dataframe on minIO gold
                logging.info(f"Updating table {table_name}...")
                
                target_table = DeltaTable.forPath(spark, output_table_path)

                # O Match do Merge é feito pelo NOVO HASH da agregação
                target_table.alias("target") \
                    .merge(
                        df_with_gold_hash.alias("updates"),
                        "target.gold_row_hash = updates.gold_row_hash"
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                
            # Logging the sucessfully process
            logging.info(f"Table {table_name} Sucessfully updated and saved in MinIO Gold on: {output_table_path}")

    except Exception as e:
        # Logging the Error
         logging.error(f"Error processing table {table_name}: {str(e)}")



if __name__ == "__main__":

    # Logging the Start process from ingestion
    logging.info("Starting incremental agregation from MinIO silver to MinIO gold...")

    spark = configure_spark()
    

    # silver path
    silver_path = config_file.data_lakehouse_path["silver"]
    # gold path
    gold_path = config_file.data_lakehouse_path["gold"]

    queries_tables = config_file.queries_gold

    # Dictionary with primary keys from tables
    dictionary_pks = config_file.gold_pks

    # Creating a ThreadPool for divide all jobs among the workers and execute in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        
        # Creating a list to Add all jobs into it
        futures = []
    
        for table_name in config_file.queries_gold.keys():
            
            # silver table path
            silver_table_path = f"{silver_path}silver_{table_name}"
            # gold table path
            gold_table_path = f"{gold_path}gold_{table_name}"
    
            query = func_file.get_query(table_name, queries_tables, silver_path)

            #Primary key from table_name
            primary_key = func_file.get_pk(table_name, dictionary_pks)
    
            # Instead of calling the function to execute it, call the function by passing it to the executor
            futures.append(executor.submit(process_table, spark, table_name, query, primary_key, gold_table_path))

        
        for future in as_completed(futures):
            try:
                # Where the all jobs is executing by the executors created with ThreadPoolExecutor
                future.result()

            except Execption as e:
                logging.error(f"Error in one of parallel taks: {str(e)}")
            
    
    # Logging the Incremental ingestion
    logging.info(f"Incremental agregation to gold layer completed!")

    # Stopping the sparkSession and clearing the cache
    spark.stop()
    spark.catalog.clearCache()


