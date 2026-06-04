from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import date_format, col, row_number
from pyspark.sql.window import Window
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    
    conf.setAppName("Incremental load from MinIO landing to MinIO bronze") # Spark application name, Usefull for logs
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


def process_table(spark, table_name, table_name_converted, primary_key, input_table_path, output_table_path):

    try:
    
        # Logging the processing
        logging.info(f"processing table {table_name_converted}")

        # 1. Tratamento para Primeira Carga (Se a tabela Bronze não existir)
        try:
            # Getting max date value from minIO bronze in the modifieddate column. limit at 1 result and get this result on 1º row at max_modifieddate column
            df_max_modifieddate_bronze = spark.read.format("delta").load(output_table_path) \
                .select(functions.max("modifieddate").alias("max_modifieddate")).limit(1).collect()[0]["max_modifieddate"]
            
            # Se a tabela existir mas estiver vazia (retornar None)
            if df_max_modifieddate_bronze is None:
                df_max_modifieddate_bronze = "1900-01-01 00:00:00"
                
        except Exception:
            
            # If the path does not exist in MinIO (First run)
            logging.info(f"Bronze table for {table_name_converted} not found. Starting initial load.")
            df_max_modifieddate_bronze = "1900-01-01 00:00:00"

        
        # Getting max date value from minIO landing on parquet format in the modifieddate column.
        df_update_data_to_bronze = spark.read.format("parquet").load(input_table_path) \
        .filter(functions.col("modifieddate") > functions.lit(df_max_modifieddate_bronze))
        
        rows_to_update = df_update_data_to_bronze.count()
        
        if  rows_to_update == 0:
            # Logging if get no rows to update in minio landing
            logging.info(f"No new data to process for table {table_name_converted}")

        else:
            # Logging number of rows to update
            logging.info(f"Number of new rows to update for table {table_name_converted}: {rows_to_update}")

            # Deduplicate incremental batches using a window function
            # Avoid the error of ambiguous rows in the Merge if the same ID has changed more than once in the Landing layer
            window_spec = Window.partitionBy(primary_key).orderBy(col("modifieddate").desc())
            df_deduplicated_batch = df_update_data_to_bronze \
                .withColumn("row_num", row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")

            # Defining the set of technical columns and metadata to ignore
            ignored_columns = {primary_key, "modifieddate", "month_key", "last_update"}

            # Get all columns except technical metadata to generate the hash
            business_columns = [functions.col(c) for c in df_update_data_to_bronze.columns if c not in ignored_columns]

            # Creating the hash expression separately to avoid polluting the .withColumn method
            # The “*” in Python is called the unpacking operator; basically, it opens the list and passes each element 
            # of the list separated by commas, as is the case with “CONCAT_WS,” 
            # which expects to receive all subsequent elements separated by commas.
            row_hash_expr = functions.sha2(functions.concat_ws("||", *business_columns), 256)
            
            # Generating the hash based on the actual business columns
            df_with_hash = df_deduplicated_batch.withColumn("row_hash", row_hash_expr)
                
            # Adding a new column date related the load data
            df_with_update_date = func_file.add_data_last_update(df_with_hash)
    
            # modifing the dataframe to add a new column "month_key" to create a partition on the minIO Bronze based on modifieddate column
            df_with_month_partition = df_with_update_date.withColumn("month_key", date_format(df_with_update_date["modifieddate"], "yyyy-MM"))
            
            # Updating the dataframe on minIO landing
            logging.info(f"Updating table {table_name_converted}...")


            try:
                # Instantiating the current bronze table mapped in MinIO as a DeltaTable object
                target_delta_table = DeltaTable.forPath(spark, output_table_path)
                
                # Execute the Merge
                target_delta_table.alias("bronze") \
                    .merge(
                        source=df_with_month_partition.alias("updates"),
                        # Condiition 1: Match by registration ID
                        condition=f"bronze.{primary_key} = updates.{primary_key}"
                    ) \
                    .whenMatchedUpdate(
                        # Condition 2: It only updates the Bronze if the hash of the new data is DIFFERENT from the old hash
                        condition="bronze.row_hash != updates.row_hash",
                        set={
                            # Update all columns with the new batch values
                            **{c: f"updates.{c}" for c in df_with_month_partition.columns}
                        }
                    ) \
                    .whenNotMatchedInsert(
                        # Condition 3: If the ID does not exist in Bronze, insert it as a new row.
                        values={c: f"updates.{c}" for c in df_with_month_partition.columns}
                    ) \
                    .execute()
                
            except Exception:
                
                # Fallback to create the table on the first load if ‘DeltaTable.forPath’ fails due to a missing directory
                logging.info(f"Creating Delta table for the first time on path: {output_table_path}")
                df_with_month_partition.write.format("delta").mode("overwrite").partitionBy("month_key").save(output_table_path)


            # Logging the sucessfully process
            logging.info(f"Table {table_name_converted} Sucessfully updated and saved in MinIO bronze on: {output_table_path}")

    except Exception as e:
        # Logging the Error
         logging.error(f"Error processing table {table_name}: {str(e)}")


if __name__ == "__main__":
    
    # Logging the Start process from ingestion
    logging.info("Starting incrmental load from MinIO landing to MinIO bronze...")

    spark = configure_spark()

    # landing path
    landing_path = config_file.data_lakehouse_path["landing"]
    
    # bronze path
    bronze_path = config_file.data_lakehouse_path["bronze"]

    # Dictionary with primary keys from tables
    dictionary_pks = config_file.tables_pk

    # Creating a ThreadPool for divide all jobs among the workers and execute in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        
        # Creating a list to Add all jobs into it
        futures = []
    
        # Getting each table was in the dictionary on config_file
        for table_name in config_file.tables_postgres_adventureworks.values():
            
            # convert the table name from postgres to name in minIO s3
            table_name_converted = func_file.convert_table_name(table_name)
    
            # Landing table path
            landing_table_path = f"{landing_path}{table_name_converted}"
            
            # Output table path
            bronze_table_path = f"{bronze_path}bronze_{table_name_converted}"

            #Primary key from table_name
            primary_key = func_file.get_pk(table_name_converted, dictionary_pks)

            # Instead of calling the function to execute it, call the function by passing it to the executor
            futures.append(executor.submit(process_table, spark, table_name, table_name_converted, primary_key, landing_table_path, bronze_table_path))


        for future in as_completed(futures):
            try:
                # Where the all jobs is executing by the executors created with ThreadPoolExecutor
                future.result()

            except Exception as e:
                logging.error(f"Error in one of parallel taks: {str(e)}")

    
    # Logging the Incremental ingestion
    logging.info(f"Incremental ingestion to bronze layer completed!")

    # Stopping the sparkSession and clearing the cache
    spark.stop()
    spark.catalog.clearCache()


