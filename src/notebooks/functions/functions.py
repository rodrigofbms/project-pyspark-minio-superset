from pyspark.sql.functions import unix_timestamp, lit, date_format
from datetime import datetime
from pyspark.sql.types import TimestampType
import pyspark


# This function create a new data column with the data from the last update 
def add_data_last_update(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df_with_data = df.withColumn("last_update", lit(datetime.now()))
    return df_with_data

# The dot(.) is necessary for read tables from postgres and the underline(_) is used to create the path in the minIO S3
def convert_table_name (table_name):
    return table_name.replace(".", "_")

