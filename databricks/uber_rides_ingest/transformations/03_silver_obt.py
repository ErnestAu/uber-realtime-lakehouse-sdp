from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="uber_catalog.silver.silver_obt",
)
def silver_obt():
    # 1. Pull the query string from your control table
    # This reaches out to the table you created in Task 1
    config_df = spark.read.table("uber_catalog.bronze.pipeline_config")
    
    # 2. Extract the actual text
    dynamic_query = config_df.collect()[0]["query_string"]
    
    # 3. Execute the SQL string
    # Because your string contains 'STREAM()', DLT knows to make this a streaming table
    return spark.sql(dynamic_query)