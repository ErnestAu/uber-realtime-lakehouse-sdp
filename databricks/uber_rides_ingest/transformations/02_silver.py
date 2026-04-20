from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Empty streaming table
dp.create_streaming_table("uber_catalog.silver.stg_rides")

# bulk load
@dp.append_flow(
  target = "uber_catalog.silver.stg_rides"
)
def rides_bulk():
    df = spark.readStream\
              .option("skipChangeCommits", "true")\
              .table("uber_catalog.bronze.bulk_rides")
    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))\
           .withColumn("pickup_timestamp", col("booking_timestamp").cast("timestamp"))\
           .withColumn("dropoff_timestamp", col("booking_timestamp").cast("timestamp"))
    return df

# streaming load
@dp.append_flow(
  target = "uber_catalog.silver.stg_rides"
)
def rides_stream():
  
  # 1. Get the schema from your already-defined UC table
  # This ensures your processing matches your destination perfectly
  target_schema = spark.table("uber_catalog.bronze.bulk_rides").schema

  # 2. Read the raw data
  raw_df = spark.readStream.table("uber_catalog.bronze.rides_raw")

  # 3. Parse and Select
  # We cast 'rides' to string first just in case it's binary
  final_df = (raw_df
      .withColumn("parsed_data", from_json(col("rides").cast("string"), target_schema))\
      .select("parsed_data.*")\
      .withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))\
      .withColumn("pickup_timestamp", col("booking_timestamp").cast("timestamp"))\
      .withColumn("dropoff_timestamp", col("booking_timestamp").cast("timestamp"))
  )
  
  return final_df

