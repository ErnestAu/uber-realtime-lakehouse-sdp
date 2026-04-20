from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# Dim Passenger
@dp.view(name = "dim_passengers_view")
def dim_passengers_view():
    df = spark.readStream.table("uber_catalog.silver.silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
    df = df.dropDuplicates(["passenger_id"])
    return df

dp.create_streaming_table("uber_catalog.gold.dim_passengers")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.dim_passengers",
  source = "dim_passengers_view",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = 1
)

# Dim Driver
@dp.view(name = "dim_drivers_view")
def dim_drivers_view():
    df = spark.readStream.table("uber_catalog.silver.silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
    df = df.dropDuplicates(["driver_id"])
    return df

dp.create_streaming_table("uber_catalog.gold.dim_drivers")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.dim_drivers",
  source = "dim_drivers_view",
  keys = ["driver_id"],
  sequence_by = col("driver_id"),
  stored_as_scd_type = 1
)

# Dim Payment Method
@dp.view(name = "dim_payments_view")
def dim_payments_view():
    df = spark.readStream.table("uber_catalog.silver.silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
    df = df.dropDuplicates(["payment_method_id"])
    return df

dp.create_streaming_table("uber_catalog.gold.dim_payments")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.dim_payments",
  source = "dim_payments_view",
  keys = ["payment_method_id"],
  sequence_by = col("payment_method_id"),
  stored_as_scd_type = 1
)

# Dim Booking
@dp.view
def dim_booking_view():
    df = spark.readStream.table("uber_catalog.silver.silver_obt")
    df = df.select("ride_id", "confirmation_number", "dropoff_location_id", "ride_status_id", "dropoff_city_id", "cancellation_reason_id", "dropoff_address", "dropoff_latitude", "dropoff_longitude", "booking_timestamp","dropoff_timestamp", "pickup_address", "pickup_latitude", "pickup_longitude")
    df = df.dropDuplicates(["ride_id"])
    return df

dp.create_streaming_table("uber_catalog.gold.dim_booking")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = col("ride_id"),
  stored_as_scd_type = 1
)

# Dim Location
@dp.table
def dim_location_view():
  df = spark.readStream.table("uber_catalog.silver.silver_obt")
  df = df.select("pickup_city_id", "pickup_city_name", "pickup_state", "pickup_region", "pickup_city_updated_at")
  df = df.dropDuplicates(["pickup_city_id", "pickup_city_updated_at"])
  return df

dp.create_streaming_table("uber_catalog.gold.dim_location")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = col("pickup_city_updated_at"),
  stored_as_scd_type = 2
)

# Fact Table
@dp.view
def fact_view():
  df = spark.readStream.table("uber_catalog.silver.silver_obt")
  df = df.select("ride_id", "distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute")
  df = df.dropDuplicates(subset = ['ride_id'])
  return df

dp.create_streaming_table("uber_catalog.gold.fact")

dp.create_auto_cdc_flow(
  target = "uber_catalog.gold.fact",
  source = "fact_view",
  keys = ["ride_id"],
  sequence_by = col("ride_id"),
  stored_as_scd_type = 1
)



