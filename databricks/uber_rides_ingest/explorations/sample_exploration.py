# Databricks notebook source
from pyspark.sql.functions import from_json, col, schema_of_json


display(final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber_catalog.bronze.rides_raw 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber_catalog.bronze.bulk_rides LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE uber_catalog.bronze.stg_rides

# COMMAND ----------

jinja_config = [
    {
        "table" : "uber_catalog.silver.stg_rides  stg_rides",
        "select" : "stg_rides.*",
        "where" : ""
    },
    {
        "table" : "uber_catalog.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.* EXCEPT (cancellation_reason_id)",
        "where" : "",
        "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    },
    {
        "table" : "uber_catalog.bronze.map_cities pickup_city",
        "select" : "pickup_city.city AS pickup_city_name, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region",
        "where" : "",
        "on" : "stg_rides.pickup_city_id = pickup_city.city_id"
    },
    {
        "table" : "uber_catalog.bronze.map_cities dropoff_city",
        "select" : "dropoff_city.city AS dropoff_city_name, dropoff_city.state AS dropoff_state, dropoff_city.region AS dropoff_region",
        "where" : "",
        "on" : "stg_rides.dropoff_city_id = dropoff_city.city_id"
    },
    {
        "table" : "uber_catalog.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.* EXCEPT (payment_method_id)",
        "where" : "",
        "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "uber_catalog.bronze.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.* EXCEPT (ride_status_id)",
        "where" : "",
        "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "uber_catalog.bronze.map_vehicle_makes map_vehicle_makes ",
        "select" : "map_vehicle_makes.* EXCEPT (vehicle_make_id)",
        "where" : "",
        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "uber_catalog.bronze.map_vehicle_types map_vehicle_types ",
        "select" : "map_vehicle_types.* EXCEPT (vehicle_type_id)",
        "where" : "",
        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    }
]


# COMMAND ----------

from jinja2 import Template

jinja_str = """

    SELECT 
        {% for config in jinja_config %}
            {{ config.select }}
                {% if not loop.last %}
                    , 
                {% endif %}
        {% endfor %}
    FROM
        {% for config in jinja_config %}
            {% if loop.first %}
                {{ config.table }}
            {% else %}
                LEFT JOIN {{ config.table }} ON {{ config.on }}
            {% endif %}
        {% endfor %}
    {% if jinja_config[0].where != "" %}
        WHERE
            {% for config in jinja_config %}
                {{ config.where }}
                    {% if not loop.last %}
                        AND
                    {% endif %}
            {% endfor %}
    {% endif %}
"""

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
df = spark.sql(rendered_template)


display(sorted(df.columns))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE uber_catalog.bronze.dim_passengers

# COMMAND ----------

df = spark.readStream.table("uber_catalog.silver.silver_obt")
df = df.select("pickup_city_id", "pickup_city_name", "pickup_state", "pickup_region", "pickup_city_updated_at")
df = df.dropDuplicates(["pickup_city_id"])
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber_catalog.bronze.dim_location_view
# MAGIC ORDER BY pickup_city_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_city_name, pickup_state, pickup_region, pickup_city_updated_at
# MAGIC FROM uber_catalog.silver.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_city.city AS pickup_city_name, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region, pickup_city.updated_at AS pickup_city_updated_at
# MAGIC
# MAGIC FROM 
# MAGIC uber_catalog.silver.stg_rides
# MAGIC LEFT JOIN uber_catalog.bronze.map_cities AS pickup_city ON pickup_city.city_id = stg_rides.pickup_city_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber_catalog.gold.dim_location

# COMMAND ----------

# MAGIC %md
# MAGIC build fact table

# COMMAND ----------

df = spark.read.table("uber_catalog.silver.silver_obt")
df = df.select("distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute")
df = df.dropDuplicates(subset = ['ride_id'])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber_catalog.silver.silver_obt
