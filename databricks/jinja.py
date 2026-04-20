# Databricks notebook source
# MAGIC %pip install jinja2
# MAGIC from jinja2 import Template
# MAGIC
# MAGIC jinja_config = [
# MAGIC     {
# MAGIC         "table" : "uber_catalog.silver.stg_rides  stg_rides",
# MAGIC         "select" : "stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating",
# MAGIC         "where" : ""
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_cancellation_reasons map_cancellation_reasons",
# MAGIC         "select" : "map_cancellation_reasons.cancellation_reason",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_cities pickup_city",
# MAGIC         "select" : "pickup_city.city AS pickup_city_name, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region, pickup_city.updated_at AS pickup_city_updated_at",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.pickup_city_id = pickup_city.city_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_cities dropoff_city",
# MAGIC         "select" : "dropoff_city.city AS dropoff_city_name, dropoff_city.state AS dropoff_state, dropoff_city.region AS dropoff_region, dropoff_city.updated_at AS dropoff_city_updated_at",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.dropoff_city_id = dropoff_city.city_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_payment_methods map_payment_methods",
# MAGIC         "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_ride_statuses map_ride_statuses",
# MAGIC         "select" : "map_ride_statuses.ride_status, map_ride_statuses.is_completed",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_vehicle_makes map_vehicle_makes ",
# MAGIC         "select" : "map_vehicle_makes.vehicle_make",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
# MAGIC     },
# MAGIC     {
# MAGIC         "table" : "uber_catalog.bronze.map_vehicle_types map_vehicle_types ",
# MAGIC         "select" : "map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute",
# MAGIC         "where" : "",
# MAGIC         "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
# MAGIC     }
# MAGIC ]
# MAGIC

# COMMAND ----------

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
                STREAM(uber_catalog.silver.stg_rides) WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
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

# save the query to a control table
spark.createDataFrame([(rendered_template,)], ["query_string"]) \
     .write.mode("overwrite") \
     .saveAsTable("uber_catalog.bronze.pipeline_config")

# dbutils.jobs.taskValues.set(key = "jinja_congif", value = jinja_config)



# COMMAND ----------

# print(rendered_template)
# df = spark.sql(rendered_template)
# display(df.limit(5))
# display(sorted(df.columns))
