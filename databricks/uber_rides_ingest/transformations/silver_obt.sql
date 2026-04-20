
-- CREATE OR REFRESH STREAMING TABLE uber_catalog.silver.silver_obt
-- AS
-- SELECT 
--         stg_rides.ride_id,
--         stg_rides.confirmation_number,
--         stg_rides.passenger_id,
--         stg_rides.driver_id,
--         stg_rides.vehicle_id,
--         stg_rides.pickup_location_id,
--         stg_rides.dropoff_location_id,
--         stg_rides.vehicle_type_id,
--         stg_rides.vehicle_make_id,
--         stg_rides.payment_method_id,
--         stg_rides.ride_status_id,
--         stg_rides.pickup_city_id,
--         stg_rides.dropoff_city_id,
--         stg_rides.cancellation_reason_id,
--         stg_rides.passenger_name,
--         stg_rides.passenger_email,
--         stg_rides.passenger_phone,
--         stg_rides.driver_name,
--         stg_rides.driver_rating,
--         stg_rides.driver_phone,
--         stg_rides.driver_license,
--         stg_rides.vehicle_model,
--         stg_rides.vehicle_color,
--         stg_rides.license_plate,
--         stg_rides.pickup_address,
--         stg_rides.pickup_latitude,
--         stg_rides.pickup_longitude,
--         stg_rides.dropoff_address,
--         stg_rides.dropoff_latitude,
--         stg_rides.dropoff_longitude,
--         stg_rides.distance_miles,
--         stg_rides.duration_minutes,
--         stg_rides.booking_timestamp,
--         stg_rides.pickup_timestamp,
--         stg_rides.dropoff_timestamp,
--         stg_rides.base_fare,
--         stg_rides.distance_fare,
--         stg_rides.time_fare,
--         stg_rides.surge_multiplier,
--         stg_rides.subtotal,
--         stg_rides.tip_amount,
--         stg_rides.total_fare,
--         stg_rides.rating

        
--                 ,  
        

--         map_cancellation_reasons.* EXCEPT (cancellation_reason_id)
        
--                 , 
        

--         pickup_city.city AS pickup_city_name, pickup_city.state AS pickup_state, pickup_city.region AS pickup_region, pickup_city.updated_at AS pickup_city_updated_at
        
--                 , 
        

--         dropoff_city.city AS dropoff_city_name, dropoff_city.state AS dropoff_state, dropoff_city.region AS dropoff_region, dropoff_city.updated_at AS dropoff_city_updated_at
        
--                 , 
        

--         map_payment_methods.* EXCEPT (payment_method_id)
        
--                 , 
        

--         map_ride_statuses.* EXCEPT (ride_status_id)
        
--                 , 
        

--         map_vehicle_makes.* EXCEPT (vehicle_make_id)
        
--                 , 
        

--         map_vehicle_types.* EXCEPT (vehicle_type_id)
        

-- FROM

        
--         STREAM(uber_catalog.silver.stg_rides)
--                 WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
--         -- uber_catalog.silver.stg_rides AS stg_rides

        
--         LEFT JOIN uber_catalog.bronze.map_cancellation_reasons map_cancellation_reasons ON stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_cities pickup_city ON stg_rides.pickup_city_id = pickup_city.city_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_cities dropoff_city ON stg_rides.dropoff_city_id = dropoff_city.city_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_payment_methods map_payment_methods ON stg_rides.payment_method_id = map_payment_methods.payment_method_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_ride_statuses map_ride_statuses ON stg_rides.ride_status_id = map_ride_statuses.ride_status_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_vehicle_makes map_vehicle_makes  ON stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
        

        
--         LEFT JOIN uber_catalog.bronze.map_vehicle_types map_vehicle_types  ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
        
        
    