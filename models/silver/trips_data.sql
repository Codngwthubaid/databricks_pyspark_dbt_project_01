-- Writing jinja code 

--incremental materialized 
{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

-- create a list of all the cols which we need for this file 
{% set cols = [ 
    "trip_id",
    "driver_id",
    "customer_id",
    "vehicle_id",
    "trip_start_time",
    "trip_end_time",
    "distance_km",
    "fare_amount",
    "last_updated_timestamp"
] %}


SELECT 
-- rendering data from cols array
    {% for col in cols %}   
        {{ col }}
        {% if not loop.last %},
        {% endif %}
    {% endfor %}
FROM 
-- this is the actual source
    {{ source("bronze_source","trips_data") }}

-- if condition checking incremental
{% if is_incremental() %}

WHERE last_updated_timestamp > (
    SELECT
       COALESCE(MAX(last_updated_timestamp), '1900-01-01')
    FROM 
        {{ this }}
)
{% endif %}
