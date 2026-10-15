SELECT 
    *
FROM    
    {{ source("bronze_source", "trips_data") }}