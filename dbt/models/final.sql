with zone as (
    select * from {{ ref('stg_zone') }}
),
taxi_data as (
    select * from {{ ref('stg_ny_green_taxi') }}
)

select vendorid, lpep_pickup_datetime, pulocationid, service_zone
from taxi_data
left join zone
on taxi_data.pulocationid = zone.locationid