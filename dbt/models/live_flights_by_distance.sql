with flights_filtered as (
    select 
        airport_origin_position_latitude,
        airport_origin_position_longitude,
        airport_destination_position_latitude,
        airport_destination_position_longitude
    from {{ source('flight_radar_dataset', 'flights') }}
    where status_live
)
select
    *,
    round(
        acos(
            sin(radians(airport_origin_position_latitude))
            * sin(radians(airport_destination_position_latitude))
            + (
                cos(radians(airport_origin_position_latitude))
                * cos(radians(airport_destination_position_latitude))
                * cos(radians(airport_origin_position_longitude)
                    - radians(airport_destination_position_longitude))
            )
        ) * 6371.0,
        4
    ) as distance_in_kms
from flights_filtered
order by distance_in_kms desc