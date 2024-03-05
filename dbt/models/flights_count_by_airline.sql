select 
    airline_name,
    count(distinct(identification_number_default)) as cnt
from {{ source('flight_radar_dataset', 'flights') }}
where status_live
group by airline_name
order by cnt desc