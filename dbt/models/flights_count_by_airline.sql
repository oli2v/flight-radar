select 
    airline_name,
    count(distinct(identification_number_default)) as cnt
from {{ source('flight_radar_dataset', 'flights') }}
where status_live and airline_name is not null
group by airline_name
order by cnt desc