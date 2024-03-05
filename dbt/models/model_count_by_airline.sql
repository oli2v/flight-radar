select
    airline_name,
    aircraft_model_text,
    count(distinct(identification_number_default)) as cnt,
from {{ source('flight_radar_dataset', 'flights') }}
where airline_name is not null and aircraft_model_text is not null
group by airline_name, aircraft_model_text
order by airline_name, aircraft_model_text, cnt desc
