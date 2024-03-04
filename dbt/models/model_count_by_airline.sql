select
    airline_name,
    aircraft_model_text,
    count(distinct(identification_number_default)) as cnt,
from `flight_radar_dataset`.flights
group by airline_name, aircraft_model_text
order by cnt desc
