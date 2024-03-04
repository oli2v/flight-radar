select
    count(distinct(identification_number_default)) as cnt,
    airline_name,
    aircraft_model_text
from `flight_radar_dataset`.flights
group by airline_name, aircraft_model_text
order by cnt desc
