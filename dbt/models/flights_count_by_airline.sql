select 
    airline_name,
    count(distinct(identification_number_default)) as cnt
from `flight_radar_dataset`.flights
where status_live is true
group by airline_name
order by cnt desc