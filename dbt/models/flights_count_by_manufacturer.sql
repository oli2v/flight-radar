with temp as (
    select
        identification_number_default,
        split(aircraft_model_text, ' ')[0] as manufacturer
    from {{ source('flight_radar_dataset', 'flights') }}
)
select
    manufacturer,
    count(distinct(identification_number_default)) as cnt,
from temp
group by manufacturer
order by cnt desc