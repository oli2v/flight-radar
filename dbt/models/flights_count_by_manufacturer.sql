with temp as (
    select
        identification_number_default,
        split(aircraft_model_text, ' ')[0] as manufacturer
)
select
    count(distinct(identification_number_default)) as cnt,
    manufacturer
from temp
group by manufacturer
order by cnt desc