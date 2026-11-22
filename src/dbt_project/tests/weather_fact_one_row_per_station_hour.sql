-- Grain test for weather_fact_table: one row per weather station and hour.
-- Counted per station CODE, so a row matched to two overlapping station
-- periods (two different station ids) is caught as well.
-- The test fails if it returns any rows.
select
    s.station_code,
    f.time_id,
    count(*) as row_count
from {{ ref('weather_fact_table') }} f
join {{ ref('weather_station_dimension') }} s
  on s.id = f.station_id
group by s.station_code, f.time_id
having count(*) > 1