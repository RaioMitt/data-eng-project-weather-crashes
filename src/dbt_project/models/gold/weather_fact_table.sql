{{ config(schema='gold', materialized='table', tags=['gold']) }}

-- Gold fact: hourly weather observations.
-- Grain: one row per weather station and hour.
-- Silver weather is already in UTC, so it is matched to the time dimension
-- directly on the UTC hour (date + hour).

select
    w.id,
    w.max_temperature,
    w.min_temperature,
    w.max_wind_speed,
    w.precipitation,
    w.sunshine,
    t.id as time_id,
    s.id as station_id
from {{ ref('weather_hourly') }} w
left join {{ ref('time_dimension') }} t
       on t.utc_timestamp = w.date + make_interval(hours => w.hour)
left join {{ ref('weather_station_dimension') }} s
       on s.station_code = w.station_code
      and w.date >= coalesce(s.valid_from, date '1900-01-01')
      and w.date <= coalesce(s.valid_to,   date '9999-12-31')