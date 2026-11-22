{{ config(schema='gold', materialized='table', tags=['gold']) }}

-- Gold dimension: time (SCD type 0, calendar values never change).
-- Grain: one row per UTC hour. Weather is stored in UTC, crashes in Estonian
-- time, so the key of this table is the UTC hour (utc_timestamp). All the other
-- calendar columns (date, hour, weekday, is_weekend ...) are in ESTONIAN time.
-- Because of the clock changes, (date, hour) is NOT unique: in October the local
-- hour 3 occurs twice (two different UTC hours), and in March local hour 3 is missing.

with bounds as (
    select
        least((select min(date) from {{ ref('weather_hourly') }}),
              (select min(date) from {{ ref('crashes') }}) - 1) as first_day,
        greatest((select max(date) from {{ ref('weather_hourly') }}),
                 (select max(date) from {{ ref('crashes') }})) as last_day
),

utc_hours as (
    select generate_series(
               first_day::timestamp,
               last_day::timestamp + interval '23 hours',
               interval '1 hour') as utc_timestamp
    from bounds
),

local_hours as (
    select
        utc_timestamp,
        (utc_timestamp at time zone 'UTC') at time zone 'Europe/Tallinn' as local_ts
    from utc_hours
)

select
    row_number() over (order by utc_timestamp)::integer as id,
    local_ts::date                                      as date,
    extract(year  from local_ts)::integer               as year,
    extract(month from local_ts)::integer               as month,
    extract(day   from local_ts)::integer               as day,
    trim(to_char(local_ts, 'Day'))                      as weekday,
    extract(hour  from local_ts)::integer               as hour,
    extract(isodow from local_ts) in (6, 7)             as is_weekend,
    utc_timestamp
from local_hours