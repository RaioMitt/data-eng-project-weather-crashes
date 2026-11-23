{{ config(schema='silver', materialized='table', tags=['silver']) }}

{{ config(schema='silver', materialized='table', tags=['silver']) }}

-- Silver: hourly weather (depends on weather_stations), one row per station and hour (pivoted from long format).
-- Source: raw.weather_data_hourly. Same logic as the old silver.sql section 2.
-- Out-of-range / inconsistent values are set to NULL instead of failing the build.

with raw_data as (

    select *
    from {{ source('raw', 'weather_data_hourly') }}
    where aasta between 1900 and 2200
      and kuu   between 1 and 12
      and paev  between 1 and 31
      and tund  between 0 and 23
      -- keep only measurements from known stations (dependency on weather_stations)
      and jaam_kood in (select station_code from {{ ref('weather_stations') }})

),

pivoted as (

    select
        jaam_kood, aasta, kuu, paev, tund,
        max(case when element_kood in ('TAX1H',  'Air temperature (hourly max)')    then vaartus end) as max_temperature,
        max(case when element_kood in ('TAN1H',  'Air temperature (hourly min)')    then vaartus end) as min_temperature,
        max(case when element_kood in ('WSX1H',  'Wind gust (hourly max)')          then vaartus end) as max_wind_speed,
        max(case when element_kood in ('PR1H',   'Precipitation (hourly sum)')      then vaartus end) as precipitation,
        max(case when element_kood in ('SDUR1H', 'Sunshine duration (hourly sum)')  then vaartus end) as sunshine
    from raw_data
    group by jaam_kood, aasta, kuu, paev, tund

),

cleaned as (

    select
        jaam_kood, aasta, kuu, paev, tund,
        case when max_temperature between -60 and 50
                  and (min_temperature is null or min_temperature <= max_temperature)
             then max_temperature end as max_temperature,
        case when min_temperature between -60 and 50
                  and (max_temperature is null or min_temperature <= max_temperature)
             then min_temperature end as min_temperature,
        case when max_wind_speed between 0 and 80 then max_wind_speed end as max_wind_speed,
        case when precipitation >= 0              then precipitation  end as precipitation,
        case when sunshine between 0 and 60       then sunshine       end as sunshine
    from pivoted

)

select
    row_number() over (order by jaam_kood, aasta, kuu, paev, tund) as id,
    jaam_kood                         as station_code,
    make_date(aasta, kuu, paev)       as date,
    tund                              as hour,
    max_temperature,
    min_temperature,
    max_wind_speed,
    precipitation,
    sunshine
from cleaned
