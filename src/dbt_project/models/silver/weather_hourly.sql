{{ config(schema='silver', materialized='table', tags=['silver']) }}
with raw_data as (
    select *
    from {{ source('raw', 'weather_data_hourly') }}
    where aasta between 1900 and 2200
      and kuu   between 1 and 12
      and paev  between 1 and 31
      and tund  between 0 and 23
      and jaam_kood in (select station_code from {{ ref('weather_stations') }})
),

pivoted as (
    select
        jaam_kood, aasta, kuu, paev, tund,
        max(case when element_kood = 'TAX1H'  then vaartus end) as max_temperature,
        max(case when element_kood = 'TAN1H'  then vaartus end) as min_temperature,
        max(case when element_kood = 'WSX1H'  then vaartus end) as max_wind_speed,
        max(case when element_kood = 'PR1H'   then vaartus end) as precipitation,
        max(case when element_kood = 'SDUR1H' then vaartus end) as sunshine
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
