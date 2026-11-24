{{ config(schema='silver', materialized='table', tags=['silver']) }}
with src as (
    select data
    from {{ source('raw', 'weather_stations') }}
),

parsed as (
    select
        data ->> 'jaam_kood'                                              as station_code,
        coalesce(nullif(trim(data ->> 'jaam_nimi'), ''), data ->> 'jaam_kood') as name,
        nullif(data ->> 'pikkuskraad', '')::numeric(9,6)                  as x_coordinate,
        nullif(data ->> 'laiuskraad', '')::numeric(9,6)                   as y_coordinate,
        (data ->> 'jaam_periood_algus')::date                             as valid_from,
        nullif(data ->> 'jaam_periood_lopp', '')::date                    as valid_to
    from src
)

select
    station_code,
    name,
    x_coordinate,
    y_coordinate,
    valid_from,
    valid_to
from parsed
where station_code   is not null
  and valid_from     is not null
  and x_coordinate   is not null
  and y_coordinate   is not null
