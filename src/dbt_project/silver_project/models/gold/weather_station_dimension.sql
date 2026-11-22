{{ config(schema='gold', materialized='table', tags=['gold']) }}

-- Gold dimension: weather stations (SCD type 2).
-- Grain: one row per station and validity period. The history comes from the
-- source: a station that moves gets a new row with its own valid_from/valid_to
-- (valid_to is NULL while the station is still active).
-- Coordinates are longitude (x) and latitude (y) in degrees, unlike the crash
-- coordinates, which are L-EST97 meters.

select
    row_number() over (order by station_code, valid_from)::integer as id,
    station_code,
    name,
    x_coordinate,
    y_coordinate,
    valid_from,
    valid_to
from {{ ref('weather_stations') }}