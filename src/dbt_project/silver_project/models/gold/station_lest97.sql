{{ config(materialized='ephemeral', tags=['gold']) }}

-- Helper (not a table in the database): weather station coordinates converted
-- from longitude/latitude in degrees to L-EST97 meters (EPSG:3301, Lambert
-- conformal conic, 2 standard parallels), the same system as the crash coordinates.
-- Used only to find the nearest station to each crash. PostGIS is not available
-- in our database image, so the standard formula is written out in plain SQL.

with params as (
    select
        6378137.0::double precision                                   as a,
        sqrt(2 * (1 / 298.257222101::double precision)
             - power(1 / 298.257222101::double precision, 2))         as e,
        radians(57.5175539305556::double precision)                   as p0,  -- latitude of origin
        radians(59.3333333333333::double precision)                   as p1,  -- standard parallel 1
        radians(58.0::double precision)                               as p2,  -- standard parallel 2
        radians(24.0::double precision)                               as l0,  -- central meridian
        500000.0::double precision                                    as e0,  -- false easting
        6375000.0::double precision                                   as n0   -- false northing
),

step1 as (
    select
        p.*,
        cos(p1) / sqrt(1 - power(e, 2) * power(sin(p1), 2)) as m1,
        cos(p2) / sqrt(1 - power(e, 2) * power(sin(p2), 2)) as m2,
        tan(pi() / 4 - p0 / 2) / power((1 - e * sin(p0)) / (1 + e * sin(p0)), e / 2) as t0,
        tan(pi() / 4 - p1 / 2) / power((1 - e * sin(p1)) / (1 + e * sin(p1)), e / 2) as t1,
        tan(pi() / 4 - p2 / 2) / power((1 - e * sin(p2)) / (1 + e * sin(p2)), e / 2) as t2
    from params p
),

step2 as (
    select s.*, (ln(m1) - ln(m2)) / (ln(t1) - ln(t2)) as n
    from step1 s
),

step3 as (
    select s.*, m1 / (n * power(t1, n)) as cf
    from step2 s
),

constants as (
    select s.*, a * cf * power(t0, n) as rf
    from step3 s
),

stations as (
    select
        st.id as station_id,
        st.valid_from,
        st.valid_to,
        radians(st.x_coordinate::double precision) as lon,
        radians(st.y_coordinate::double precision) as lat
    from {{ ref('weather_station_dimension') }} st
),

projected as (
    select
        s.station_id,
        s.valid_from,
        s.valid_to,
        k.a * k.cf * power(
            tan(pi() / 4 - s.lat / 2)
            / power((1 - k.e * sin(s.lat)) / (1 + k.e * sin(s.lat)), k.e / 2),
            k.n) as r,
        k.n * (s.lon - k.l0) as th,
        k.e0, k.n0, k.rf
    from stations s
    cross join constants k
)

select
    station_id,
    valid_from,
    valid_to,
    e0 + r * sin(th)      as easting,
    n0 + rf - r * cos(th) as northing
from projected