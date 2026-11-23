{{ config(
    schema='gold',
    materialized='table',
    tags=['gold'],
    post_hook="""
        alter table {{ this }}
            add primary key (id),
            add constraint crashes_fact_time_fk
                foreign key (time_id)      references {{ ref('time_dimension') }} (id),
            add constraint crashes_fact_location_fk
                foreign key (location_id)  references {{ ref('location_dimension') }} (id),
            add constraint crashes_fact_crashtype_fk
                foreign key (crashtype_id) references {{ ref('crash_type_dimension') }} (id),
            add constraint crashes_fact_station_fk
                foreign key (station_id)   references {{ ref('weather_station_dimension') }} (id)
    """
) }}

-- Gold fact: traffic crashes.
-- Grain: one row per crash (accident case).
-- Dimension keys are looked up from the gold dimensions:
--   time_id      crash date and time are Estonian local time, the time dimension is
--                keyed by UTC hour, so the crash time is converted to UTC and
--                truncated to the hour first
--   station_id   nearest weather station (straight-line distance in L-EST97 meters)
--                among the stations active on the crash date
-- Left joins keep every crash even if a lookup fails; the key is then NULL.
-- The post_hook adds the primary key and foreign keys to the dimensions after the
-- table is built (NULL keys are allowed by a foreign key).

select
    cr.id::bigint     as id,
    cr.damage_amount,
    cr.personal_injury,
    t.id              as time_id,
    l.id              as location_id,
    ct.id             as crashtype_id,
    cr.x_coordinate,
    cr.y_coordinate,
    st.station_id,
    cr.damage_qualifier
from {{ ref('crashes') }} cr
left join {{ ref('time_dimension') }} t
       on t.utc_timestamp = date_trunc(
              'hour',
              ((cr.date + cr.time) at time zone 'Europe/Tallinn') at time zone 'UTC')
left join {{ ref('location_dimension') }} l
       on l.county       is not distinct from cr.county
      and l.municipality is not distinct from cr.municipality
      and l.settlement   is not distinct from cr.settlement
left join {{ ref('crash_type_dimension') }} ct
       on ct.crash_type = cr.crash_type
left join lateral (
    select s.station_id
    from {{ ref('station_lest97') }} s
    where cr.x_coordinate is not null
      and cr.y_coordinate is not null
      and cr.date >= coalesce(s.valid_from, date '1900-01-01')
      and cr.date <= coalesce(s.valid_to,   date '9999-12-31')
    order by power(s.easting  - cr.x_coordinate, 2)
           + power(s.northing - cr.y_coordinate, 2)
    limit 1
) st on true