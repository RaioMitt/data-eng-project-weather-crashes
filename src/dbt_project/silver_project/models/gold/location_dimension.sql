{{ config(schema='gold', materialized='table', tags=['gold']) }}

-- Gold dimension: one row per distinct county / municipality / settlement
-- found in silver crashes (SCD type 1: rebuilt from silver on every run).
-- Grain: one row per distinct location. The id is deterministic within a run
-- (sorted by county, municipality, settlement); the fact tables are rebuilt
-- in the same run, so they always receive matching ids.

select
    row_number() over (order by county, municipality, settlement)::integer as id,
    county,
    municipality,
    settlement
from (
    select distinct county, municipality, settlement
    from {{ ref('crashes') }}
    where county is not null
) as distinct_locations