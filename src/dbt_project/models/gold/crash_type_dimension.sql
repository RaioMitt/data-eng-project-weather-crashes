{{ config(
    schema='gold',
    materialized='table',
    tags=['gold'],
    post_hook="alter table {{ this }} add primary key (id)"
) }}

-- Gold dimension: one row per distinct crash type.
-- Grain: one row per crash type. The id is a surrogate key, deterministic within a run; rebuilt every run 
-- (alphabetical order), rebuilt on every run.

select
    row_number() over (order by crash_type)::integer as id,
    crash_type
from (
    select distinct crash_type
    from {{ ref('crashes') }}
    where crash_type is not null
) as distinct_types