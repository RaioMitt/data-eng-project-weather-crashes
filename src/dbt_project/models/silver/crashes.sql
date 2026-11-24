{{ config(schema='silver', materialized='table', tags=['silver']) }}

select
    juhtumi_id as id,

    case
        when kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'
            then replace(kahju_suurus, ',', '.')::numeric::integer
        when kahju_suurus ~* '(alla|below|alates|starting\s+from)'
            then substring(kahju_suurus from '\d+')::integer
    end as damage_amount,

    case
        when lower(trim(isikukahju)) in ('jah', 'yes', 'true', '1')  then true
        when lower(trim(isikukahju)) in ('ei', 'no', 'false', '0')   then false
    end as personal_injury,

    case
        when kuupaev ~ '^\d{4}-\d{2}-\d{2}$'   then kuupaev::date
        when kuupaev ~ '^\d{2}\.\d{2}\.\d{4}$' then to_date(kuupaev, 'DD.MM.YYYY')
    end as date,

    case when kellaaeg ~ '^\d{1,2}:\d{2}(:\d{2})?$' then kellaaeg::time end as time,

    maakond      as county,
    omavalitsus  as municipality,
    asula        as settlement,
    nullif(trim(situatsioon), '') as crash_type,

    case when x_koord ~ '^-?\d+([.,]\d+)?$' then replace(x_koord, ',', '.')::numeric::integer end as x_coordinate,
    case when y_koord ~ '^-?\d+([.,]\d+)?$' then replace(y_koord, ',', '.')::numeric::integer end as y_coordinate,

    case
        when kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'            then 'exact'
        when kahju_suurus ~* '(alla|below)'                    then 'below'
        when kahju_suurus ~* '(alates|starting\s+from)'        then 'starting from'
        when kahju_suurus is null or trim(kahju_suurus) = ''   then null
        else 'unknown'
    end as damage_qualifier

from {{ source('raw', 'crashes_raw') }}
where juhtumi_id is not null
