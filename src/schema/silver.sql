CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.crashes         CASCADE;
DROP TABLE IF EXISTS silver.weather_hourly  CASCADE;
DROP TABLE IF EXISTS silver.weather_stations CASCADE;
DROP VIEW  IF EXISTS silver.v_unmatched_elements;

-- ============================================================
-- 1) WEATHER STATIONS
-- ============================================================
CREATE TABLE silver.weather_stations (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_code  varchar(50)  NOT NULL,
    name          varchar(500) NOT NULL,
    x_coordinate  numeric(9,6) NOT NULL,
    y_coordinate  numeric(9,6) NOT NULL,
    valid_from    date         NOT NULL,
    valid_to      date,
    CONSTRAINT uq_ws UNIQUE (station_code, valid_from),
    CONSTRAINT chk_ws_x CHECK (x_coordinate BETWEEN -180 AND 180),
    CONSTRAINT chk_ws_y CHECK (y_coordinate BETWEEN  -90 AND  90),
    CONSTRAINT chk_ws_period CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

INSERT INTO silver.weather_stations (station_code, name, x_coordinate, y_coordinate, valid_from, valid_to)
SELECT
    data ->> 'jaam_kood',
    COALESCE(NULLIF(trim(data ->> 'jaam_nimi'), ''), data ->> 'jaam_kood'),
    NULLIF(data ->> 'pikkuskraad','')::numeric(9,6),
    NULLIF(data ->> 'laiuskraad','')::numeric(9,6),
    (data ->> 'jaam_periood_algus')::date,
    NULLIF(data ->> 'jaam_periood_lopp','')::date
FROM raw.weather_stations
WHERE data ->> 'jaam_kood'          IS NOT NULL
  AND data ->> 'jaam_periood_algus' IS NOT NULL
  AND NULLIF(data ->> 'pikkuskraad','')::numeric IS NOT NULL
  AND NULLIF(data ->> 'laiuskraad','')::numeric  IS NOT NULL
ON CONFLICT (station_code, valid_from) DO UPDATE
    SET name = EXCLUDED.name,
        x_coordinate = EXCLUDED.x_coordinate,
        y_coordinate = EXCLUDED.y_coordinate,
        valid_to = EXCLUDED.valid_to;

-- ============================================================
-- 2) WEATHER HOURLY
-- ============================================================
CREATE TABLE silver.weather_hourly (
    id              integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_code    varchar(50) NOT NULL,
    date            date        NOT NULL,
    hour            integer     NOT NULL,
    max_temperature numeric,
    min_temperature numeric,
    max_wind_speed  numeric,
    precipitation   numeric,
    sunshine        numeric,
    CONSTRAINT uq_wh UNIQUE (station_code, date, hour),
    CONSTRAINT chk_wh_hour   CHECK (hour BETWEEN 0 AND 23),
    CONSTRAINT chk_wh_tmax   CHECK (max_temperature IS NULL OR max_temperature BETWEEN -60 AND 50),
    CONSTRAINT chk_wh_tmin   CHECK (min_temperature IS NULL OR min_temperature BETWEEN -60 AND 50),
    CONSTRAINT chk_wh_tmin_le_tmax CHECK (
        min_temperature IS NULL OR max_temperature IS NULL
        OR min_temperature <= max_temperature
    ),
    CONSTRAINT chk_wh_wind   CHECK (max_wind_speed IS NULL OR max_wind_speed BETWEEN 0 AND 80),
    CONSTRAINT chk_wh_precip CHECK (precipitation  IS NULL OR precipitation  >= 0),
    CONSTRAINT chk_wh_sun    CHECK (sunshine       IS NULL OR (sunshine >= 0 AND sunshine <= 60))
);

WITH pivoted AS (
    SELECT
        jaam_kood, aasta, kuu, paev, tund,
        MAX(CASE WHEN element_kood IN ('TAX1H', 'Air temperature (hourly max)') THEN vaartus END) AS max_temperature,
        MAX(CASE WHEN element_kood IN ('TAN1H', 'Air temperature (hourly min)') THEN vaartus END) AS min_temperature,
        MAX(CASE WHEN element_kood IN ('WSX1H', 'Wind gust (hourly max)')      THEN vaartus END) AS max_wind_speed,
        MAX(CASE WHEN element_kood IN ('PR1H',  'Precipitation (hourly sum)')  THEN vaartus END) AS precipitation,
        MAX(CASE WHEN element_kood IN ('SDUR1H','Sunshine duration (hourly sum)') THEN vaartus END) AS sunshine
    FROM raw.weather_data_hourly
    WHERE aasta BETWEEN 1900 AND 2200
      AND kuu   BETWEEN 1 AND 12
      AND paev  BETWEEN 1 AND 31
      AND tund  BETWEEN 0 AND 23
    GROUP BY jaam_kood, aasta, kuu, paev, tund
),
cleaned AS (
    SELECT
        jaam_kood, aasta, kuu, paev, tund,
        CASE WHEN max_temperature BETWEEN -60 AND 50
                  AND (min_temperature IS NULL OR min_temperature <= max_temperature)
             THEN max_temperature END AS max_temperature,
        CASE WHEN min_temperature BETWEEN -60 AND 50
                  AND (max_temperature IS NULL OR min_temperature <= max_temperature)
             THEN min_temperature END AS min_temperature,
        CASE WHEN max_wind_speed BETWEEN 0 AND 80 THEN max_wind_speed END AS max_wind_speed,
        CASE WHEN precipitation >= 0            THEN precipitation  END AS precipitation,
        CASE WHEN sunshine BETWEEN 0 AND 60     THEN sunshine       END AS sunshine
    FROM pivoted
)
INSERT INTO silver.weather_hourly (station_code, date, hour,
    max_temperature, min_temperature, max_wind_speed, precipitation, sunshine)
SELECT
    jaam_kood,
    make_date(aasta, kuu, paev),
    tund,
    max_temperature, min_temperature, max_wind_speed, precipitation, sunshine
FROM cleaned
ON CONFLICT (station_code, date, hour) DO UPDATE
    SET max_temperature = EXCLUDED.max_temperature,
        min_temperature = EXCLUDED.min_temperature,
        max_wind_speed  = EXCLUDED.max_wind_speed,
        precipitation   = EXCLUDED.precipitation,
        sunshine        = EXCLUDED.sunshine;

-- ============================================================
-- 3) CRASHES
-- ============================================================
CREATE TABLE silver.crashes (
    id               bigint      PRIMARY KEY,
    damage_amount    integer,
    personal_injury  boolean,
    date             date,
    time             time,
    county           varchar(255),
    municipality     varchar(500),
    settlement       varchar(500),
    crash_type       varchar(500),  -- <--- MUUDETUD: 50 asemel 500
    x_coordinate     integer,
    y_coordinate     integer,
    station_code     varchar(50),
    damage_qualifier varchar(20),
    CONSTRAINT chk_cr_amount CHECK (damage_amount IS NULL OR damage_amount >= 0),
    CONSTRAINT chk_cr_qual   CHECK (damage_qualifier IS NULL OR damage_qualifier IN ('exact','below','starting from','unknown')),
    CONSTRAINT chk_cr_date   CHECK (date IS NULL OR date BETWEEN DATE '2000-01-01' AND DATE '2200-12-31')
);

INSERT INTO silver.crashes (
    id, damage_amount, personal_injury, date, time,
    county, municipality, settlement, crash_type,
    x_coordinate, y_coordinate, station_code, damage_qualifier
)
SELECT
    juhtumi_id,
    CASE
        WHEN kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'
            THEN replace(kahju_suurus,',','.')::numeric::integer
        WHEN kahju_suurus ~* '(alla|below|alates|starting\s+from)'
            THEN substring(kahju_suurus FROM '\d+')::integer
    END,
    CASE
        WHEN lower(trim(isikukahju)) IN ('jah','yes','true','1') THEN TRUE
        WHEN lower(trim(isikukahju)) IN ('ei','no','false','0')  THEN FALSE
    END,
    CASE
        WHEN kuupaev ~ '^\d{4}-\d{2}-\d{2}$'   THEN kuupaev::date
        WHEN kuupaev ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(kuupaev,'DD.MM.YYYY')
    END,
    CASE WHEN kellaaeg ~ '^\d{1,2}:\d{2}(:\d{2})?$' THEN kellaaeg::time END,
    maakond, omavalitsus, asula,
    NULLIF(trim(situatsioon), ''),  -- <--- MUUDETUD: eemaldatud left(..., 50)
    CASE WHEN x_koord ~ '^-?\d+([.,]\d+)?$' THEN replace(x_koord,',','.')::numeric::integer END,
    CASE WHEN y_koord ~ '^-?\d+([.,]\d+)?$' THEN replace(y_koord,',','.')::numeric::integer END,
    NULL,
    CASE
        WHEN kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'          THEN 'exact'
        WHEN kahju_suurus ~* '(alla|below)'                  THEN 'below'
        WHEN kahju_suurus ~* '(alates|starting\s+from)'      THEN 'starting from'
        WHEN kahju_suurus IS NULL OR trim(kahju_suurus) = '' THEN NULL
        ELSE 'unknown'
    END
FROM raw.crashes_raw
WHERE juhtumi_id IS NOT NULL
ON CONFLICT (id) DO UPDATE
    SET damage_amount    = EXCLUDED.damage_amount,
        personal_injury  = EXCLUDED.personal_injury,
        date             = EXCLUDED.date,
        time             = EXCLUDED.time,
        county           = EXCLUDED.county,
        municipality     = EXCLUDED.municipality,
        settlement       = EXCLUDED.settlement,
        crash_type       = EXCLUDED.crash_type,
        x_coordinate     = EXCLUDED.x_coordinate,
        y_coordinate     = EXCLUDED.y_coordinate,
        station_code     = EXCLUDED.station_code,
        damage_qualifier = EXCLUDED.damage_qualifier;

CREATE OR REPLACE VIEW silver.v_unmatched_elements AS
SELECT element_kood AS element_code, COUNT(*) AS row_count
FROM raw.weather_data_hourly
GROUP BY element_kood
ORDER BY row_count DESC;