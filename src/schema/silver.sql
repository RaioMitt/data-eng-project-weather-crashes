CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.weather_stations (
    station_code          varchar(50)     NOT NULL,
    name                  varchar(500),
    x_coordinate          numeric(9,6),
    y_coordinate          numeric(9,6),
    elevation             numeric(6,1),
    valid_from            date            NOT NULL,
    valid_to              date,
    raw_data              jsonb,
    loaded_at             timestamptz     DEFAULT now(),
    updated_at            timestamptz     DEFAULT now(),
    PRIMARY KEY (station_code, valid_from),

    CONSTRAINT chk_ws_x      CHECK (x_coordinate IS NULL OR x_coordinate BETWEEN -180 AND 180),
    CONSTRAINT chk_ws_y      CHECK (y_coordinate IS NULL OR y_coordinate BETWEEN  -90 AND  90),
    CONSTRAINT chk_ws_elev   CHECK (elevation    IS NULL OR elevation    BETWEEN  -50 AND 500),
    CONSTRAINT chk_ws_period CHECK (valid_to     IS NULL OR valid_to >= valid_from)
);

CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    station_code    varchar(50) NOT NULL,
    year            integer     NOT NULL,
    month           integer     NOT NULL,
    day             integer     NOT NULL,
    hour            integer     NOT NULL,
    max_temperature numeric,
    min_temperature numeric,
    max_wind_speed  numeric,
    precipitation   numeric,
    sunshine        numeric,
    loaded_at       timestamptz DEFAULT now(),
    PRIMARY KEY (station_code, year, month, day, hour),

    CONSTRAINT chk_wh_year     CHECK (year  BETWEEN 1900 AND 2200),
    CONSTRAINT chk_wh_month    CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_wh_day      CHECK (day   BETWEEN 1 AND 31),
    CONSTRAINT chk_wh_hour     CHECK (hour  BETWEEN 0 AND 23),
    CONSTRAINT chk_wh_tmax     CHECK (max_temperature IS NULL OR max_temperature BETWEEN -60 AND 50),
    CONSTRAINT chk_wh_tmin     CHECK (min_temperature IS NULL OR min_temperature BETWEEN -60 AND 50),
    CONSTRAINT chk_wh_tmin_le_tmax CHECK (
        min_temperature IS NULL OR max_temperature IS NULL
        OR min_temperature <= max_temperature
    ),
    CONSTRAINT chk_wh_wind     CHECK (max_wind_speed IS NULL OR max_wind_speed BETWEEN 0 AND 80),
    CONSTRAINT chk_wh_precip   CHECK (precipitation  IS NULL OR precipitation  >= 0),
    CONSTRAINT chk_wh_sunshine CHECK (sunshine       IS NULL OR (sunshine >= 0 AND sunshine <= 60))
);

CREATE TABLE IF NOT EXISTS silver.crashes (
    id                 bigint        NOT NULL,
    date               date,
    time               time,
    weekday            varchar(50),
    crash_type         varchar(2000),
    damage_amount      integer,
    damage_qualifier   varchar(30),
    personal_injury    boolean,
    county             varchar(255),
    municipality       varchar(500),
    settlement         varchar(500),
    x_coordinate       integer,
    y_coordinate       integer,
    extract_date       date,
    loaded_at          timestamptz DEFAULT now(),
    PRIMARY KEY (id),

    CONSTRAINT chk_cr_damage_amt CHECK (damage_amount    IS NULL OR damage_amount >= 0),
    CONSTRAINT chk_cr_damage_q   CHECK (damage_qualifier IS NULL OR damage_qualifier IN ('exact','below','starting from','unknown')),
    CONSTRAINT chk_cr_date       CHECK (date             IS NULL OR date BETWEEN DATE '2000-01-01' AND DATE '2200-12-31')
);

INSERT INTO silver.weather_stations (
    station_code, name,
    x_coordinate, y_coordinate, elevation,
    valid_from, valid_to, raw_data
)
SELECT
    data ->> 'jaam_kood',
    data ->> 'jaam_nimi',
    NULLIF(data ->> 'pikkuskraad','')::numeric(9,6),
    NULLIF(data ->> 'laiuskraad','')::numeric(9,6),
    NULLIF(data ->> 'korgus_merepinnast_m','')::numeric(6,1),
    (data ->> 'jaam_periood_algus')::date,
    NULLIF(data ->> 'jaam_periood_lopp','')::date,
    data
FROM raw.weather_stations
WHERE data ->> 'jaam_kood'          IS NOT NULL
  AND data ->> 'jaam_periood_algus' IS NOT NULL
ON CONFLICT (station_code, valid_from) DO UPDATE
    SET name         = EXCLUDED.name,
        x_coordinate = EXCLUDED.x_coordinate,
        y_coordinate = EXCLUDED.y_coordinate,
        elevation    = EXCLUDED.elevation,
        valid_to     = EXCLUDED.valid_to,
        raw_data     = EXCLUDED.raw_data,
        updated_at   = now();

WITH pivoted AS (
    SELECT
        jaam_kood, aasta, kuu, paev, tund,
        MAX(CASE WHEN element_kood IN ('TEMP_MAX','TMAX')      THEN vaartus END) AS max_temperature,
        MAX(CASE WHEN element_kood IN ('TEMP_MIN','TMIN')      THEN vaartus END) AS min_temperature,
        MAX(CASE WHEN element_kood IN ('WIND_GUST','WS_MAX')   THEN vaartus END) AS max_wind_speed,
        MAX(CASE WHEN element_kood IN ('PRECIP','RR')          THEN vaartus END) AS precipitation,
        MAX(CASE WHEN element_kood IN ('SUNSHINE','SUN','SSD') THEN vaartus END) AS sunshine
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
INSERT INTO silver.weather_hourly (
    station_code, year, month, day, hour,
    max_temperature, min_temperature, max_wind_speed, precipitation, sunshine
)
SELECT
    jaam_kood, aasta, kuu, paev, tund,
    max_temperature, min_temperature, max_wind_speed, precipitation, sunshine
FROM cleaned
ON CONFLICT (station_code, year, month, day, hour) DO UPDATE
    SET max_temperature = EXCLUDED.max_temperature,
        min_temperature = EXCLUDED.min_temperature,
        max_wind_speed  = EXCLUDED.max_wind_speed,
        precipitation   = EXCLUDED.precipitation,
        sunshine        = EXCLUDED.sunshine;

INSERT INTO silver.crashes (
    id, date, time, weekday, crash_type,
    damage_amount, damage_qualifier, personal_injury,
    county, municipality, settlement,
    x_coordinate, y_coordinate, extract_date
)
SELECT
    juhtumi_id,
    CASE
        WHEN kuupaev ~ '^\d{4}-\d{2}-\d{2}$'   THEN kuupaev::date
        WHEN kuupaev ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(kuupaev,'DD.MM.YYYY')
    END,
    CASE WHEN kellaaeg ~ '^\d{1,2}:\d{2}(:\d{2})?$' THEN kellaaeg::time END,
    nadalapaev,
    situatsioon,
    CASE
        WHEN kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'                THEN replace(kahju_suurus,',','.')::numeric::integer
        WHEN kahju_suurus ~* '(alla|below|alates|starting\s+from)' THEN substring(kahju_suurus FROM '\d+')::integer
    END,
    CASE
        WHEN kahju_suurus ~ '^\s*\d+([.,]\d+)?\s*$'                THEN 'exact'
        WHEN kahju_suurus ~* '(alla|below)'                        THEN 'below'
        WHEN kahju_suurus ~* '(alates|starting\s+from)'            THEN 'starting from'
        WHEN kahju_suurus IS NULL OR trim(kahju_suurus) = ''       THEN NULL
        ELSE 'unknown'
    END,
    CASE
        WHEN lower(trim(isikukahju)) IN ('jah','yes','true','1')   THEN TRUE
        WHEN lower(trim(isikukahju)) IN ('ei','no','false','0')    THEN FALSE
    END,
    maakond,
    omavalitsus,
    asula,
    CASE WHEN x_koord ~ '^-?\d+([.,]\d+)?$' THEN replace(x_koord,',','.')::numeric::integer END,
    CASE WHEN y_koord ~ '^-?\d+([.,]\d+)?$' THEN replace(y_koord,',','.')::numeric::integer END,
    CASE
        WHEN valjavotte_kuupaev ~ '^\d{4}-\d{2}-\d{2}$'   THEN valjavotte_kuupaev::date
        WHEN valjavotte_kuupaev ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(valjavotte_kuupaev,'DD.MM.YYYY')
    END
FROM raw.crashes_raw
WHERE juhtumi_id IS NOT NULL
ON CONFLICT (id) DO UPDATE
    SET date             = EXCLUDED.date,
        time             = EXCLUDED.time,
        weekday          = EXCLUDED.weekday,
        crash_type       = EXCLUDED.crash_type,
        damage_amount    = EXCLUDED.damage_amount,
        damage_qualifier = EXCLUDED.damage_qualifier,
        personal_injury  = EXCLUDED.personal_injury,
        county           = EXCLUDED.county,
        municipality     = EXCLUDED.municipality,
        settlement       = EXCLUDED.settlement,
        x_coordinate     = EXCLUDED.x_coordinate,
        y_coordinate     = EXCLUDED.y_coordinate,
        extract_date     = EXCLUDED.extract_date;

CREATE OR REPLACE VIEW silver.v_unmatched_elements AS
SELECT element_kood AS element_code, COUNT(*) AS row_count
FROM raw.weather_data_hourly
GROUP BY element_kood
ORDER BY row_count DESC;