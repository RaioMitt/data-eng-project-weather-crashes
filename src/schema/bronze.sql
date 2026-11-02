CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_data_hourly (
    jaam_kood    varchar(10)  NOT NULL,
    jaam_nimi    varchar(100),
    aasta        integer      NOT NULL,
    kuu          integer      NOT NULL,
    paev         integer      NOT NULL,
    tund         integer      NOT NULL,
    element_kood varchar(10)  NOT NULL,
    vaartus      double precision,
    loaded_at    timestamptz  DEFAULT now(),
    PRIMARY KEY (jaam_kood, aasta, kuu, paev, tund, element_kood)
);
-- Weather statio metadata from f_kliima_jaam_vaatlus, one JSONB document per
-- station period. Key columns are derived from the JSON so the loader only
-- inserts `data`. Loaded by the DAG fetch_station_data_pipeline.
CREATE TABLE IF NOT EXISTS raw.weather_stations (
    data               jsonb       NOT NULL,
    jaam_kood          text        GENERATED ALWAYS AS (data ->> 'jaam_kood') STORED NOT NULL,
    jaam_periood_algus text        GENERATED ALWAYS AS (data ->> 'jaam_periood_algus') STORED NOT NULL,
    loaded_at          timestamptz DEFAULT now(),
    updated_at         timestamptz DEFAULT now(),
    PRIMARY KEY (jaam_kood, jaam_periood_algus)
);


CREATE TABLE IF NOT EXISTS raw.crashes_raw (
    juhtumi_id         BIGINT PRIMARY KEY,
    kuupaev            TEXT,
    kellaaeg           TEXT,
    nadalapaev         TEXT,
    situatsioon        TEXT,
    kahju_suurus       TEXT,
    isikukahju         TEXT,
    x_koord            TEXT,
    y_koord            TEXT,
    maakond            TEXT,
    omavalitsus        TEXT,
    asula              TEXT,
    valjavotte_kuupaev TEXT,
    loaded_at          TIMESTAMPTZ DEFAULT now()
);