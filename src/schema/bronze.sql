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