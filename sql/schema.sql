-- Star schema for the air-quality warehouse (DuckDB locally, Athena/Redshift on AWS)

-- Drop in dependency order (fact first) so the load is safely re-runnable.
DROP TABLE IF EXISTS fact_air_quality;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_city;

CREATE TABLE dim_city (
    city_key    INTEGER PRIMARY KEY,
    city_name   VARCHAR NOT NULL,
    state       VARCHAR NOT NULL,
    latitude    DOUBLE,
    longitude   DOUBLE
);

CREATE TABLE dim_time (
    time_key     BIGINT PRIMARY KEY,      -- yyyymmddHH
    observed_at  TIMESTAMP NOT NULL,
    date         DATE NOT NULL,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    hour         INTEGER,
    day_of_week  VARCHAR,
    is_weekend   BOOLEAN,
    time_of_day  VARCHAR                  -- Night / Morning / Afternoon / Evening
);

CREATE TABLE fact_air_quality (
    city_key          INTEGER NOT NULL REFERENCES dim_city(city_key),
    time_key          BIGINT  NOT NULL REFERENCES dim_time(time_key),
    pm2_5             DOUBLE,
    pm10              DOUBLE,
    carbon_monoxide   DOUBLE,
    nitrogen_dioxide  DOUBLE,
    ozone             DOUBLE,
    aqi_category      VARCHAR,
    PRIMARY KEY (city_key, time_key)
);
