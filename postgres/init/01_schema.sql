-- =============================================================
-- PostgreSQL Schema — Flight Delay Pipeline
-- IU International University of Applied Sciences
-- Module: Data Engineering (DLMDSEDE02) | Task 1 | Phase 2
-- Author: Yuvraj Shekhar
--
-- Defines:
--   1. Databases and users (spark_user WRITE, ml_readonly READ)
--   2. Four ML feature tables written by the Spark batch job
--
-- Auto-executed by PostgreSQL on first container start via
-- the docker-entrypoint-initdb.d mount in docker-compose.yml
-- =============================================================


-- =============================================================
-- 1. USER ACCESS CONTROL
-- =============================================================

-- Create read-only ML application user
CREATE USER ml_readonly WITH PASSWORD 'Ml_readonly@123';

-- spark_user already created by POSTGRES_USER env var
-- Grant spark_user full access to the feature database
GRANT ALL PRIVILEGES ON DATABASE flight_features TO spark_user;

-- =============================================================
-- 2. FEATURE TABLES
-- =============================================================

-- Switch to the flight_features database context
\connect flight_features;

-- Grant schema usage to both users
GRANT USAGE ON SCHEMA public TO spark_user;
GRANT USAGE ON SCHEMA public TO ml_readonly;


-- -------------------------------------------------------------
-- Table 1: airline_features
-- Quarterly delay statistics aggregated per airline
-- Written by: Spark batch job (Stage 4)
-- Read by:    ML application for carrier-level features
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS airline_features (
    id                      SERIAL PRIMARY KEY,
    airline                 VARCHAR(10)     NOT NULL,
    airline_name            VARCHAR(100),
    quarter                 VARCHAR(5)      NOT NULL,   -- Q1, Q2, Q3, Q4
    total_flights           INTEGER,
    delayed_flights         INTEGER,
    avg_arrival_delay_min   FLOAT,
    avg_departure_delay_min FLOAT,
    std_arrival_delay       FLOAT,
    delay_rate_pct          FLOAT,          -- % of flights delayed > 15 min
    avg_weather_delay_min   FLOAT,
    avg_carrier_delay_min   FLOAT,
    avg_nas_delay_min       FLOAT,
    processed_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_airline_features_airline
    ON airline_features(airline);
CREATE INDEX IF NOT EXISTS idx_airline_features_quarter
    ON airline_features(quarter);


-- -------------------------------------------------------------
-- Table 2: route_features
-- Quarterly delay statistics aggregated per route (OD pair)
-- Written by: Spark batch job (Stage 4)
-- Read by:    ML application for route-level features
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS route_features (
    id                      SERIAL PRIMARY KEY,
    route                   VARCHAR(10)     NOT NULL,   -- e.g. LAX-JFK
    origin_airport          VARCHAR(10),
    destination_airport     VARCHAR(10),
    origin_city             VARCHAR(100),
    destination_city        VARCHAR(100),
    quarter                 VARCHAR(5)      NOT NULL,
    total_flights           INTEGER,
    delayed_flights         INTEGER,
    avg_arrival_delay_min   FLOAT,
    avg_departure_delay_min FLOAT,
    delay_rate_pct          FLOAT,
    avg_distance_miles      FLOAT,
    avg_air_time_min        FLOAT,
    processed_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_route_features_route
    ON route_features(route);
CREATE INDEX IF NOT EXISTS idx_route_features_quarter
    ON route_features(quarter);
CREATE INDEX IF NOT EXISTS idx_route_features_origin
    ON route_features(origin_airport);


-- -------------------------------------------------------------
-- Table 3: airport_features
-- Quarterly departure performance per origin airport
-- Written by: Spark batch job (Stage 4)
-- Read by:    ML application for airport-level features
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS airport_features (
    id                          SERIAL PRIMARY KEY,
    origin_airport              VARCHAR(10)     NOT NULL,
    origin_airport_name         VARCHAR(200),
    origin_city                 VARCHAR(100),
    origin_state                VARCHAR(50),
    quarter                     VARCHAR(5)      NOT NULL,
    total_departures            INTEGER,
    delayed_departures          INTEGER,
    avg_departure_delay_min     FLOAT,
    departure_delay_rate_pct    FLOAT,
    avg_taxi_out_min            FLOAT,
    processed_at                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_airport_features_airport
    ON airport_features(origin_airport);
CREATE INDEX IF NOT EXISTS idx_airport_features_quarter
    ON airport_features(quarter);


-- -------------------------------------------------------------
-- Table 4: hourly_features
-- Delay patterns by hour of day and day of week
-- Written by: Spark batch job (Stage 4)
-- Read by:    ML application for temporal features
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hourly_features (
    id                          SERIAL PRIMARY KEY,
    scheduled_departure_hour    INTEGER,        -- 0-23
    day_of_week                 INTEGER,        -- 1=Monday, 7=Sunday
    quarter                     VARCHAR(5)      NOT NULL,
    total_flights               INTEGER,
    delayed_flights             INTEGER,
    avg_arrival_delay_min       FLOAT,
    delay_rate_pct              FLOAT,
    processed_at                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hourly_features_hour
    ON hourly_features(scheduled_departure_hour);
CREATE INDEX IF NOT EXISTS idx_hourly_features_dow
    ON hourly_features(day_of_week);


-- =============================================================
-- 3. GRANT READ-ONLY ACCESS TO ML USER
-- =============================================================

-- Grant SELECT on all current feature tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ml_readonly;

-- Ensure future tables created by spark_user are also readable
ALTER DEFAULT PRIVILEGES FOR USER spark_user IN SCHEMA public
    GRANT SELECT ON TABLES TO ml_readonly;


-- =============================================================
-- 4. PIPELINE AUDIT LOG
-- Tracks every Spark batch run for governance + observability
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(50)     NOT NULL,
    quarter         VARCHAR(5),
    stage           VARCHAR(50),
    status          VARCHAR(20),    -- SUCCESS, FAILED, RUNNING
    records_in      INTEGER,
    records_out     INTEGER,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_message   TEXT
);

GRANT SELECT ON pipeline_runs TO ml_readonly;
GRANT ALL    ON pipeline_runs TO spark_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO spark_user;

-- =============================================================
-- Done
-- =============================================================
\echo 'Schema initialised successfully.'