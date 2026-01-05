-- ================================================================
-- FILE: db_scripts/dwh_schema.sql
-- MÔ HÌNH: STAR SCHEMA (Khớp 100% ảnh thiết kế)
-- ================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ----------------------------------------------------------------
-- PHẦN 1: STAGING (Thêm bảng Status cho đủ bộ)
-- ----------------------------------------------------------------

-- 1. Drivers
DROP TABLE IF EXISTS staging.drivers;
CREATE TABLE staging.drivers (
    driverId INT, driverRef VARCHAR(255), number INT, code VARCHAR(10),
    forename VARCHAR(255), surname VARCHAR(255), dob DATE, nationality VARCHAR(255), url VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    source_system VARCHAR(50)                           
);

-- 2. Constructors
DROP TABLE IF EXISTS staging.constructors;
CREATE TABLE staging.constructors (
    constructorId INT, constructorRef VARCHAR(255), name VARCHAR(255),
    nationality VARCHAR(255), url VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    source_system VARCHAR(50)                           
);

-- 3. Circuits
DROP TABLE IF EXISTS staging.circuits;
CREATE TABLE staging.circuits (
    circuitId INT, circuitRef VARCHAR(255), name VARCHAR(255), location VARCHAR(255),
    country VARCHAR(255), lat FLOAT, lng FLOAT, alt INT, url VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    source_system VARCHAR(50)                           
);

-- 4. Races
DROP TABLE IF EXISTS staging.races;
CREATE TABLE staging.races (
    raceId INT, year INT, round INT, circuitId INT, name VARCHAR(255),
    date DATE, time TIME, url VARCHAR(255),
    fp1_date DATE, fp1_time TIME, fp2_date DATE, fp2_time TIME,
    fp3_date DATE, fp3_time TIME, quali_date DATE, quali_time TIME,
    sprint_date DATE, sprint_time TIME,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50)                           
);

-- 5. Results
DROP TABLE IF EXISTS staging.results;
CREATE TABLE staging.results (
    resultId INT, raceId INT, driverId INT, constructorId INT,
    number INT, grid INT, position INT, positionText VARCHAR(255),
    positionOrder INT, points FLOAT, laps INT, time VARCHAR(255), milliseconds INT,
    fastestLap INT, rank INT, fastestLapTime VARCHAR(255), fastestLapSpeed VARCHAR(255),
    statusId INT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50)                           
);

-- 6. Status (Bảng MỚI cần thêm để khớp ảnh)
DROP TABLE IF EXISTS staging.status;
CREATE TABLE staging.status (
    statusId INT,
    status VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50)                       
);


-- ----------------------------------------------------------------
-- PHẦN 2: PUBLIC (Star Schema)
-- ----------------------------------------------------------------
-- 2.1 Dimension: Circuits
DROP TABLE IF EXISTS public.dim_circuits CASCADE;
CREATE TABLE public.dim_circuits (
    circuitId INT PRIMARY KEY, 
    name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(255)
);

-- 2.2 Dimension: Drivers
DROP TABLE IF EXISTS public.dim_drivers CASCADE;
CREATE TABLE public.dim_drivers (
    driverId INT PRIMARY KEY,   
    driver_name VARCHAR(255),  
    nationality VARCHAR(255),
    code VARCHAR(10)
);

-- 2.3 Dimension: Constructors
DROP TABLE IF EXISTS public.dim_constructors CASCADE;
CREATE TABLE public.dim_constructors (
    constructorId INT PRIMARY KEY, 
    name VARCHAR(255),
    nationality VARCHAR(255)
);

-- 2.4 Dimension: Races
DROP TABLE IF EXISTS public.dim_races CASCADE;
CREATE TABLE public.dim_races (
    raceId INT PRIMARY KEY,     
    year INT,
    race_name VARCHAR(255),     
    date DATE
);

-- 2.5 Dimension: Status 
DROP TABLE IF EXISTS public.dim_status CASCADE;
CREATE TABLE public.dim_status (
    statusId INT PRIMARY KEY, 
    status VARCHAR(255)
);

-- 2.6 Fact Table
DROP TABLE IF EXISTS public.fact_race_results;
CREATE TABLE public.fact_race_results (
    resultId INT PRIMARY KEY,
    
    -- Foreign Keys (5 nhánh nối ra 5 bảng Dim)
    raceId INT,            -- -> dim_races
    driverId INT,          -- -> dim_drivers
    constructorId INT,     -- -> dim_constructors
    circuitId INT,         -- -> dim_circuits (Lưu ý: Cần join Races để lấy ID này)
    statusId INT,          -- -> dim_status
    
    -- Measures
    grid INT,
    positionOrder INT,
    points FLOAT,
    laps INT,
    milliseconds INT
);