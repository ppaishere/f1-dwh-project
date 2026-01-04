-- =============================================
-- DATABASE: f1_dwh (Port 5435)
-- =============================================

-- ---------------------------------------------
-- PHẦN 1: KHỞI TẠO SCHEMA
-- ---------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------
-- PHẦN 2: TẠO BẢNG STAGING (Vùng đệm chứa dữ liệu thô)
-- Lưu ý: Các bảng này cấu trúc y hệt nguồn, không cần Primary Key cứng
-- ---------------------------------------------

-- 2.1 Từ nguồn Logistics
DROP TABLE IF EXISTS staging.drivers;
CREATE TABLE staging.drivers (
    driverId INT,
    driverRef VARCHAR(255),
    number INT,
    code VARCHAR(10),
    forename VARCHAR(255),
    surname VARCHAR(255),
    dob DATE,
    nationality VARCHAR(255),
    url VARCHAR(255)
);

DROP TABLE IF EXISTS staging.constructors;
CREATE TABLE staging.constructors (
    constructorId INT,
    constructorRef VARCHAR(255),
    name VARCHAR(255),
    nationality VARCHAR(255),
    url VARCHAR(255)
);

DROP TABLE IF EXISTS staging.circuits;
CREATE TABLE staging.circuits (
    circuitId INT,
    circuitRef VARCHAR(255),
    name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(255),
    lat FLOAT,         
    lng FLOAT,         
    alt INT,           
    url VARCHAR(255)
);

-- 2.2 Từ nguồn Telemetry
DROP TABLE IF EXISTS staging.races;

CREATE TABLE staging.races (
    raceId INT,
    year INT,
    round INT,
    circuitId INT,
    name VARCHAR(255),
    date DATE,
    time TIME,
    url VARCHAR(255),
    fp1_date DATE,
    fp1_time TIME,
    fp2_date DATE,
    fp2_time TIME,
    fp3_date DATE,
    fp3_time TIME,
    quali_date DATE,
    quali_time TIME,
    sprint_date DATE,
    sprint_time TIME
);

DROP TABLE IF EXISTS staging.results;
CREATE TABLE staging.results (
    resultId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    grid INT,
    position INT,
    positionText VARCHAR(255),
    positionOrder INT,
    points FLOAT,
    laps INT,
    time VARCHAR(255),
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime VARCHAR(255),
    fastestLapSpeed VARCHAR(255),
    statusId INT
);

-- ---------------------------------------------
-- PHẦN 3: TẠO BẢNG PUBLIC (Star Schema cho Power BI)
-- ---------------------------------------------

-- 3.1 Dimensions
DROP TABLE IF EXISTS dim_circuits CASCADE;
CREATE TABLE dim_circuits (
    circuit_id INT PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(255)
);

DROP TABLE IF EXISTS dim_drivers CASCADE;
CREATE TABLE dim_drivers (
    driver_id INT PRIMARY KEY,
    driver_ref VARCHAR(255),
    number INT,
    code VARCHAR(10),
    fullname VARCHAR(255), -- Sẽ ghép từ forename + surname
    nationality VARCHAR(255),
    dob DATE
);

DROP TABLE IF EXISTS dim_constructors CASCADE;
CREATE TABLE dim_constructors (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(255),
    name VARCHAR(255),
    nationality VARCHAR(255)
);

DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT,
    day_name VARCHAR(20),
    month_name VARCHAR(20)
);

-- 3.2 Fact Table
DROP TABLE IF EXISTS fact_race_results;
CREATE TABLE fact_race_results (
    result_id INT PRIMARY KEY,
    race_id INT,            
    driver_id INT,          -- FK tới dim_drivers
    constructor_id INT,     -- FK tới dim_constructors
    circuit_id INT,         -- FK tới dim_circuits
    date_id DATE,           -- FK tới dim_date
    
    -- Measures
    grid_position INT,
    finish_position INT,
    points FLOAT,
    laps_completed INT,
    fastest_lap_time VARCHAR(255),
    milliseconds INT        -- Thời gian đua (ms)
);