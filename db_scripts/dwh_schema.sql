-- =============================================
-- DATABASE: f1_dwh (Port 5435)
-- MÔ HÌNH: Star Schema (Fact & Dimension)
-- =============================================

-- 1. Tạo Schema riêng cho Staging (Nơi chứa dữ liệu thô vừa tải về)
CREATE SCHEMA IF NOT EXISTS staging;

-- (Tùy chọn: Bạn sẽ cần tạo các bảng staging.races, staging.drivers... 
--  tương tự như bảng nguồn để Airflow đổ dữ liệu vào đây trước khi Transform.
--  Ở đây mình tập trung vào bảng kết quả cuối cùng (Public schema) trước).

-- 2. Tạo các bảng DIMENSION (Bảng vệ tinh - Chứa thông tin mô tả)
DROP TABLE IF EXISTS dim_circuits;
CREATE TABLE dim_circuits (
    circuit_id INT PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(255)
);

DROP TABLE IF EXISTS dim_drivers;
CREATE TABLE dim_drivers (
    driver_id INT PRIMARY KEY,
    driver_ref VARCHAR(255),
    number INT,
    code VARCHAR(10),
    fullname VARCHAR(255), -- Đã gộp forename + surname
    nationality VARCHAR(255),
    dob DATE
);

DROP TABLE IF EXISTS dim_constructors;
CREATE TABLE dim_constructors (
    constructor_id INT PRIMARY KEY,
    constructor_ref VARCHAR(255),
    name VARCHAR(255),
    nationality VARCHAR(255)
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT
);

-- 3. Tạo bảng FACT (Bảng sự kiện - Chứa số liệu để tính toán)
DROP TABLE IF EXISTS fact_race_results;
CREATE TABLE fact_race_results (
    result_id INT PRIMARY KEY,
    race_id INT,            -- FK tới bảng races (hoặc dim_date)
    driver_id INT,          -- FK tới dim_drivers
    constructor_id INT,     -- FK tới dim_constructors
    circuit_id INT,         -- FK tới dim_circuits
    date_id DATE,           -- FK tới dim_date
    
    -- Các chỉ số đo lường (Measures)
    grid_position INT,
    finish_position INT,
    points FLOAT,
    laps_completed INT,
    fastest_lap_time VARCHAR(255),
    milliseconds INT        -- Dùng để tính toán thời gian đua trung bình
);