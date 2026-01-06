-- import các data bảng dim

INSERT INTO public.dim_circuits (circuitId, name, location, country)
SELECT 
    circuitId,
    TRIM(name) AS name,           -- Xóa khoảng trắng thừa
    TRIM(location) AS location,
    TRIM(country) AS country
FROM staging.circuits
WHERE circuitId IS NOT NULL       -- Chỉ lấy records hợp lệ
  AND name IS NOT NULL;

  INSERT INTO public.dim_drivers (driverId, driver_name, nationality, code)
SELECT 
    driverId,
    CONCAT(TRIM(forename), ' ', TRIM(surname)) AS driver_name,
    COALESCE(TRIM(nationality), 'Unknown') AS nationality,
    LEFT(COALESCE(code, driverRef), 10) AS code
FROM staging.drivers
WHERE driverId IS NOT NULL
  AND forename IS NOT NULL
  AND surname IS NOT NULL;


INSERT INTO public.dim_constructors (constructorId, name, nationality)
SELECT 
    constructorId,
    TRIM(name) AS name,
    COALESCE(TRIM(nationality), 'Unknown') AS nationality
FROM staging.constructors
WHERE constructorId IS NOT NULL
  AND name IS NOT NULL;

  INSERT INTO public.dim_status (statusId, status)
SELECT 
    statusId,
    TRIM(status) AS status
FROM staging.status
WHERE statusId IS NOT NULL
  AND status IS NOT NULL;

INSERT INTO public.dim_races (raceId, year, race_name, date)
SELECT 
    r.raceId,
    r.year,
    TRIM(r.name) AS race_name,
    r.date
FROM staging.races r
INNER JOIN staging.circuits c ON r.circuitId = c.circuitId  -- Validate FK
WHERE r.raceId IS NOT NULL
  AND r.date IS NOT NULL
  AND r.name IS NOT NULL;

  INSERT INTO public.fact_race_results (
    resultId, raceId, driverId, constructorId, circuitId, statusId,
    grid, positionOrder, points, laps, milliseconds
)
SELECT 
    res.resultId,
    res.raceId,
    res.driverId,
    res.constructorId,
    rac.circuitId,
    res.statusId,
    COALESCE(res.grid, 0) AS grid,
    COALESCE(res.positionOrder, 999) AS positionOrder,
    COALESCE(res.points, 0) AS points,
    COALESCE(res.laps, 0) AS laps,
    COALESCE(res.milliseconds, 0) AS milliseconds
FROM staging.results res
INNER JOIN staging.races rac ON res.raceId = rac.raceId
INNER JOIN staging.drivers dri ON res.driverId = dri.driverId
INNER JOIN staging.constructors con ON res.constructorId = con.constructorId
INNER JOIN staging.status sta ON res.statusId = sta.statusId
WHERE res.resultId IS NOT NULL
  AND res.raceId IS NOT NULL
  AND res.driverId IS NOT NULL
  AND res.constructorId IS NOT NULL;

-- kiểm tra dữ liệu các bảng dim 
-- 1. Kiểm tra dim_circuits
SELECT COUNT(*) FROM public.dim_circuits;
-- Kỳ vọng: ~77

SELECT * FROM public.dim_circuits LIMIT 5;

-- 2. Kiểm tra dim_drivers
SELECT COUNT(*) FROM public.dim_drivers;
-- Kỳ vọng: ~850

SELECT * FROM public.dim_drivers LIMIT 5;
-- Kiểm tra driver_name đã được CONCAT: "Lewis Hamilton"

-- 3. Kiểm tra dim_constructors
SELECT COUNT(*) FROM public.dim_constructors;
-- Kỳ vọng: ~210

-- 4. Kiểm tra dim_races
SELECT COUNT(*) FROM public.dim_races;
-- Kỳ vọng: ~1,100

-- 5. Kiểm tra dim_status
SELECT COUNT(*) FROM public.dim_status;
-- Kỳ vọng: ~139

-- Top 10 drivers theo tổng điểm
SELECT 
    d.driver_name,
    c.name AS constructor,
    COUNT(*) AS total_races,
    SUM(f.points) AS total_points
FROM public.fact_race_results f
JOIN public.dim_drivers d ON f.driverId = d.driverId
JOIN public.dim_constructors c ON f.constructorId = c.constructorId
GROUP BY d.driver_name, c.name
ORDER BY total_points DESC
LIMIT 10;

-- check validation
-- Kiểm tra orphan records trong fact table
SELECT 
    'Missing Drivers' AS check_type,
    COUNT(*) AS orphan_count
FROM public.fact_race_results f
WHERE NOT EXISTS (SELECT 1 FROM public.dim_drivers d WHERE d.driverId = f.driverId)

UNION ALL

SELECT 
    'Missing Constructors',
    COUNT(*)
FROM public.fact_race_results f
WHERE NOT EXISTS (SELECT 1 FROM public.dim_constructors c WHERE c.constructorId = f.constructorId)

UNION ALL

SELECT 
    'Missing Races',
    COUNT(*)
FROM public.fact_race_results f
WHERE NOT EXISTS (SELECT 1 FROM public.dim_races r WHERE r.raceId = f.raceId)

UNION ALL

SELECT 
    'Missing Circuits',
    COUNT(*)
FROM public.fact_race_results f
WHERE NOT EXISTS (SELECT 1 FROM public.dim_circuits c WHERE c.circuitId = f.circuitId)

UNION ALL

SELECT 
    'Missing Status',
    COUNT(*)
FROM public.fact_race_results f
WHERE NOT EXISTS (SELECT 1 FROM public.dim_status s WHERE s.statusId = f.statusId);

