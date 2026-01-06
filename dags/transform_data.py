from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

# ==================== CẤU HÌNH DAG ====================
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'f1_02_transform_dwh',
    default_args=default_args,
    description='Transform staged data into star schema',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['f1', 'transform', 'dwh']
)

# ==================== HÀM VALIDATION ====================
def validate_dimension_count(table_name):
    """Kiểm tra số lượng records trong dimension table"""
    hook = PostgresHook(postgres_conn_id='conn_f1_dwh')
    count = hook.get_first(f"SELECT COUNT(*) FROM public.{table_name}")[0]
    
    logging.info(f"📊 {table_name}: {count} records")
    
    if count == 0:
        raise ValueError(f"⚠️ {table_name} is empty!")
    
    return count

def validate_fact_count():
    """Kiểm tra fact table và foreign key integrity"""
    hook = PostgresHook(postgres_conn_id='conn_f1_dwh')
    count = hook.get_first("SELECT COUNT(*) FROM public.fact_race_results")[0]
    
    logging.info(f"📊 fact_race_results: {count} records")
    
    if count == 0:
        raise ValueError("⚠️ fact_race_results is empty!")
    
    # Kiểm tra foreign key integrity
    orphans_query = """
    SELECT 
        (SELECT COUNT(*) FROM fact_race_results f 
         WHERE NOT EXISTS (SELECT 1 FROM dim_drivers d WHERE d.driverId = f.driverId)) AS orphan_drivers,
        (SELECT COUNT(*) FROM fact_race_results f 
         WHERE NOT EXISTS (SELECT 1 FROM dim_constructors c WHERE c.constructorId = f.constructorId)) AS orphan_constructors,
        (SELECT COUNT(*) FROM fact_race_results f 
         WHERE NOT EXISTS (SELECT 1 FROM dim_races r WHERE r.raceId = f.raceId)) AS orphan_races,
        (SELECT COUNT(*) FROM fact_race_results f 
         WHERE NOT EXISTS (SELECT 1 FROM dim_circuits c WHERE c.circuitId = f.circuitId)) AS orphan_circuits,
        (SELECT COUNT(*) FROM fact_race_results f 
         WHERE NOT EXISTS (SELECT 1 FROM dim_status s WHERE s.statusId = f.statusId)) AS orphan_status
    """
    
    result = hook.get_first(orphans_query)
    
    if any(result):
        logging.warning(f"⚠️ Orphan records detected: Drivers={result[0]}, Constructors={result[1]}, Races={result[2]}, Circuits={result[3]}, Status={result[4]}")
    else:
        logging.info("✅ All foreign keys are valid!")
    
    return count

# ==================== TRANSFORM DIMENSIONS ====================

# 1. Transform dim_circuits
transform_circuits = PostgresOperator(
    task_id='transform_dim_circuits',
postgres_conn_id='conn_f1_dwh',
    sql="""
    -- Truncate và rebuild
    TRUNCATE TABLE public.dim_circuits CASCADE;
    
    INSERT INTO public.dim_circuits (circuitId, name, location, country)
    SELECT 
        circuitId,
        TRIM(name) AS name,
        TRIM(location) AS location,
        TRIM(country) AS country
    FROM staging.circuits
    WHERE circuitId IS NOT NULL
      AND name IS NOT NULL;
    """,
    dag=dag
)

# 2. Transform dim_drivers
transform_drivers = PostgresOperator(
    task_id='transform_dim_drivers',
    postgres_conn_id='conn_f1_dwh',
    sql="""
    TRUNCATE TABLE public.dim_drivers CASCADE;
    
    INSERT INTO public.dim_drivers (driverId, driver_name, nationality, code)
    SELECT 
        driverId,
        CONCAT(TRIM(forename), ' ', TRIM(surname)) AS driver_name,
        COALESCE(TRIM(nationality), 'Unknown') AS nationality,
        COALESCE(code, driverRef) AS code
    FROM staging.drivers
    WHERE driverId IS NOT NULL
      AND forename IS NOT NULL
      AND surname IS NOT NULL;
    """,
    dag=dag
)

# 3. Transform dim_constructors
transform_constructors = PostgresOperator(
    task_id='transform_dim_constructors',
    postgres_conn_id='conn_f1_dwh',
    sql="""
    TRUNCATE TABLE public.dim_constructors CASCADE;
    
    INSERT INTO public.dim_constructors (constructorId, name, nationality)
    SELECT 
        constructorId,
        TRIM(name) AS name,
        COALESCE(TRIM(nationality), 'Unknown') AS nationality
    FROM staging.constructors
    WHERE constructorId IS NOT NULL
      AND name IS NOT NULL;
    """,
    dag=dag
)

# 4. Transform dim_status
transform_status = PostgresOperator(
    task_id='transform_dim_status',
    postgres_conn_id='conn_f1_dwh',
    sql="""
    TRUNCATE TABLE public.dim_status CASCADE;
    
    INSERT INTO public.dim_status (statusId, status)
    SELECT 
        statusId,
        TRIM(status) AS status
    FROM staging.status
    WHERE statusId IS NOT NULL
      AND status IS NOT NULL;
    """,
    dag=dag
)

# 5. Transform dim_races (phụ thuộc vào dim_circuits)
transform_races = PostgresOperator(
    task_id='transform_dim_races',
    postgres_conn_id='conn_f1_dwh',
    sql="""
    TRUNCATE TABLE public.dim_races CASCADE;
    
    INSERT INTO public.dim_races (raceId, year, race_name, date)
    SELECT 
        r.raceId,
        r.year,
        TRIM(r.name) AS race_name,
        r.date
    FROM staging.races r
    INNER JOIN staging.circuits c ON r.circuitId = c.circuitId
    WHERE r.raceId IS NOT NULL
      AND r.date IS NOT NULL
      AND r.name IS NOT NULL;
    """,
    dag=dag
)

# ==================== VALIDATE DIMENSIONS ====================
validate_circuits = PythonOperator(
    task_id='validate_dim_circuits',
    python_callable=validate_dimension_count,
    op_kwargs={'table_name': 'dim_circuits'},
    dag=dag
)

validate_drivers = PythonOperator(
    task_id='validate_dim_drivers',
    python_callable=validate_dimension_count,
op_kwargs={'table_name': 'dim_drivers'},
    dag=dag
)

validate_constructors = PythonOperator(
    task_id='validate_dim_constructors',
    python_callable=validate_dimension_count,
    op_kwargs={'table_name': 'dim_constructors'},
    dag=dag
)

validate_status = PythonOperator(
    task_id='validate_dim_status',
    python_callable=validate_dimension_count,
    op_kwargs={'table_name': 'dim_status'},
    dag=dag
)

validate_races = PythonOperator(
    task_id='validate_dim_races',
    python_callable=validate_dimension_count,
    op_kwargs={'table_name': 'dim_races'},
    dag=dag
)

# ==================== TRANSFORM FACT TABLE ====================
transform_fact = PostgresOperator(
    task_id='transform_fact_race_results',
    postgres_conn_id='conn_f1_dwh',
    sql="""
    TRUNCATE TABLE public.fact_race_results;
    
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
        
        -- Measures
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
    """,
    dag=dag
)

# ==================== VALIDATE FACT TABLE ====================
validate_fact = PythonOperator(
    task_id='validate_fact_race_results',
    python_callable=validate_fact_count,
    dag=dag
)
