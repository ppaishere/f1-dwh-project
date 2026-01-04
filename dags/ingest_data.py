from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# 1. Hàm chung để chép dữ liệu từ Nguồn -> Staging
def ingest_table(source_conn_id, source_table, target_conn_id, target_table):
    # Kết nối tới nguồn (Source)
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    # Kết nối tới đích (DWH)
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)
    
    # --- ĐOẠN CODE KIỂM TRA (DEBUG) ---
    # Lấy thông tin kết nối thực tế để xem nó đang chui vào đâu
    conn = source_hook.get_conn()
    print(f"\n--- [DEBUG INFO] ---")
    print(f"Task đang chạy với Connection ID: {source_conn_id}")
    print(f"Đang kết nối tới DB Name: {conn.info.dbname}") # <--- QUAN TRỌNG
    print(f"Đang kết nối tới Host: {conn.info.host}")
    print(f"Đang tìm bảng: {source_table}")
    print(f"--------------------\n")
    # ----------------------------------

    # Lấy dữ liệu
    print(f"Extracting data from {source_table}...")
    records = source_hook.get_records(sql=f"SELECT * FROM {source_table}")
    
    # Xóa dữ liệu cũ ở Staging (để tránh trùng lặp khi chạy lại)
    print(f"Clearing staging table {target_table}...")
    target_hook.run(f"TRUNCATE TABLE {target_table}")
    
    # Đổ dữ liệu mới vào
    print(f"Loading {len(records)} rows into {target_table}...")
    target_hook.insert_rows(table=target_table, rows=records)

# 2. Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False 
}

with DAG('01_ingest_raw_data', default_args=default_args, schedule_interval=None) as dag:

    # --- NHÓM 1: Lấy từ Logistics (Port 5434) ---
    task_load_drivers = PythonOperator(
        task_id='load_drivers',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_logistics',
            'source_table': 'public.drivers',      # Đã thêm public. (CHUẨN)
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.drivers'
        }
    )

    task_load_circuits = PythonOperator(
        task_id='load_circuits',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_logistics',
            'source_table': 'public.circuits',
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.circuits'
        }
    )

    task_load_constructors = PythonOperator(
        task_id='load_constructors',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_logistics',
            'source_table': 'public.constructors',
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.constructors'
        }
    )

    # --- NHÓM 2: Lấy từ Telemetry (Port 5433) ---
    task_load_races = PythonOperator(
        task_id='load_races',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_telemetry',
            'source_table': 'public.races',
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.races'
        }
    )

    task_load_results = PythonOperator(
        task_id='load_results',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_telemetry',
            'source_table': 'public.results',
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.results'
        }
    )