from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from airflow.providers.postgres.hooks.postgres import PostgresHook # pyright: ignore[reportMissingImports]
from datetime import datetime

# 1. Hàm chung để chép dữ liệu từ Nguồn -> Staging
def ingest_table(source_conn_id, source_table, target_conn_id, target_table):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)
    
    # 1. Lấy dữ liệu
    print(f"Extracting data from {source_table}...")
    records = source_hook.get_records(sql=f"SELECT * FROM {source_table}")
    
    # 2. Bổ sung Metadata: load_timestamp và source_system
    load_timestamp = datetime.now()
    # Thêm 2 cột metadata vào cuối mỗi row (tuple)
    enriched_records = [tuple(list(row) + [load_timestamp, source_conn_id]) for row in records]
    
    # 3. Xóa và nạp lại
    print(f"Clearing staging table {target_table}...")
    target_hook.run(f"TRUNCATE TABLE {target_table}")
    
    # 4. Lưu ý: target_hook.insert_rows cần biết danh sách cột nếu table đích có nhiều cột hơn source
    # Hoặc bạn phải đảm bảo cấu trúc bảng staging đã có sẵn 2 cột này ở cuối
    print(f"Loading {len(enriched_records)} rows into {target_table} with metadata...")
    target_hook.insert_rows(table=target_table, rows=enriched_records)

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
            'source_table': 'public.drivers',     
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

    task_load_status = PythonOperator(
        task_id='load_status',
        python_callable=ingest_table,
        op_kwargs={
            'source_conn_id': 'conn_f1_telemetry',  # Lấy từ Telemetry
            'source_table': 'public.status',        # Bảng nguồn
            'target_conn_id': 'conn_f1_dwh',
            'target_table': 'staging.status'        # Bảng đích
        }
    )