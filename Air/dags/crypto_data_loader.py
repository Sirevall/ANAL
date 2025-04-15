from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import logging
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 12, tzinfo=timezone.utc),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_table_if_not_exists():
    """Создание таблицы для сырых данных"""
    sql = """
    CREATE TABLE IF NOT EXISTS sirevall.DAGGER (
        asset_id VARCHAR(50) NOT NULL,
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        price_usd NUMERIC(20, 8) NOT NULL,
        PRIMARY KEY (asset_id, timestamp)
    );
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        logging.info("Таблица DAGGER создана/проверена")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка создания таблицы: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def get_top_2_crypto_assets():
    """Получение топ-2 криптовалют"""
    try:
        response = requests.get(
            "https://rest.coincap.io/v3/assets",
            params={
                'limit': 2,
                'apiKey': os.getenv('API_KEY')},
            timeout=10
        )
        response.raise_for_status()
        return [asset['id'] for asset in response.json()['data']]
    except Exception as e:
        logging.error(f"Ошибка при получении списка активов: {e}")
        raise

def fetch_hourly_crypto_data(**context):
    """Получение часовых данных в UTC"""
    assets = context['ti'].xcom_pull(task_ids='get_top_2_assets')
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    logging.info(f"Fetching data from {start_time} to {end_time} (UTC)")
    
    all_data = []
    for asset_id in assets:
        try:
            response = requests.get(
                f"https://rest.coincap.io/v3/assets/{asset_id}/history",
                params={
                    'interval': 'm5',
                    'start': int(start_time.timestamp() * 1000),
                    'end': int(end_time.timestamp() * 1000),
                    'apiKey': os.getenv('API_KEY')
                },
                timeout=15
            )
            response.raise_for_status()
            
            for entry in response.json()['data']:
                dt = datetime.fromtimestamp(entry['time'] / 1000, tz=timezone.utc)
                all_data.append((
                    asset_id,
                    dt,
                    float(entry['priceUsd'])
                ))
                
        except Exception as e:
            logging.error(f"Ошибка при получении данных для {asset_id}: {e}")
            continue
    
    if not all_data:
        logging.warning("No data fetched for any assets")
    else:
        logging.info(f"Fetched {len(all_data)} records total")
    
    return all_data

def insert_crypto_data(**context):
    """Вставка данных в PostgreSQL"""
    data = context['ti'].xcom_pull(task_ids='fetch_hourly_data')
    if not data:
        logging.warning("No data to insert")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        execute_values(
            cursor,
            """INSERT INTO sirevall.DAGGER (asset_id, timestamp, price_usd) 
            VALUES %s
            ON CONFLICT (asset_id, timestamp) DO NOTHING""",
            data,
            page_size=100
        )
        conn.commit()
        logging.info(f"Успешно вставлено {cursor.rowcount} записей")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при вставке данных: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

with DAG(
    'crypto_hourly_loader',
    default_args=default_args,
    description='Ежечасная загрузка данных о криптовалютах (UTC)',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'data_load'],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table_if_not_exists,
    )

    get_assets_task = PythonOperator(
        task_id='get_top_2_assets',
        python_callable=get_top_2_crypto_assets,
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_hourly_data',
        python_callable=fetch_hourly_crypto_data,
    )

    insert_data_task = PythonOperator(
        task_id='insert_crypto_data',
        python_callable=insert_crypto_data,
    )

    trigger_aggregation = TriggerDagRunOperator(
    task_id='trigger_aggregation',
    trigger_dag_id='crypto_hourly_aggregation',
    execution_date='{{ execution_date }}',  # Явная передача даты выполнения
    wait_for_completion=False,
    )

    create_table_task >> get_assets_task >> fetch_data_task >> insert_data_task >> trigger_aggregation