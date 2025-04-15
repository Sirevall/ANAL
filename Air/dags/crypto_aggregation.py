from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 12, tzinfo=timezone.utc),  # Синхронизировано с первым DAG
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_aggregation_table():
    """Создание таблицы для агрегированных данных с временной зоной"""
    sql = """
    CREATE TABLE IF NOT EXISTS sirevall.DAGGER_AGGREGATED (
        hour_start TIMESTAMP WITH TIME ZONE NOT NULL,
        asset_id VARCHAR(50) NOT NULL,
        avg_price NUMERIC(20, 8) NOT NULL,
        min_price NUMERIC(20, 8) NOT NULL,
        max_price NUMERIC(20, 8) NOT NULL,
        PRIMARY KEY (hour_start, asset_id)
    );
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        logging.info("Таблица DAGGER_AGGREGATED создана/проверена")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка создания таблицы: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_hourly_stats(**context):
    """Расчет агрегированных статистик с UTC временем"""
    execution_date = context['execution_date'].replace(tzinfo=timezone.utc)
    hour_start = execution_date - timedelta(hours=1)
    
    sql = """
    INSERT INTO sirevall.DAGGER_AGGREGATED (
        hour_start, 
        asset_id, 
        avg_price, 
        min_price, 
        max_price
    )
    SELECT 
        DATE_TRUNC('hour', timestamp) AS hour_start,
        asset_id,
        AVG(price_usd) AS avg_price,
        MIN(price_usd) AS min_price,
        MAX(price_usd) AS max_price
    FROM sirevall.DAGGER
    WHERE timestamp >= %s AND timestamp < %s
    GROUP BY DATE_TRUNC('hour', timestamp), asset_id
    ON CONFLICT (hour_start, asset_id) 
    DO UPDATE SET
        avg_price = EXCLUDED.avg_price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price
    """
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql, (hour_start, execution_date))
        conn.commit()
        logging.info(f"Агрегированы данные за час {hour_start} UTC")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка агрегации: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def verify_aggregation(**context):
    """Проверка результатов агрегации"""
    execution_date = context['execution_date'].replace(tzinfo=timezone.utc)
    hour_start = execution_date - timedelta(hours=1)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT asset_id, avg_price, min_price, max_price
            FROM sirevall.DAGGER_AGGREGATED
            WHERE hour_start = %s
        """, (hour_start,))
        results = cursor.fetchall()
        if results:
            for row in results:
                logging.info(f"Агрегированные данные для {row[0]}: avg={row[1]}, min={row[2]}, max={row[3]}")
        else:
            logging.warning(f"Нет данных для часа {hour_start}")
    finally:
        cursor.close()
        conn.close()

with DAG(
    'crypto_hourly_aggregation',
    default_args=default_args,
    description='Агрегация часовых данных криптовалют',
    schedule_interval=None,  # Только по триггеру
    catchup=False,
    tags=['crypto', 'aggregation'],
) as dag:

    wait_for_data = ExternalTaskSensor(
        task_id='wait_for_raw_data',
        external_dag_id='crypto_hourly_loader',
        external_task_id='insert_crypto_data',  # Конкретная задача
        execution_date_fn=lambda dt: datetime.strptime(dt.strftime('%Y-%m-%dT%H:00:00')),  # Округление до часа
        allowed_states=['success'],
        poke_interval=60,
        timeout=3600,
        mode='poke',
        check_existence=True
    )

    create_agg_table_task = PythonOperator(
        task_id='create_aggregation_table',
        python_callable=create_aggregation_table,
    )

    calc_stats_task = PythonOperator(
        task_id='calculate_hourly_stats',
        python_callable=calculate_hourly_stats,
    )

    verify_task = PythonOperator(
        task_id='verify_aggregation',
        python_callable=verify_aggregation,
    )

    wait_for_data >> create_agg_table_task >> calc_stats_task >> verify_task