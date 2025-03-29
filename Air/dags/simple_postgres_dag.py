from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Определяем DAG
with DAG(
    dag_id='simple_postgres_dag',
    start_date=datetime(2024, 3, 29),  # Установите актуальную дату
    schedule_interval='@daily',  # Запуск каждый день
    catchup=False,
    tags=['postgres', 'example'],
) as dag:
    
    # Оператор выполнения SQL-запроса
    select_task = PostgresOperator(
        task_id='select_1',
        postgres_conn_id='postgres_default',  # Укажите ваш connection ID
        sql='SELECT 1;',
    )

    select_task
