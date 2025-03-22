from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random

def print_random_number():
    number = random.randint(1, 100)
    print(f"Случайное число: {number}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'random_number_dag',
    default_args=default_args,
    description='DAG для вывода случайного числа каждую минуту',
    schedule_interval=timedelta(minutes=1),
    catchup=False,  # Отключаем выполнение пропущенных задач
)

print_random_number_task = PythonOperator(
    task_id='print_random_number',
    python_callable=print_random_number,
    dag=dag,
)

print_random_number_task
