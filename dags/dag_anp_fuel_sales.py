from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.etl import etl
from operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 12, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'raizen_challenge',
    schedule_interval=None,
    default_args=default_args
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

etl = PythonOperator(
    task_id='etl',
    python_callable=etl, 
    dag=dag
)

data_quality = DataQualityOperator(
    task_id='data_quality_check',
    table=['tb_anp_fuel_sales'],
    dag=dag,
    postgres_conn_id='postgres_conn'
)
start >> etl >> data_quality