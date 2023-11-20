import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import timedelta

default_args = {
    'owner': 'hsayram',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'btc_rate_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # every 5 minutes
)

def get_btc_rate(**kwargs):
    try:
        url = "https://api.coincap.io/v2/rates/bitcoin"
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad requests

        btc_rate = response.json()['data']['rateUsd']
        kwargs['ti'].xcom_push(key='btc_rate', value=btc_rate)
        kwargs['ti'].xcom_push(key='datetime', value=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    except requests.exceptions.RequestException as e:
        print(f"Error fetching BTC rate: {e}")

get_btc_rate_task = PythonOperator(
    task_id='get_btc_rate',
    python_callable=get_btc_rate,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_otus',
    sql='CREATE TABLE IF NOT EXISTS btc_rate (datetime TIMESTAMP, btc_rate FLOAT);',
    dag=dag,
)

write_to_table_task = PostgresOperator(
    task_id='write_to_table',
    postgres_conn_id='postgres_otus',
    sql='''INSERT INTO btc_rate (datetime, btc_rate) VALUES ('{{ task_instance.xcom_pull(task_ids="get_btc_rate", key="datetime") }}', '{{ task_instance.xcom_pull(task_ids="get_btc_rate", key="btc_rate") }}');''',
     dag=dag,
)

get_btc_rate_task >> create_table_task >> write_to_table_task

