"""Data ingestion example"""
from datetime import timedelta
import requests
import time
import logging


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# Handy dandy date util
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Cool alerting options!
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
}

dag = DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='An example data ingestion pipeline.',
    schedule_interval='@daily', # Cron job syntax can also be used here
    dagrun_timeout=timedelta(minutes=30))
dag.doc_md = __doc__

def get_json_data(url: str):
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception('Failed response: {}'.format(resp.status_code))
    json_response = resp.json()
    if not json_response:
        logging.warning('got a weird response: {}'.format(json_response))
    # Knowing the headers of our data sources will help us create an aggregated schema
    headers = json_response[0].keys()
    # For now let's just print them, we can see these in the task logs
    print(headers)

get_cdc_data_task = PythonOperator(
    task_id='get_cdc_data',
    python_callable=get_json_data,
    op_kwargs={'url': 'https://data.cdc.gov/resource/ks3g-spdg.json'},
    dag=dag,
)

def get_other_data():
    print('TODO')

get_other_data_task = PythonOperator(
    task_id='get_other_data',
    python_callable=get_other_data,
    dag=dag,
)

sleepy_bash_task = BashOperator(
    task_id='sleepy_bash',
    depends_on_past=False,
    bash_command='sleep 15',
    retries=3,
    dag=dag,
)
sleepy_bash_task.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

def sleeper():
    time.sleep(3)
    
sleepy_python_task = PythonOperator(
    task_id='sleeping_python',
    python_callable=sleeper,
    dag=dag,
)

def print_done():
    print('Done!')

print_done_task = PythonOperator(
    task_id='done',
    python_callable=print_done,
    dag=dag,
)
# This will create task connections
[get_cdc_data_task, sleepy_bash_task, get_other_data_task] >> sleepy_python_task >> print_done_task