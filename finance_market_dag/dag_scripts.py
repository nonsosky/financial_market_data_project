from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 6, 8),
    'email' : 'nonsoskyokpara@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 3,
    'retries_delay' : timedelta(minutes=1)
    #'max_active_runs': 1
}

dag = DAG(
    'finance_Data_pipeline',
    default_args = default_args,
    description = 'This represents Finance Market Data Management pipeline',
    schedule="@daily",
    catchup=False
)

extraction = PythonOperator(
    task_id = 'extraction_layer',
    python_callable = run_extraction,
    dag=dag,
)

transformation = PythonOperator(
    task_id = 'transformation_layer',
    python_callable = run_transformation,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

loading = PythonOperator(
    task_id = 'loading_layer',
    python_callable = run_loading,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

extraction >> transformation >> loading