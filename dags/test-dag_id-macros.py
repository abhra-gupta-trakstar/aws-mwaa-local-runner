from datetime import timedelta, datetime
import pytz
import airflow  
import time
from airflow import DAG  
from airflow.operators.dummy import DummyOperator


default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ["abhra.gupta@trakstar.com"],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    'Test-dag_id-macros',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='45 5 * * *'
)

def _get_message(action) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "TS-SalesForce-Stage-To-Raw-And-Raw-To-Processed-Daily DAG {} AT {} Mountain Time".format(action, mt_time)

begin_DAG = DummyOperator(task_id='begin_DAG_{}'.format({{ dag.dag_id }}), dag=dag)
begin_DAG
