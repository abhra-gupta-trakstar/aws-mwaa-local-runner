from datetime import timedelta 
import airflow  
from airflow import AirflowException
from airflow import DAG  
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['abhra.gupta@trakstar.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    'TS-Learn-Check-Trigger-Rule-Daily',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 12 * * *'
)

def folder_exists_and_not_empty(bucket, path) -> bool:
    '''Always Fail'''
    raise AirflowException("Partition Not Found")


account_table_partition_check_begin = DummyOperator(task_id='account_table_partition_check_begin', dag=dag)

account_table_partition_check_end = DummyOperator(task_id='account_table_partition_check_end', trigger_rule=TriggerRule.ALL_DONE, dag=dag)


python_task  = PythonOperator(task_id='find_partition', python_callable=folder_exists_and_not_empty,  dag=dag)

account_table_partition_check_begin >> account_table_partition_check_end

python_task >> account_table_partition_check_end