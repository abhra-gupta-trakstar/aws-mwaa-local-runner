from datetime import timedelta, datetime
import pytz
import airflow  
from airflow import DAG  
from airflow.operators.dummy import DummyOperator
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator


env_name = Variable.get("deploy_environment")


default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ["abhra.gupta@trakstar.com", "brian.kasen@trakstar.com"],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    'TS-Learn-Source-To-Stage-Daily',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='0 8 * * *'
)

start_task = DummyOperator(task_id='start_task', dag=dag)

glue_task = AWSGlueJobOperator(  
    task_id="TS-Learn-Source-To-Stage",  
    job_name='ts-learn-source-to-stage',  
    iam_role_name='_AWSGlueDataBrewS3Access',  
    dag=dag)

def _get_message(action) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "TS-Learn-Source-To-Stage-Daily DAG {} on {} Mountain Time".format(action, mt_time)


    
end_task = DummyOperator(task_id='end_task', dag=dag)


start_task >> glue_task >> end_task