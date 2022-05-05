from datetime import timedelta, datetime
import pytz
import airflow  
import time
from airflow import DAG  
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from jinja2 import Template

env_name = Variable.get("deploy_environment")

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ["abhra.gupta@trakstar.com", "brian.kasen@trakstar.com", "ritika.naidu@trakstar.com"],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    'Test-Email',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='45 5 * * *'
)

dag_name = 'Test-Email'

send_email_notification = EmailOperator(
        task_id="send_Email",
        trigger_rule='one_success',
        to=["abhra.gupta@trakstar.com"],
        subject='{}'.format(dag_name),
        html_content="{} DAG has failed in {}".format(dag_name, env_name ),
        dag=dag
    )