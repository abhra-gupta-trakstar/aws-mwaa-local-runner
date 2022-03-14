from datetime import timedelta, datetime
import pytz
import airflow  
from airflow import DAG  
from airflow.operators.dummy import DummyOperator
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator


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

send_slack_success_notification = SlackWebhookOperator(
        task_id="send_slack_success_notification",
        trigger_rule='all_success',
        http_conn_id="slack_conn",
        message=_get_message("completed successfully"),
        channel="#airflow-monitoring-{}".format(env_name),
        dag=dag
    )

send_slack_failure_notification = SlackWebhookOperator(
        task_id="send_slack_failure_notification",
        trigger_rule='one_failed',
        http_conn_id="slack_conn",
        message=_get_message("failed"),
        channel="#airflow-monitoring-{}".format(env_name),
        dag=dag
    )
    
end_task = DummyOperator(task_id='end_task', dag=dag)

send_email_notification = EmailOperator(
        task_id="send_Email",
        trigger_rule='one_success',
        to=["abhra.gupta@trakstar.com", "brian.kasen@trakstar.com"],
        subject='TS-Learn-Source-To-Stage-Daily',
        html_content="TS-Learn-Source-To-Stage-Daily DAG has failed",
        dag=dag
    )
start_task >> glue_task >> end_task >> send_slack_success_notification >> send_slack_failure_notification >> send_email_notification