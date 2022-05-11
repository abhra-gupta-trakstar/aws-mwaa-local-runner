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
dag_name = "TS-Insights-Admin-Source-To-Stage-Daily"
email_receipients = ["abhra.gupta@trakstar.com"]

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': [ "bi_data_engineering@trakstar.com" ],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='15 7 * * *'
)

start_task = DummyOperator(task_id='start_task', dag=dag)

glue_task = AWSGlueJobOperator(  
    task_id="TS-Insights-Admin-Source-To-Stage",  
    job_name='ts-insights-admin-source-to-stage',  
    iam_role_name='_AWSGlueDataBrewS3Access',  
    dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

def _get_message(action) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "{} DAG {} on {} Mountain Time".format( dag_name, action, mt_time)

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

send_email_notification = EmailOperator(
        task_id="send_Email",
        trigger_rule='one_success',
        to=email_receipients,
        subject='{}-DAG - {} ALERT'.format(env_name, dag_name),
        html_content="{} DAG has failed in the {}-environment".format( dag_name, env_name),
        dag=dag
    ) 
start_task >> glue_task >> end_task >> send_slack_success_notification >> send_slack_failure_notification >> send_email_notification