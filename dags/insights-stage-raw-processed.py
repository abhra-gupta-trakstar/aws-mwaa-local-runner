from datetime import timedelta, datetime
import email
import pytz
import airflow  
from airflow import DAG  
from airflow.operators.dummy import DummyOperator
from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.operators.email import EmailOperator

env_name = Variable.get("deploy_environment")
dag_name = "TS-Insights-Admin-Stage-To-Raw-And-Raw-To-Processed-Daily"
email_receipients = ["abhra.gupta@trakstar.com"]

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': email_receipients,
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='45 7 * * *'
)

def _get_message(action) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "{} DAG {} on {} Mountain Time".format( dag_name ,action, mt_time)


tables = [ 'customers', 'customer_product_instances', 'insights_users', 'products', 'quicksight_sessions' ]

begin_DAG = DummyOperator(task_id='begin_DAG', dag=dag)
stop_DAG = DummyOperator(task_id='stop_DAG', dag=dag)
send_email_notification = EmailOperator(
        task_id="send_Email",
        trigger_rule='one_success',
        to=email_receipients,
        subject='{}-DAG - {} ALERT'.format( env_name, dag_name),
        html_content="{} DAG has failed in the {}-environment".format( dag_name, env_name),
        dag=dag
    )

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
for table in tables:
    start_task = DummyOperator(task_id='start-{}'.format(table), dag=dag)

    stage_to_raw_task = AWSGlueJobOperator(  
        task_id="TS-Insights-Admin-Stage-To-Raw-{}".format(table),  
        job_name='ts-insights-admin-{}-stage-to-raw'.format(table),  
        iam_role_name='_AWSGlueDataBrewS3Access',  
        dag=dag)
    
    r_to_p_task = AWSGlueJobOperator(  
        task_id="TS-Insights-Admin-Raw-To-Processed-{}".format(table),  
        job_name='ts-insights-admin-{}-raw-to-processed'.format(table),  
        iam_role_name='_AWSGlueDataBrewS3Access',  
        dag=dag)

    end_task = DummyOperator(task_id='end-{}'.format(table), dag=dag)

    begin_DAG >> start_task >> stage_to_raw_task >> r_to_p_task >> end_task >> stop_DAG

stop_DAG >> send_slack_success_notification >> send_slack_failure_notification >> send_email_notification