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
    'TS-SalesForce-Stage-To-Raw-And-Raw-To-Processed-Daily',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='45 5 * * *'
)

dag_name = 'TS-SalesForce-Stage-To-Raw-And-Raw-To-Processed-Daily'


def _get_message(action, dag_name, **kwargs) -> str:
    context = kwargs
    print(dag_name)
    dag_id = context['dag_run'].dag_id
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "{} DAG {} AT {} Mountain Time".format(dag_id, action, mt_time)

tables = [
    'account'
]
template = Template("{{ params.dag_name }}")

# Method 1: pass values as a dict
params = {'dag_name': dag_name}
print(template.render({'params': params}))

dag_id_template  = Template("{{ dag.dag_id }}")

print(template.render({'params': params}))

# Method 2: pass values as keyword arguments
print(template.render(params={'dag_name': dag_name}))
begin_DAG = DummyOperator(task_id='begin_DAG', dag=dag)
python_task  = PythonOperator(task_id='python_task', python_callable=_get_message, provide_context=True, op_args=[ "completed" , template.render({'params': params}) ], dag=dag)
python_task >> begin_DAG
stop_DAG = DummyOperator(task_id='stop_DAG', dag=dag)
send_email_notification = EmailOperator(
        task_id="send_Email",
        trigger_rule='one_success',
        to=["abhra.gupta@trakstar.com"],
        subject='{}'.format(dag_name),
        html_content="{} DAG has failed in {}".format(dag_name, env_name ),
        dag=dag
    )

# send_slack_success_notification = SlackWebhookOperator(
#         task_id="send_slack_success_notification",
#         trigger_rule='all_success',
#         http_conn_id="slack_conn",
#         message=_get_message("completed successfully"),
#         channel="#airflow-monitoring-{}".format(env_name),
#         dag=dag
#     )

# send_slack_failure_notification = SlackWebhookOperator(
#         task_id="send_slack_failure_notification",
#         trigger_rule='one_failed',
#         http_conn_id="slack_conn",
#         message=_get_message("failed"),
#         channel="#airflow-monitoring-{}".format(env_name),
#         dag=dag
#     )

for table in tables:
    start_task = DummyOperator(task_id='start-{}'.format(table), dag=dag)

    stage_to_raw_task = AWSGlueJobOperator(  
        task_id="TS-Salesforce-Stage-To-Raw-{}".format(table),  
        job_name='ts-salesforce-{}-stage-to-raw'.format(table),  
        iam_role_name='_AWSGlueDataBrewS3Access',  
        dag=dag)
    
    r_to_p_task = AWSGlueJobOperator(  
        task_id="TS-Salesforce-Raw-To-Processed-{}".format(table),  
        job_name='ts-salesforce-{}-raw-to-processed'.format(table),  
        iam_role_name='_AWSGlueDataBrewS3Access',  
        dag=dag)

    end_task = DummyOperator(task_id='end-{}'.format(table), dag=dag)

    begin_DAG >> start_task >> stage_to_raw_task >> r_to_p_task >> end_task >> stop_DAG

stop_DAG >> send_email_notification