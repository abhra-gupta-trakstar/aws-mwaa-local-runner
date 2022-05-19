from datetime import timedelta , datetime
import pytz
import airflow  
from airflow import AirflowException
from airflow import DAG  
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from airflow.models import Variable
from airflow.operators.bash import BashOperator
import boto3
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from common.insights_lib import send_slack_message, send_email_message

env_name = Variable.get("deploy_environment")
dag_name = 'TS-Learn-Check-Partitions-Daily'
email_recipients_list = ['abhra.gupta@trakstar.com']
s3 = boto3.resource('s3')
env_name = Variable.get("deploy_environment")

S3_BUCKET_NAME = Variable.get("data_lake_bucket")
EXEC_DATE = '{{ macros.ds_add(ds, 1) }}'
EXEC_YEAR = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y") }}'
EXEC_MONTH = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%m") }}'
EXEC_DAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'
path_prefix = 'processed/ts-learn/pacifica/'
MT = pytz.timezone('America/Denver')
mt_time = datetime.now(MT)
current_year = mt_time.year
current_month = str(mt_time.month).zfill(2)
current_day = str(mt_time.day).zfill(2)

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': email_recipients_list,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 11 * * *'
)


account_table_partition_check_begin = DummyOperator(task_id='account_table_partition_check_begin', dag=dag)
course_table_partition_check_begin = DummyOperator(task_id='course_table_partition_check_begin', dag=dag)

account_table_partition_check_end = DummyOperator(task_id='account_table_partition_check_end', dag=dag)
course_table_partition_check_end = DummyOperator(task_id='course_table_partition_check_end', dag=dag)
accountrelation_table_partition_check_end = DummyOperator(task_id='accountrelation_table_partition_check_end', trigger_rule=TriggerRule.ALL_DONE, dag=dag)


def folder_exists_and_not_empty(bucket, path) -> bool:
    '''
    Folder should exists. 
    Folder should not be empty.
    '''
    boto3.setup_default_session(profile_name='biqamfa')
    s3 = boto3.client('s3')
    if not path.endswith('/'):
        path = path+'/' 
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/',MaxKeys=1)
    if 'Contents' in resp:
        print("Latest Partition found")
    else:
        raise AirflowException("Partition Not Found")


account_table = ['account', 'accountmetadata', 'accountrelation',
          'userrecord', 'usergroupuser', 'usergroup', 'domain']

course_table = ['cmodule', 'course', 'coursestate', 'cmodulestate' ,
           'coursestatehistory', 'mtype', 'coursecmodule']

send_slack_success_notification_account_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_account_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', account_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_success_notification_course_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_course_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', course_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_failure_notification_account_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_account_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', account_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_slack_failure_notification_course_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_course_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', course_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_email_notification_account_table = EmailOperator(
        task_id="send_Email_account_table",
        trigger_rule='one_success',
        to= email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(account_table),
        dag=dag
    ) 
send_email_notification_course_table = EmailOperator(
        task_id="send_Email_course_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(course_table),
        dag=dag
    ) 


for table in account_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)        
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    if table == 'accountrelation':
        account_table_partition_check_begin >> python_task >> accountrelation_table_partition_check_end 
    else:
        account_table_partition_check_begin >> python_task >> account_table_partition_check_end

for table in course_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    course_table_partition_check_begin >> python_task >> course_table_partition_check_end


account_table_partition_check_end >> send_slack_success_notification_account_table >> send_slack_failure_notification_account_table >> send_email_notification_account_table
course_table_partition_check_end >> send_slack_success_notification_course_table >> send_slack_failure_notification_course_table >> send_email_notification_course_table


