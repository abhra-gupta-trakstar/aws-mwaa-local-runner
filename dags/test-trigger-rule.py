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
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
# env_name = Variable.get("deploy_environment")


s3 = boto3.resource('s3')
# env_name = Variable.get("deploy_environment")

S3_BUCKET_NAME = "applied-data-lake"
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
    'email': ['abhra.gupta@trakstar.com', 'brian.kasen@trakstar.com', 'ritika.naidu@trakstar.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    'Test-Trigger-Rule',
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

def _get_message(action, table_list) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "TS-Learn-Check-Partitions-Daily DAG {} for {} on {} Mountain Time".format(action, table_list, mt_time)


# send_slack_success_notification_account_table = SlackWebhookOperator(
#     task_id="send_slack_success_notification_account_table",
#     trigger_rule='all_success',
#     http_conn_id="slack_conn",
#     message=_get_message("completed successfully", "account_table"),
#     channel="#airflow-monitoring-{}".format(env_name),
#     dag=dag
# )

# send_slack_success_notification_course_table = SlackWebhookOperator(
#     task_id="send_slack_success_notification_course_table",
#     trigger_rule='all_success',
#     http_conn_id="slack_conn",
#     message=_get_message("completed successfully", "course_table"),
#     channel="#airflow-monitoring-{}".format(env_name),
#     dag=dag
# )

# send_slack_failure_notification_account_table = SlackWebhookOperator(
#             task_id="send_slack_failure_notification_account_table",
#             trigger_rule='one_failed',
#             http_conn_id="slack_conn",
#             message=_get_message("failed", "account_table"),
#             channel="#airflow-monitoring-{}".format(env_name),
#             dag=dag
#         )

# send_slack_failure_notification_course_table = SlackWebhookOperator(
#             task_id="send_slack_failure_notification_course_table",
#             trigger_rule='one_failed',
#             http_conn_id="slack_conn",
#             message=_get_message("failed", "course_table"),
#             channel="#airflow-monitoring-{}".format(env_name),
#             dag=dag
#         )

# send_email_notification_account_table = EmailOperator(
#         task_id="send_Email_account_table",
#         trigger_rule='one_success',
#         to=["abhra.gupta@trakstar.com", "brian.kasen@trakstar.com"],
#         subject='TS-Learn-Check-Partitions-Daily-{}'.format(env_name),
#         html_content="TS-Learn-Check-Partitions-Daily DAG has failed on account_table in {} environment".format(env_name),
#         dag=dag
#     ) 
# send_email_notification_course_table = EmailOperator(
#         task_id="send_Email_course_table",
#         trigger_rule='one_success',
#         to=["abhra.gupta@trakstar.com", "brian.kasen@trakstar.com"],
#         subject='TS-Learn-Check-Partitions-Daily-{}'.format(env_name),
#         html_content="TS-Learn-Check-Partitions-Daily DAG has failed on course_table in {} environment".format(env_name),
#         dag=dag
#     ) 


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





