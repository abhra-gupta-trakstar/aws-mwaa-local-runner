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
from common.insights_lib import send_slack_message, send_email_message

env_name = Variable.get("deploy_environment")
dag_name = 'TS-Perform-Check-Partitions-Daily'
email_recipients_list = ['abhra.gupta@trakstar.com']
s3 = boto3.resource('s3')

S3_BUCKET_NAME = Variable.get("data_lake_bucket")
EXEC_DATE = '{{ macros.ds_add(ds, 1) }}'
EXEC_YEAR = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y") }}'
EXEC_MONTH = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%m") }}'
EXEC_DAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'
path_prefix = 'processed/ts-perform/trakstar/'
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



# s3_sensor_1 = S3PrefixSensor(  
#   task_id='s3_sensor_11',  
#   bucket_name=S3_BUCKET_NAME,  
#   prefix='processed/ts-perform/trakstar/employee_plans/ingest_year=2022/ingest_month=01/ingest_day=11/',  
#   dag=dag  
# )

# s3_sensor_2 = S3PrefixSensor(  
#   task_id='s3_sensor_15',  
#   bucket_name=S3_BUCKET_NAME,  
#   prefix='processed/ts-perform/trakstar/employee_plans/ingest_year=2022/ingest_month=01/ingest_day=15/',  
#   dag=dag  
# )

# echo_date = BashOperator(task_id='echo_date', bash_command="echo " + EXEC_DATE, dag=dag)
# echo_year = BashOperator(task_id='echo_year', bash_command="echo " + EXEC_YEAR, dag=dag)
# echo_month = BashOperator(task_id='echo_month', bash_command="echo " + EXEC_MONTH, dag=dag)
# echo_day = BashOperator(task_id='echo_day', bash_command="echo " + EXEC_DAY, dag=dag)

appraisals_table_partition_check_begin = DummyOperator(task_id='appraisals_table_partition_check_begin', dag=dag)
positions_table_partition_check_begin = DummyOperator(task_id='positions_table_partition_check_begin', dag=dag)
rating_table_partition_check_begin = DummyOperator(task_id='rating_table_partition_check_begin', dag=dag)
appraisals_table_partition_check_end = DummyOperator(task_id='appraisals_table_partition_check_end', dag=dag)
positions_table_partition_check_end = DummyOperator(task_id='positions_table_partition_check_end', dag=dag)
rating_table_partition_check_end = DummyOperator(task_id='rating_table_partition_check_end', dag=dag)

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


appraisal_tables = [
    'users',
    'user_roles',
    'insights_admin_users',
    'user_terminations',
    'user_organizations',
    'reviews',
    'review_sets',
    'appraisals',
    'appraisal_processes'
]

positions_table = [
    'organization_types',
    'measurements',
    'roles',
    'contacts',
    'positions',
    'position_plans',
    'companies'
]

rating_table = [
    'employee_plans',
    'plan_element_reviews',
    'score_formatters',
    'organizations',
    'rating_scales',
    'rating_scale_elements',
    'multirater_collections'
]

# def _get_message(action, table_list) -> str:
#     MT = pytz.timezone('America/Denver')
#     mt_time = datetime.now(MT)
#     return "TS-Perform-Check-Partitions-Daily DAG {} for {} on {} Mountain Time".format(action, table_list, mt_time)


send_slack_success_notification_appraisals_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_appraisals_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', appraisal_tables),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_success_notification_positions_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_positions_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', positions_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_success_notification_rating_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_rating_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', rating_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_failure_notification_appraisals_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_appraisals_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', appraisal_tables),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_slack_failure_notification_positions_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_positions_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', positions_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_slack_failure_notification_rating_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_rating_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', rating_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_email_notification_appraisals_table = EmailOperator(
        task_id="send_Email_appraisals_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(appraisal_tables),
        dag=dag
    ) 
send_email_notification_positions_table = EmailOperator(
        task_id="send_Email_positions_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(positions_table),
        dag=dag
    ) 

send_email_notification_rating_table = EmailOperator(
        task_id="send_Email_rating_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(rating_table),
        dag=dag
    ) 

for table in appraisal_tables:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    appraisals_table_partition_check_begin >> python_task >> appraisals_table_partition_check_end

for table in positions_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    positions_table_partition_check_begin >> python_task >> positions_table_partition_check_end

for table in rating_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    rating_table_partition_check_begin >> python_task >> rating_table_partition_check_end

appraisals_table_partition_check_end >> send_slack_success_notification_appraisals_table >> send_slack_failure_notification_appraisals_table >> send_email_notification_appraisals_table
positions_table_partition_check_end >> send_slack_success_notification_positions_table >> send_slack_failure_notification_positions_table >> send_email_notification_positions_table
rating_table_partition_check_end >> send_slack_success_notification_rating_table >> send_slack_failure_notification_rating_table >> send_email_notification_rating_table


