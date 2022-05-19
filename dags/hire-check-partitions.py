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
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from common.insights_lib import send_slack_message, send_email_message

env_name = Variable.get("deploy_environment")
dag_name = 'TS-Hire-Check-Partitions-Daily'
email_recipients_list = ['abhra.gupta@trakstar.com']
s3 = boto3.resource('s3')

S3_BUCKET_NAME = Variable.get("data_lake_bucket")
EXEC_DATE = '{{ macros.ds_add(ds, 1) }}'
EXEC_YEAR = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y") }}'
EXEC_MONTH = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%m") }}'
EXEC_DAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'
path_prefix = 'processed/ts-hire/recruiterbox/'
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
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='10 11 * * *'
)


appraisals_table_partition_check_begin = DummyOperator(task_id='appraisals_table_partition_check_begin', dag=dag)
opening_table_partition_check_begin = DummyOperator(task_id='opening_table_partition_check_begin', dag=dag)
user_manager_table_partition_check_begin = DummyOperator(task_id='user_manager_table_partition_check_begin', dag=dag)
cold_table_partition_check_begin = DummyOperator(task_id='cold_table_partition_check_begin', dag=dag)

appraisals_table_partition_check_end = DummyOperator(task_id='appraisals_table_partition_check_end', dag=dag)
opening_table_partition_check_end = DummyOperator(task_id='opening_table_partition_check_end', dag=dag)
user_manager_table_partition_check_end = DummyOperator(task_id='user_manager_table_partition_check_end', dag=dag)
cold_table_partition_check_end = DummyOperator(task_id='cold_table_partition_check_end', trigger_rule=TriggerRule.ALL_DONE, dag=dag)


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


candidate_table = [
    'candidate_candidatejobrelevance'
    ,'candidate_candidatestatelog'
    ,'candidate_candidatestatemetadata'
    ,'candidate_review_review'
    ,'candidate_review_review_reviewers'
    ,'candidate_review_reviewfeedback'
    ,'candidate_candidate'
    ,'candidate_state_reason_candidatestatereason'
    ,'candidate_candidatesourcetracker'
    ,'meeting_feedback'
    ,'meeting_evaluationrequest'
    ,'meeting_evaluationresponse'
]

opening_table = [
    'dapp_openings_openingtype_openings'
    ,'dapp_openings_openingtype'
    ,'dapp_openings_opening'
    ,'publishing_publishedjob'
    ,'meeting_meeting'
    ,'dapp_openings_openingadditionalfield'
    ,'publishing_team'
    ,'dapp_openings_stagemovementlog'
    ,'dapp_openings_openingstage'
    ,'opening_offerstage'
    ,'opening_openingstatechangelog'
]

user_manager_table = [
    'analytics_logintracker'
    ,'auth_user'
    ,'UserManager_userroles'
    ,'UserManager_roles'
    ,'UserManager_clientattritiondetails'
    ,'UserManager_client'
    ,'UserManager_clientsettings'
    ,'UserManager_userprofile'
    ,'UserManager_client_candidatesources'
]

cold_tables = [
    'candidate_candidatestate'
    , 'opening_openingstate'
    , 'UserManager_client_candidatesourcetypes'
    , 'UserManager_pricingplan'
    , 'UserManager_useremail'
]

# def _get_message(action, table_list) -> str:
#     MT = pytz.timezone('America/Denver')
#     mt_time = datetime.now(MT)
#     return "TS-Perform-Check-Partitions-Daily DAG {} for {} on {} Mountain Time".format(action, table_list, mt_time)


send_slack_success_notification_appraisals_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_appraisals_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', candidate_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_success_notification_opening_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_opening_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', opening_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_success_notification_user_manager_table = SlackWebhookOperator(
    task_id="send_slack_success_notification_user_manager_table",
    trigger_rule='all_success',
    http_conn_id="slack_conn",
    message=send_slack_message(dag_name, "completed successfully", 'America/Denver', user_manager_table),
    channel="#airflow-monitoring-{}".format(env_name),
    dag=dag
)

send_slack_failure_notification_appraisals_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_appraisals_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', candidate_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_slack_failure_notification_opening_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_opening_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', opening_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_slack_failure_notification_user_manager_table = SlackWebhookOperator(
            task_id="send_slack_failure_notification_user_manager_table",
            trigger_rule='one_failed',
            http_conn_id="slack_conn",
            message=send_slack_message(dag_name, "failed", 'America/Denver', user_manager_table),
            channel="#airflow-monitoring-{}".format(env_name),
            dag=dag
        )

send_email_notification_appraisals_table = EmailOperator(
        task_id="send_Email_appraisals_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(candidate_table),
        dag=dag
    ) 
send_email_notification_opening_table = EmailOperator(
        task_id="send_Email_opening_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(opening_table),
        dag=dag
    ) 

send_email_notification_user_manager_table = EmailOperator(
        task_id="send_Email_user_manager_table",
        trigger_rule='one_success',
        to=email_recipients_list,
        subject='{}-{}'.format(dag_name, env_name),
        html_content=send_email_message(user_manager_table),
        dag=dag
    ) 

for table in candidate_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    appraisals_table_partition_check_begin >> python_task >> appraisals_table_partition_check_end

for table in opening_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    opening_table_partition_check_begin >> python_task >> opening_table_partition_check_end

for table in user_manager_table:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    user_manager_table_partition_check_begin >> python_task >> user_manager_table_partition_check_end

for table in cold_tables:
    full_path = path_prefix + "{}/ingest_year={}/ingest_month={}/ingest_day={}/".format(table, current_year, current_month, current_day)
    python_task  = PythonOperator(task_id='find_partition_{}'.format(table), python_callable=folder_exists_and_not_empty, op_args=[S3_BUCKET_NAME, full_path ], dag=dag)
    cold_table_partition_check_begin >> python_task >> cold_table_partition_check_end

appraisals_table_partition_check_end >> send_slack_success_notification_appraisals_table >> send_slack_failure_notification_appraisals_table >> send_email_notification_appraisals_table
opening_table_partition_check_end >> send_slack_success_notification_opening_table >> send_slack_failure_notification_opening_table >> send_email_notification_opening_table
user_manager_table_partition_check_end >> send_slack_success_notification_user_manager_table >> send_slack_failure_notification_user_manager_table >> send_email_notification_user_manager_table
