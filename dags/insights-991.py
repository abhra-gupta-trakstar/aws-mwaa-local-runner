import airflow  
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

import time,io
import pandas
import boto3
from datetime import datetime, date, timedelta
import requests, pytz
import pandas as pd
from pandas import *

dag_name = 'TS-Insights-991'
execution_date = date.today().strftime("%Y%m%d")
candidate_candidate_list_s3_path = "s3f2-daily/ts-hire/candidate_candidate-list-s3f2-{}.csv".format(execution_date)
candidate_review_reviewfeedback_list_s3_path = "s3f2-daily/ts-hire/candidate_review_reviewfeedback-list-s3f2-{}.csv".format(execution_date)
meeting_feedback_list_s3_path = "s3f2-daily/ts-hire/meeting_feedback-list-s3f2-{}.csv".format(execution_date)
meeting_evaluation_response_list_s3_path = "s3f2-daily/ts-hire/meeting_evaluation_response-list-s3f2-{}.csv".format(execution_date)

AWS_S3_BUCKET = Variable.get("data_lake_bucket")
email_recipients_list = ['abhra.gupta@trakstar.com']
env_name = Variable.get("deploy_environment")
s3f2_username = Variable.get("s3f2_user")
s3f2_password = Variable.get("s3f2_password")
s3f2_clientid = Variable.get("s3f2_clientid")
cognito_user_pool_id = Variable.get("cognito_user_pool_id")
S3F2_URL = Variable.get("s3f2_url")

boto3.setup_default_session(profile_name='biqamfa')
session = boto3.session.Session(profile_name='biqamfa')


Redshift_Query = "select distinct cast(candidate_id as varchar) as id from core.stg_hire_deleted_candidates limit 10"
Redshift_Query_For_CandidateReviewReviewFeedback = "with deleted_candidates as ( select candidate_id from core.stg_hire_deleted_candidates ) \
    select distinct cast(review_id as varchar) as id from spectrum_data_lake_processed.processed_hire_recruiterbox_candidate_review_reviewfeedback revf \
    INNER JOIN spectrum_data_lake_processed.processed_hire_recruiterbox_candidate_review_review rev on rev.id = revf.review_id \
    INNER JOIN deleted_candidates c on rev.candidate_id = c.candidate_id limit 10"
Redshift_Query_For_MeetingFeedback = "with deleted_candidates as ( select candidate_id from core.stg_hire_deleted_candidates ) \
    select distinct cast(meeting_id as varchar) as id from spectrum_data_lake_processed.processed_hire_recruiterbox_meeting_feedback fdbck \
    INNER JOIN spectrum_data_lake_processed.processed_hire_recruiterbox_meeting_meeting m on fdbck.meeting_id = m.id \
    INNER JOIN deleted_candidates c on m.source_object_id = c.candidate_id AND m.source_object_type_id = 14"
Redshift_Query_For_MeetingEvaluationResponse = " with deleted_candidates as ( select candidate_id from core.stg_hire_deleted_candidates ) \
    select distinct concat( meeting_id, '-') as id from spectrum_data_lake_processed.processed_hire_recruiterbox_meeting_evaluationresponse eval_res \
    INNER JOIN spectrum_data_lake_processed.processed_hire_recruiterbox_meeting_meeting m on eval_res.meeting_id = m.id \
    INNER JOIN deleted_candidates c on m.source_object_id = c.candidate_id AND m.source_object_type_id = 14 \
    UNION select distinct concat('-', review_id) as id from spectrum_data_lake_processed.processed_hire_recruiterbox_meeting_evaluationresponse eval_res \
    INNER JOIN spectrum_data_lake_processed.processed_hire_recruiterbox_candidate_review_review r on eval_res.review_id = r.id \
    INNER JOIN deleted_candidates c on r.candidate_id = c.candidate_id"

URL = "{}".format(S3F2_URL)

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': email_recipients_list,
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='15 12 * * *',
    tags= ['S3F2', 'TS-Hire']
)
def _get_message(action) -> str:
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "{} DAG {} AT {} Mountain Time".format(dag_name, action, mt_time)


def redshift_to_s3(**kwargs):
    print("Welcome to Airflow ", env_name)

    s3 = session.resource('s3')
    s3_client = boto3.client("s3")
    client = boto3.client('redshift-data')
    bucket = AWS_S3_BUCKET
    query = kwargs['query']
    table_name = kwargs['table_name']
    execute_response = client.execute_statement(
        ClusterIdentifier='redshift-cluster-qa',
        Database='insight',
        DbUser='abhra_gupta',
        Sql=query,
        StatementName='create_list_for_{}'.format(table_name)
    )
    print(execute_response['Id'])
    time.sleep(60)

    try:
        query_result = client.get_statement_result(Id=execute_response['Id'])
        candidate_list = [query_result["Records"][i][0]["stringValue"] for i in range(len(query_result["Records"])) if "stringValue" in query_result["Records"][i][0]]
        df = pandas.DataFrame(data={"id": candidate_list})
        print("Number of Objects to be deleted = ", len(df))
        df.to_csv('{}-list-s3f2-{}.csv'.format(table_name,execution_date), index=False)
    except Exception as e:
        print("Results Not Found For The Query", e)

    # object = s3.Object(AWS_S3_BUCKET, 'test_abhra/s3f2/candidate-list-{}.csv'.format(env_name) )
    object = s3.Object(AWS_S3_BUCKET, kwargs['s3_download_path'] )


    result = object.put(Body=open('{}-list-s3f2-{}.csv'.format(table_name ,execution_date), 'rb'))


def authenticate_cognito_user_s3f2_enqueue(ti, **kwargs):  
    client = boto3.client('cognito-idp')
    response = client.admin_initiate_auth(
        UserPoolId=cognito_user_pool_id,
        ClientId=s3f2_clientid,
        AuthFlow='ADMIN_NO_SRP_AUTH',
        AuthParameters={"USERNAME":s3f2_username,"PASSWORD":s3f2_password}
    )
    id_token = response['AuthenticationResult']['IdToken']
    headers = {"Authorization": "Bearer {}".format(id_token),
                "Content-Type": "application/json"}

    s3_client = boto3.client(
        "s3"
    )
    
    item_list = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=kwargs['s3_download_path'])

    status = item_list.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(item_list.get("Body"))
        print(df['id'])
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    for item in df['id']:
        data = {
        "Type": "Simple",
        "MatchId": "{}".format(item),
        "DataMappers": [
            kwargs['data-mapper']
        ]
        }
        r = requests.patch( URL, json=data, headers=headers)
        print(" ID= " , item , r.text)
    
    # ti.xcom_push(key="s3f2_delete_endpoint_api_headers", value=headers)

def start_deletion_job(ti):
    client = boto3.client('cognito-idp')
    response = client.admin_initiate_auth(
        UserPoolId=cognito_user_pool_id,
        ClientId=s3f2_clientid,
        AuthFlow='ADMIN_NO_SRP_AUTH',
        AuthParameters={"USERNAME":s3f2_username,"PASSWORD":s3f2_password}
    )
    id_token = response['AuthenticationResult']['IdToken']
    headers = {"Authorization": "Bearer {}".format(id_token),
                "Content-Type": "application/json"}
    try:
        response = requests.delete(
                URL,
                headers=headers
            )
        print(response.text)
    except Exception as e:
        print(e)

begin_DAG = DummyOperator(task_id='begin_DAG', dag=dag)
stop_DAG = DummyOperator(task_id='stop_DAG', dag=dag)
    
download_candidate_list = PythonOperator(task_id='download_candidate_list_to_s3', provide_context=True, python_callable=redshift_to_s3, op_kwargs={"query" : Redshift_Query, "table_name": "candidate_candidate", "s3_download_path": candidate_candidate_list_s3_path }, dag=dag)
# enqueue_raw_hire_recruiterbox_candidate_candidate_for_deletion = PythonOperator(task_id='enqueue_raw_hire_recruiterbox_candidate_candidate_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "RawCandidateCandidateDataMapper", "s3_download_path": candidate_candidate_list_s3_path }, dag=dag)
# enqueue_processed_hire_recruiterbox_candidate_candidate_for_deletion = PythonOperator(task_id='enqueue_processed_hire_recruiterbox_candidate_candidate_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "ProcessedCandidateCandidateDataMapper", "s3_download_path": candidate_candidate_list_s3_path }, dag=dag)
wait_after_enqueue = BashOperator(task_id="wait_after_enqueue", trigger_rule='all_success', bash_command='sleep 5m', dag=dag)

download_candidate_review_reviewfeedback_list = PythonOperator(task_id='download_candidate_review_reviewfeedback_list_to_s3', provide_context=True, python_callable=redshift_to_s3, op_kwargs={"query" : Redshift_Query_For_CandidateReviewReviewFeedback, "table_name": "candidate_review_reviewfeedback", "s3_download_path": candidate_review_reviewfeedback_list_s3_path }, dag=dag)
# enqueue_raw_hire_recruiterbox_candidate_review_reviewfeedback_for_deletion = PythonOperator(task_id='enqueue_raw_hire_recruiterbox_candidate_review_reviewfeedback_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "RawCandidateReviewReviewFeedbackDataMapper", "s3_download_path": candidate_review_reviewfeedback_list_s3_path }, dag=dag)
# enqueue_processed_hire_recruiterbox_candidate_review_reviewfeedback_for_deletion = PythonOperator(task_id='enqueue_processed_hire_recruiterbox_candidate_review_reviewfeedback_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "ProcessedCandidateReviewReviewFeedbackDataMapper", "s3_download_path": candidate_review_reviewfeedback_list_s3_path }, dag=dag)

download_meeting_feedback_list = PythonOperator(task_id='download_meeting_feedback_list_to_s3', provide_context=True, python_callable=redshift_to_s3, op_kwargs={"query" : Redshift_Query_For_MeetingFeedback, "table_name": "meeting_feedback", "s3_download_path": meeting_feedback_list_s3_path }, dag=dag)
# enqueue_raw_hire_recruiterbox_meeting_feedback_for_deletion = PythonOperator(task_id='enqueue_raw_hire_recruiterbox_meeting_feedback_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "RawMeetingFeedbackDataMapper", "s3_download_path": meeting_feedback_list_s3_path }, dag=dag)
# enqueue_processed_hire_recruiterbox_meeting_feedback_for_deletion = PythonOperator(task_id='enqueue_processed_hire_recruiterbox_meeting_feedback_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "ProcessedMeetingFeedbackDataMapper", "s3_download_path": meeting_feedback_list_s3_path }, dag=dag)

download_meeting_evaluation_response_list = PythonOperator(task_id='download_meeting_evaluation_response_list_to_s3', provide_context=True, python_callable=redshift_to_s3, op_kwargs={"query" : Redshift_Query_For_MeetingEvaluationResponse, "table_name": "meeting_evaluation_response", "s3_download_path": meeting_evaluation_response_list_s3_path }, dag=dag)
# enqueue_raw_hire_recruiterbox_meeting_evaluation_response_for_deletion = PythonOperator(task_id='enqueue_raw_hire_recruiterbox_meeting_evaluation_response_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "RawMeetingEvaluationResponseDataMapper", "s3_download_path": meeting_evaluation_response_list_s3_path }, dag=dag)
# enqueue_processed_hire_recruiterbox_meeting_evaluation_response_for_deletion = PythonOperator(task_id='enqueue_processed_hire_recruiterbox_meeting_evaluation_response_for_deletion', provide_context=True, python_callable=authenticate_cognito_user_s3f2_enqueue, op_kwargs={"data-mapper" : "ProcessedMeetingEvaluationResponseDataMapper", "s3_download_path": meeting_evaluation_response_list_s3_path }, dag=dag)


# wait_after_deletion_starts = BashOperator(task_id="wait_after_deletion_starts", bash_command='sleep 5m', dag=dag)

# start_deletion_task = PythonOperator(task_id='start_deletion_job', provide_context=True, python_callable=start_deletion_job, dag=dag)

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
    to=email_recipients_list,
    subject=dag_name,
    html_content="{} DAG has failed".format(dag_name),
    dag=dag
)


begin_DAG >> download_candidate_list >> wait_after_enqueue
begin_DAG >> download_candidate_review_reviewfeedback_list >> wait_after_enqueue
begin_DAG >> download_meeting_feedback_list >> wait_after_enqueue
begin_DAG >> download_meeting_evaluation_response_list >> wait_after_enqueue

wait_after_enqueue >> stop_DAG >> send_slack_success_notification >> send_slack_failure_notification >> send_email_notification 