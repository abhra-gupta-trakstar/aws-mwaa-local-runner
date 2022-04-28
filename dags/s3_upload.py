from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import boto3


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    session = boto3.session.Session(profile_name='biqamfa')
    s3 = session.resource('s3')

    object = s3.Object('qa-abhra', 'file_name.txt')

    result = object.put(Body=open('/usr/local/airflow/file_name.txt', 'rb'))

    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')
    # hook = S3Hook('s3_conn')
    # hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/abhragupta/Documents/employee1_photo.jpg',
            'key': 'employee1_photo.jpg',
            'bucket_name': 'qa-abhra'
        }
    )