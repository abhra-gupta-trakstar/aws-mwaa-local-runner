from datetime import timedelta, datetime
import pytz
import airflow  
import time
from airflow import DAG  
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from jinja2 import Template
from airflow.operators.python import PythonOperator

default_args = {  
    'owner': 'Abhra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ["abhra.gupta@trakstar.com"],
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(  
    'Test-dag_id-macros',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=4),
    schedule_interval='45 5 * * *'
)

dag_name = 'Test-dag_id-macros'
template = Template("{{ params.dag_name }}")

# Method 1: pass values as a dict
params = {'dag_name': dag_name}
print(template.render({'params': params}))

dag_id_template  = Template("{{ dag.dag_id }}")

print(template.render({'params': params}))

# Method 2: pass values as keyword arguments
print(template.render(params={'dag_name': dag_name}))


def _get_message(action, dag_name, **kwargs) -> str:
    context = kwargs
    dag_id = context['dag_run'].dag_id
    MT = pytz.timezone('America/Denver')
    mt_time = datetime.now(MT)
    return "{} DAG {} on {} Mountain Time: DAG ID is : {}".format(dag_name, action, mt_time, dag_id)

python_task  = PythonOperator(task_id='python_task', python_callable=_get_message, provide_context=True, op_args=[ "completed" , template.render({'params': params}) ], dag=dag)
clear_upstream = BashOperator( 
    task_id='all_failed',
    do_xcom_push=True,
    bash_command="""
      echo {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} {{ dag.dag_id }}
    """,
    dag=dag
)
begin = DummyOperator(task_id='begin_{}'.format(dag_name), dag=dag)
end = DummyOperator(task_id='end_{}'.format(template.render({'params': params})), dag=dag)

fetching_data = BashOperator(
task_id='fetching_data',
bash_command="echo 'XCom fetched: {{ ti.xcom_pull(task_ids=[\'all_failed\']) }}'",
do_xcom_push=False,
dag=dag
)

begin >> clear_upstream >> fetching_data >> python_task >> end
