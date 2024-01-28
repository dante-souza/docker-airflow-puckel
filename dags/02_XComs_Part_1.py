import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()


def push_function(**kwargs):
    message = 'This is the pushed message.'
    ti = kwargs['ti']
    ti.xcom_push(key="message", value=message)


def pull_function(task_instance):
    pulled_message = task_instance.xcom_pull(key='message', task_ids='push_task')
    print("Pulled Message: '%s'" % pulled_message)


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

DAG = DAG(
    dag_id=filename,
    default_args=args,
    schedule_interval="@daily",
)

t1 = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

t2 = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=DAG)

t1 >> t2
