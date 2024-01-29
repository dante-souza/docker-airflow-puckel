import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

DAG = DAG(
    dag_id=filename,
    default_args=args,
    schedule_interval="@daily",
)


def push_function(**kwargs):
    message = 'This is the pushed message.'
    ti = kwargs['ti']
    ti.xcom_push(key="message", value=message)


def pull_function(**kwargs):
    ti = kwargs['ti']
    pulled_message = ti.xcom_pull(key='message', task_ids='new_push_task')
    print(f"Pulled Message: {pulled_message}")


def pull_function_returned(task_instance):
    pulled_message = task_instance.xcom_pull(task_ids='new_push_function_returned_value')
    print(f"Pulled Message: {pulled_message}")


def new_push_function(**kwargs):
    message = 'This is the NEW pushed message.'
    ti = kwargs['ti']
    ti.xcom_push(key="message", value=message)


def new_push_function_returned_value():
    return 'This is the NEW pushed message using returned value.'


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


t3 = PythonOperator(
    task_id='new_push_task',
    python_callable=new_push_function,
    provide_context=True,
    dag=DAG)

t4 = PythonOperator(
    task_id='new_push_function_returned_value',
    python_callable=new_push_function_returned_value,
    provide_context=True,
    dag=DAG)


t5 = PythonOperator(
    task_id='pull_task_returned',
    python_callable=pull_function_returned,
    provide_context=True,
    dag=DAG)

t1 >> [t3, t4]
t3 >> t2
t4 >> t5
