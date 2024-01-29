import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
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


def push_function(**kwargs):
    pushed_value = 5
    ti = kwargs['ti']
    ti.xcom_push(key="pushed_value", value=pushed_value)


def branch_function(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='pushed_value', task_ids='push_task')
    if pulled_value % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'


with DAG(dag_id=filename, default_args=args, schedule_interval="@daily") as dag:
    push_task = PythonOperator(task_id='push_task', python_callable=push_function, provide_context=True)

    branch_task = BranchPythonOperator(task_id='branch_task', python_callable=branch_function, provide_context=True)

    even_task = BashOperator(task_id='even_task', bash_command='echo "Got an even value."')

    odd_task = BashOperator(task_id='odd_task', bash_command='echo "Got an odd value."')

    push_task >> branch_task >> [even_task, odd_task]
