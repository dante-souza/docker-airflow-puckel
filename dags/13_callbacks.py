import datetime
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from include.ms_teams_callback_functions import failure_callback

import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()


def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")


def raise_exception():
    return 1 / 0


with DAG(
        dag_id=filename,
        schedule=None,
        # start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        start_date=days_ago(1),
        dagrun_timeout=datetime.timedelta(minutes=60),
        catchup=False,
        on_success_callback=None,
        # on_failure_callback=task_failure_alert,
        on_failure_callback=failure_callback,
        tags=["callbacks example from airflow docs webhook teams - 1"],
):
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3", on_success_callback=[dag_success_alert])

    raiser = PythonOperator(
        task_id="raise_exception"
        , python_callable=raise_exception
        , on_failure_callback=[failure_callback]
    )

    # task1 >> task2 >> task3
    task1 >> task2 >> raiser >> task3
