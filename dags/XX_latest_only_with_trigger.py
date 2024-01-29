from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()
with DAG(dag_id=filename, schedule_interval=timedelta(hours=6), start_date=datetime(2020, 1, 24), catchup=True) as dag:

    t1 = LatestOnlyOperator(task_id = 'latest_only')

    t2 = DummyOperator(task_id='task2')

    t3 = DummyOperator(task_id='task3')

    t4 = DummyOperator(task_id='task4')

    t5 = DummyOperator(task_id='task5')

    t1 >> [t2, t4, t5]
