from airflow.utils.dates import days_ago
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

dag = DAG(filename, default_args=default_args, schedule_interval=timedelta(days=1))

t1 = BashOperator(task_id="print_path", bash_command="echo {{var.value.source_path}}", dag=dag)
