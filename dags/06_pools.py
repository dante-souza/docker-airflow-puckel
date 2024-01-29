from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import timedelta
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()

default_args = {
    "owner": "airflow",
    "start_date":  days_ago(1),
    }

with DAG(filename, default_args=default_args, schedule_interval=timedelta(days=1)) as dag:

    t1 = BashOperator(task_id="task-1", bash_command="sleep 35", pool="pool_1")

    t2 = BashOperator(task_id="task-2", bash_command="sleep 35", pool="pool_1")

    t3 = BashOperator(task_id="task-3", bash_command="sleep 35", pool="pool_2"
                      # , priority_weight=2
                      )

    t4 = BashOperator(task_id="task-4", bash_command="sleep 35", pool="pool_2")
