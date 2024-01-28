from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import os


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 12, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(filename, default_args=default_args, schedule_interval=timedelta(1))

t1 = BashOperator(task_id="print_date1", bash_command="date", dag=dag)

t2 = BashOperator(task_id="print_date2", bash_command="date", dag=dag)

t3 = BashOperator(task_id="print_date3", bash_command="date", dag=dag)

t4 = BashOperator(task_id="print_date4", bash_command="date", dag=dag)

t5 = BashOperator(task_id="print_date5", bash_command="date", dag=dag)

t6 = BashOperator(task_id="print_hi1", bash_command="echo 'Hi'", dag=dag)

t7 = BashOperator(task_id="print_hi2", bash_command="echo 'Hi'", dag=dag)

t8 = BashOperator(task_id="print_hi3", bash_command="echo 'Hi'", dag=dag)

t9 = BashOperator(task_id="print_hi4", bash_command="echo 'Hi'", dag=dag)

t10 = BashOperator(task_id="print_hi5", bash_command="echo 'Hi'", dag=dag)

td = DummyOperator(task_id='dummy', dag=dag)

[t1, t2, t3, t4, t5] >> td >> [t6, t7, t8, t9, t10]
