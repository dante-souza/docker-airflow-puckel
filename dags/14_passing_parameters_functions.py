import time

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from datetime import timedelta
from rich.pretty import pprint
import os
import warnings

warnings.filterwarnings("ignore")


def get_file_name():
    file_name_with_extension = os.path.basename(__file__)  # Get file name with extension
    file_name_without_extension = os.path.splitext(file_name_with_extension)[0]  # Remove extension
    return file_name_without_extension


filename = get_file_name()


def print_context(ds, **kwargs):
    print('*'*120)
    print('KWARGS:')
    pprint(kwargs)
    print('*' * 120)
    print('ARGS:')
    print(ds)
    print('*' * 120)
    return 'Whatever you return does not gets printed in the logs. Will appear in XCom.'


def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)
    return random_base


default_args = {
    "owner": "MEEEEEEEEEEEEEEEEEEEEEEEEEEE",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(hours=5),
}

with DAG(
        dag_id=filename
        , default_args=default_args
        , tags=["args", "kwargs", "loops"]
):
    run_this = PythonOperator(
        task_id='print_the_context',
        provide_context=True,   # erro de import se isso for False
        python_callable=print_context,
    )
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id='sleep_for_' + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={'random_base': float(i)}
        )

        run_this >> task
