from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import the get_file_name macro
from filename_plugin_operator import get_file_name


def print_filename(file_path):
    print("Filename without extension:", get_file_name(file_path))


with DAG(get_file_name(__file__), schedule_interval="@daily", start_date=days_ago(1)) as dag:
    task = PythonOperator(
        task_id="print_filename_task",
        python_callable=print_filename,
        op_kwargs={"file_path": __file__}  # Pass the current file path to the function
    )
