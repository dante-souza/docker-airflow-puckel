from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from filename_plugin_operator import get_file_name

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

databricks_conn_id = "dsdbsdev"

with DAG(get_file_name(__file__),
         start_date=days_ago(1),
         schedule_interval=None,
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id="airflow_notebook_submit_run",
        databricks_conn_id=databricks_conn_id,
        new_cluster={
            "spark_version": "10.4.x-scala2.12",
            "num_workers": 1,
            "node_type_id": "Standard_F4",
        },
        notebook_task={
            "notebook_path": "/Users/eric.buzato@viavarejo.com.br/test-airflow",
        }
    )
