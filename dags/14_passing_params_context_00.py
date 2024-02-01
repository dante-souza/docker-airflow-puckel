from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def my_task_func(**context):
    name = context['params']['name']
    print('*'*120)
    print(f'Hello, {name}')
    print('*' * 120)


with DAG(
        'my_dag'
        , start_date=days_ago(1)):
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task_func,
        provide_context=True,
        params={'name': 'Dog'},
    )
