from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    return len(df.index)


def e_valida(task_instance):
    qtd = task_instance.xcom_pull(task_ids="captura_conta_dados")
    if qtd > 1000:
        # esse devolucao e o nome da proxima task
        return 'valida'
    return 'invalida'


with DAG('02_my_first_dag'
        , start_date=datetime(2024, 1, 24)
        , schedule_interval="*/5 * * * *"
        , catchup=False) as dag:

    captura_conta_dados = PythonOperator(
        task_id="captura_conta_dados"
        , python_callable=captura_conta_dados
    )

    e_valida = BranchPythonOperator(
        task_id="e_valida"
        , python_callable=e_valida
    )

    valida = BashOperator(
        task_id='valida'
        , bash_command="echo 'Quantidade OK'"
    )

    invalida = BashOperator(
        task_id='invalida'
        , bash_command="echo 'Quantidade NOK'"
    )

    captura_conta_dados >> e_valida >> [valida, invalida]
