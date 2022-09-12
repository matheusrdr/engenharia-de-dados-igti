## Primeira DAG com Airflow com Scheduler

## Importando libs e operadores:

from airflow import DAG
from airflow.operators.bash.operator import BashOperator
from airflow.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Matheus Rodrigues',
    'depends_on_past': False,
    'start_date': datetime(),
    'email': '', ## emails devem ser configurados no arquivo airflow.config [smtp].
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

## Definindo DAG:

dag_02 = DAG(
    "dag-02",
    description="Extrai dados do database Titaninc e calcula a idade média",
    default_args = default_args,
    schedule_interval = timedelta(minutes=2)
)

## Pegando dados:

task_get_data = BashOperator(
    task_id = 'get_data',
    bash_command = 'curl {url}',
    dag=dag_02
)

## Definindo funcoes para PythonOperator:

def calculate_mean():
    df = pd.read_csv('')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context ['task_instance'].xcom_pull(task_ids='calcula_idade_media')
    print(f"A idade media era {value} anos.")

task_idade_media = PythonOperator(
    task_id='calcula_idade_media',
    python_callable=calculate_mean(),
    dag=dag_02
)

task_print_idade = PythonOperator(
    task_id='mostra-idade',
    python_callable = print_age(),
    provide_context = True,
    dag=dag_02
)

## Ordenacao:

task_get_data >> task_idade_media >> task_print_idade
