## Primeira DAG com Airflow

## Importando libs e operadores:

from airflow import DAG
from airflow.operators.bash.operator import BashOperator
from airflow.python_operator import PythonOperator
from datetime import datetime, timedelta

## Argumentos default:

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

dag = DAG(
    "dag-01",
    description="Básico de Bash Operators e Python Operators",
    default_args = default_args,
    schedule_interval = timedelta(minutes=2)
)

## Adicionando tasks:

hello_bash = BashOperator(

    task_id = "Hello Bash",
    bash_command = 'echo "Hello"',
    dag = dag
)

def say_hello():
    print("Hello")

hello_pyhton = PythonOperator(
    task_id = "Hello - Python",
    python_callable = say_hello(),
    dag=dag
)

## Construindo encadeamentos:

hello_bash >> hello_pyhton