## DAG condicional

## Importando libs e operadores:

from airflow import DAG
from airflow.operators.bash.operator import BashOperator
from airflow.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

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

dag_03 = DAG(
    "dag-03",
    description="Extrai dados do database Titaninc e calcula a idade média para homem ou mulher",
    default_args = default_args,
    schedule_interval = timedelta(minutes=2)
)

## Definindo funcoes:

def sorteio():
    return random.choice(['male', 'female'])

def male_of_female(**context):
    parametro = context['task_instance'].xcomm_pull(task_ids='sorteia-valores')
    if parametro == 'male':
        return 'task_male'
    if parametro == 'female':
        return 'task_female'

def mean_male():
    df = pd.read.csv()
    df = df.loc[df.Sex == 'male']
    print(f"Média: {df.Age.mean()}")

def mean_female():
    df = pd.read.csv()
    df = df.loc[df.Sex == 'female']
    print(f"Média: {df.Age.mean()}")


## Criando tasks:

    ## Pegando dados:

task_get_data = BashOperator(
    task_id = 'get_data',
    bash_command = 'curl {url}',
    dag=dag_03
)

task_sorteio = PythonOperator(
    task_id='sorteia-valores',
    python_callable=sorteio(),
    dag=dag_03
)

task_escolhe = BranchPythonOperator(
    task_id='escolhe-male-ou-female',
    python_callable=male_of_female(),
    provide_context = True,
    dag=dag_03
)

task_mean_male = PythonOperator(
    task_id ='branch_male',
    python_callable = mean_male(),
    dag=dag_03
)

task_mean_female = PythonOperator(
    task_id ='branch_female',
    python_callable = mean_female(),
    dag=dag_03
)

## Ordenacao:

task_get_data >> task_sorteio >> task_escolhe >> [task_mean_female,task_mean_male]
