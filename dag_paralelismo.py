## DAG paralelismo

## Importando libs e operadores:

from airflow import DAG
from airflow.operators.bash.operator import BashOperator
from airflow.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile

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
# Consts:

file_path = ''
file_path_to_extract = ''
file = file_path + 'microdados_enade_2019.txt'

## Definindo DAG:

dag_04 = DAG(
    "dag-04",
    description="Extrai dados do database Titaninc e calcula a idade média para homem ou mulher",
    default_args = default_args,
    schedule_interval = "*/10 * * * *"
)

## Definindo funcoes:

def unzip_file():
    with zipfile.ZipFile(file_path, 'r') as zip:
        zipped.extractall(file_path_to_extract)

def filters():
    cols = []
    enade = pd.read_csv(file , sep = ';', decimal = ',', usecols = cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(file_path + 'enade_filtrado.csv' , index = False)

def build_central_age():
    age = pd.read_csv(file_path + 'enade_filtrado.csv', usecols = ['NU_IDADE'])
    age['centralage'] = age.NU_IDADE - age.NU_IDADE.mean()
    age[['centralage']].to_csv(file_path + 'central_age.csv', index = False)

def build_age_squared():
    centralage = pd.read_csv(file_path + "central_age.csv")
    centralage['age_2'] = centralage.centralage ** 2
    centralage[['age_2']].to_csv(file_path + 'age_squared.csv', index=False)


def build_status():
    filter = pd.read_csv(file_path + 'enade_filtrado.csv' , usecols = ['QE_I01'])
    filter['estcivil'] = filter.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viuvo',
        'E': 'Outro'
    })
    filter[['estcivil']].to_csv(file_path + 'est_civil.csv', index = False)

def build_skin_color():
    filter = pd.read_csv(file_path + 'enade_filtrado.csv' , usecols = ['QE_I02'])
    filter['skincolor'] = filter.QE_I01.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indigena',
        'F': 'Outro'
    })
    filter[['skincolor']].to_csv(file_path + 'skin_color.csv', index = False)

def join_data():
    filter = pd.read_csv(file_path + 'enade_filtrado.csv')
    central_age = pd.read_csv(file_path + 'central_age.csv')
    age_squared = pd.read_csv(file_path + 'age_squared.csv')
    est_civil = pd.read_csv(file_path + 'est_civil.csv')
    skin_color = pd.read_csv(file_path + 'skin_color.csv')
    df_final = pd.concat([filter, central_age, age_squared, est_civil, skin_color], axis = 1)

## Criando tasks:

start_preprocessing = BashOperator(
    task_id = 'start_preprocessing',
    bash_command = 'echo "Start Preprocessing"',
    dag=dag_04
)

task_get_data = BashOperator(
    task_id = 'get_data',
    bash_command = 'curl {url}',
    dag=dag_04
)

unzip_data = PythonOperator(
    task_id = 'unzip_data',
    python_callable = unzip_file(),
    dag=dag_04
)

set_filter = PythonOperator(
    task_id = 'set_filter',
    python_callable = filters(),
    dag=dag_04
)

task_central_age = PythonOperator(
    task_id = 'central_age',
    python_callable = build_central_age(),
    dag=dag_04
)

task_age_squared = PythonOperator(
    task_id = 'age_squared',
    python_callable = build_age_squared(),
    dag=dag_04
)

task_build_status = PythonOperator(
    task_id = 'build_status',
    python_callable = build_status(),
    dag=dag_04
)

task_build_skin_color = PythonOperator(
    task_id = 'build_skin_color',
    python_callable = build_skin_color(),
    dag=dag_04
)

task_join = PythonOperator(
    task_id = 'join_data',
    python_callable = join_data(),
    dag=dag_04
)

## Ordenacao:

start_preprocessing >> task_get_data >> unzip_data >> set_filter
set_filter >> [task_central_age, task_build_status, task_build_skin_color]
task_age_squared.set_upstream(task_central_age)
task_join.set_upstream(task_build_status, task_build_skin_color)
