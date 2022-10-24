from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

import ETL_categorias as cat
import ETL_funcionarioS as func
import ETL_vendas as vdas

def carrega_cat():
    cat.ETL_CATEGORIAS()

def carrega_func():
    func.ETL_FUNCIONARIOS()

def carrega_vdas():
    vdas.ETL_VENDAS()   

# DAG_BIX_TESTE
with DAG('Bix_teste', start_date = datetime(2022,10,23),
         schedule_interval='0 3 * * *', catchup = False) as dag:

    # Task CARREGA CATEGORIAS
    Tcat = PythonOperator(
        task_id = 'task_cat',
        python_callable = carrega_cat
    )

    # Task CARREGA FUNCIONARIOS
    Tfunc = PythonOperator(
        task_id = 'task_func',
        python_callable = carrega_func
    )

    # Task CARREGA VENDAS
    Tvdas = PythonOperator(
        task_id = 'task_vdas',
        python_callable = carrega_vdas
    )

    # Define a ordem de execução das tasks
    Tcat >> Tfunc >> Tvdas