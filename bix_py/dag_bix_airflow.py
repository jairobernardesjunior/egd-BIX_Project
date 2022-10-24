from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

import pandas as pd
import requests
from sqlalchemy import create_engine

# Faz a conexão com o banco de dados passando parâmetros
import psycopg2

def conecta_db(hostx, databasex, userx, passwordx):
  con = psycopg2.connect(host= hostx, 
                         database= databasex,
                         user= userx, 
                         password= passwordx)
  return con

# Executa query sql
def executa_query(sql, hostx, databasex, userx, passwordx):
  con = conecta_db(hostx, databasex, userx, passwordx)
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close()

# Cria a tabela de vendas local
def Cria_table_vendas():
    sql = '''CREATE TABLE IF NOT EXISTS vendas (
    id_venda integer NOT NULL,
    id_funcionario integer NOT NULL,
    id_categoria integer NOT NULL,
    data_venda date NOT NULL,
    venda numeric(10, 2),
    PRIMARY KEY (id_venda)
    )'''
    executa_query(sql, 'localhost', 'db_bix_teste', 'postgres', 'postgres')

# Seleciona maior id_venda incluido
def Maior_id_venda_incluido():
    con = conecta_db('localhost', 'db_bix_teste', 'postgres', 'postgres')
    cur = con.cursor()

    sql = '''Select max(id_venda) from vendas'''
    cur.execute(sql)
    myresult = cur.fetchall()
    con.close

    if str(myresult[0][0]) == 'None':
        max_idvenda = 0
    else:
        for x in myresult:
            max_idvenda = int(myresult[0][0])
        
    return max_idvenda

# Seleciona vendas a serem incluidas no banco local
def Novas_vendas(max_idvenda):
    con = conecta_db('34.173.103.16', 'postgres', 'junior', '|?7LXmg+FWL&,2(')
    cur = con.cursor()

    #sql = '''Select * from venda where id_venda > %s'''
    strSelect = f"Select * from venda where id_venda > {max_idvenda}"
    sql = strSelect
    cur.execute(sql)
    myresult = cur.fetchall()
    con.close

    return myresult

# Converte list para dataframe pandas
def Converte_list_to_dataframe(mylist):
  
    id_venda= []
    id_funcionario= []
    id_categoria= []
    data_venda= []
    venda= []    


    for x in mylist:
        id_venda.append(x[0])
        id_funcionario.append(x[1])
        id_categoria.append(x[2])
        data_venda.append(x[3])
        venda.append(x[4])

    df=pd.DataFrame({
            "id_venda":id_venda,
            "id_funcionario":id_funcionario,
            "id_categoria":id_categoria,
            "data_venda":data_venda,
            "venda":venda,                
            })

    return df

# Le a tabela de vendas no postgresql na nuvem e grava na tabela vendas no banco de dados postgreSQL local
def ETL_VENDAS():
    Cria_table_vendas()

    max_idvenda = Maior_id_venda_incluido()

    myresult = Novas_vendas(max_idvenda)

    if len(myresult) == 0:
        print('***** NÃO EXISTE VENDA A SER INCLUÍDA')
    else:
        df_vendas = Converte_list_to_dataframe(myresult)

        engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
        df_vendas.to_sql('vendas', engine, if_exists='append', index=False)

        print('VENDAS INCLUÍDAS')
        print(df_vendas)



# Le API de funcionário variando o id de 1 até 9
def Consome_API_funcionario(url):

    response = requests.get(url)
    linha = response.text
    return linha

# Consome API de funcionario e grava na tabela funcionario no banco de dados local
def ETL_FUNCIONARIOS():
    id_funcionario= []
    nome_funcionario= []

    url='https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id='

    seq_id=1
    while seq_id <= 9:

        linha = Consome_API_funcionario(url + str(seq_id))
        id_funcionario.append(int(seq_id))
        nome_funcionario.append(str.lstrip(linha))

        seq_id=seq_id+1

    df_funcionario=pd.DataFrame({
            "id_funcionario":id_funcionario,
            "nome_funcionario":nome_funcionario,
            })

    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
    df_funcionario.to_sql('funcionarios', engine, if_exists='replace', index=False)

    print('FUNCIONÁRIOS INCLUÍDOS')
    print(df_funcionario)



# Baixa e le arquivo de categorias
def Baixa_le_arquivo_categoria(url):

    arqCat = requests.get(url, verify = False) 
    with open('categorias.parquet', 'wb') as arq:
        arq.write(arqCat.content)    

    df = pd.read_parquet('categorias.parquet')
    return df

# Grava na tabela de categorias no banco de dados postgreSQL
def ETL_CATEGORIAS():
    url='https://storage.googleapis.com/challenge_junior/categoria.parquet'
    df_categorias = Baixa_le_arquivo_categoria(url)

    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
    df_categorias.to_sql('categorias', engine, if_exists='replace', index=False)

    print('CATEGORIAS INCLUÍDAS')
    print(df_categorias)



def carrega_cat():
    ETL_CATEGORIAS()

def carrega_func():
    ETL_FUNCIONARIOS()

def carrega_vdas():
    ETL_VENDAS()   

# DAG_BIX_TESTE
with DAG('Dag_bix_teste_airflow', start_date = datetime(2021,10,21),
         schedule_interval='* * * * *', catchup = False) as dag:
         #schedule_interval='0 3 * * *', catchup = False) as dag:         

    # Task CARREGA CATEGORIAS
    Tcat = PythonOperator(
        task_id = 'task_categorias',
        python_callable = carrega_cat
    )

    # Task CARREGA FUNCIONARIOS
    Tfunc = PythonOperator(
        task_id = 'task_funcionarios',
        python_callable = carrega_func
    )

    # Task CARREGA VENDAS
    Tvdas = PythonOperator(
        task_id = 'task_vendas',
        python_callable = carrega_vdas
    )

    # Define a ordem de execução das tasks
    [Tcat, Tfunc, Tvdas]