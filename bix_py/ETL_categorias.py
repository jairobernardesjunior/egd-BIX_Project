''' BIX Teste:
    BAIXA O ARQUIVO DE CATEGORIAS E GRAVA DADOS DE CATEGORIA NA TABELA CATEGORIAS NO BANCO DE DADOS LOCAL
'''

import pandas as pd
import requests
from sqlalchemy import create_engine

# Le API de funcionário variando o id de 1 até 9
def Baixa_le_arquivo_categoria(url):

    arqCat = requests.get(url, verify = False) 

    with open('arquivo_parquet/categorias.parquet', 'wb') as arq:
        arq.write(arqCat.content)    

    df = pd.read_parquet('arquivo_parquet/categorias.parquet')
    return df

# Grava na tabela de categorias no banco de dados postgreSQL
def ETL_CATEGORIAS():
    url='https://storage.googleapis.com/challenge_junior/categoria.parquet'
    df_categorias = Baixa_le_arquivo_categoria(url)

    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/db_bix_teste')
    df_categorias.to_sql('categorias', engine, if_exists='replace', index=False)

    print('CATEGORIAS INCLUÍDAS')
    print(df_categorias)

# Start
ETL_CATEGORIAS()