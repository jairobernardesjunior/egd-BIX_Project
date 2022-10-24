''' BIX Teste:
    CONSOME API FUNCIONARIO E GRAVA DADOS DE FUNCIONARIO NA TABELA FUNCIONARIOS NO BANCO DE DADOS LOCAL
'''

import pandas as pd
import requests
from sqlalchemy import create_engine

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

# Start
ETL_FUNCIONARIOS()