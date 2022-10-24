''' BIX Teste:
    LE A TABELA DE VENDAS NA NUVEM E SALVA ESSES DADOS NA TABELA DE VENDAS NO BANCO DE DADOS LOCAL
'''

import conecta_db as cn
import executa_query_sql as ex
from sqlalchemy import create_engine
import pandas as pd

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
    ex.executa_query(sql, 'localhost', 'db_bix_teste', 'postgres', 'postgres')

# Seleciona maior id_venda incluido
def Maior_id_venda_incluido():
    con = cn.conecta_db('localhost', 'db_bix_teste', 'postgres', 'postgres')
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
    con = cn.conecta_db('34.173.103.16', 'postgres', 'junior', '|?7LXmg+FWL&,2(')
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

# start
ETL_VENDAS()