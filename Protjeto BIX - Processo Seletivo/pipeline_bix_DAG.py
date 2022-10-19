import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
import pandas as pd
import requests
import io

# extração dos dados do banco postgre disponibilizado pela bix
def postgresql_connection():

# conexão com o banco bix
    def conecta_db():
        con = psycopg2.connect(
            host = "34.173.103.16",
            user = 'junior',
            password = '|?7LXmg+FWL&,2(',
            database = 'postgres'
        )
        return con

# consulta no banco bix
    def consultar_db(sql):
        con = conecta_db()
        cur = con.cursor()
        cur.execute(sql)
        recset = cur.fetchall()
        tabela = []
        for rec in recset:
            tabela.append(rec)
        con.close()
        return tabela

    tabela_venda =  consultar_db('''select * from venda''')

# transforma a consulta feita no banco em um dataframe
    df = pd.DataFrame(tabela_venda)

# como as colunas não vêm com seus nomes, é necessário inseri-los
    df.columns = ['id_venda', 'id_funcionario', 'id_categoria', 'data_venda', 'venda']

# conexão com o banco de dados local   
    def conecta_db_local():
        con2 = psycopg2.connect(
            host = 'postgres',
            database = 'bix',
            user = 'airflow',
            password = 'airflow',
            port = '5432')
        
        return con2 

# cria tabela venda 
    def criar_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        cur2.execute(sql)
        con2.commit()
        con2.close()

    sql = '''
            CREATE TABLE IF NOT EXISTS venda
            (id_venda INT NOT NULL,
            id_funcionario INT NOT NULL,
            id_categoria INT NOT NULL,
            data_venda DATE,
            venda VARCHAR (255))
            '''
    criar_db(sql)

# insere dados na tabela venda do banco local
    def inserir_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        try:
            cur2.execute(sql)
            con2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error %s" % error)
            con2.rollback()
            cur2.close
            return 1

        cur2.close()

# nesta etapa foi feito o for para a inserção da grande quantidade de dados na tabela
    for i in df.index:
        sql = """
        INSERT into public.venda (id_venda, id_funcionario, id_categoria, data_venda, venda)
        values('%s', '%s', '%s', '%s', '%s');
        """ % (df['id_venda'][i], df['id_funcionario'][i], df['id_categoria'][i], df['data_venda'][i], df['venda'][i])
        inserir_db(sql)
    return 'success'

# extração de dados da api
def api_connection():
    id = 1
    funcionarios = []

# como a quantidade de funcionários é 9, o while tem a condição id < 10. Caso esse número sofra mudança, a alteração deverá ser feita. Futuramente arrumar uma forma de automatizar para não precisar trocar manualmente
    while id < 10:
        response = requests.get(f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={id}')

        response = (response.content)

        response = response.decode()

        funcionarios.append(response)

        id = id + 1

# transforma a resposta da api em um df
    df = pd.DataFrame(funcionarios)

# denomina a coluna como "funcionario"
    df.columns = ['funcionario']

# insere a coluna id. Essa etapa foi feita devido a coluna id_funcionário na tabela venda
    id = 1
    x = []
    while id <= (len(df.index)):
        x.append(id)    
        id = id + 1

    df['id'] = x

# conexão com o banco de dados local
    def conecta_db_local():
        con2 = psycopg2.connect(
            host = 'postgres',
            database = 'bix',
            user = 'airflow',
            password = 'airflow',
            port = '5432')
        
        return con2 

# função para criar a tabela funcionarios
    def criar_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        cur2.execute(sql)
        con2.commit()
        con2.close()

    sql = '''
            CREATE TABLE IF NOT EXISTS funcionarios
            (funcionario VARCHAR (255),
            id INT NOT NULL)
            '''
    criar_db(sql)

# função para inserir os dados na tabela funcionarios
    def inserir_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        try:
            cur2.execute(sql)
            con2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error %s" % error)
            con2.rollback()
            cur2.close
            return 1

        cur2.close()

# inserção dos dados na tabela funcionarios
    for i in df.index:
        sql = """
        INSERT into public.funcionarios (funcionario, id)
        values('%s', '%s');
        """ % (df['funcionario'][i], df['id'][i])
        inserir_db(sql)

    return 'success'

# extração dos dados do arquivo parquet
def parquet_file():

# requisição dos dados para o link informado
    url = 'https://storage.googleapis.com/challenge_junior/categoria.parquet'

    response = requests.get(url)

    response = response.content

    response = io.BytesIO(response)

# leitura dos dados do arquivo parquet
    df = pd.read_parquet(response)

# transformação da resposta da api em um df
    df = pd.DataFrame(df)

#conexão com o banco local
    def conecta_db_local():
        con2 = psycopg2.connect(
            host = 'postgres',
            database = 'bix',
            user = 'airflow',
            password = 'airflow',
            port = '5432')
        
        return con2 

# criação da tabela categoria
    def criar_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        cur2.execute(sql)
        con2.commit()
        con2.close()

    sql = '''
            CREATE TABLE IF NOT EXISTS categoria
            (id INT NOT NULL,
            nome_categoria VARCHAR (255))
            '''
    criar_db(sql)

# inserção de dados na tabela categoria
    def inserir_db(sql):
        con2 = conecta_db_local()
        cur2 = con2.cursor()
        try:
            cur2.execute(sql)
            con2.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error %s" % error)
            con2.rollback()
            cur2.close
            return 1

        cur2.close()

    for i in df.index:
        sql = """
        INSERT into public.categoria (id, nome_categoria)
        values('%s', '%s');
        """ % (df['id'][i], df['nome_categoria'][i])
        inserir_db(sql)

    return 'success'

# parâmetros do airflow colocando a dag e suas tasks para rodar
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 10, 11, 00, 00, 00),
    'concurrency': 1,
    'email': 'hugolobato93@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'concurrency': 8
}

with DAG('pipeline_bix_DAG',
         default_args=default_args,
         schedule_interval='0 1 * * *',
         ) as dag:
    opr_run1 = PythonOperator(task_id = 'postgresql_connection', python_callable= postgresql_connection)
    opr_run2 = PythonOperator(task_id = 'api_connection', python_callable= api_connection)
    opr_run3 = PythonOperator(task_id = 'parquet_file', python_callable= parquet_file)

opr_run1 >> opr_run2 >> opr_run3 