import pandas as pd
import requests
import io
import psycopg2

url = 'https://storage.googleapis.com/challenge_junior/categoria.parquet'

response = requests.get(url)

response = response.content

response = io.BytesIO(response)

df = pd.read_parquet(response)

df = pd.DataFrame(df)

def conecta_db_local():
    con2 = psycopg2.connect(
        host = 'localhost',
        database = 'bix',
        user = 'airflow',
        password = 'airflow',
        port = '5432')
    
    return con2 

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

def inserir_db(sql):
    con2 = conecta_db_local()
    cur2 = con2.cursor()
    cur2.execute(sql)
    con2.commit()
    
    cur2.close()

for i in df.index:
    sql = """
    INSERT into public.categoria (id, nome_categoria)
    values('%s', '%s');
    """ % (df['id'][i], df['nome_categoria'][i])
    inserir_db(sql)