import pandas as pd
import requests
import psycopg2

id = 1
funcionarios = []

while id < 10:
    response = requests.get(f'https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={id}')

    response = (response.content)

    response = response.decode()

    funcionarios.append(response)

    id = id + 1

df = pd.DataFrame(funcionarios)

df.columns = ['funcionario']

id = 1
x = []
while id <= (len(df.index)):
    x.append(id)    
    id = id + 1

df['id'] = x

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
        CREATE TABLE IF NOT EXISTS funcionarios
        (funcionario VARCHAR (255),
        id INT NOT NULL)
        '''
criar_db(sql)

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
    INSERT into public.funcionarios (funcionario, id)
    values('%s', '%s');
    """ % (df['funcionario'][i], df['id'][i])
    inserir_db(sql)