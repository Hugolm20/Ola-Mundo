import psycopg2
import pandas as pd

def conecta_db():
    con = psycopg2.connect(
        host = "34.173.103.16",
        user = 'junior',
        password = '|?7LXmg+FWL&,2(',
        database = 'postgres'
    )
    return con

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

df = pd.DataFrame(tabela_venda)

# x = df.rename(columns={'0': 'id_venda', '1': 'id_funcionario', '2':'id_categoria', '3': 'data_venda', '4': 'venda'}, inplace = True)

df.columns = ['id_venda', 'id_funcionario', 'id_categoria', 'data_venda', 'venda']

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
        CREATE TABLE IF NOT EXISTS venda
        (id_venda INT NOT NULL,
        id_funcionario INT NOT NULL,
        id_categoria INT NOT NULL,
        data_venda DATE,
        venda VARCHAR (255))
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
    INSERT into public.venda (id_venda, id_funcionario, id_categoria, data_venda, venda)
    values('%s', '%s', '%s', '%s', '%s');
    """ % (df['id_venda'][i], df['id_funcionario'][i], df['id_categoria'][i], df['data_venda'][i], df['venda'][i])
    inserir_db(sql)