
from getpass import getpass
import mysql.connector
from mysql.connector import connect, Error
import pandas as pd
import math
import db.table_const as table_const 

class mysql_conn:

    def __init__(self, host, usr, pswd, port = 3307, database = 'enoe'):
        """
        Se definen los parametros para conectarse a la base de datos
        usr = usuario
        pswd = contraseña de la base
        port = puerto donde escucha
        database = base a conectarse
        """
        self.host = host
        self.usr = usr
        self.pswd = pswd
        self.port = port
        self.database = database

    def init_conn(self):
        """
        Inicializa la conexion a la db
        """
        conn = connect(host = self.host,
            user = self.usr,
            password = self.pswd,
            port = self.port,
            database = self.database)
        return conn

    def get_var(self, x):
        if isinstance(x, str):
            if x.strip() == '':
                return None
            return x
            #else:
            #    try:
            #        x = float(x)
            #    except:
            #        return x
                    #print("Non numeric: ", x)
            #    return x
            
        if math.isnan(x):
            return None
        return str(x)

    def query_maker(self, table, names, trim):
        colums = ','.join(tuple(names))
        params = ','.join(tuple(['%s' for i in range(len(names))]))
        query = f'INSERT INTO {table}_{trim}({colums}) VALUES ({params})'
        return query

    def populate_db(self, table, df):
        """
        Recibe de parametro un dataframe y lo inserta en la base de datos 
        """

        try:
            trim = df['per'][0]
        except Exception as e:
            trim = df['PER'][0]

        table_query = table_const.get_table_query(table, trim)

        conn = self.init_conn()
        cursor = conn.cursor()

        cursor.execute(table_query)
        conn.commit()

        query = self.query_maker(table, df.columns, trim)

        #print(query)

        table_values = []

        for index, row in df.iterrows():
            params = [self.get_var(i) for i in tuple(list(row))]
            table_values.append(params)

            if len(table_values) == 4000:
                cursor.executemany(query, table_values)
                table_values = []

            #cursor.execute(query, params)

        if len(table_values) > 0:
            cursor.executemany(query, table_values)

        conn.commit()
        conn.close()

    def select_trim_raw(self, trim):
        """
        Regresa toda la informacion pertinente al trimestre indicado
        """
        conn = self.init_conn()
        cursor = conn.cursor()

        query = "SELECT * FROM film"
        cursor.execute(query)

        result = cursor.fetchall()

        for x in result:
            print(x)

        conn.close()


    def select_trim_df(self, trim):
        """
        Regresa toda la informacion pertinente al trimestre indicado
        """
        conn = self.init_conn()

        cursor = conn.cursor()

        query = "SELECT * FROM film"
        df = pd.read_sql(query,conn)

        print(df)

        conn.close()


