from db.conn import mysql_conn

ms = mysql_conn('localhost', 'root', 'root', 3307, 'ENOE')

tables = ['COE1', 'COE2', 'SDEM', 'VIV', 'HOG']

for i in range(11,22):
    for j in range(1,5):
        for k in (tables):
            print(f'{j * 100 + i}, {k}')
            ms.schema_db(k, (j * 100 + i))


#ms.select_trim_df(3)

