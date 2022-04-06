from db.conn import mysql_conn

ms = mysql_conn('localhost', 'root', 'root', 3307, 'ENOE')
tables = ['COE1', 'COE2', 'SDEM', 'VIV', 'HOG']

# Carga del esqueleto de la db
for i in range(11,22):
    for j in range(1,5):
        for k in (tables):
            ms.schema_db(k, (j * 100 + i))


