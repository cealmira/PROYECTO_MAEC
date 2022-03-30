from db.conn import mysql_conn

ms = mysql_conn('localhost', 'root', 'root', 3307, 'sakila')
ms.select_trim_df(3)

