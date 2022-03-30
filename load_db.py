
from os import listdir
from os.path import isfile, join
import zipfile
import pandas as pd

def get_table_name(csv_name):
    if 'coe1' in csv_name.lower():
        return 'COE1'
    if 'coe2' in csv_name.lower():
        return 'COE2'
    if 'hog' in csv_name.lower():
        return 'HOG'
    if 'sdem' in csv_name.lower():
        return 'SDEM'
    if 'viv' in csv_name.lower():
        return 'VIV'

csv_folder = 'sql/csv/'

zip_files = [f for f in listdir(csv_folder) if isfile(join(csv_folder, f))]

from db.conn import mysql_conn
#ms = mysql_conn('192.168.100.8', 'root', 'root', 3306, 'ENOE')
ms = mysql_conn('localhost', 'root', 'root', 3307, 'ENOE')

for zipf in zip_files:
    
    if '.zip' not in zipf.lower():
        continue

    print(f'{csv_folder}{zipf}')
    zip = zipfile.ZipFile(f'{csv_folder}{zipf}')
    
    for izip in zip.namelist():
        
        if '.csv' not in izip.lower():
            print(f'\t{izip}')
            continue

        f = zip.open(izip)
        
        content = f.read()
        f = open(f'{csv_folder}temp.csv', 'wb')
        f.write(content)
        f.close()

        table = get_table_name(izip)

        print(izip, table)

        try:
            df = pd.read_csv(f'{csv_folder}temp.csv')

        except Exception as e:
            df = pd.read_csv(f'{csv_folder}temp.csv', encoding='latin-1')
            print(e)

        ms.populate_db(table, df)

