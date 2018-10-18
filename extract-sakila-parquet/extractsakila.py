# Esse arquivo é usado para simular os dados de origens, extraindo
#  as tabelas do shema Sakila 

import pandas as pd
import MySQLdb # pip install mysqlclient    

# Conexão com o Mysql
MyHost     = 'localhost'
MyPortHost = 3306
MyUser     = 'root'
MyPassword = 'estudo'
MyDB       = 'sakila'

mysql_cn= MySQLdb.connect(host=MyHost, 
                          port=MyPortHost,
                          user=MyUser,
                          passwd=MyPassword, 
                          db=MyDB)


select_tables = "select table_name from "\
"information_schema.TABLES where TABLE_SCHEMA = '"+ MyDB+"'"
df_mysql = pd.read_sql(select_tables, con=mysql_cn)
    

df_tables = df_mysql.to_dict(orient='list')
list_tables = list(df_tables.values())[0]

for i in range(len(list_tables)):
    select_tables = "select * from "+list_tables[i] 
    pd.read_sql(select_tables, con=mysql_cn).to_parquet(list_tables[i]+'.parquet')

mysql_cn.close()
