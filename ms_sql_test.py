import pyodbc 

MSSQL_DRIVER = 'SQL Server' # Alternative: ODBC Driver 18 for SQL Server
SQL_SERVER_NAME = r"85.215.196.5"
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_OLD'
PW = 'Gosling1'
USER = 'kian'

connection = pyodbc.connect(f"DRIVER={MSSQL_DRIVER};Server={SQL_SERVER_NAME};Database={DATABASE};UID={USER};PWD={PW};") 
cursor = connection.cursor()
cursor.execute(f"SELECT * FROM {SQL_TABLE_NAME}")
result = cursor.fetchall()
a = 5

text = "blue,green,yellow,red"
print(f'List of colors = {text.split(',')}')
