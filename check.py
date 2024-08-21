import pyodbc

# Connection parameters
MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server'
SQL_SERVER_NAME = r"85.215.196.5"  # IP: 85.215.196.5, Instance name: WIN-CIH1M1J41BG
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_SCHEMA'
USER = 'kian'
PW = 'Gosling1'

# Create the connection string
connection_string = (
    f'DRIVER={{{MSSQL_DRIVER}}};'
    f'SERVER={SQL_SERVER_NAME};'
    f'DATABASE={DATABASE};'
    f'UID={USER};'
    f'PWD={PW}'
)

# Connect to the database
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Execute SQL commands
try:
    # Use the specified database (in this case 'master')
    cursor.execute(f"USE {DATABASE}")
    
    # Run the DBCC CHECKTABLE command
    cursor.execute(f"DBCC CHECKTABLE ('{SQL_TABLE_NAME}')")

    # Commit the transaction if needed (though it's not needed for DBCC commands)
    connection.commit()

    # Print a success message
    print(f"DBCC CHECKTABLE executed successfully on '{SQL_TABLE_NAME}'.")

except pyodbc.Error as e:
    print(f"Error occurred: {e}")
finally:
    # Close the connection
    cursor.close()
    connection.close()
