import keyboard
import webbrowser
import pandas
import sqlalchemy
import pyperclip
import time

# Database credentials
MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server' # Alternative: ODBC Driver 17 for SQL Server
SQL_SERVER_NAME = r"85.215.196.5" # IP: 85.215.196.5, Instance name: WIN-CIH1M1J41BG
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_SCHEMA'
SQL_STAGING_TABLE_NAME = 'DHL_STAGING'
USER = 'kian'
PW = 'Gosling1'

# Hotkeys
HK_NEXT = "right"
HK_QUIT = "esc"

def quit(df, con):
    print("updating database before quitting...")
    # Clear staging table and put dataframe in
    df.to_sql(SQL_STAGING_TABLE_NAME, con, if_exists='replace', index=False) # Commits automatically 

    # Only update existing entries
    con.execute(sqlalchemy.text(f"DELETE FROM {SQL_STAGING_TABLE_NAME} WHERE ID NOT IN (SELECT ID FROM {SQL_TABLE_NAME});"))
    con.execute(sqlalchemy.text(f"DELETE FROM {SQL_TABLE_NAME} WHERE ID IN (SELECT ID FROM {SQL_STAGING_TABLE_NAME});"))
    con.execute(sqlalchemy.text(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME};"))
    # Merge new rows to main table (ignore dupes) and empty staging table
    con.execute(sqlalchemy.text(f"DELETE FROM {SQL_STAGING_TABLE_NAME};"))
    con.commit()
    print("done")
    print("quitting...")
    exit()

# Connect to DB
print("start")
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{USER}:{PW}@{SQL_SERVER_NAME}/{DATABASE}?driver={MSSQL_DRIVER}")
print("connecting to db...")
con = engine.connect()
if not con:
    print("problem connecting to DB, exiting...")
    exit()
print("done")

# Get responses
try:
    df = pandas.read_sql(f"SELECT Link, Response FROM {SQL_TABLE_NAME} WHERE ResponsePostedYesNo='No' AND OnlineYesNo='Yes' AND ApprovalStatus='Approved' OR ApprovalStatus='ApplyNewResponse'", con)
except:
    print("Error connecting to database, exiting...")
    time.sleep(1)
    con.close()

# Set hotkeys
keyboard.add_hotkey(HK_QUIT, quit, args=(df, con))

# Start posting
for row in df.itertuples():
    webbrowser.open(row.Link)
    pyperclip.copy(row.Response)
    keyboard.wait(HK_NEXT)
    #df.at[row.Index, "ResponsePostedYesNo"] = "Yes"

print("All responses posted! Saving and exiting...")
quit(con)