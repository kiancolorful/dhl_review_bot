import keyboard
import webbrowser
import pandas
import sqlalchemy
import pyperclip
import time

# Grobe Idee erklärt:
# Die IT-Sicherheitsteams von Indeed, Glassdoor und kununu sind mit Sicherheit bessere Programmierer und Programmiererinnen als wir. 
# Da es sich beim Posting von den Responses um etwas handelt, welches ein sehr hohes Risiko trägt (von der Plattform gebannt zu werden) 
# und nur ein sehr kleines Gewinn bringt (jeden Tag 3 Minuten sparen), haben wir beschlossen, das Posting nur semiautomatisch zu machen, 
# damit eventuell auftretende Captchas von einem echten Menschen gelöst werden können. So vermeiden wir auch potenzielle legale Hürden. 
# Das semiautomatische Posting soll folgendermaßen erfolgen:
# 
# 1. Hochzuladene Responses werden aus der Datenbank in eine DataFrame geladen.
# 2. Der Text des ersten Responses wird in die Zwischenablege des Rechners kopiert, und der Link zur Review wird im Browser geöffnet. 
# 3. Man meldet sich auf der Plattform im DHL-Konto an, fügt den Text per Strg-V in das Textfeld ein und postet sie. 
# 4. Nachdem die Antwort erfolgreich gepostet wurde, drückt man die Rechts-Taste (Diese kann unten im Code beliebig geändert werden) um 
#    die nächste Bewertung im Browser zu öffnen und den zugehörigen ResponseText in die Zwischenlage zu kopieren. Man macht dann weiter 
#    bis alle Bewertungen gepostet sind. Man kann mit der escape-Taste das Programm jederzeit beenden. Bereits hochgeladene Responses werden dabei 
#    in der Datenbank als hochgeladen markiert. 

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