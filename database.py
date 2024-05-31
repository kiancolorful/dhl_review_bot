import pyodbc
import sqlalchemy
import pandas
from utils import log

MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server' # Alternative: ODBC Driver 17 for SQL Server
SQL_SERVER_NAME = r"85.215.196.5"
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_SCHEMA'#'CC_DATA'
SQL_STAGING_TABLE_NAME = 'DHL_STAGING'
USER = 'kian'
PW = 'Gosling1'
DATABASE_COLUMNS_AND_DATA_TYPES = {
    "Portal": "nvarchar(10)", 
    "ID": "nvarchar(50)", 
    "Link": "nvarchar(255)", 
    "ReviewTitle": "nvarchar(255)", 
    "ReviewDate": "date", 
    "OverallSatisfaction": "float", 
    "JobTitle": "nvarchar(50)",
    "Department": "nvarchar(50)", 
    "CurrentFormerEmployee": "nvarchar(10)", 
    "ContractTerminationKununuOnly": "int", 
    "Location": "nvarchar(50)", 
    "StateRegion": "nvarchar(50)", 
    "Country": "nvarchar(50)", 
    "ReviewText": "nvarchar(MAX)", 
    "MainpositiveAspect": "nvarchar(255)", 
    "MainAreaofImprovement": "nvarchar(255)", 
    "SensitiveTopic": "nvarchar(10)", 
    "ResponseYesNo": "nvarchar(10)", 
    "Response": "nvarchar(MAX)", 
    "EstResponseDate": "date", 
    "EmpathyScore": "float", 
    "HelpfulnessScore": "float", 
    "IndividualityScore": "float", 
    "ResponseTimeScore": "float", 
    "OverallScore": "float", 
    "WeightedScore": "float"
}

def sql_insert_row(table_name, row, connection): 
    try:
        row_dict = row.to_dict()
        df = pandas.DataFrame([row_dict])
        df.to_sql(name=table_name, con=connection, if_exists='append', index=False)
    except Exception as e:
        print(f"Error inserting row into SQL table: {e}")

def put_df_in_sql(df : pandas.DataFrame, con : sqlalchemy.Connection, insert_new=True, update_existing=False): 
    # DEFAULT: ONLY INSERT NEW RECORDS, DON'T UPDATE
    if not(insert_new or update_existing) or (df.empty): # Don't insert new + don't update old = no action, empty df = no action
        return
    
    # Clear staging table and put dataframe in
    df.to_sql(SQL_STAGING_TABLE_NAME, con, if_exists='replace', index=False) # Commits automatically 

    if (insert_new and update_existing): # Do both 
        con.execute(sqlalchemy.text(f"DELETE FROM {SQL_TABLE_NAME} WHERE ID IN (SELECT ID FROM {SQL_STAGING_TABLE_NAME});")) 
        con.execute(sqlalchemy.text(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME};"))
    elif insert_new: # Only insert new entries (default)
        con.execute(sqlalchemy.text(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME} staging WHERE staging.ID NOT IN (SELECT ID FROM {SQL_TABLE_NAME});"))
    elif update_existing: # Only update existing entries
        con.execute(sqlalchemy.text(f"DELETE FROM {SQL_STAGING_TABLE_NAME} WHERE ID NOT IN (SELECT ID FROM {SQL_TABLE_NAME});"))
        con.execute(sqlalchemy.text(f"DELETE FROM {SQL_TABLE_NAME} WHERE ID IN (SELECT ID FROM {SQL_STAGING_TABLE_NAME});"))
        con.execute(sqlalchemy.text(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME};"))
    # Merge new rows to main table (ignore dupes) and empty staging table
    con.execute(sqlalchemy.text(f"DELETE FROM {SQL_STAGING_TABLE_NAME};"))
    con.commit()

def update_sql_entries(reviews : pandas.DataFrame, con: sqlalchemy.Connection): 
    try:
        for review in reviews.iterrows():
            con.execute(f"UPDATE progress SET CockpitDrill = '{review['Answer (text)']}' WHERE [Dialogue ID] = {review['Dialogue ID']}")
        con.commit()
    except Exception as e:
        log(e)

def fetch_unanswered_reviews(engine, since=False) -> pandas.DataFrame: 
    try:
        if since:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE (ResponseYesNo='No' OR ResponseYesNo IS NULL) AND ReviewDate>='{since.strftime('%Y-%m-%d')}'", engine)
        else:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE (ResponseYesNo='No' OR ResponseYesNo IS NULL)", engine)
        return df
    except pyodbc.Error as exception:
        log(exception)
