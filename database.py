import pyodbc
import sqlalchemy
import pandas
import datetime
from utils import log

# Review refresh parameters (see documentation)
NEW_REVIEW_REFRESH_DELAY = 14 # After how many days should new reviews be refreshed?
SENSITIVE_REVIEW_REFRESH_FREQUENCY = 4 # How many days between refreshes for sensitive reviews?
SENSITIVE_REVIEW_REFRESH_INTERVAL = 21 # For how many days should sensitive reivews be refreshed?
OLD_REVIEW_REFRESH_COUNT = 50 # How many of the oldest reviews should be refreshed? (negative values mean all reviews for the oldest day in the DB)

# SQL Auth stuff
MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server' # Alternative: ODBC Driver 17 for SQL Server
SQL_SERVER_NAME = r"85.215.196.5" # IP: 85.215.196.5, Instance name: WIN-CIH1M1J41BG
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_SCHEMA'
SQL_STAGING_TABLE_NAME = 'DHL_STAGING'
USER = 'kian'
PW = 'Gosling1'

# Data model
DATABASE_COLUMNS_AND_DATA_TYPES = {
    "Portal": "nvarchar(10)", 
    "ID": "nvarchar(50)", 
    "Link": "nvarchar(255)", 
    "ReviewTitle": "nvarchar(255)", 
    "RefreshDate": "date", 
    "ReviewDate": "date", 
    "OnlineYesNo": "nvarchar(10)",
    "OverallSatisfaction": "float", 
    "JobTitle": "nvarchar(50)",
    "Department": "nvarchar(50)", 
    "CurrentFormerEmployee": "nvarchar(10)", 
    "ContractTerminationKununuOnly": "int", 
    "Location": "nvarchar(50)", 
    "StateRegion": "nvarchar(50)", 
    "Country": "nvarchar(50)", 
    "Language": "nvarchar(10)",
    "ReviewText": "nvarchar(MAX)", 
    "ReviewTextEN": "nvarchar(MAX)", 
    "MainpositiveAspect": "nvarchar(255)", 
    "MainAreaofImprovement": "nvarchar(255)", 
    "SensitiveTopic": "nvarchar(10)", 
    "ApprovalStatus": "nvarchar(30)",
    "ResponsePostedYesNo": "nvarchar(10)", 
    "Response": "nvarchar(MAX)", 
    "ResponseEN": "nvarchar(MAX)", 
    "EditedResponse": "nvarchar(MAX)",
    "EstResponseDate": "date", 
    "ResponseTimeDays": "int", 
    "EmpathyScore": "float", 
    "HelpfulnessScore": "float", 
    "IndividualityScore": "float", 
    "OverallScore": "float", 
    "WeightedScore": "float",
    "DeveloperComment": "nvarchar(255)",
    "last_modified": "datetime"
}

# Diese Funktion fügt einen DataFrame in die Datenbank ein. insert_new und update_existing bestimmen jeweils, 
# ob neue Zeilen eingefügt werden sollen, und ob existierende Zeilen überschrieben werden sollen. 
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

# Diese Funktion lädt alle unbeantworteten Reviews aus der Datenbank herunter. Mit since kann man eine Untergrenze für das Alter der Reviews angeben. 
def fetch_unanswered_reviews(con, since=False) -> pandas.DataFrame: 
    try:
        if since:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ((Response='' OR Response IS NULL) AND ReviewDate>='{since.strftime('%Y-%m-%d')}') OR ApprovalStatus='Regenerate'", con)
        else:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE (Response='' OR Response IS NULL) OR ApprovalStatus='Regenerate'", con)
        return df
    except pyodbc.Error as ex:
        log(ex, __file__)

# These reviews were answered on kununu before the bot got to them. The goal of this is to fill out the extra information columns for these, which are usually filled out by GAIA when generating a response. 
def fetch_incomplete_rows(con, count : int=None) -> pandas.DataFrame: 
    try:
        if count:
            df = pandas.read_sql(f"SELECT TOP {count} * FROM {SQL_TABLE_NAME} WHERE NOT (Response='' OR Response IS NULL) AND SensitiveTopic IS NULL", con) # Using SensitiveTopic arbitrarily, scores would work as well
        else:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE NOT (Response='' OR Response IS NULL) AND SensitiveTopic IS NULL", con)
        return df
    except pyodbc.Error as ex:
        log(ex, __file__)

# Diese Funktion lädt alle Reviews us der Datenbank herunter, die "refresht" werden müssen (es hauptsächlich wird geschaut, ob die Review noch online ist).
# df_new besteht aus Reviews, die genau 2 Wochen alt sind. 
# df_sen besteht aus Reviews, die SensitiveTopics sind und weniger als 3 Wochen alt sind. 
# df_old besteht aus den Reviews, die am längsten nicht "refresht" worden sind. 
def fetch_refresh_reviews(con) -> pandas.DataFrame:
    try:
        # new reviews
        df_new = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ReviewDate='{(datetime.date.today() - datetime.timedelta(NEW_REVIEW_REFRESH_DELAY)).strftime('%Y-%m-%d')}' AND (OnlineYesNo IS NULL OR OnlineYesNo='Yes')", con)
        
        #sensitive reviews
        df_sen = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ReviewDate>='{(datetime.date.today() - datetime.timedelta(SENSITIVE_REVIEW_REFRESH_INTERVAL)).strftime('%Y-%m-%d')}' AND RefreshDate<='{(datetime.date.today() - datetime.timedelta(SENSITIVE_REVIEW_REFRESH_FREQUENCY)).strftime('%Y-%m-%d')}' AND SensitiveTopic='Yes' AND (OnlineYesNo IS NULL OR OnlineYesNo='Yes')", con)
        
        #old reviews
        if(OLD_REVIEW_REFRESH_COUNT < 0): # Get all reviews that share the oldest RefreshDate
            df_old = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE RefreshDate=(SELECT MIN(RefreshDate) from {SQL_TABLE_NAME}) AND (OnlineYesNo='Yes' OR OnlineYesNo IS NULL)", con)
        else:
            df_old = pandas.read_sql(f"SELECT TOP {OLD_REVIEW_REFRESH_COUNT} * FROM {SQL_TABLE_NAME} ORDER BY RefreshDate ASC", con)
        df_all = pandas.concat([df_new, df_sen, df_old], ignore_index=True) # Remove empty dfs (previous behavior deprecated)
        return df_all 
    except Exception as ex:
        log(ex, __file__)

# Evtl. sind die DHL-Mitarbeitenden ab und zu unzufrieden mit einer von GAIA generierten Response. In dem Fall können Sie Ihn mit "Regenerate" in der Datenbank markieren. 
# Dies sagt aus, dass die Antwort von GAIA nochmal neu generiert werden soll. Diese Funktion holt alle Reviews, die mit "Regenerate" markiert sind. Mit count kann 
# eine Maximalanzahl an zurückgegebene Reviews gesetzt werden. 
def fetch_regenerate_reviews(con, count : int=None) -> pandas.DataFrame:
    try:
        if count:
            df = pandas.read_sql(f"SELECT TOP {count} * FROM {SQL_TABLE_NAME} WHERE ApprovalStatus='Regenerate'", con) # Regenerate might be replace with refresh
        else:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ApprovalStatus='Regenerate'", con)
        return df
    except pyodbc.Error as ex:
        log(ex, __file__)

# Diese Funktion holt alle reviews, wo eine Response schon exitiert, aber keine EN-Übersetzung. 
def fetch_translate_reviews(con, num : int=None) -> pandas.DataFrame:
    try:
        if(num):
            df = pandas.read_sql(f"SELECT TOP {num} * FROM {SQL_TABLE_NAME} WHERE ReviewTextEN IS NULL AND ResponseEN IS NULL AND Response IS NOT NULL ORDER BY ReviewDate DESC", con)
        else:
            df = pandas.read_sql(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ReviewTextEN IS NULL AND ResponseEN IS NULL AND Response IS NOT NULL ORDER BY ReviewDate DESC", con)
        return df
    except Exception as ex:
        log(ex, __file__, "Error connecting to database while trying to translate reviews.")
        pass