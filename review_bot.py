import datetime
import sqlalchemy
import gaia 
import database 
import posting 
import scraping

# Constants
WEXTRACTOR_AUTH_TOKEN = "68e2113b07b07c6cede5d513b66eba5f8db1701b" 
MAX_WEX_CALLS = 1

SCRAPINGDOG_API_KEY = "6619300786d2b244207115b9"

# Main

    # Steps: 
    #   1. Get new reviews from Wextractor
    #   2. Put these reviews into the SQL Database (staging, then main) (does SQLalchemy eliminate the need for staging?)
    #   3. Pull unanswered reviews from SQL Database (most likely only recent ones)
    #   4. Feed these in GAIA one by one

try:
    print("start")
    engine = sqlalchemy.create_engine(f"mssql+pyodbc://{database.USER}:{database.PW}@{database.SQL_SERVER_NAME}/{database.DATABASE}?driver={database.MSSQL_DRIVER}")
    con = engine.connect()
    if not con:
        print("problem connecting to DB, exiting...")
        exit()
    print("connected to db")
    new_reviews_indeed = scraping.extract_new_reviews("Indeed", datetime.datetime.now() - datetime.timedelta(2))
    print("scraped indeed")
    database.put_df_in_sql(new_reviews_indeed, con)
    print("indeed into db")
    new_reviews_glassdoor = scraping.extract_new_reviews("Glassdoor", datetime.datetime.now() - datetime.timedelta(5))
    print("scraped glassdoor")    
    database.put_df_in_sql(new_reviews_glassdoor, con)
    print("glassdoor into db")
    new_reviews_kununu = scraping.extract_new_reviews("Kununu", datetime.datetime.now() - datetime.timedelta(3))
    print("scraped kununu")
    database.put_df_in_sql(new_reviews_kununu, con)
    print("kununu into db")
    print("finished, exiting...")
except Exception as e:
     # creating/opening a file
    f = open("logs.txt", "a")
 
    # writing in the file
    f.write(str(e))
     
    # closing the file
    f.close() 
