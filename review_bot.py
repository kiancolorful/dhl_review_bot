import datetime
import sqlalchemy
import gaia 
import database 
import posting 
import scraping
import pandas
from utils import log

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
    print("connecting to db...")
    con = engine.connect()
    if not con:
        print("problem connecting to DB, exiting...")
        exit()
    print("done")
    print("extracting new indeed reviews...")
    new_reviews_indeed = scraping.extract_new_reviews("Indeed", datetime.datetime.now() - datetime.timedelta(2))
    print("done")
    print("putting indeed reviews into database...")
    database.put_df_in_sql(new_reviews_indeed, con)
    print("done")
    print("extracting new glassdoor reviews...")
    new_reviews_glassdoor = scraping.extract_new_reviews("Glassdoor", datetime.datetime.now() - datetime.timedelta(5))
    print("done")
    print("putting glassdoor reviews into database...")
    database.put_df_in_sql(new_reviews_glassdoor, con)
    print("done")
    print("extracting new kununu reviews...")
    new_reviews_kununu = scraping.extract_new_reviews("kununu", datetime.datetime.now() - datetime.timedelta(3))
    print("done")
    print("putting kununu reviews into database...")
    database.put_df_in_sql(new_reviews_kununu, con)
    print("done")

    print("pulling unanswered reviews from the past few days from database...")
    unanswered_reviews = database.fetch_unanswered_reviews(engine, datetime.datetime.now() - datetime.timedelta(3))
    print("done")
    
    f = open("df.txt", "w")
    f.write(unanswered_reviews.to_string())
    f.close()

    print("generating response and gaia data for unanswered reviews...")
    gaia.generate_responses(unanswered_reviews)

    f = open("df.txt", "a")
    f.write("\n\n\n" + unanswered_reviews.to_string())
    f.close()

    print("done")

    # UPLOAD HERE

    print("updating database entries to include answers and gaia data...")
    database.put_df_in_sql(unanswered_reviews, con, True, True)
    print("done")

    print("finished, exiting...")
except Exception as e:
    log(e)
