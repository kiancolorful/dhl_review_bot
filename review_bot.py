import datetime
import sqlalchemy
import gaia 
import database 
import posting 
import scraping
import pandas
from utils import log, backup

# Main

    # Steps: 
    #   1. Get new reviews from Wextractor
    #   2. Put these reviews into the SQL Database (staging, then main)
    #   3. Repeat Steps 1 & 2 for each portal
    #   4. Pull unanswered reviews from SQL Database (most likely only recent ones)
    #   5. Feed these into GAIA one by one
    #       a. Determine language of review
    #       b. Generate response in this language
    #   6. Upload GAIA responses to database
    #   7. Review refreshing
    #       a. Select some reviews from the database that are a bit older
    #       b. Check if they are still online, updating the DB if they aren't
    #   8. Review/Response translation
    #       a. Fetch 10 reviews from DB with no translation
    #       b. If they are not in English already, generate an English translation of the review text and the response
    #       c. If they are already in English, use the original texts as the EN translations

try:
    
    # NOTE: Init 
    print("start")
    engine = sqlalchemy.create_engine(f"mssql+pyodbc://{database.USER}:{database.PW}@{database.SQL_SERVER_NAME}/{database.DATABASE}?driver={database.MSSQL_DRIVER}")
    print("connecting to db...")
    con = engine.connect()
    if not con:
        log("problem connecting to DB, exiting...", __file__)
        exit()
    print("done")
    
    # NOTE: Scraping reviews
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

    # NOTE: Refreshing reviews
    print("checking if older reviews have been removed from platforms or otherwise updated...")
    refresh = database.fetch_refresh_reviews(con)
    scraping.refresh_reviews(refresh, con)
    print("done")
    print("updating database")
    database.put_df_in_sql(refresh, con, False, True)
    print("done")
    
    # NOTE: Completing kununu reviews
    print("pulling reviews with incomplete information from database...")
    incomplete_rows = database.fetch_incomplete_rows(con, 5)
    print("done")
    print("generating missing data with gaia...")
    gaia.complete_rows(incomplete_rows)
    print("done")
    print("updating reviews...")
    database.put_df_in_sql(incomplete_rows, con, False, True)
        
    # NOTE: Generating responses
    print("pulling unanswered reviews from the past few days from database...")
    unanswered_reviews = database.fetch_unanswered_reviews(engine, datetime.datetime.now() - datetime.timedelta(5))
    print("done")
    f = open("df.txt", "w") # Overwrite
    f.write(unanswered_reviews.to_string())
    f.close()
    print("generating response and gaia data for unanswered reviews...")
    gaia.generate_responses(unanswered_reviews)
    f = open("df.txt", "a")
    f.write("\n\n\n" + unanswered_reviews.to_string())
    f.close()
    print("done")
    print("updating database entries to include answers and gaia data...")
    database.put_df_in_sql(unanswered_reviews, con, False, True)
    print("done")
    
    f = open("df.txt", "a")
    f.write("\n\n\n" + unanswered_reviews.to_string())
    f.close()

    # NOTE: Generating translations
    print("fetching reviews to be translated into english...")
    to_translate = database.fetch_translate_reviews(con, 10)
    print("done")
    print("generating translations...")
    gaia.generate_translations(to_translate)
    print("done")
    print("inserting review translations back into database...")
    database.put_df_in_sql(to_translate, con, True, True)
    print("done")
    
    print("done")

    # NOTE: Backup
    print("generating backup csv")
    backup(con, database.SQL_TABLE_NAME)
    print("done")
    
    # NOTE: Check for duplicates
    print("checking for duplicates...")
    dupes = pandas.read_sql("SELECT ID, COUNT(ID) FROM DHL_SCHEMA GROUP BY ID HAVING COUNT(ID) > 1")
    if dupes:
        if (not df.empty):
            log("Dupes found, saving")
            f = open("dupes.txt", "w") 
            f.write(dupes.to_string())
            f.write(f"\n\n Timestamp: {str((datetime.date.today()).strftime('%Y-%m-%d'))}")
            f.close()
    print("done")
    
    print("finished, exiting...")
except Exception as e:
    log(e, __file__)
