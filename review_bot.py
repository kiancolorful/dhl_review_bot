import datetime
import sqlalchemy
import gaia 
import database 
import scraping
from utils import log, backup, check_for_dupes

# Main

    # Steps: 
    #   1. Initialize database connection
    #   2. Get new reviews from Wextractor for each portal 
    #   3. Refresh a few reviews from the database (check if they are still online)
    #       a. Select some reviews from the database that are a bit older
    #       b. Check if they are still online, updating the DB if they aren't
    #   4. If there are any kununu reviews that are missing some information, use GAIA to fill in this information
    #   5. Pull unanswered reviews from SQL Database (most likely only recent ones), and feed them into GAIA one by one
    #       a. Determine language of review
    #       b. Generate response in this language
    #   7. Regenerate responses that are marked by the DHL team as "regenerate"
    #   8. Review/Response translation
    #       a. Fetch 10 reviews from DB with no translation
    #       b. If they are not in English already, generate an English translation of the review text and the response
    #       c. If they are already in English, use the original texts as the EN translations
    #   9. Create CSV backup of database
    #   10. Check for duplicate entries in database
    #   11. Close connection to database

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
    new_reviews_indeed = scraping.extract_new_reviews("Indeed", datetime.datetime.now() - datetime.timedelta(3))
    new_reviews_indeed.drop_duplicates(subset=['ID'])
    print("done")
    print("putting indeed reviews into database...")
    database.put_df_in_sql(new_reviews_indeed, con)
    print("done")
    print("extracting new glassdoor reviews...")
    new_reviews_glassdoor = scraping.extract_new_reviews("Glassdoor", datetime.datetime.now() - datetime.timedelta(5))
    new_reviews_glassdoor.drop_duplicates(subset=['ID'])
    print("done")
    print("putting glassdoor reviews into database...")
    database.put_df_in_sql(new_reviews_glassdoor, con)
    print("done")
    print("extracting new kununu reviews...")
    new_reviews_kununu = scraping.extract_new_reviews("kununu", datetime.datetime.now() - datetime.timedelta(3))
    new_reviews_kununu.drop_duplicates(subset=['ID'])
    print("done")
    print("putting kununu reviews into database...")
    database.put_df_in_sql(new_reviews_kununu, con)
    print("done")

    # NOTE: Refreshing reviews
    # print("checking if older reviews have been removed from platforms or otherwise updated...")
    # refresh = database.fetch_refresh_reviews(con)
    # scraping.refresh_reviews(refresh, con)
    # print("done")
    # print("updating database")
    # try:
    #     database.put_df_in_sql(refresh, con, False, True)
    # except Exception as ex:
    #     log(ex, header="(Refreshing)")
    # print("done")
        
    # NOTE: Completing kununu reviews
    print("pulling reviews with incomplete information from database...")
    incomplete_rows = database.fetch_incomplete_rows(con, 50)
    print("done")
    print("generating missing data with gaia...")
    gaia.complete_rows(incomplete_rows)
    print("done")    
    print("updating reviews...")
    try:
        database.put_df_in_sql(incomplete_rows, con, False, True)
    except Exception as ex:
        log(ex, header="(Completing)")
    print("done")

    # NOTE: Generating responses
    print("pulling unanswered reviews from the past few days from database...")
    unanswered_reviews = database.fetch_unanswered_reviews(con, datetime.datetime.now() - datetime.timedelta(days=10))
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
    try:
        database.put_df_in_sql(unanswered_reviews, con, False, True)
    except Exception as ex:
        log(ex, header="(Generating)")
    print("done")
    
    # NOTE: Regenerating responses
    print("pulling reviews marked for regeneration from database...")
    regenerate = database.fetch_regenerate_reviews(con, count=10)
    print("done")
    print("generating response and gaia data for unanswered reviews...")
    gaia.generate_responses(regenerate)
    print("done")
    print("updating database entries to include answers and gaia data...")
    try:
        database.put_df_in_sql(regenerate, con, False, True)
    except Exception as ex:
        log(ex, header="(Regenerating)")
    print("done")

    # NOTE: Generating translations
    print("fetching reviews to be translated into english...")
    to_translate = database.fetch_translate_reviews(con, 20)
    print("done")
    print("generating translations...")
    gaia.generate_translations(to_translate)
    print("done")
    print("inserting review translations back into database...")
    try:
        database.put_df_in_sql(to_translate, con, True, True)
    except Exception as ex:
        log(ex, header="(Translations)")
    print("done")

    # NOTE: Backup
    print("generating backup csv")
    backup(con, database.SQL_TABLE_NAME)
    print("done")
    
    # NOTE: Check for duplicates
    check_for_dupes(con)
    
    print("finished, exiting...")
    con.close()
except Exception as ex:
    # NOTE: Check for duplicates
    check_for_dupes(con)
    
    log(ex, __file__)
