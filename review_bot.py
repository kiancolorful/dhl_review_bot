from selenium import webdriver
from selenium.webdriver.common.by import By
import selenium.common.exceptions as se
import time
import pyodbc 
import pandas
import requests
import random
import json
import re
import datetime
import dateparser

# Constants
MSSQL_DRIVER = 'ODBC Driver 17 for SQL Server' # Alternative: ODBC Driver 17 for SQL Server
SQL_SERVER_NAME = r"85.215.196.5"
DATABASE = 'master'
SQL_TABLE_NAME = 'DHL_SCHEMA'#'CC_DATA'
SQL_STAGING_TABLE_NAME = 'DHL_STAGING'
USER = 'kian'
PW = 'Gosling1'
DATABASE_COLUMNS_AND_DATA_TYPES = [
    ("Portal", "nvarchar(10)"), # Derived from SCREAMINGFROG_CSV_PATH
    ("ID", "nvarchar(50)"), # Derived from relative link
    ("Link", "nvarchar(255)"), # Derived from relative link and domain
    ("ReviewTitle", "nvarchar(255)"), # Scraped
    ("ReviewDate", "date"), # Scraped (bad format, can I please just use system clock, we will be scraping at least once a day)
    ("OverallSatisfaction", "float"), # Scraped (Change comma to period)
    ("JobTitle", "nvarchar(50)"), # Scraped, cleanup necessary
    ("Department", "nvarchar(50)"), # Scraped, bad format TODO: nur bei kununu?
    ("CurrentFormerEmployee", "nvarchar(10)"), # Scraped
    ("ContractTerminationKununuOnly", "int"), # ????? 
    ("Location", "nvarchar(50)"), # Scraped (bad format with Glassdoor, State Included)
    ("StateRegion", "nvarchar(50)"), # PowerBI takes care of it, leave blank
    ("Country", "nvarchar(50)"), # PowerBI takes care of it, leave blank
    ("ReviewText1", "nvarchar(MAX)"), # Scraped 
    ("ReviewText2", "nvarchar(MAX)"), 
    ("ReviewText", "nvarchar(MAX)"), 
    ("MainpositiveAspect", "nvarchar(255)"), 
    ("MainAreaofImprovement", "nvarchar(255)"), 
    ("SensitiveTopic", "nvarchar(10)"), 
    ("ResponseYesNo", "nvarchar(10)"), 
    ("Response", "nvarchar(MAX)"), 
    ("EstResponseDate", "date"), 
    ("EmpathyScore", "float"), 
    ("HelpfulnessScore", "float"), 
    ("IndividualityScore", "float"), 
    ("ResponseTimeScore", "float"), 
    ("OverallScore", "float"), 
    ("WeightedScore", "float")
]

CHROME_USER_DATA_DIR = r"C:\Users\KianGosling\AppData\Local\Google\Chrome\User Data"
SCREAMINGFROG_CSV_PATH = [r"C:\Users\KianGosling\Music\Kununu\benutzerdefinierte_extraktion_alle.csv"]

GAIA_DEPLOYMENT_ID = "gpt-35-turbo-0301"
API_VERSION = "2023-05-15"
GAIA_TOKENS_PER_RESPONSE = 200
API_KEY = "eNzXkGNEdMzfay2hfeD8i22WTfMyaazXUOXitVgG3VYDszuT"
PROMPT_PREFIX = '''Du arbeitest im HR-Bereich von DHL und bist für das Beantworten von Reviews und Kommentaren auf 
Jobplattformen wie Indeed, Glassdoor und Kununu verantwortlich. Deine Rolle erfordert ein hohes 
Maß an Empathie und Sympathie, während du auf die Nachrichten der Mitarbeiter eingehst. Es ist wichtig, 
dass du dabei keine Versprechungen machst oder direkte Lösungsvorschläge anbietest, sondern vielmehr 
empathisch auf die genannten Punkte eingehst. Für jede Bewertung solltest du mindestens zwei Absätze 
verfassen, die eine ausgewogene Mischung aus Verständnis für die Situation des Mitarbeiters, Anerkennung 
ihrer Sorgen und eine positive, unterstützende Haltung zum Ausdruck bringen. Achte darauf, eine neutrale 
Ansprache und Verabschiedung zu verwenden, um Professionalität und Respekt gegenüber allen Kommentatoren 
zu wahren. Dein Ziel ist es, eine offene, verständnisvolle und positive Kommunikation zu fördern, die 
das Engagement und das Wohlbefinden der Mitarbeiter widerspiegelt, während du gleichzeitig das positive 
Image von DHL als Arbeitgeber stärkst. Formulieren Sie nun eine passende Antwort auf diese 
Unternehmensbewertung von Indeed: \n''' # \n ist wichtig!

# Data Structures 
queue = list()
error_queue = list() # Fish failures out at every stage of pipeline, handle at end or store (REMOVE THIS?)


# Methods
def upload_demo(URL, review_response):
    # Initialisieren
    options = webdriver.ChromeOptions()
    options.add_argument(r"--user-data-dir=C:\Users\kiang\AppData\Local\Google\Chrome\User Data") #e.g. C:\Users\You\AppData\Local\Google\Chrome\User Data
    options.add_argument(r"--remote-debugging-port=9222")
    driver = webdriver.Chrome(options=options)
    timer(random.uniform(3.0, 5.0))

    # Aufrufen 
    driver.get(URL)
    timer(random.uniform(3.0, 5.0))
    next_elem = driver.find_element(By.XPATH, "//button[@class='css-1vapj3m e8ju0x50']")
    next_elem.click()
    timer(random.uniform(3.0, 5.0))
    next_elem = driver.find_element(By.XPATH, "//textarea[@id='ifl-TextAreaFormField-:r0:']")
    next_elem.send_keys(review_response)
    timer(random.uniform(3.0, 5.0))
    next_elem = driver.find_element(By.XPATH, "//button[@class='css-1orlm12 e8ju0x50']")
    next_elem.click()
    timer(10)

def timer(secs):
    while secs > 0.0:
        print(str(secs) + "Sekunden")
        secs -= 1
        if secs >= 1:
            time.sleep(1)
        else:
            time.sleep(secs)
            return

def sql_insert_row(table_name, row, curs): 
    insert_string = f"INSERT INTO {table_name} ("
    for column in DATABASE_COLUMNS_AND_DATA_TYPES:
        insert_string += column[0] + ", "
    insert_string = insert_string[:-2]
    insert_string += ") VALUES ("
    for column in DATABASE_COLUMNS_AND_DATA_TYPES:
        insert_string += "?,"
    insert_string = insert_string[:-1]
    insert_string += ");"
    curs.execute(insert_string, 
                row.Portal, 
                row.ID, 
                row.Link, 
                row.ReviewTitle, 
                row.ReviewDate, 
                row.OverallSatisfaction, 
                row.JobTitle, 
                row.Department, 
                row.CurrentFormerEmployee, ##
                row.ContractTerminationKununuOnly, ##
                row.Location, 
                row.StateRegion, ##
                row.Country, 
                row.ReviewText1, 
                row.ReviewText2,
                row.ReviewText, 
                row.MainpositiveAspect, 
                row.MainAreaofImprovement, 
                row.SensitiveTopic, 
                row.ResponseYesNo, ##
                row.Response, 
                row.EstResponseDate, ###
                row.EmpathyScore, 
                row.HelpfulnessScore, 
                row.IndividualityScore, 
                row.ResponseTimeScore, 
                row.OverallScore, 
                row.WeightedScore, 
                )

def process_scraped_data(src, dest, csv_path): # Cleans and supplements scraped data
    for index in range(len(src.index)):
        if("Kununu" in csv_path):
            dest.at[index, "Portal"] = "Kununu"
            # TODO: ID and Link? (Waiting on Elena)
            dest.at[index, "ID"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dest.at[index, "ReviewTitle"] = src.at[index, "ReviewTitle"]
            #dest.at[index, "ReviewDate"] = src.at[index, "ReviewDate"] # Today's date?
            #OverallSatisfaction done later
            dest.at[index, "JobTitle"] = src.at[index, "JobTitle"]
            department = re.search('im Bereich (.+?) bei', src.at[index, "Location"])
            if department:
                dest.at[index, "Department"] = department.group(1)
            if "Ex-" in src.at[index, "JobTitle"]:
                dest.at[index, "CurrentFormerEmployee"] = "Former"
            elif "Bewerber" in src.at[index, "JobTitle"]:
                dest.at[index, "CurrentFormerEmployee"] = None
            else: 
                dest.at[index, "CurrentFormerEmployee"] = "Current"
            year = re.search('20(.+?) ', src.at[index, "Location"])
            if year and dest.at[index, "CurrentFormerEmployee"]: # CFE so that Bewerbende don't get included
                dest.at[index, "ContractTerminationKununuOnly"] = "20" + year.group(1)
            location = re.search('in (.+?) gearbeitet', src.at[index, "Location"])
            if location:
                dest.at[index, "Location"] = location.group(1)
            dest.at[index, "ReviewText"] = src.at[index, "ReviewText"]
            # TODO: Positive and negative aspects?
            # Sensitive Topic decided by GAIA
            dest.at[index, "Response"] = src.at[index, "Response"]
            if src.at[index, "Response"]:
                dest.at[index, "ResponseYesNo"] = "Yes"
                dest.at[index, "EstResponseDate"] = datetime.date.today() - datetime.timedelta(1)#src.at[index, "ResponseDate"]
                # If no score, evaluate with GAIA
            else:
                dest.at[index, "ResponseYesNo"] = "No"
            
        elif("Glassdoor" in csv_path): 
            dest.at[index, "Portal"] = "Glassdoor"
            id = re.search('Bewertungen-DHL-(.+?).htm', src.at[index, "Link"])
            if id:
                dest.at[index, "ID"] = id.group(1)
            dest.at[index, "Link"] = "https://glassdoor.de" + src.at[index, "Link"]

        elif("Indeed" in csv_path):
            dest.at[index, "Portal"] = "Indeed"
            id = re.search('id=(.+?)', src.at[index, "Link"])
            if id:
                dest.at[index, "ID"] = id.group(1)
            dest.at[index, "Link"] = "https://de.indeed.com" + src.at[index, "Link"]
            #dest.at[index, "ReviewTitle"] = 

        else:
            print("Fehler beim Dateipfad!")
        
        # TODO: Caluclate date like this?
        dest.at[index, "ReviewDate"] = datetime.date.today() - datetime.timedelta(1) # Yesterday's date
        
        dest.at[index, "OverallSatisfaction"] = float((src.at[index, "OverallSatisfaction"]).replace(",", "."))

def put_csv_in_sql(paths, conn, curs):
    df = pandas.DataFrame() # DataFrame to be added to Staging table
    for path in paths:
        try: 
            data = pandas.read_csv(path)
        except: 
            print("CSV File not found, Path may be incorrect, File may be missing, or File may be using wrong encoding.")
        src = pandas.DataFrame(data).astype(str)
        src.columns = src.columns.str.replace(' 1', '')
        if(len(src.index) == 0): # Skip if empty
            continue
        for field in DATABASE_COLUMNS_AND_DATA_TYPES:
            df.at[0, field[0]] = None
        process_scraped_data(src, df, path) # Clean up data, derive missing data
        print(df) # for debugging
        for row in df.itertuples():
            sql_insert_row(SQL_STAGING_TABLE_NAME, row, curs)
            if row.Index % 10 == 9: # Commit records every 10 rows in case of crash or something
                conn.commit()
        conn.commit()
        # Commit new rows to main table (ignore dupes) and empty staging table
        curs.execute(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME} staging WHERE staging.ID NOT IN (SELECT ID FROM {SQL_TABLE_NAME});")
        conn.commit()
        curs.execute(f"DELETE FROM {SQL_STAGING_TABLE_NAME}")
        conn.commit()

def fetch_new_reviews(queue):
    try:
        connection = pyodbc.connect(f"DRIVER={MSSQL_DRIVER};Server={SQL_SERVER_NAME};Database={DATABASE};UID={USER};PWD={PW};") 
        cursor = connection.cursor()
        put_csv_in_sql(SCREAMINGFROG_CSV_PATH, connection, cursor)
        cursor.execute(f"SELECT * FROM {SQL_TABLE_NAME} WHERE [AnswerYesNo]='No'")
        result = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        for row in result:
            queue.append(dict(zip(columns, row)))
    except pyodbc.Error as exception:
        print(exception)

def generate_responses(reviews, errors):
    gaia_url = f"https://apihub.dhl.com/genai/openai/deployments/{GAIA_DEPLOYMENT_ID}/completions"
    gaia_querystring = {"api-version":API_VERSION}
    headers = {
            "content-type": "application/json",
            "api-key": API_KEY
    }
    for review in reviews:
        gaia_payload = {
            "prompt": PROMPT_PREFIX + review.get('ReviewText') + "<|endoftext|>", #review.get('Rating'), # Specify format together with Weichert
            "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
            "temperature": 0.7, # Higher number = more risks
            "top_p": 0.0, # Similar to temperature
            #"logit_bias": {},
            #"user": "string",
            "n": 1, # Number of responses
            "stream": False, # Stream partial progress
            #"logprobs": None,
            #"suffix": "", # After input or output string
            #"echo": False, # Echo prompt 
            #"stop": "",    
            #"completion_config": "string",
            #"presence_penalty": 0,
            #"frequency_penalty": 0,
            #"best_of": 0
        }
        # TODO: SQL Datenbank "Rating" bereinigen, aktuell sind noch sehr viele numerischen Werte drin, die keinen längenren
        response = requests.request("POST", gaia_url, json=gaia_payload, headers=headers, params=gaia_querystring)
        match response.status_code:
            case 200: # Success (Handle fail codes?)
                review['Answer (text)'] = json.loads(response.text)['choices'][0]['text']
                review['Answer (yes / no )'] = "Yes"
            case other: # TODO Handle other HTTP Codes?
                print("other")
                reviews.remove(review) # Do not give to upload method
                errors.append(review)
    return reviews

def upload_responses(reviews, errors):
    # WebDriver config
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}") #e.g. C:\Users\You\AppData\Local\Google\Chrome\User Data
    options.add_argument(r"--remote-debugging-port=9222")
    driver = webdriver.Chrome(options=options)

    for review in reviews:
        try: 
            driver.get(review.get('Link')) # Go to review page
            timer(random.uniform(3.0, 5.0))
            if review.get('Platform') == 'Indeed': # Indeed 
                next_elem = driver.find_element(By.XPATH, "//button[@class='css-1vapj3m e8ju0x50']") # Comment button
                next_elem.click() 
                timer(random.uniform(3.0, 5.0))
                next_elem = driver.find_element(By.XPATH, "//textarea[@id='ifl-TextAreaFormField-:r0:']") # Comment Textarea
                next_elem.send_keys(review.get('Answer (text)'))
                timer(random.uniform(3.0, 5.0))
                next_elem = driver.find_element(By.XPATH, "//button[@class='css-1orlm12 e8ju0x50']") # Post button
                next_elem.click()
            if review.get('Platform') == 'Glassdoor': # Glassdoor 
                next_elem = driver.find_element(By.XPATH, '//*[@id="empReview_54905531"]/div/div[3]/a/button') # Comment button
            if review.get('Platform') == 'kununu': # Kununu 
                next_elem = driver.find_element(By.XPATH, "//*[@id='__next']/div/div[1]/main/div/div[4]/div/article[1]/div/div/div[17]/a") # Comment button
                next_elem.click() 
                timer(random.uniform(3.0, 5.0)) # Wait for page to reload
                next_elem = driver.find_element(By.XPATH, "//*[@id='new-response']/form/textarea") # Comment Textarea
                next_elem.send_keys(review.get('Answer (text)'))
                timer(random.uniform(3.0, 5.0))
                next_elem = driver.find_element(By.XPATH, "//*[@id='new-response']/form/div/button[1]") # Post button
                next_elem.click()
            else: # 
                reviews.remove(review)
                errors.append(review)
            # TODO: Check if successful. If yes, update SQL entry, if no, ??? (Don't requeue?)
        except (se.NoSuchElementException, se.ElementNotInteractableException) as exception:
            if(exception == se.NoSuchElementException):
                driver.close()
                upload_responses() # Try again???? TODO: Is this right???
                return

def update_sql_entries(reviews):
    try:
        # Call by ref or val?
        connection = pyodbc.connect(f"Driver={MSSQL_DRIVER};Server={SQL_SERVER_NAME};Database=master;Trusted_Connection=True;")
        cursor = connection.cursor()
        for review in reviews:
            # TODO:
            cursor.execute(f"UPDATE progress SET CockpitDrill = {review.get('Answer (text)')} WHERE [Dialogue ID]={review.get('Dialogue ID')}")
            reviews.remove(review)
    except pyodbc.Error as exception:
        print(exception)

# Main (TODO)
fetch_new_reviews(queue)
if(len(queue) != 0):
    generate_responses(queue, error_queue) # Feed reviews into GAIA to obtain response (EVALUATE RESPONSE)
    upload_responses(queue, error_queue) # Upload GAIA's responses
    update_sql_entries() # Update SQL Database with responses