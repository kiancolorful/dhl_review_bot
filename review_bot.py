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
DATABASE_COLUMNS_AND_DATA_TYPES = {
    "Portal": "nvarchar(10)", # Derived from SCREAMINGFROG_CSV_PATH
    "ID": "nvarchar(50)", # Derived from relative link
    "Link": "nvarchar(255)", # Derived from relative link and domain
    "ReviewTitle": "nvarchar(255)", # Scraped
    "ReviewDate": "date", # Scraped (bad format, can I please just use system clock, we will be scraping at least once a day)
    "OverallSatisfaction": "float", # Scraped (Change comma to period)
    "JobTitle": "nvarchar(50)", # Scraped, cleanup necessary
    "Department": "nvarchar(50)", # Scraped, bad format TODO: nur bei kununu?
    "CurrentFormerEmployee": "nvarchar(10)", # Scraped
    "ContractTerminationKununuOnly": "int", # ????? 
    "Location": "nvarchar(50)", # Scraped (bad format with Glassdoor, State Included)
    "StateRegion": "nvarchar(50)", # PowerBI takes care of it, leave blank
    "Country": "nvarchar(50)", # PowerBI takes care of it, leave blank
    "ReviewText1": "nvarchar(MAX)", # Scraped 
    "ReviewText2": "nvarchar(MAX)", 
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

CHROME_USER_DATA_DIR = r"C:\Users\KianGosling\AppData\Local\Google\Chrome\User Data"
SCREAMINGFROG_CSV_PATH = [r"C:\Users\KianGosling\Music\Kununu\benutzerdefinierte_extraktion_alle.csv", r"C:\Users\KianGosling\Music\Indeed\benutzerdefinierte_extraktion_alle.csv", r"C:\Users\KianGosling\Music\Glassdoor\benutzerdefinierte_extraktion_alle.csv"]
WEXTRACTOR_AUTH_TOKEN = "68e2113b07b07c6cede5d513b66eba5f8db1701b" 
MAX_WEX_CALLS = 1

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
EVAL_PROMPT = '''fill out the following JSON object with random int values between 1 and 5: 
{
"EmpathyScore":
"HelpfulnessScore":
"IndividualityScore":
"ResponseTimeScore":
}

do not provide any additional text.
'''
GAIA_URL = f"https://apihub.dhl.com/genai/openai/deployments/{GAIA_DEPLOYMENT_ID}/completions"
GAIA_QUERYSTRING = {"api-version":API_VERSION}
GAIA_HEADERS = {
            "content-type": "application/json",
            "api-key": API_KEY
    }

# Methods
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
    curs.execute(insert_string, # Wär cool, wenn wir hier einfach "row" übergeben könnten, doch dann wird index mitgegeben
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

def evaluate_responses(df):
    for row in df.itertuples():
        gaia_payload = {
            "prompt": EVAL_PROMPT + "<|endoftext|>",#row["Response"], # Specify format together with Weichert
                "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
                "temperature": 0, # Higher number = more risks
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
        response = requests.request("POST", GAIA_URL, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
        try: 
            text = json.loads(response.text)["choices"][0]["text"].replace("\n", "")
            text = text.replace("<|im_end|>", "")
            scores = json.loads(text)
            if not scores:
                return
            scores = json.loads(scores.group(1))
            row.EmpathyScore = scores["EmpathyScore"]
            row.HelpfulnessScore = scores["HelpfulnessScore"]
            row.IndividualityScore = scores["IndividualityScore"]
            row.ResponseTimeScore = scores["ResponseTimeScore"]
        except:
            print("Error processing GAIA reply while evaluating response.")
    return df

def put_df_in_sql(df): # Needs to be ported to SQLalchemy
    for row in df.itertuples():
        sql_insert_row(SQL_STAGING_TABLE_NAME, row, curs)
        if row.Index % 10 == 9: # Commit records every 10 rows in case of crash or something
            conn.commit()
    conn.commit()
    # Commit new rows to main table (ignore dupes) and empty staging table
    curs.execute(f"INSERT INTO {SQL_TABLE_NAME} SELECT * FROM {SQL_STAGING_TABLE_NAME} staging WHERE staging.ID NOT IN (SELECT ID FROM {SQL_TABLE_NAME});")
    conn.commit()
    curs.execute(f"DELETE FROM {SQL_STAGING_TABLE_NAME};")
    conn.commit

def fetch_unanswered_reviews(curs): # Needs to be ported to SQLalchemy and df
    try:
        curs.execute(f"SELECT * FROM {SQL_TABLE_NAME} WHERE ResponseYesNo='No'")
        result = curs.fetchall()
        columns = [column[0] for column in curs.description]
    except pyodbc.Error as exception:
        print(exception)

def generate_responses(reviews_df): # Needs to be ported to SQLalchemy
    for review in reviews_df:
        gaia_payload = {
            "prompt": PROMPT_PREFIX + "Es wird sehr stressig man wird zwar geschult aber die Realität sieht anders aus. Erster Arbeitstag wird man gleich ins Kaos geschickt. Würde es nicht empfehlen " + "<|endoftext|>", # Specify format together with Weichert
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
        response = requests.request("POST", GAIA_URL, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
        if(response.status_code < 200 or response.status_code > 299):
            print(f"Error connecting to GAIA API. HTTP Status Code: {response.status_code}")
            continue
        review['Response'] = json.loads(response.text)['choices'][0]['text'] # Interpret JSON Correctly
        review['ResponseYesNo'] = "Yes"
    return reviews_df

def post_responses(df):
    # WebDriver config
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}") #e.g. C:\Users\You\AppData\Local\Google\Chrome\User Data
    options.add_argument(r"--remote-debugging-port=9222")
    driver = webdriver.Chrome(options=options)

    for row in df:
        try: 
            driver.get(row["Link"]) # Go to review page
            timer(random.uniform(3.0, 5.0))
            match row["Platform"].lower():
                case "indeed":
                    next_elem = driver.find_element(By.XPATH, "//button[@class='css-1vapj3m e8ju0x50']") # Comment button
                    next_elem.click() 
                    timer(random.uniform(3.0, 5.0))
                    next_elem = driver.find_element(By.XPATH, "//textarea[@id='ifl-TextAreaFormField-:r0:']") # Comment Textarea
                    next_elem.send_keys(row["Response"])
                    row["ResponseYesNo"] = "Yes"
                    timer(random.uniform(3.0, 5.0))
                    next_elem = driver.find_element(By.XPATH, "//button[@class='css-1orlm12 e8ju0x50']") # Post button
                    next_elem.click()
                    continue
                case "glassdoor":
                    next_elem = driver.find_element(By.XPATH, '//*[@id="empReview_54905531"]/div/div[3]/a/button') # Comment button
                    continue
                case "kununu":
                    next_elem = driver.find_element(By.XPATH, "//*[@id='__next']/div/div[1]/main/div/div[4]/div/article[1]/div/div/div[17]/a") # Comment button
                    next_elem.click() 
                    timer(random.uniform(3.0, 5.0)) # Wait for page to reload
                    next_elem = driver.find_element(By.XPATH, "//*[@id='new-response']/form/textarea") # Comment Textarea
                    next_elem.send_keys(row["Response"])
                    row["ResponseYesNo"] = "Yes"
                    timer(random.uniform(3.0, 5.0))
                    next_elem = driver.find_element(By.XPATH, "//*[@id='new-response']/form/div/button[1]") # Post button
                    next_elem.click()
                    continue
                case other:
                    continue
            # TODO: Check if successful. If yes, update SQL entry, if no, ??? (Don't requeue?)
        except (se.NoSuchElementException, se.ElementNotInteractableException) as exception:
            if(exception == se.NoSuchElementException):
                driver.close()
                return

def update_sql_entries(reviews): # Needs to be ported to df and sqlalchemy
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

def wex_string_to_datetime(str): # new 
    return datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")

def extract_new_reviews(portal, since): # new version
    list_of_dicts = []
    match portal.lower():
        case "indeed":
            pagenum = 0
            while(pagenum < MAX_WEX_CALLS):
                response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/indeed?id=DHL&auth_token={WEXTRACTOR_AUTH_TOKEN}&offset={pagenum * 20}")
                if(response.status_code < 200 or response.status_code > 299):
                    print(f"Error connecting to Wexrtactor. Status code: {response.status_code}")
                    return
                responsetext = response.text.replace("\n", " ")
                responsetext = responsetext.replace("\r", "")
                time.sleep(1) # Avoid rate limiting
                json_data = json.loads(responsetext)

                # Sometimes the top review is also scraped, discard it if it's an old one
                if(wex_string_to_datetime(json_data["reviews"][0]["datetime"]) < datetime.datetime.now() - datetime.timedelta(7)):
                    json_data["reviews"].pop(0) 

                for review in json_data["reviews"]:
                    if(wex_string_to_datetime(review["datetime"]) < since): # Review is old, return dataframe as-is
                        df = pandas.DataFrame(list_of_dicts)
                        return df
                    row = dict.fromkeys(DATABASE_COLUMNS_AND_DATA_TYPES)
                    row["Portal"] = "Indeed"
                    row["ID"] = review["id"]
                    row["Link"] = review["url"]
                    row["ReviewTitle"] = review["title"]
                    row["ReviewDate"] = wex_string_to_datetime(review["datetime"].split('.', 1)[0]) 
                    row["OverallSatisfaction"] = float(review["rating"])
                    row["JobTitle"] = review["reviewer"]
                    row["Department"] = None
                    row["CurrentFormerEmployee"] = review["reviewer_employee_type"].split(' ', 1)[0]
                    row["ContractTerminationKununuOnly"] = None
                    row["Location"] = review["location"]
                    row["StateRegion"] = None
                    row["Country"] = None
                    row["ReviewText1"] = review["pros"]
                    row["ReviewText2"] = review["cons"]
                    row["ReviewText"] = review["text"] # TODO: Do we have to remove newlines?
                    row["MainpositiveAspect"] = None # ?????
                    row["MainAreaofImprovement"] = None # ?????
                    row["SensitiveTopic"] = None # To be checked later
                    row["ResponseYesNo"] = None
                    row["Response"] = None
                    row["EstResponseDate"] = None
                    list_of_dicts.append(row)

                pagenum += 1 # Go to next page if all reviews on current page are new

        case "glassdoor":
            languages = ["de", "en", "nl", "it", "es", "fr", "pt"] # ISO 639
            for lang in languages:
                pagenum = 0
                while(pagenum < MAX_WEX_CALLS): 
                    response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/glassdoor?id=650250&language={lang}&auth_token={WEXTRACTOR_AUTH_TOKEN}&offset={pagenum * 10}")
                    responsetext = response.text.replace("\n", " ")
                    responsetext = responsetext.replace("\r", "")
                    time.sleep(1) # Avoid rate limiting
                    json_data = json.loads(responsetext)
                    
                    for review in json_data["reviews"]:
                        if(wex_string_to_datetime(review["datetime"].split('.', 1)[0]) < since): # Review is old, move on to next language
                            break
                        row = dict.fromkeys(DATABASE_COLUMNS_AND_DATA_TYPES)
                        row["Portal"] = "Glassdoor"
                        row["ID"] = review["id"]
                        row["Link"] = review["url"]
                        row["ReviewTitle"] = review["title"]
                        row["ReviewDate"] = wex_string_to_datetime(review["datetime"].split('.', 1)[0]) # Remove milliseconds
                        row["OverallSatisfaction"] = float(review["rating"])
                        row["JobTitle"] = review["reviewer"]
                        row["Department"] = None
                        if(bool(review["is_current_job"])):
                            row["CurrentFormerEmployee"] = "Current"
                        else: 
                            row["CurrentFormerEmployee"] = "Former"
                        row["ContractTerminationKununuOnly"] = None
                        row["Location"] = review["location"]
                        row["StateRegion"] = None
                        row["Country"] = None
                        row["ReviewText1"] = review["pros"]
                        row["ReviewText2"] = review["cons"]
                        row["ReviewText"] = None # TODO: Check this
                        row["MainpositiveAspect"] = None # ?????
                        row["MainAreaofImprovement"] = None # ?????
                        row["SensitiveTopic"] = None # To be checked later
                        row["ResponseYesNo"] = None
                        row["Response"] = None
                        row["EstResponseDate"] = None
                        list_of_dicts.append(row)
                    pagenum += 1 # Go to next page if all reviews on current page are new
            df = pandas.DataFrame(list_of_dicts)
            return df
        case "kununu":
            pagenum = 0
            while(pagenum < MAX_WEX_CALLS):
                response = ""
                responsetext = response.text.replace("\n", " ")
                responsetext = responsetext.replace("\r", "")

        # TODO: Add kununu
        case other:
            print(f"Error extracting reviews from Wextractor, \"{portal}\" is not a supported portal.")
            return

# Main

    # Steps: 
    #   1. Get new reviews from Wextractor
    #   2. Put these reviews into the SQL Database (staging, then main) (does SQLalchemy eliminate the need for staging?)
    #   3. Pull unanswered reviews from SQL Database (most likely only recent ones)
    #   4. Feed these in GAIA one by one


conn = pyodbc.connect(f"DRIVER={MSSQL_DRIVER};Server={SQL_SERVER_NAME};Database={DATABASE};UID={USER};PWD={PW};") 
curs = conn.cursor()

new_reviews_df = extract_new_reviews("Indeed", datetime.datetime.now() - datetime.timedelta(7))
put_df_in_sql(new_reviews_df)
unanswered_reviews_df = fetch_unanswered_reviews(curs)
#generate_responses(unanswered_reviews_df)
evaluate_responses(unanswered_reviews_df)
#post_responses(unanswered_reviews_df)

