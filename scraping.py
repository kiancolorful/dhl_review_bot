import pandas
import bs4
import requests
import datetime
import time
import json
import database 
import re
from utils import log

# Wextractor info
MAX_WEX_CALLS = 2
WEXTRACTOR_AUTH_TOKEN = "68e2113b07b07c6cede5d513b66eba5f8db1701b" 

# Scrapingdog info
SCRAPINGDOG_API_KEY = "67092708f5c5d4b13601b94a"
# SCRAPINGDOG_API_KEY_TOOLS = "66c72a43bf5a414c4fe14687"
# SCRAPINGDOG_API_KEY_KIAN = "65a943782f78003318229a41"
# SCRAPINGDOG_API_KEY_ELENA = "6619300786d2b244207115b9"

# Wextractor gibt Datetimes als String zurück. Diese Funktion wandelt gibt die String als Datetime-Objekt zurück.
def wex_string_to_datetime(str): 
    return datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")

# Nicht alle benötigten Daten werden von Wextractor gesammelt. Diese Funktion scrapt Department, Position, 
# CurrentFormerEmployee und ContractTerminationKununuOnly und fügt die Werte in die DateFrame ein 
def supplement_kununu_data(row): 
    url = row["Link"]
    response = requests.get(url) 
    if(response.status_code < 200 or response.status_code > 299): # Bad request
        log(f"Error connecting to Kununu through Scrapingdog. Row ID: {row.ID}, Status code: {response.status_code}", __file__)
        return
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    try:
        div = soup.find(class_="index__employmentInfoBlock__wuOtj p-tiny-regular")
        if not div:
            return
        position = div.find("b")
        if not position:
            return
        position = position.text
        department = div.find("span")
        if not department:
            return
        department = department.text
        row["JobTitle"] = position
        if "Ex-" in position:
            row["CurrentFormerEmployee"] = "Former"
        else: 
            row["CurrentFormerEmployee"] = "Current"
        if "im Bereich" in department:
            right = department.split("im Bereich ")[1]
            center = right.split(" bei")[0]
            row["Department"] = center
        year = re.search(" 2[0-9]{3} ", department)
        if year:
            row["ContractTerminationKununuOnly"] = int((year.group())[1:-1])
    except: 
        print("Error parsing Kununu! Link: " + row["Link"])

# kununu-Bewertungen enthalten nicht nur einen ReviewText, sondern auch Sternebewertungen. Diese Funktion gibt diese Sternebewertungen als String zurück.
def append_kununu_scores(review):
    scores = "\n"
    for key in review:
        if "_rating" in key:
            scores += "\n" + key.replace("_", " ") + ": " + str(review[key]["rating"]) + "/5 "
            if (review[key]["text"] != "" and review[key]["text"] != None):
                scores += review[key]["text"]
    if (review['text'] == None or review['text'] == ''):
        scores = scores[2:]
    return scores

# Diese Funktion nutzt Wextractor um für ein gegebenes Portal die neusten Reviews herunterzuladen. Der Parameter "since" setzt eine Untergrenze für das Alter der gesammelten Reviews. 
def extract_new_reviews(portal : str, since : datetime): # new version
    list_of_dicts = []
    match portal.lower():
        case "indeed":
            pagenum = 0
            while(pagenum < MAX_WEX_CALLS):
                response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/indeed?id=DHL&auth_token={WEXTRACTOR_AUTH_TOKEN}&offset={pagenum * 20}")
                if(response.status_code < 200 or response.status_code > 299):
                    log(f"Error connecting to Wextractor. Status code: {response.status_code}", __file__)
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
                    row = dict.fromkeys(database.DATABASE_COLUMNS_AND_DATA_TYPES)
                    row["Portal"] = "Indeed"
                    row["ID"] = review["id"]
                    row["Link"] = review["url"]
                    row["ReviewTitle"] = review["title"]
                    row["ReviewDate"] = wex_string_to_datetime(review["datetime"].split('.', 1)[0]) 
                    row["RefreshDate"] = (datetime.date.today()).strftime('%Y-%m-%d')
                    row["OnlineYesNo"] = "Yes"
                    row["OverallSatisfaction"] = float(review["rating"])
                    row["JobTitle"] = review["reviewer"]
                    row["Department"] = None # Kununu only?
                    row["CurrentFormerEmployee"] = review["reviewer_employee_type"].split(' ', 1)[0]
                    row["ContractTerminationKununuOnly"] = None
                    row["Location"] = review["location"]
                    row["StateRegion"] = None # Generated by GAIA
                    row["Country"] = None # Generated by GAIA
                    row["ReviewText"] = review["text"] 
                    if review["cons"]:
                        row["ReviewText"] = "Cons: " + review["cons"] + "\n\n" + row["ReviewText"]
                    if review["pros"]:
                        row["ReviewText"] = "Pros: " + review["pros"] + "\n\n" + row["ReviewText"]
                    row["MainpositiveAspect"] = None # Generated by GAIA
                    row["MainAreaofImprovement"] = None # Generated by GAIA
                    row["SensitiveTopic"] = None # Generated by GAIA
                    row["ResponsePostedYesNo"] = "No"
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
                        row = dict.fromkeys(database.DATABASE_COLUMNS_AND_DATA_TYPES)
                        row["Portal"] = "Glassdoor"
                        row["ID"] = review["id"]
                        row["Link"] = review["url"]
                        row["ReviewTitle"] = review["title"]
                        row["ReviewDate"] = wex_string_to_datetime(review["datetime"].split('.', 1)[0]) # Remove milliseconds
                        row["RefreshDate"] = (datetime.date.today()).strftime('%Y-%m-%d')
                        row["OnlineYesNo"] = "Yes"
                        row["OverallSatisfaction"] = float(review["rating"])
                        row["JobTitle"] = review["reviewer"]
                        row["Department"] = None # Kununu only?
                        if(bool(review["is_current_job"])):
                            row["CurrentFormerEmployee"] = "Current"
                        else: 
                            row["CurrentFormerEmployee"] = "Former"
                        row["ContractTerminationKununuOnly"] = None
                        row["Location"] = review["location"]
                        row["StateRegion"] = None # Generated by GAIA
                        row["Country"] = None # Generated by GAIA
                        if review["pros"]:
                            row["ReviewText"] = "Pros: " + review["pros"] 
                        if review["pros"] and review["cons"]:
                            row["ReviewText"] = row["ReviewText"] + "\n\n"
                        if review["cons"]:
                            row["ReviewText"] = row["ReviewText"] + "Cons: " + review["cons"]
                        row["MainpositiveAspect"] = None # Generated by GAIA
                        row["MainAreaofImprovement"] = None # Generated by GAIA
                        row["SensitiveTopic"] = None # Generated by GAIA
                        row["ResponsePostedYesNo"] = "No"
                        row["Response"] = None
                        row["EstResponseDate"] = None
                        list_of_dicts.append(row)
                    pagenum += 1 # Go to next page if all reviews on current page are new
            df = pandas.DataFrame(list_of_dicts)
            return df
        case "kununu":
            pagenum = 0 
            while(pagenum < MAX_WEX_CALLS):
                response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/kununu?auth_token={WEXTRACTOR_AUTH_TOKEN}&id=https://www.kununu.com/de/deutsche-post&offset={pagenum * 10}")
                if(response.status_code < 200 or response.status_code > 299):
                    log(f"Error connecting to Wextractor. Status code: {response.status_code}", __file__)
                    return
                responsetext = response.text.replace("\n", " ")
                responsetext = responsetext.replace("\r", "")
                time.sleep(1) # Avoid rate limiting
                json_data = json.loads(responsetext)

                for review in json_data["reviews"]:
                    if(wex_string_to_datetime(review["datetime"]) < since): # Review is old, return dataframe as-is
                        df = pandas.DataFrame(list_of_dicts)
                        return df
                    row = dict.fromkeys(database.DATABASE_COLUMNS_AND_DATA_TYPES)
                    row["Portal"] = "kununu"
                    row["ID"] = review["id"]
                    row["Link"] = review["url"]
                    row["ReviewTitle"] = review["title"]
                    row["ReviewDate"] = wex_string_to_datetime(review["datetime"].split('.', 1)[0]) 
                    row["RefreshDate"] = (datetime.date.today()).strftime('%Y-%m-%d')
                    row["OnlineYesNo"] = "Yes"
                    row["OverallSatisfaction"] = float(review["rating"])
                    row["Department"] = None # Filled by supplement_kununu_data
                    row["ContractTerminationKununuOnly"] = None # Filled by supplement_kununu_data
                    row["Location"] = review['company']['city']
                    row["StateRegion"] = None # Generated by GAIA
                    row["Country"] = None # Generated by GAIA
                    row["ReviewText"] = review["text"] + append_kununu_scores(review)
                    row["MainpositiveAspect"] = None # Generated by GAIA
                    row["MainAreaofImprovement"] = None # Generated by GAIA
                    row["SensitiveTopic"] = None # Generated by GAIA
                    row["Response"] = review['reply']['text']
                    if row["Response"]:
                        row["ResponsePostedYesNo"] = 'Yes'
                        row["ApprovalStatus"] = 'Approved'
                    else:
                        row["ResponsePostedYesNo"] = "No"
                    row["EstResponseDate"] = None
                    supplement_kununu_data(row)
                    list_of_dicts.append(row)
                pagenum += 1
        case other:
            log(f"Error extracting reviews from Wextractor, \"{portal}\" is not a supported portal.", __file__)
            return None
    return pandas.DataFrame(list_of_dicts)

# Diese Funktion schaut, ob Reviews noch online sind. Falls ja, wird anschließend der ResponseText nochmal gescrapt und in dem DataFrame aktualisiert. 
def refresh_reviews(df : pandas.DataFrame, con):
    for row in df.itertuples():
        #time.sleep(1)
        params = {
            'api_key': SCRAPINGDOG_API_KEY,
            'url': row.Link,
            'dynamic': 'false',
        }
        response = None # Initialize response in this scope

        if(row.Portal.lower() == "indeed"):
            params["premium"] = "true" # Indeed requires premium proxies
            
        for i in range(10): # Retry up to 10 times
                response = requests.get("https://api.scrapingdog.com/scrape", params)
                if(response.status_code in range(200, 300) or response.status_code == 404):
                    break

        if(response.status_code not in range(200, 300)): # Bad request TODO: Handle scraping dog error codes
            if(response.status_code == 404): # Review was taken offline
                df.at[row.Index, "OnlineYesNo"] = "No"
                df.at[row.Index, "RefreshDate"] = (datetime.date.today()).strftime('%Y-%m-%d')
                print(f"({str(row.Index + 1)}/{str(len(df.index))})\trefreshed review {row.ID} (offline)")
            elif(response.status_code == 404): # Out of API credits
                print("Out of Scrapigdog credits!")
                return
            else:
                print(f"({str(row.Index + 1)}/{str(len(df.index))})\tSomething weird happened. Scrapingdog code: {response.status_code}, ID: {row.ID}")
            continue

        soup = bs4.BeautifulSoup(response.text, "html.parser")
        try:
            match row.Portal.lower():
                case "indeed":
                    rev = soup.find(class_="css-14nhnfd e37uo190")
                    resp = rev.find(class_="css-j3kgaw e1wnkr790")
                    if resp:
                        df.at[row.Index, "ResponsePostedYesNo"] = "Yes"
                        for br in resp.find_all("br"):
                            br.replace_with("\n")
                        df.at[row.Index, "Response"] = resp.text
                        df.at[row.Index, "ApprovalStatus"] = "Approved"
                    else:
                        df.at[row.Index, "ResponsePostedYesNo"] = "No"
                case "glassdoor":
                    rev = soup.find(class_="review-details_reviewDetails__4N3am")
                    resp = rev.find('span', attrs={"data-test": "review-text-undefined"})
                    if resp:
                        df.at[row.Index, "ResponsePostedYesNo"] = "Yes"
                        for br in resp.find_all("br"):
                            br.replace_with("\n")
                        df.at[row.Index, "Response"] = resp.text
                        df.at[row.Index, "ApprovalStatus"] = "Approved"
                    else:
                        df.at[row.Index, "ResponsePostedYesNo"] = "No"
                case "kununu":
                    resp = soup.find(class_="index__responseBlock__A5fqZ")
                    if resp:
                        df.at[row.Index, "ResponsePostedYesNo"] = "Yes"
                        resp = resp.find(class_="p-small-regular")
                        for br in resp.find_all("br"):
                            br.replace_with("\n")
                        df.at[row.Index, "Response"] = resp.text
                        df.at[row.Index, "ApprovalStatus"] = "Approved"
                    else:
                        df.at[row.Index, "ResponsePostedYesNo"] = "No"
                case other:
                    log(f"Non-supported portal found while refreshing reviews. (ID = {row.ID})")
        except Exception as ex:
            log(ex, __file__, "Error while parsing portal website, most likely an element name was changed.")
            pass
        df.at[row.Index, "RefreshDate"] = (datetime.date.today()).strftime('%Y-%m-%d')
        df.at[row.Index, "OnlineYesNo"] = "Yes"
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\trefreshed review {row.ID}")
    # Return anyway
    return df