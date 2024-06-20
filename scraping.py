import pandas
import bs4
import requests
import datetime
import time
import json
import database 
import re
from utils import log

MAX_WEX_CALLS = 1
WEXTRACTOR_AUTH_TOKEN = "68e2113b07b07c6cede5d513b66eba5f8db1701b" 
SCRAPINGDOG_API_KEY_KIAN = "65a943782f78003318229a41"
SCRAPINGDOG_API_KEY_ELENA = "6619300786d2b244207115b9"

def check_if_responses_exist(df : pandas.DataFrame): # Checks if reviews have responses already and updates dataframe
    for row in df.itertuples():
        response = requests.get(f"https://api.scrapingdog.com/scrape?api_key={SCRAPINGDOG_API_KEY_ELENA}&url={row.Link}&dynamic=false")
        if(response.status_code < 200 or response.status_code > 299): # Bad request
            continue
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        match row.Portal.lower():
            case "indeed":
                rev = soup.find(class_="css-14nhnfd e37uo190")
                resp = rev.find(class_="css-j3kgaw e1wnkr790")
                if resp:
                    df.at[row.Index, "ResponsePostedYesNo"] = "Yes"
                    df.at[row.Index, "Response"] = resp.text
                else:
                    df.at[row.Index, "ResponsePostedYesNo"] = "No"
            case "glassdoor":
                rev = soup.find(class_="review-details_reviewDetails__4N3am")
                resp = rev.find(class_="review-details_isCollapsed__5mhq_ newEmployerResponseText px-std")
                if resp:
                    df.at[row.Index, "ResponsePostedYesNo"] = "Yes"
                    df.at[row.Index, "Response"] = resp.text
                else:
                    df.at[row.Index, "ResponsePostedYesNo"] = "No"
    return df # Return anyway


def wex_string_to_datetime(str): 
    return datetime.datetime.strptime(str, "%Y-%m-%dT%H:%M:%S")

def supplement_kununu_data(row): # Supplements Department, Position, CurrentFormerEmployee, ContractTerminationKununuOnly
    url = row["Link"]
    #response = requests.get(f"https://api.scrapingdog.com/scrape?api_key={SCRAPINGDOG_API_KEY_ELENA}&url={url}&dynamic=false")
    response = requests.get(url) # HOTFIX!!! TODO: REMOVE
    if(response.status_code < 200 or response.status_code > 299): # Bad request
        log(f"Error connecting to Kununu through Scrapingdog. Row ID: {row.ID}, Status code: {response.status_code}")
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

def extract_new_reviews(portal : str, since : datetime): # new version
    list_of_dicts = []
    match portal.lower():
        case "indeed":
            pagenum = 0
            while(pagenum < MAX_WEX_CALLS):
                response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/indeed?id=DHL&auth_token={WEXTRACTOR_AUTH_TOKEN}&offset={pagenum * 20}")
                if(response.status_code < 200 or response.status_code > 299):
                    log(f"Error connecting to Wextractor. Status code: {response.status_code}")
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
        # TODO: Add kununu
        case "kununu":
            pagenum = 0 
            while(pagenum < MAX_WEX_CALLS):
                response = requests.request("GET", f"https://wextractor.com/api/v1/reviews/kununu?auth_token={WEXTRACTOR_AUTH_TOKEN}&id=https://www.kununu.com/de/deutsche-post&offset={pagenum * 10}")
                if(response.status_code < 200 or response.status_code > 299):
                    log(f"Error connecting to Wextractor. Status code: {response.status_code}")
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
                    else:
                        row["ResponsePostedYesNo"] = "No"
                    row["EstResponseDate"] = None
                    supplement_kununu_data(row)
                    list_of_dicts.append(row)
                pagenum += 1
        case other:
            log(f"Error extracting reviews from Wextractor, \"{portal}\" is not a supported portal.")
            return None
    return pandas.DataFrame(list_of_dicts)
