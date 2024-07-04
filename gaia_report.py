import requests
import gaia
import json
import time

# PARAMS
START_DATE = "2024-05-01" # Format: YYYY-MM-DD
END_DATE = "2024-05-31" # Format: YYYY-MM-DD

# NOTE: For some reason, the reports come as a json.gz, which must be extracted. 
# 7zip wasn't able to do this reliably in my testing, but Winrar worked very well. 

url = "https://apihub.dhl.com/genai/reporting/queries"

querystring = {"start":f"{START_DATE}T00:00:00Z","end":f"{END_DATE}T00:00:00Z"}

headers = {
	"api-key": gaia.API_KEY,
	"Content-Length": "0"
}

response = requests.request("POST", url, headers=headers, params=querystring)

if(response.status_code != 200):
    raise Exception("Did not get a successful response, either the DHL servers are having problems, or there is a problem with the start and end dates.")

qid = (json.loads(response.content))["queryId"]

url = f"https://apihub.dhl.com/genai/reporting/queries/{qid}/result"

headers = {"api-key": gaia.API_KEY}

time.sleep(20)

response = requests.request("GET", url, headers=headers)

open("may.json.gz", "wb").write(response.content)
