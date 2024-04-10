from bs4 import BeautifulSoup
import requests
import cloudscraper
import pymssql
import time


URL = "https://ionos.de/digitalguide/"
glassdoorURL = "https://www.glassdoor.de/Bewertungen/Deutsche-Post-and-DHL-Bewertungen-E658188.htm"
indeedURL = "https://www.indeed.com/cmp/DHL/reviews?fcountry=ALL"

# Process dynamic URL: 
#dynamicURL = {"?fcountry=ALL"}
#for chunk in dynamicURL:
#        URL += chunk


head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
    "Accept-Encoding": "identity",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Connection": "keep-alive",
    "Accept-Language": "en-US,en;q=0.9,lt;q=0.8,et;q=0.7,de;q=0.6",
}

# Trying out scrapingdog
resp = requests.get("https://api.scrapingdog.com/scrape?api_key=65a943782f78003318229a41&url=https://www.indeed.com/cmp/DHL/reviews?fcountry=ALL&dynamic=false")

#resp = requests.get(URL, headers=head)
print(resp.status_code)

#cs_scraper = cloudscraper.create_scraper()
#resp = cs_scraper.get(URL2)
#print(resp.status_code)
#print(resp.text)


soup = BeautifulSoup(resp.text, 'html.parser')
allOnPage = soup.find_all("div", class_ = "14nhnfd e37uo190")

print(allOnPage)