import pandas
from selenium import webdriver
from selenium.webdriver.common.by import By
import selenium.common.exceptions as se
import random
import time

CHROME_USER_DATA_DIR = r"C:\Users\KianGosling\AppData\Local\Google\Chrome\User Data"

def timer(secs):
    while secs > 0.0:
        print(str(secs) + "Sekunden")
        secs -= 1
        if secs >= 1:
            time.sleep(1)
        else:
            time.sleep(secs)
            return

def post_responses(df : pandas.DataFrame):
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
                    row["ResponsePostedYesNo"] = "Yes"
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
                    row["ResponsePostedYesNo"] = "Yes"
                    timer(random.uniform(3.0, 5.0))
                    next_elem = driver.find_element(By.XPATH, "//*[@id='new-response']/form/div/button[1]") # Post button
                    next_elem.click()
                    continue
                case other:
                    continue
            # TODO: Check if successful
        except (se.NoSuchElementException, se.ElementNotInteractableException) as exception:
            if(exception == se.NoSuchElementException):
                driver.close()
                return