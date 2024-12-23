import pandas
import requests
import json
import datetime
import time
import re
from utils import log

GAIA_DEPLOYMENT_ID = "gpt-4-1106" #"gpt-35-turbo-0301"
API_VERSION = "2023-05-15"
GAIA_TOKENS_PER_RESPONSE = 1000
API_KEY = "eNzXkGNEdMzfay2hfeD8i22WTfMyaazXUOXitVgG3VYDszuT"
GAIA_CHAT_ENDPOINT = f"https://apihub.dhl.com/genai/openai/deployments/{GAIA_DEPLOYMENT_ID}/chat/completions"
GAIA_QUERYSTRING = {"api-version":API_VERSION}
GAIA_HEADERS = {
            "content-type": "application/json",
            "api-key": API_KEY
    }

DELAY_429 = 60

json_template_incomplete = {
	"StateRegion": "",
	"Country": "",
	"MainPositiveAspect": "", 
	"MainAreaOfImprovement": "", 
	"SensitiveTopic": "", 
	"EmpathyScore": "",
	"HelpfulnessScore": "", 
	"IndividualityScore": ""
}

json_template_resp = {
	"Response": "",
	"StateRegion": "",
	"Country": "",
	"MainPositiveAspect": "", 
	"MainAreaOfImprovement": "", 
	"SensitiveTopic": "", 
	"EmpathyScore": "",
	"HelpfulnessScore": "", 
	"IndividualityScore": ""
}

SYSTEM_MESSAGE_INCOMPLETE = { # 'complete'/'incomplete' refers to the prompt for completing incomplete reviews (reviews where a response was posted before the bot/GAIA got to it, so there is no additional iformation from GAIA (Country, Scores, etc.))
    "role": "system",
    "content": f'''Sie bekommen zwei nachrichten. Die erste beinhaltet eine Unternehmensbewertung der Firma DHL, und die zweite beinhaltet DHLs Antwort auf diese Unternehmensbewertung.
    Sie werden Ihre Antwort in Form eines JSON-Objekts zurückgeben. Das Format soll folgendermaßen aussehen: {json.dumps(json_template_incomplete)}
    Sie werden aus der Unternehmensbewertung herauslesen, welche Eigenschaft des Jobs der Arbeitnehmer besonders gut findet, und welche er 
oder sie besonders schlecht findet. Wählen Sie aus den folgenden Listen positiver und negativer Eigenschaften jeweils die passendste Kategorie. 
Falls keine Kategorie zutrifft, werden Sie die Kategorie "Uncategorized" wählen. Falls der Arbeitnehmer keine Eigenschaft des Jobs als besonders positiv oder begativ empfindet, werden 
Sie auch die Kategorie "Uncategorized" wählen. Schreiben Sie Ihre gewählten Eigenschaften jeweils in die Felder "MainPositiveAspect" und "MainAreaOfImprovement".

Clusters for positive aspects:
- Career advancement
- Company in general
- Good / stable job
- Superiors
- Positive atmosphere
- Regular salary
- Colleagues
- Big company benefits
- Fair / good salary
- No stress
- Trust / autonomous work
- Professional development
- Flexibility / Work-life balance
- Collaboration
- Uncategorized
- Good organization
- Transparency
- Communication

Clusters for negative aspects:
- Poor Salary
- Hard work / workload
- Uncategorized
- Boring tasks
- Old office / working place
- Big company disadvantages
- Intransparency
- Bad organization
- Contract / working conditions
- Company in general
- Old equipment / vehicles
- Flexibility / Work-life balance
- Colleagues
- Not enough manpower
- Career advancement
- Superiors
- Professional development
- Communication
    
Zusätzlich werden Sie die Empathie, Hilfsbereitschaft und Individualität von DHLs Antwort auf die Unternmehmensbewertung auf 
einer Skala von 1 bis 5 Bewerten, wobei 5 die beste Note ist. Diese Punkte werden Sie jeweils in den Feldern "EmpathyScore", "HelpfulnessScore" und "IndividualityScore" eintragen.
Ein hoher IndividualityScore würde zum Beispiel passen, wenn die Antwort auf die spezifischen Anliegen oder Eigenschaften des Arbeitsnehmers eingeht.

Falls in der Unternehmensbewertung ein Ort angegeben ist, werden Sie auch die Region (bzw. das Bundesland) und das Land, in denen sich dieser Ort befindet, bestimmen und in Ihrer Antwort zurückgeben.
Die Namen des Ortes und der Region werden Sie auf Englisch zurückgeben.

Falls es sich in der Unternehemnsbewertung um ein sensibles Thema handelt (z.B. Rassismus, Sexismus, Beleidigung, Belästigung, usw.), werden Sie im Feld "SensitiveTopic" "Yes" eintragen, sonst "No".
'''
}

SYSTEM_MESSAGE_TRANSLATION = {
    "role": "system",
    "content": "Translate the following text into English. If it is already in English, you will return the text without any modification."
}

SYSTEM_MESSAGE_LANG = {
    "role": "system",
    "content": '''Determine the language in which the following text is written and give the language back as a two-letter response. 
    If the text is written in German, your response will be 'DE'. 
    If the text is written in English, your response will be 'EN'. 
    If the text is written in Dutch, your response will be 'NL'. 
    If the text is written in Italian, your response will be 'IT'. 
    If the text is written in Spanish, your response will be 'ES'. 
    If the text is written in French, your response will be 'FR'. 
    If the text is written in Portuguese, your response will be 'PT'. 
    If the text is written in another language or you cannot determine the language, you will determine the language based off of the location.
    If the text contains multiple languages, you will choose the main language if the text and disregard the others.
    If the review text contains rating labels such as "atmosphere rating: 5/5", you will not factor these rating labels into your decision.
    If there is no location and you still cannot determine the language, your response will be 'Other'.
    '''
}

SYSTEM_MESSAGE = {
		"role": "system",
		"content": f'''Du arbeitest im HR-Bereich von DHL und bist für das Beantworten von Reviews und Kommentaren auf 
Jobplattformen wie Indeed, Glassdoor und Kununu verantwortlich. Deine Rolle erfordert ein hohes 
Maß an Empathie und Sympathie, während du auf die Nachrichten der Mitarbeiter eingehst. Es ist wichtig, 
dass du dabei keine Versprechungen machst oder direkte Lösungsvorschläge anbietest, sondern vielmehr 
empathisch auf die genannten Punkte eingehst. Für jede Bewertung solltest du mindestens zwei Absätze 
verfassen, die eine ausgewogene Mischung aus Verständnis für die Situation des Mitarbeiters, Anerkennung 
ihrer Sorgen und eine positive, unterstützende Haltung zum Ausdruck bringen. Achte darauf, eine neutrale 
Ansprache und Verabschiedung zu verwenden, um Professionalität und Respekt gegenüber allen Kommentatoren 
zu wahren. Dein Ziel ist es, eine offene, verständnisvolle und positive Kommunikation zu fördern, die 
das Engagement und das Wohlbefinden der Mitarbeiter widerspiegelt, während du gleichzeitig das positive 
Image von DHL als Arbeitgeber stärkst. Beenden Sie Ihre Antwort immer mit einer Signatur, und bauen Sie 
zudem passend Zeilenumbrüche in Ihre Antwort ein.  

Sie werden Ihre Antwort in Form eines JSON-Objekts zurückgeben. Das Format soll folgendermaßen aussehen: {json.dumps(json_template_resp)}

Die eigentliche Antwort auf die Unternehmensbewertung soll im Feld "Response" stehen. 
Falls eine Sprache angegeben ist, sollen Sie Ihre eigentliche Antwort auf die Unternehmensbewertung in dieser Sprache schreiben.
Sonst können Sie die Antwort auf Englisch schreiben. 

Zusätzlich werden Sie die Empathie, Hilfsbereitschaft und Individualität Ihrer Antwort auf die Unternmehmensbewertung auf 
einer Skala von 1 bis 5 Bewerten, wobei 5 die beste Note ist. 

Falls ein Ort angegeben ist, werden Sie auch die Region (bzw. das Bundesland) und das Land, in denen sich dieser Ort befindet, bestimmen und in Ihrer Antwort zurückgeben. 
Sie werden diese Namen auf Englisch zurückgeben. 

Falls es sich in der Bewertung um ein sensibles Thema handelt (z.B. Rassismus, Sexismus, Beleidigung, Belästigung, usw.), werden Sie im Feld "SensitiveTopic" "Yes" eintragen, sonst "No".

Als letztes werden Sie aus der Unternehmensbewertung herauslesen, welche Eigenschaft des Jobs der Arbeitnehmer besonders gut findet, und welche er 
oder sie besonders schlecht findet. Wählen Sie aus den folgenden Listen positiver und negativer Eigenschaften jeweils die passendste Kategorie. 
Falls keine Kategorie zutrifft, sollen Sie die Kategorie "Uncategorized" wählen.
Schreiben Sie Ihre gewählten Eigenschaften jeweils in die Felder "MainPositiveAspect" und "MainAreaOfImprovement".

Clusters for positive aspects:
- Career advancement
- Company in general
- Good / stable job
- Superiors
- Positive atmosphere
- Regular salary
- Colleagues
- Big company benefits
- Fair / good salary
- No stress
- Trust / autonomous work
- Professional development
- Flexibility / Work-life balance
- Collaboration
- Uncategorized
- Good organization
- Transparency
- Communication

Clusters for negative aspects:
- Poor Salary
- Hard work / workload
- Uncategorized
- Boring tasks
- Old office / working place
- Big company disadvantages
- Intransparency
- Bad organization
- Contract / working conditions
- Company in general
- Old equipment / vehicles
- Flexibility / Work-life balance
- Colleagues
- Not enough manpower
- Career advancement
- Superiors
- Professional development
- Communication

Falls der Arbeitnehmer keinen Bewertungstext hinterlassen hat, werden Sie alle Felder außer "State/Region", "Country" und "SensitiveTopic" mit "" ausfüllen.
'''
} 

# This function removes the English labels that we add to review text for Indeed and Kununu, and returns the review text as a string (df and row are not affected). This helps with language detection.
def remove_english_labels(row) -> str: 
    match row.Portal.lower():
        case 'indeed':
            text = (row.ReviewText).replace("Pros: ", "")
            text = text.replace("Cons: ", "")
            return text
        case 'kununu':
            text = re.sub('([^\s]+) rating: [1-5]\/5 ', '', row.ReviewText) # Remove star ratings
            while('\n\n' in text):
                text = text.replace('\n\n', '\n')
            if text == '\n':
                return ''
            return text
        case other:
            return row.ReviewText

# This function uses GAIA to determine the language of a given text. The lanugage is returned as a two-letter all-caps string, e.g. "ES". Set "just_string" to true if you just want to determine the language of a simple string, not a review
def determine_lang(row, just_string=False):
    user_message = {
		"role": "user",
		"content": ""
	}

    # Construct user message content depending on whether a review is being processed or just a string
    if(just_string):
        if(isinstance(row, str)):
            user_message["content"] = row
        else:
            log("Improper use of determine_lang. A non-str object has been passed, but just_string is True.", __file__)
            raise Exception("Improper use of determine_lang. A non-str object has been passed, but just_string is True.")
    else:
        user_message["content"] = "Title: " + row.ReviewTitle
        revtext = remove_english_labels(row)
        if (not (revtext == None or revtext == '')):
            user_message['content'] += "\nText: " + revtext
        if row.Location:
            user_message['content'] += f"\n(Location: {row.Location})"
    
    messages = [SYSTEM_MESSAGE_LANG, user_message]
    
    gaia_payload = {
        "messages": messages, 
        "max_tokens": 50, # Length of response
        "temperature": 0, # Higher number = less deterministic
        "top_p": 0.0, # Similar to temperature, don't use both
        "n": 1, # Number of responses
        #"stop": "",    # Stop sequence, e.g. STOP123
        "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
        "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
    }
    response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
    if(response.status_code == 429): # Too many requests
                print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                time.sleep(DELAY_429)
                response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)        
    if(response.status_code < 200 or response.status_code > 299):
        log(f"Error connecting to GAIA for getting language of review {row.ID}. [{response.status_code}]", __file__)
        return response.status_code
    try:
        lang = json.loads(response.text)['choices'][0]['message']['content']
    except Exception as ex:
        log(ex, __file__, "Could not extract language from GAIA response")
        pass
    return lang 

# This function combines a review's title, text, location and language, and then puts it in a list with the System message (for the GAIA Chat endpoint), which is then returned. 
def append_user_review(row, lang : str=None) -> list: 
    user_message = {
		"role": "user",
		"content": f"Bewertungstitel: {row.ReviewTitle}\n\n{row.ReviewText}"
	}
    user_message['content'] += f"\n\n(Bewertung insgesamt: {row.OverallSatisfaction}/5.0)"
    if row.Location:
        user_message['content'] += f"\n\n(Ort: {row.Location})"
    if lang:
        user_message['content'] += f"\n\n(Sprache: {lang})"
    return [SYSTEM_MESSAGE, user_message]

# This function generates GAIA responses for an entire DataFrame based on the content and language of each ReviewText. GAIA analyzes the ReviewText and generates values for StateRegion, Country, SensitiveTopic, etc.
def generate_responses(df : pandas.DataFrame):
    for row in df.itertuples():
        lang = row.Language
        # Determine language of reviewtext
        if(lang == None):
            lang = determine_lang(row)
            if(lang == 429): # Too many requests
                    print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                    time.sleep(DELAY_429)
                    lang = determine_lang(row)
            if (isinstance(lang, int)): # Request gave an error
                continue
            # No more errors
            df.at[row.Index, "Language"] = lang.upper()
        if (lang.upper() not in ["DE", "EN", "NL", "IT", "ES", "FR", "PT"]): # Respond in English if language is not in core 7, or in German if Portal is kununu
            if(row.Portal.lower() == 'kununu'):
                lang = "DE"
            else:
                lang = "EN"
        
        # Generate response
        messages = append_user_review(row, lang)
        gaia_payload = {
            "messages": messages, 
	        "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
	        "temperature": 1, # Higher number = less deterministic
	        "top_p": 0.0, # Similar to temperature, don't use both
	        "n": 1, # Number of responses
	        #"stop": "",    # Stop sequence, e.g. STOP123
	        "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
	        "frequency_penalty": 0, # [-2, 2]: Positive = don't repeat phrases verbatim
        }
        if(row.ApprovalStatus == "Regenerate"):
            gaia_payload["presence_penalty"] = 1 # TODO: Is this a good idea?
        response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
        if(response.status_code == 429):
                print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                time.sleep(DELAY_429)
                response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
        if(response.status_code < 200 or response.status_code > 299):
            log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]", __file__)
            df.at[row.Index, "DeveloperComment"] = str(response.status_code)
            continue
        try:
            temp = json.loads(response.text)['choices'][0]['message']['content']
            temp = temp.split("{")[1]
            temp = temp.split("}")[0]
            temp = "{" + temp + "}"
            gaia_answer = json.loads(temp)
            gaia_answer = requests.structures.CaseInsensitiveDict(gaia_answer) # Sometimes GAIA messes up the case of the dictionary keys
        except Exception as ex:
            log(ex, "Error processing GAIA reply while evaluating response", __file__)
            pass
        try: 
            if(row.ApprovalStatus == "Regenerate"):
                df.at[row.Index, "Response"] = gaia_answer["Response"].replace("\\n", "\n")
                df.at[row.Index, "ApprovalStatus"] = "Pending"
                continue
            
            df.at[row.Index, "SensitiveTopic"] = gaia_answer["SensitiveTopic"]
            if row.Location:
                df.at[row.Index, "StateRegion"] = gaia_answer["StateRegion"]
                df.at[row.Index, "Country"] = gaia_answer["Country"]

            # If no review/response, move on to the next row
            if(gaia_answer["Response"] == "" or ("(Leer)" in gaia_answer["Response"])):
                continue
            
            if(df.at[row.Index, "Response"] == None or df.at[row.Index, "Response"] == ""):
                df.at[row.Index, "Response"] = gaia_answer["Response"].replace("\\n", "\n")
                df.at[row.Index, "EstResponseDate"] = datetime.date.today()
                df.at[row.Index, "ResponseTimeDays"] = (datetime.date.today() - df.at[row.Index, "ReviewDate"]).days
                df.at[row.Index, "MainpositiveAspect"] = gaia_answer["MainpositiveAspect"]
                df.at[row.Index, "MainAreaofImprovement"] = gaia_answer["MainAreaofImprovement"]
                df.at[row.Index, "ApprovalStatus"] = "Pending"
            df.at[row.Index, "EmpathyScore"] = gaia_answer["EmpathyScore"]
            df.at[row.Index, "HelpfulnessScore"] = gaia_answer["HelpfulnessScore"]
            df.at[row.Index, "IndividualityScore"] = gaia_answer["IndividualityScore"]
        except Exception as ex:
            log(ex, "Error filling GAIA JSON into DF, most likely KeyError", __file__)
            pass
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\tgenerated {lang} response for review {row.ID}")
    return df

# This function generates EN-Translations for ReviewText and Response. 
def generate_translations(df : pandas.DataFrame):
    for row in df.itertuples():
        # Determine language
        lang_orig = row.Language
        if(lang_orig == None):
            lang_orig = determine_lang(row)
        if(lang_orig == 'EN'): # Already English
            df.at[row.Index, "ReviewTextEN"] = row.ReviewText
            df.at[row.Index, "ResponseEN"] = row.Response
            print(f"({str(row.Index + 1)}/{str(len(df.index))})\t{row.ID} is already EN")
            continue
        if(isinstance(lang_orig, int)): # Failure
            df.at[row.Index, "ReviewTextEN"] = None
            df.at[row.Index, "ResponseEN"] = None
            continue
        
        # Needs to be translated
        fields = { # This tuple-dict is the easiest way I found to write the code cleanly
            ("ReviewTextEN", row.ReviewText),
            ("ResponseEN", row.Response)
        }
        for tup in fields:
            # Skip if there is already an EN version
            if(tup[0] == "ReviewTextEN"):
                if(row.ReviewTextEN and row.ReviewTextEN != ""):
                    continue
                # If the kununu ReviewText is only the star ratings:
                elif(remove_english_labels(row) == ''):
                    df.at[row.Index, tup[0]] = row.ReviewText
                    continue
            elif(tup[0] == "ResponseEN" and (row.ResponseEN and row.ResponseEN != "")):
                continue    

            user_message = {
                "role": "user",
                "content": tup[1]
            }
            gaia_payload = {
                "messages": [SYSTEM_MESSAGE_TRANSLATION, user_message], 
                "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
                "temperature": 0, # Higher number = less deterministic # TODO should temperature be 0 here?
                "top_p": 0.0, # Similar to temperature, don't use both
                "n": 1, # Number of responses
                #"stop": "",    # Stop sequence, e.g. STOP123
                "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
                "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
            }

            # Generate translation and verify that generated translations are both english
            is_english = False
            tries = 3 # Max number of tries before giving up
            while(not is_english and tries > 0):
                # Generate translation:
                response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
                if(response.status_code == 429): # Too many requests, waiting and trying again
                    print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                    time.sleep(DELAY_429)
                    continue
                if(response.status_code < 200 or response.status_code > 299):
                    log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]", __file__)
                    df.at[row.Index, "DeveloperComment"] = str(response.status_code) + "(translation)"
                    continue
                try:
                    result = json.loads(response.text)['choices'][0]['message']['content']
                except Exception as ex:
                    log(ex, "Error processing GAIA reply while translating.", __file__)
                    continue

                # Verify language:
                lang_translation = determine_lang(result, True)
                if(lang_translation.upper() == "EN"):
                    is_english = True
                    df.at[row.Index, tup[0]] = result
                else: 
                    tries -= 1 # Decrement number of tries
            if(not is_english):
                print(f"({str(row.Index + 1)}/{str(len(df.index))})\tfailed to generate {tup[0]} for review {row.ID}")
                df.at[row.Index, "DeveloperComment"] = "Check lang"
                continue
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\tgenerated EN translation for review {row.ID}")
    return df

# This function completes missing data which is usually generated by GAIA, for instance in case a review is scraped from kununu that was already answered without manually without using anything from this program. 
def complete_rows(df : pandas.DataFrame):
    for row in df.itertuples():
        if(row.Response == None): # Do not process reviews with no response yet (shouldn't happen anyway, this is just a failsafe)
            continue
        
        # Craft messages
        messages = [SYSTEM_MESSAGE_INCOMPLETE, {"role": "user", "content": row.ReviewText}, {"role": "user", "content": row.Response}]
        if(row.Location):
            messages[1]["content"] += f"\n(Ort: {row.Location})"
        gaia_payload = {
            "messages": messages, 
	        "max_tokens": 1000, # Length of response
	        "temperature": 0, # Higher number = less deterministic
	        "top_p": 0.0, # Similar to temperature, don't use both
	        "n": 1, # Number of responses
	        #"stop": "",    # Stop sequence, e.g. STOP123
	        "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
	        "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
        }
        
        # Request 
        response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
        if(response.status_code == 429): # Too many requests, waiting and trying again
                print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                time.sleep(DELAY_429)
                response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)  
        if(response.status_code < 200 or response.status_code > 299):
            log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]", __file__)
            df.at[row.Index, "DeveloperComment"] = str(response.status_code)
            continue
        try:
            temp = json.loads(response.text)['choices'][0]['message']['content']
            temp = temp.split("{")[1]
            temp = temp.split("}")[0]
            temp = "{" + temp + "}"
            gaia_answer = json.loads(temp)
            gaia_answer = requests.structures.CaseInsensitiveDict(gaia_answer) # Sometimes GAIA messes up the case of the dictionary keys
        except Exception as ex:
            log(ex, "Error processing GAIA reply while evaluating response", __file__)
            pass
        
        # Entering data
        try:
            if row.Location:
                df.at[row.Index, "StateRegion"] = gaia_answer["StateRegion"]
                df.at[row.Index, "Country"] = gaia_answer["Country"]
            df.at[row.Index, "SensitiveTopic"] = gaia_answer["SensitiveTopic"]
            df.at[row.Index, "MainpositiveAspect"] = gaia_answer["MainpositiveAspect"]
            df.at[row.Index, "MainAreaofImprovement"] = gaia_answer["MainAreaofImprovement"]
            df.at[row.Index, "EmpathyScore"] = float(gaia_answer["EmpathyScore"])
            df.at[row.Index, "HelpfulnessScore"] = float(gaia_answer["HelpfulnessScore"])
            df.at[row.Index, "IndividualityScore"] = float(gaia_answer["IndividualityScore"])
            if(row.Language == None):
                lang = determine_lang(row)
                if(lang == 429): # Too many requests
                        print(f"Too many requests! [429] Waiting {DELAY_429} seconds and trying again...")
                        time.sleep(DELAY_429)
                        lang = determine_lang(row)
                if (isinstance(lang, int)): # Request gave an error
                    continue
                # No error
                df.at[row.Index, "Language"] = lang.upper()
            print(f"({str(row.Index + 1)}/{str(len(df.index))})\tadded missing info for {row.ID}")
        except Exception as ex:
            log(ex, "Error filling GAIA JSON into DF, most likely KeyError", __file__)
            pass
    return df
