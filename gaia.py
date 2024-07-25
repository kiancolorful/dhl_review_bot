import pandas
import requests
import json
import datetime
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

Falls es sich in der Unternehemnsbewertung um ein sensibles Thema handelt (z.B. Rassismus, Sexismus, Beleidigung, Belästigung, usw.), werden Sie im Feld "SensitiveTopic" "Yes" eintragen, sonst "No".
'''
}

SYSTEM_MESSAGE_TRANSLATION = {
    "role": "system",
    "content": "Translate the following text into English."
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
    If the text is written in another language, there is no text, or you cannot determine the language, you will determine th language based off of the location.
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

def determine_lang(row):
    user_message = {
		"role": "user",
		"content": "Title: " + row.ReviewTitle + "\nText: " + row.ReviewText
	}
    # if row.Location:
    #     user_message['content'] += f"\n(Ort: {row.Location})"
    
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
    if(response.status_code < 200 or response.status_code > 299):
        log(f"Error connecting to GAIA for getting language of review {row.ID}. [{response.status_code}]", __file__)
        df.at[row.Index, "DeveloperComment"] = str(response.status_code) + "(lang)"
        return response.status_code
    try:
        lang = json.loads(response.text)['choices'][0]['message']['content']
    except Exception as ex:
        log(ex, __file__, "Could not extract language from GAIA response")
        pass
    return lang 
    
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
    
def generate_responses(df : pandas.DataFrame):
    for row in df.itertuples():
        # Determine language of reviewtext
        lang = determine_lang(row)
        if (isinstance(lang, int)): # Request gave an error
            continue
        if (lang.upper() not in ["DE", "EN", "NL", "IT", "ES", "FR", "PT"]): # Respond in English if language is not in core 7
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
	        "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
        }
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
        except Exception as e:
            log(e, "Error processing GAIA reply while evaluating response", __file__)
            pass
        try: 
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
            df.at[row.Index, "EmpathyScore"] = gaia_answer["EmpathyScore"]
            df.at[row.Index, "HelpfulnessScore"] = gaia_answer["HelpfulnessScore"]
            df.at[row.Index, "IndividualityScore"] = gaia_answer["IndividualityScore"]
        except Exception as ex:
            log(ex, "Error filling GAIA JSON into DF, most likely KeyError", __file__)
            pass
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\tgenerated {lang} response for review {row.ID}")
    return df

def generate_translations(df : pandas.DataFrame):
    for row in df.itertuples():
        # Determine language
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
            user_message = {
                "role": "user",
                "content": tup[1]
            }
            gaia_payload = {
                "messages": [SYSTEM_MESSAGE_TRANSLATION, user_message], 
                "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
                "temperature": 1, # Higher number = less deterministic
                "top_p": 0.0, # Similar to temperature, don't use both
                "n": 1, # Number of responses
                #"stop": "",    # Stop sequence, e.g. STOP123
                "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
                "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
            }
            response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)        
            if(response.status_code < 200 or response.status_code > 299):
                log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]", __file__)
                df.at[row.Index, "DeveloperComment"] = str(response.status_code) + "(tr)"
                continue
            try:
                result = json.loads(response.text)['choices'][0]['message']['content']
            except Exception as ex:
                log(e, "Error processing GAIA reply while translating.", __file__)
                continue
            df.at[row.Index, tup[0]] = result
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\tgenerated EN translation for review {row.ID}")
    return df

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
            df.at[row.Index, "EmpathyScore"] = gaia_answer["EmpathyScore"]
            df.at[row.Index, "HelpfulnessScore"] = gaia_answer["HelpfulnessScore"]
            df.at[row.Index, "IndividualityScore"] = gaia_answer["IndividualityScore"]
            print(f"({str(row.Index + 1)}/{str(len(df.index))})\tadded missing info for {row.ID}")
        except Exception as ex:
            log(ex, "Error filling GAIA JSON into DF, most likely KeyError", __file__)
            pass
    return df

def generate_translations(df : pandas.DataFrame):
    for row in df.itertuples():
        # Determine language
        lang_orig = determine_lang(row)
        if(lang_orig == 'EN'): # Already English
            df.at[row.Index, "ReviewTextEN"] = row.ReviewText
            df.at[row.Index, "ResponseEN"] = row.Response
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
            user_message = {
                "role": "user",
                "content": tup[1]
            }
            gaia_payload = {
                "messages": [SYSTEM_MESSAGE_TRANSLATION, user_message], 
                "max_tokens": GAIA_TOKENS_PER_RESPONSE, # Length of response
                "temperature": 1, # Higher number = less deterministic
                "top_p": 0.0, # Similar to temperature, don't use both
                "n": 1, # Number of responses
                #"stop": "",    # Stop sequence, e.g. STOP123
                "presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
                "frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
            }
            response = requests.request("POST", GAIA_CHAT_ENDPOINT, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)        
            if(response.status_code < 200 or response.status_code > 299):
                log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]", __file__)
                df.at[row.Index, "DeveloperComment"] = str(response.status_code) + "(tr)"
                continue
            try:
                result = json.loads(response.text)['choices'][0]['message']['content']
            except Exception as ex:
                log(e, "Error processing GAIA reply while translating.", __file__)
                continue
            df.at[row.Index, tup[0]] = result
        print(f"({str(row.Index + 1)}/{str(len(df.index))})\tgenerated EN translation for review {row.ID}")
