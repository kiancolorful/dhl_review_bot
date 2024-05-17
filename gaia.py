import pandas
import requests
import json

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

def evaluate_responses(df : pandas.DataFrame):
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

def generate_responses(reviews_df : pandas.DataFrame): # Needs to be ported to SQLalchemy
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