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

json_template_chat = {
    "Language": "",
    "ReviewTextEN": "",
	"Response": "",
    "ResponseEN": "",
	"StateRegion": "",
	"Country": "",
	"MainPositiveAspect": "", 
	"MainAreaOfImprovement": "", 
	"SensitiveTopic": "", 
	"EmpathyScore": "",
	"HelpfulnessScore": "", 
	"IndividualityScore": ""
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
Image von DHL als Arbeitgeber stärkst. Beenden Sie Ihre Antwort immer mit einer Signatur, zum Beispiel "Dein DHL Team".
Bauen Sie zudem passend Zeilenumbrüche in Ihre Antwort ein.  

Sie werden Ihre Antwort in Form eines JSON-Objekts zurückgeben. Das Format soll folgendermaßen aussehen: {json.dumps(json_template_chat)}

Schreiben Sie die Sprache der Unternehmensbewertung ins Feld "Language".

Die eigentliche Antwort auf die Unternehmensbewertung soll auf der selben Sprache sein, die Sie gerade ins Feld "Language" geschrieben haben.
Die eigentliche Antwort auf die Unternehmensbewertung soll im Feld "Response" stehen. 

Zusätzlich werden Sie die Empathie, Hilfsbereitschaft und Individualität Ihrer Antwort auf die Unternmehmensbewertung auf 
einer Skala von 1 bis 5 Bewerten, wobei 5 die beste Note ist. Sie werden Ihre Bewertungen der Empathie, Hilfsbereitschaft 
und Individualität an folgenden Kriterien orientieren:

Empathie: 
- 1 Punkt: keine Antwort überhaupt ODER Text lautet „(Leer)“ ODER sehr unhöfliche Antwort
- 2 Punkte: sehr kurze Antwort ohne Grußformel
- 3 Punkte: neutrale (unpersönliche) aber dennoch konstruktive Antwort
- 4 Punkte: freundliche Grußformel und freundliche Antwort, mit deutlichem Signal des Verständnisses für das Problem
- 5 Punkte: Bestfall, empathische und persönliche Antwort mit perfektem Verständnis des Problems, falls notwendig mit Entschuldigungen

Hilfsbereitschaft
- 1 Punkt: keine Antwort überhaupt ODER Text lautet „(Leer)“ ODER Antwort ohne jeglichen Bezug zu den Schmerzpunkten des Mitarbeiters
- 2 Punkte: automatisierte oder unpersönliche Antwort mit geringem Bezug zu den Schmerzpunkten des Mitarbeiters
- 3 Punkte: individuelle Antwort mit geringem Maß an Engagement
- 4 Punkte: individuelle Antwort mit allgemeinem Engagement, eine Lösung für die Schmerzpunkte des Mitarbeiters zu finden
- 5 Punkte: individuelle Antwort mit einer oder mehreren spezifischen Lösungen für die Schmerzpunkte des Mitarbeiters oder klarer Verpflichtung zu realen Verbesserungen

Individualität:
- 1 Punkt: keine Antwort überhaupt ODER Antwort lautet „(Leer)“ ODER die Antwort wurde bereits für eine andere Bewertung verwendet und ohne Änderungen kopiert
- 2 Punkte: die Antwort enthält 5 oder mehr standardisierte Bausteine
- 3 Punkte: die Antwort enthält 3 oder 4 der standardisierten Bausteine, sie ist sinnvoll, aber ziemlich allgemein
- 4 Punkte: die Antwort enthält maximal 2 der standardisierten Bausteine und enthält wenig, aber klare Bezüge zur Bewertung
- 5 Punkte: für die Antwort wurde maximal 1 der standardisierten Bausteine verwendet und der Rest der Antwort ist um die Bewertung des Mitarbeiters herum aufgebaut

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

Falls der Arbeitnehmer keinen Bewertungstext hinterlassen hat, werden Sie alle Felder außer "State/Region", "Country" und "SensitiveTopic" mit "" ausgefüllt.
'''
} 

def append_user_review(reviewtext : str, city : str=None) -> list: 
    user_message = {
		"role": "user",
		"content": reviewtext
	}
    if city:
        user_message['content'] += f"\n(Ort: {city})"
    return [SYSTEM_MESSAGE, user_message]
    
def generate_responses(df : pandas.DataFrame):
    for row in df.itertuples():
        messages = append_user_review(row.ReviewText, row.Location)
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
            log(f"Error connecting to GAIA for review {row.ID}. [{response.status_code}]")
            df.at[row.Index, "DeveloperComment"] = str(response.status_code)
            continue
        try:
            temp = json.loads(response.text)['choices'][0]['message']['content']
            temp = temp.split("{")[1]
            temp = temp.split("}")[0]
            temp = "{" + temp + "}"
            gaia_answer = json.loads(temp)
            gaia_answer = requests.structures.CaseInsensitiveDict(gaia_answer) # Sometimes GAIA messes up the casing of the keys
        except Exception as e:
            log(e, "Error processing GAIA reply while evaluating response")
            pass

        df.at[row.Index, "SensitiveTopic"] = gaia_answer["SensitiveTopic"]
        if row.Location:
            df.at[row.Index, "StateRegion"] = gaia_answer["StateRegion"]
            df.at[row.Index, "Country"] = gaia_answer["Country"]

        # If no review/response, move on to the next row
        if(gaia_answer["Response"] == "" or ("(Leer)" in gaia_answer["Response"])):
            continue

        df.at[row.Index, "Response"] = gaia_answer["Response"].replace("\\n", "\n")
        df.at[row.Index, "EstResponseDate"] = datetime.date.today()
        df.at[row.Index, "ResponseTimeDays"] = (datetime.date.today() - df.at[row.Index, "ReviewDate"]).days
        df.at[row.Index, "MainpositiveAspect"] = gaia_answer["MainpositiveAspect"]
        df.at[row.Index, "MainAreaofImprovement"] = gaia_answer["MainAreaofImprovement"]
        df.at[row.Index, "EmpathyScore"] = gaia_answer["EmpathyScore"]
        df.at[row.Index, "HelpfulnessScore"] = gaia_answer["HelpfulnessScore"]
        df.at[row.Index, "IndividualityScore"] = gaia_answer["IndividualityScore"]
        print(f"({str(row.Index)}/{str(len(df.index))}) generated response for review {row.ID}")
    return df