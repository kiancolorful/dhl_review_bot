import requests
import json

# What do we need from the AI?
# - Response
# - State/Region
# - Country
# - MainpositiveAspect
# - MainAreaofImprovement
# - SensitiveTopic
# - EmpathyScore
# - HelpfulnessScore
# - IndividualityScore
# - EN Übersetzung

REVIEWTEXTS = ['''Es wird sehr stressig man wird zwar geschult aber die Realität sieht anders aus. Erster Arbeitstag wird man gleich ins Kaos geschickt. Würde es nicht empfehlen''', 
			   '''Es ist zwar manchmal ein bisschen viel Arbeit am gleichen Tag gewesen, aber der Job hat mir immer Spaß gemacht und Ich wurde gut bezahlt. Für jeden der gerne aktiv ist ist dieser Job gut''',
			   '''Die Kollegen sind sehr hilfsbereit und sind sehr nett.
Ich würde gerne Freunde und Familie weiter empfehlen.''',
'']

json_template_completions = {
	"ReviewText": REVIEWTEXTS[1],
	"Response": "null",
	"City": "Bayreuth",
	"State/Region": "null",
	"Country": "null",
	"MainpositiveAspect": "null", 
	"MainAreaofImprovement": "null", 
	"SensitiveTopic": "null", 
	"EmpathyScore": "null",
	"HelpfulnessScore": "null", 
	"IndividualityScore": "null"
}

json_template_chat = {
	"Response": "",
	"StateRegion": "",
	"Country": "",
	"MainpositiveAspect": "", 
	"MainAreaofImprovement": "", 
	"SensitiveTopic": "", 
	"EmpathyScore": "",
	"HelpfulnessScore": "", 
	"IndividualityScore": ""
}

jstringco = json.dumps(json_template_completions)
jstringch = json.dumps(json_template_chat)

DEPLOYMENT_COMPLETIONS = "gpt-35-turbo-0301"
DEPLOYMENT_CHAT = "gpt-4-1106"

COMPLETIONS_ENDPOINT = f"https://apihub.dhl.com/genai/openai/deployments/{DEPLOYMENT_COMPLETIONS}/completions"
CHAT_ENDPOINT = f"https://apihub.dhl.com/genai/openai/deployments/{DEPLOYMENT_CHAT}/chat/completions"

PROMPT_COMPLETIONS = f'''Du arbeitest im HR-Bereich von DHL und bist für das Beantworten von Reviews und Kommentaren auf 
Jobplattformen wie Indeed, Glassdoor und Kununu verantwortlich. Deine Rolle erfordert ein hohes 
Maß an Empathie und Sympathie, während du auf die Nachrichten der Mitarbeiter eingehst. Es ist wichtig, 
dass du dabei keine Versprechungen machst oder direkte Lösungsvorschläge anbietest, sondern vielmehr 
empathisch auf die genannten Punkte eingehst. Für jede Bewertung solltest du mindestens zwei Absätze 
verfassen, die eine ausgewogene Mischung aus Verständnis für die Situation des Mitarbeiters, Anerkennung 
ihrer Sorgen und eine positive, unterstützende Haltung zum Ausdruck bringen. Achte darauf, eine neutrale 
Ansprache und Verabschiedung zu verwenden, um Professionalität und Respekt gegenüber allen Kommentatoren 
zu wahren. Dein Ziel ist es, eine offene, verständnisvolle und positive Kommunikation zu fördern, die 
das Engagement und das Wohlbefinden der Mitarbeiter widerspiegelt, während du gleichzeitig das positive 
Image von DHL als Arbeitgeber stärkst. 

Du bekommst nun ein JSON-Objekt, dessen leeren Feldern Sie ausfüllen sollen: {jstringco}

Formuliere in etwa 100 Wörtern eine passende Antwort auf die Unternehmensbewertung, die du gleich bekommst. Schrebe deine Antwort ins Feld "Response".
Anschließend wirst du die Antwort auf die 

Bestimmen Sie zudem, ob es sich in der Unternehmensbewertung um ein sensibles Thema handelt, wie zum Beispiel Rassismus. 
Tragen Sie im Feld "SensitiveTopic" entweder "Yes" oder "No" ein.
Im Feld "City" ist eine Stadt gespeichert. 
Füllen Sie die Felder "State/Region" und "Country" jeweils mit der Region und dem Land aus, in denen sich diese Stadt befindet. 

Geben Sie das ausgefüllte JSON-Objekt zurück. \n''' # \n ist wichtig!

CHAT_MESSAGES = [
	{
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

Sie werden Ihre Antwort in Form eines JSON-Objekts zurückgeben. Das Format soll folgendermaßen aussehen: {jstringch}
Die eigentliche Antwort auf die Unternehmensbewertung soll im Feld "Response" stehen.

Zusätzlich werden Sie die Empathie, Hilfsbereitschaft und Individualität Ihrer Antwort auf die Unternmehmensbewertung auf 
einer Skala von 1 bis 5 Bewerten, wobei 5 die beste Note ist. Sie werden Ihre Bewertungen der Empathie, Hilfsbereitschaft 
und Individualität an folgenden Kriterien orientieren:

Empathie: 
- 1 Punkt: keine Antwort überhaupt ODER sehr unhöfliche Antwort
- 2 Punkte: sehr kurze Antwort ohne Grußformel
- 3 Punkte: neutrale (unpersönliche) aber dennoch konstruktive Antwort
- 4 Punkte: freundliche Grußformel und freundliche Antwort, mit deutlichem Signal des Verständnisses für das Problem
- 5 Punkte: Bestfall, empathische und persönliche Antwort mit perfektem Verständnis des Problems, falls notwendig mit Entschuldigungen

Hilfsbereitschaft
- 1 Punkt: keine Antwort überhaupt ODER Antwort ohne jeglichen Bezug zu den Schmerzpunkten des Mitarbeiters
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

Falls es sich in der Bewertung um ein sensibles Thema handelt (z.B. Rassismus, Sexismus, Beleidigung, usw.), werden Sie im Feld "SensitiveTopic" "Yes" eintragen, sonst "No".

Als letztes werden Sie aus der Unternehmensbewertung herauslesen, welche Eigenschaft des Jobs der Arbeitnehmer besonders gut findet, und welche er 
oder sie besonders schlecht findet. Wählen Sie aus den folgenden Listen positiver und negativer Eigenschaften jeweils die passendste Kategorie. 
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
	}, {
		"role": "user",
		"content": REVIEWTEXTS[3] + "Stadt: Redwood City"
	}
]

GAIA_HEADERS = {
            "content-type": "application/json",
            "api-key": "eNzXkGNEdMzfay2hfeD8i22WTfMyaazXUOXitVgG3VYDszuT"
}
GAIA_QUERYSTRING = {"api-version":"2023-05-15"}
gaia_chat_payload = {
	"messages": CHAT_MESSAGES, 
	"max_tokens": 1500, # Length of response
	"temperature": 1, # Higher number = less deterministic
	"top_p": 0.0, # Similar to temperature, don't use both
	"n": 1, # Number of responses
	#"stop": "",    # Stop sequence, e.g. STOP123
	"presence_penalty": 0, # [-2, 2]: Positive = Talk about new topics
	"frequency_penalty": 0, # [-2, 2]: Positive = don't phrases verbatim
}
response = requests.request("POST", CHAT_ENDPOINT, json=gaia_chat_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
if(response.status_code < 200 or response.status_code > 299):
	print(f"Error connecting to GAIA API. HTTP Status Code: {response.status_code}")
	exit()
a = json.loads(response.text)['choices'][0]['message']['content'] # Interpret JSON Correctly
a = a.split("{")[1]
a = a.split("}")[0]
a = "{" + a + "}"
a = json.loads(a)
print(a)
platzhalter = 12345

