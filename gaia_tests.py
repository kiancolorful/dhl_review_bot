import requests
import json




url = "https://apihub.dhl.com/genai/openai/deployments/gpt-35-turbo-0301/completions"

querystring = {"api-version":"2023-05-15"}

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
Image von DHL als Arbeitgeber stärkst. Die Antwort sollte nur den reinen Antworttext enthalten. Formulieren Sie nun eine passende Antwort auf diese 
Unternehmensbewertung von Glassdoor: \n''' # \n ist wichtig!

GAIA_HEADERS = {
            "content-type": "application/json",
            "api-key": "eNzXkGNEdMzfay2hfeD8i22WTfMyaazXUOXitVgG3VYDszuT"
}
GAIA_QUERYSTRING = {"api-version":"2023-05-15"}
gaia_payload = {
	"prompt": PROMPT_PREFIX + "Es wird sehr stressig man wird zwar geschult aber die Realität sieht anders aus. Erster Arbeitstag wird man gleich ins Kaos geschickt. Würde es nicht empfehlen " + "<|endoftext|>", # Specify format together with Weichert
	"max_tokens": 200, # Length of response
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
response = requests.request("POST", url, json=gaia_payload, headers=GAIA_HEADERS, params=GAIA_QUERYSTRING)
if(response.status_code < 200 or response.status_code > 299):
	print(f"Error connecting to GAIA API. HTTP Status Code: {response.status_code}")
a = json.loads(response.text)['choices'][0]['text'] # Interpret JSON Correctly
b = 1

x = None
x += "test"
b = 2