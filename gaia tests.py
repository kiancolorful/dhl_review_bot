import requests

url = "https://apihub.dhl.com/genai/openai/deployments/gpt-4-1106/completions"

querystring = {"api-version":"2023-05-15"}

payload = {
	"prompt": "Formulieren Sie eine passende Antwort auf diese Unternehmensbewertung: Sehr entspanntes und freundliches Team und offene Arbeitskultur! Als Arbeitgeber ziemlich flexibel bzgl. Arbeitszeiten und sonstiges, je nachdem wo die eigenen Interessen liegen kann man auch nach Absprache in andere Projekte eingebracht werden. ",
	"max_tokens": 200,
	#"temperature": 1,
	#"top_p": 1,
	#"logit_bias": {},
	#"user": "string",
	"n": 1,
	#"stream": False,
	#"logprobs": None,
	#"suffix": "string",
	#"echo": False,
	#"stop": "",
	#"completion_config": "string",
	#"presence_penalty": 0,
	#"frequency_penalty": 0,
	#"best_of": 0
}
headers = {
	"content-type": "application/json",
	"api-key": "eNzXkGNEdMzfay2hfeD8i22WTfMyaazXUOXitVgG3VYDszuT"
}

response = requests.request("POST", url, json=payload, headers=headers, params=querystring)
#response = requests.request("GET", "https://developer.dhl.com/sites/default/files/2024-02/swagger_2.yaml")

print(response.text)