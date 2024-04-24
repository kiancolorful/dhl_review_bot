import requests




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
Image von DHL als Arbeitgeber stärkst. Formulieren Sie nun eine passende Antwort auf diese 
Unternehmensbewertung von Glassdoor: \n''' # \n ist wichtig!

payload = {
	"prompt": PROMPT_PREFIX + '''<div class="index__heading__+SqmZ"><span class="index__dateBlock__POvKt"><time class="p-tiny-regular text-dark-63" datetime="2024-04-11T00:00:00+00:00">April 2024</time></span><h2 class="index__title__xakS9 h3-semibold">Alles in allem super!</h2></div><hr aria-hidden="true" class="index__separator__PHkW3"><div class="index__rating__I2+qI"><div class="index__ratingBlock__ANYGb"><div class="index__block__7hodp index__scoreBlock__4qZUo"><span class="h3-semibold index__score__BktQY">5,0</span><span class="index__stars__nfK6S index__large__9C47L" data-fillcolor="butterscotch" data-score="5"></span></div><span class="index__recommendationBlock__2zhEJ"><span class="p-tiny-bold">Zusage</span></span></div><span class="index__dateBlock__lB3JE"><time class="p-tiny-regular text-dark-63" datetime="2024-04-11T00:00:00+00:00">April 2024</time></span></div><div class="index__employmentInfoBlock__wuOtj p-tiny-regular"><b>Bewerber/in</b><span class="index__sentence__j5Cc3 text-dark-63 index__middot__jwHNi">Hat sich 2024 bei Deutsche Post DHL in Berlin als Zusteller/Postbote beworben und eine Zusage erhalten.</span></div><div class="index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Verbesserungsvorschläge</h4><p class="index__plainText__JgbHE">Gibt es meiner Meinung nicht,was den Bewerbungsprozess betrifft, es war ein schöner und lehrreicher Tag mit super netten Menschen.</p></div><div class="index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Bewerbungsfragen</h4><div><ul class="index__interviewQuestions__3qMwc p-base-regular"><li><span>Stärken, Schwächen,Hobbys</span></li><li><span>Allgemeine Fragen zum Lebenslauf</span></li><li><span>Nachfrage auf Flexibilität</span></li></ul></div></div><hr class="index__separator__VX7aC"><div class="index__collapsible__B8dtw index__columns__VSl8V"><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Zufriedenstellende Reaktion</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Schnelle Antwort</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Erwartbarkeit des Prozesses</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Professionalität des Gesprächs</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Vollständigkeit der Infos</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Angenehme Atmosphäre</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Wertschätzende Behandlung</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Zufriedenstellende Antworten</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Erklärung der weiteren Schritte</h4></div><div class="index__factor__TvBs0 index__factor__Mo6xW p-base-regular"><h4 class="index__title__Rq0Po">Zeitgerechte Zu- oder Absage</h4></div></div><div class="index__controls__gbcou"><button class="reviews-show-star-ratings-button index__collapsibleButton__0sIwc index__button__AlOLQ index__secondary__08Tb0 index__semiRounded__QdDxr undefined index__text__en4kg" type="button">Sterne anzeigen</button></div><div class="index__reviewOptionsContainer__+i5kJ"><span class="index__hidden__NLc4l">Hilfreich</span><span class="index__hidden__NLc4l">Hilfreich?</span><span class="index__hidden__NLc4l">Zustimmen</span><span class="index__hidden__NLc4l">Zustimmen?</span><span class="index__hidden__NLc4l">Melden</span><span class="index__hidden__NLc4l">Teilen</span></div><div class="index__reviewFooter__Nhzn5"><a class="reviews-respond-to-review-cta index__ctaRespond__5yOLD p-tiny-regular index__button__AlOLQ index__secondary__08Tb0 index__semiRounded__QdDxr undefined index__text__en4kg" href="https://www.kununu.com/de/statements/dfd5a004-7e68-4774-a21c-448c713148c0/review/82a0d7e1-38d5-4801-be2e-9f711fd4d69e?linkFrom=profile" rel="nofollow"><span class="text-dark-63">Ihr Unternehmen? </span><span class="index__ctaRespondReview__zdXj6">Als Arbeitgeber kommentieren</span></a></div><|endoftext|>''',
	"max_tokens": 200,
	"temperature": 0.7,
	#"top_p": 0,
	#"logit_bias": {},
	#"user": "string",
	"n": 2,
	"stream": False,
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

