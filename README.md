#  Dummy data creatie

Dit script (`dummy_data_creatie.py`) genereert een realistische dataset van netwerkverkeer, inclusief verschillende types anomalieën. 
Deze dummydata wordt gebruikt om een Machine Learning model te trainen voor anomaliedetectie binnen netwerklogs.

Het script maakt volgende soortrn records aan:

- Normaal netwerkverkeer: willekeurige verbindingen tussen source en destination IP’s en poorten
- Verticale poortscans: 1 source IP probeert meerdere poorten op één bestemming
- Horizontale poortscans: 1 source IP scant dezelfde poort op meerdere bestemmingen
- Destination IP spikes: 1 IP krijgt plots veel verkeer op korte tijd
- Ongebruikelijke IP-combinaties: zeldzame IP adressen die met elkaar communiceren, wat normaal nooit gebeurt

---


Na het uitvoeren van het script, wordt de dataset opgeslagen als dummy_network_logs.csv



 Wat zien we als een anomalie?
In netwerkverkeer is een anomalie een patroon dat afwijkt van het normale gedrag. Hieronder zie je de soorten  die we bewust gesimuleerd hebben:

1. Verticale poortscan
Eén source.ip scant een groot aantal verschillende poorten op één enkel destination.ip.
Typisch het gedrag van een hacker die zoekt welke poorten openstaan op een specifieke server.

Voorbeeld:

source.ip	destination.ip	destination.port
10.0.0.4	192.168.0.10	21
10.0.0.4	192.168.0.10	22
10.0.0.4	192.168.0.10	23
..
10.0.0.4	192.168.0.10	1050


2. Horizontale poortscan


Eén source.ip probeert dezelfde poort op veel verschillende destination.ip’s.
Dit wijst op een poging om een bekende poort op veel systemen te vinden.

Voorbeeld:

source.ip	destination.ip	destination.port
10.0.0.99	192.168.0.10	22
10.0.0.99	192.168.0.11	22
10.0.0.99	192.168.0.12	22
...
10.0.0.99	192.168.0.50	22

3. Destination IP spike
Een destination.ip ontvangt plotseling veel verbindingen in een korte timeframe.
Lijkt op een DDoS-attacj of ongebruikelijk piekverkeer naar 1 doel.

Voorbeeld:

timestamp	source.ip	destination.ip	destination.port
2025-03-29 10:00:01	10.0.1.1	192.168.1.100	443
2025-03-29 10:00:02	10.0.1.2	192.168.1.100	443
2025-03-29 10:00:03	10.0.1.3	192.168.1.100	443

4. Ongebruikelijke IP-combinatie

source.ip en destination.ip communiceren terwijl dit nog nooit eerder is gebeurd in normaal netwerkverkeer.
Wijst mogelijk op insider threat, laterale beweging, of ongewenst verkeer.

Voorbeeld:

source.ip	destination.ip
10.10.10.10	 172.16.0.15
In deze context: geen historisch verkeer tussen deze IP's = verdacht

In onze dataset: 

label met 1 - anomalie
label met 0 - normaal verkeer


Resultaten van de ML Modellen
Na het trainen van 3  supervised modellen op de gegenereerde dummydata, werden volgende resultaten behaald:

Model	Accuracy	Precision	Recall	F1-score
Random Forest	~100%	1.00	1.00	1.00
Logistic Regression	~99.9%	0.99–1.00	0.99	0.99
XGBoost	~100%	1.00	1.00	1.00


Interpretatie:
Deze resultaten zijn verwacht omdat we werken met dummy-data waarin de anomalieën bewust duidelijk afwijken van normaal netwerkgedrag.
Doordat de patronen sterk contrast tonen met het normale verkeer, kunnen ML modellen ze goed herkennen .



