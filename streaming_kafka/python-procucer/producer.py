from kafka import KafkaProducer
import time
import requests
import json
import logging as log


print("Launching Python producer")

producer = KafkaProducer(bootstrap_servers="kafka:9092")
link = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
topic = 'test-topic'

def call_api(link):
    """Call the API to get the data from the link

    Args:
        link (string): link of the json file of velib data

    Returns:
        string: json file of the response 
    """

    r = requests.get(link)

    print(r.status_code)
    print(type(r.status_code))
    if r.status_code == 200:
        print('Request done sucessfully')
        log.info('Fini')
        return r.json()
    else:
        print('Failed Request')


while True:
    """Send the data in json format to kafka topic every 30 seconds
    """
    velib_json = call_api(link)
    print(f'Sending message to topic {topic}')
    log.info('Envoie')
    print(type(velib_json))
    str_json = json.dumps(velib_json)
    print(type(str_json))
    encoded_velib = str.encode(str_json, "utf-8")
    producer.send("test-topic", encoded_velib)
    print(type(velib_json))
    #print(velib_json)
    print(f'Message successfully sent to topic {topic}')
    time.sleep(10)
