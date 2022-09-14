from kafka import KafkaConsumer
import time


consumer = KafkaConsumer("test-topic", bootstrap_servers='kafka:9092', group_id='test-consumer-group')

for msg in consumer:
    print("Message bien reçu")
