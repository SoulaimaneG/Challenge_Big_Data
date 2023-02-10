#!/usr/bin/env python
# coding: utf-8

# Importation des libraries
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from json import loads
import json
import sys
      
# Kafka consumer
bootstrap_servers = ['localhost:9092']
topicName = ['test.topic.raw','test.topic.threshold_costs']
consumer = KafkaConsumer (group_id = 'big_data',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'earliest',value_deserializer=lambda x: loads(x.decode('utf-8'))) ## You can also set it as latest
consumer.subscribe(['test.topic.raw','test.topic.threshold_costs'])

# Kafka Producer                        
topic = 'test.topic.'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

# Itération sur chaque ligne du fichier "Commentaires.csv" et l'envoyer par le Producer
with open('Commentaires.csv','r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        ack = producer.send(topicname, json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)

# Lire le message depuis Consumer
try:
    for message in consumer:
        msg = message.value
        commentaire = str(msg['Commentaire'])
        topic = 'test.topic.'
        print(topic)
        ack = producer.send(topic,json.dumps(msg).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)
        
except KeyboardInterrupt:
    sys.exit()