# Adopted from : https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html

from confluent_kafka import Producer
import socket
import time
import sys
import random


conf = {
    'bootstrap.servers': "localhost:9092,kafka1:19092", # running on host and docker
    'client.id': socket.gethostname()}

producer = Producer(conf)

topic = "topic1"
try:
    for i in range(1, 1000+1):
            idx = random.randint(1,5)
            key = "k-{}".format(idx)
            value = 'v-{}--{}'.format(idx, i)
            producer.produce(topic, key=key, value=value)
            print ("Sending message #{}. key={}, value={}".format(i, key, value))
        #     time.sleep(0.5)

except KeyboardInterrupt:
    print ("Handling interrupt...")

finally:
    print('Finishing up.  Flushing all messages ...')
    producer.flush()  # send all messages
# ---- end: try


print ("Exiting...")
sys.exit(0)
