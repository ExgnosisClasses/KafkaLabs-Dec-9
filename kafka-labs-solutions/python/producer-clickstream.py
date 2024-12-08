# Adopted from : https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html

# install required library:  pip install confluent_kafka

from confluent_kafka import Producer
import socket
import time
import sys
import random
import json

topic = "clickstream"
number_of_records_to_send = 1000
delay_between_records_in_secs = 0.2  # e.g. 0 / 0.5

kafka_conf = {
    # 'bootstrap.servers': "kafka1:19092",  # running within docker
    # 'bootstrap.servers': "localhost:9092", # running on host
    'bootstrap.servers': "localhost:9092,kafka1:19092", # running on host and docker
    'client.id': socket.gethostname()}

#----------------
def get_random_ip_address():
    a = random.randint(1,4)
    b = random.randint(1,4)
    c = random.randint (0, 10)
    d = random.randint (0, 10)
    return '{}.{}.{}.{}'.format(a,b,c,d)
#----------------

#----------------
def get_clickstream_record():
    time_now_ms = time.time_ns() // 1_000_000
    ip = get_random_ip_address()
    user = 'user-{}'.format(random.randint(1,100))
    action = random.choice (['clicked', 'viewed', 'blocked'])
    domain = random.choice(['facebook.com', 'youtube.com', 'twitter.com', 'linkedin.com', 'gmail.com'])
    campaign = "campaign-{}".format(random.randint(1,100))
    cost = random.randint(1,100)
    return {
        'timestamp' : time_now_ms,
        'ip' : ip,
        'user' : user,
        'action' : action,
        'domain' : domain,
        'campaign' : campaign,
        'cost' : cost
    }
#----------------

#----------------
def clickstream_record_to_csv(clickstream):
    return '{},{},{},{},{},{},{}'.format(clickstream['timestamp'],
     clickstream['ip'], clickstream['user'], clickstream['action'], 
     clickstream['domain'],clickstream['campaign'], clickstream['cost'])
#----------------
    
#----------------
def clickstream_record_to_json(clickstream):
    return json.dumps(clickstream)
#----------------

print ("Sending to topic: ", topic)

producer = Producer(kafka_conf)

try:
    for i in range(1, number_of_records_to_send+1):
            clickstream = get_clickstream_record()

            key = clickstream['domain']  # make domain as key
            value = clickstream_record_to_json(clickstream)

            producer.produce(topic, key=key, value=value)
            print ("Sending message #{}. key={}, value={}".format(i, key, value))

            if delay_between_records_in_secs > 0:
                time.sleep(delay_between_records_in_secs)

except KeyboardInterrupt:
    print ("Handling interrupt...")

finally:
    print('Finishing up.  Flushing all messages ...')
    producer.flush()  # send all messages
# ---- end: try


print ("Exiting...")
sys.exit(0)