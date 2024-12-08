# Adopted from : https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html

# install required library:  pip install confluent_kafka

from confluent_kafka import Producer
import socket
import time
import sys

topic = "test"
number_of_records_to_send = 1000
delay_between_records_in_secs = 0.5  # e.g. 0 / 0.5

kafka_conf = {
    # 'bootstrap.servers': "kafka1:19092",  # running within docker
    # 'bootstrap.servers': "localhost:9092", # running on host
    'bootstrap.servers': "localhost:9092,kafka1:19092", # running on host and docker
    'client.id': socket.gethostname()}

producer = Producer(kafka_conf)

time_now_ms = time.time_ns() // 1_000_000
try:
    for i in range(1, number_of_records_to_send+1):
            key = "{}".format (time_now_ms + i)   # time in ms
            value = 'hello world @ {}'.format(key)

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