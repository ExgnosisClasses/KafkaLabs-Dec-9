<link rel='stylesheet' href='../assets/css/main.css'/>

# Lab 6: Domain Count

### Overview

Count Domain based stats of clickstream

### Depends On

### Run time

30 mins

## Step 1 : Create a 'clickstream' topic

```bash
$   ~/apps/kafka/bin/kafka-topics.sh  --bootstrap-server localhost:9092  --create --topic clickstream --replication-factor 1  --partitions 2
```

## Step-2: Listen on `clickstream` topic

Use Kafkacat to see messages in the topic

```bash
$    kafkacat -q -C -b localhost:9092 -t clickstream -f 'Partition %t[%p], offset: %o, key: %k, value: %s\n'
```

Or use console consumer

```bash
$   ~/apps/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --property print.key=true --property key.separator=":" \
        --topic clickstream

```

## Step-3 : Clickstream Producer

* Inspect file and make any fixes : `src/main/java/x/utils/ClickStreamProducer.java`  
* Run the producer in Eclipse, Right click on the file and run as 'Java Application'
* Make sure it is sending messages as follows
  - key : Domain
  - value : clickstream data
  - example  :

```console
  key=facebook.com, value={"timestamp":1451635200005,"session":"session_251","domain":"facebook.com","cost":91,"user":"user_16","campaign":"campaign_5","ip":"ip_67","action":"clicked"}
```

Inspect 'console-consumer' output, it may look something like this

```console
facebook.com:{"timestamp":1451635200005,"session":"session_251","domain":"facebook.com","cost":91,"user":"user_16","campaign":"campaign_5","ip":"ip_67","action":"clicked"}
cnn.com:{"timestamp":1451635200020,"session":"session_66","domain":"cnn.com","cost":31,"user":"user_29","campaign":"campaign_3","ip":"ip_49","action":"blocked"}
```


## Step-4 :  DomainCount Consumer

This consumer will keep an running total of domain count seen in clickstream.

* Inspect file : `src/main/java/x/lab06_domain_count/DomainCountConsumer.java`  
* Fix the TODO items

Use reference Java API [for Consumer](https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)


## Step-5 :Run

* Run the `lab06_domain_count.DomainCountConsumer` in Eclipse,
* Run the `utils.ClickStreamProducer` in Eclipse,
* Expected output

```console
Got 10 messages
Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=1]

Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=1, foxnews.com=1]

Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=2, foxnews.com=1]
...
```

## BONUS: Caclculate click ratio

This is a bonus task for you to try.

Say we saw 10 records from facebook.com.  3 of them were clicks.  That makes the click-ratio = 3 / 10 = 30%

Calculate click ratios for all domains.

And print out the domains with the highest click-ratio.

Hint:

- You will want to keep track of total traffic (which we are doing here)
- And also keep a click-count (you will need to implement this)
- You can use another hashmap to keep track of clicks.
- And print out the ratio