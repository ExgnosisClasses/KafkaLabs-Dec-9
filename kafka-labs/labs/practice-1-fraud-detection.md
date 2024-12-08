<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

# Practice Lab: Fraud Detection

### Overview

Let's use Kafka Streams to detect fraud IPs in clickstream

### Run time

30 mins

## Step-1: Undertstanding Clickstream data

Here is a sample clickstream data in JSON format

```json
{"timestamp": 1642840429496, "ip": "4.4.8.2", "user": "user-70", "action": "clicked", "domain": "twitter.com", "campaign": "campaign-99", "cost": 26}
```

we are going scan records and filter-out fraudulent IP address - these IP addresses are marked as spam / bot originators

## Step-2: Source files

* Producer
    - python: `python/producer-clickstream.py`
    - Java: `src/main/java/x/utils/ClickstreamProducer.java`
* Consumer
    - consumer: `src/main/java/x/practice_labs/FraudDetectionApp.java`
    - Fraud IP Lookup: `src/main/java/x/practice_labs/CacheIPLookup.java`

## Step-3:  Inspect Consumer

Inspect consumer file : `src/main/java/x/practice_labs/FraudDetectionApp.java`

Fix TODO items

## Step-4: Run the consumer

You can run it via Eclipse

or run it command line as follows

```bash
$   cd ~/kafka-labs

$    mvn exec:java  -Dexec.mainClass=x.practice_labs.FraudDetectionApp
```

## Step-5: Run the Producer

To run the Java version `src/main/java/x/utils/ClickstreamProducer.java`

- you can run it through Eclipse
- or commnad line as follows

```bash
$   cd ~/kafka-labs

$   mvn exec:java  -Dexec.mainClass=x.utils.ClickstreamProducer
```

Or you can run a python clickstream producer as well

```bash
$   cd   ~/kafka-labs/python

$   python   producer-clickstream.py
```

## Step-6: Inspect the Consumer Output

Watch the consumer output

## Step-7: Update Fraud IP Lookup

Add a few more IPs to `src/main/java/x/practice_labs/CacheIPLookup.java`

```java
public  CacheIPLookup() {
    fraudIPs.add("1.1");  // new 
    fraudIPs.add("3.3");
    fraudIPs.add("4.4");
}
```

## Step-8: Re run the Consumer

See if the new IPs are getting flagged.

