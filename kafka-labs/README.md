<link rel='stylesheet' href='assets/css/main.css'/>

# Kafka Labs

Welcome to Kafka labs bundle.

## Tips

Copy-Paste Shortcuts on Jupyter terminal  

* On Windows try: `Shift + Insert`  
* On Windows try: `Shift + Right Click`

## Intro Labs

### 1 - Setup Kafka

* [1 - Installing Kafka](labs/01-installing-kafka.md)

### 2 - Kafka Utils

* [2.1 - Kafka Utilities](labs/02.1-kafka-utils.md)
* [2.2 - Kafkacat](labs/02.2-kafkacat.md)
* [2.3 - Producing with keys](labs/02.3-keys-partitions.md)
* [2.4 - Compacted topics](labs/02.4-log-compaction.md) - Optional

### 3 - Kafka API

* [3.1 - Importing Project](labs/03.1-import-project.md)
* [3.2 - Simple Producer and Consumer](labs/03.2-kafka-api.md)
* [3.3 - Kafka API](labs/03.3-kafka-api.md)

### 4. More Kafka API

* [4.1 - Kafka Producer benchmarking](labs/04.1-producer-benchmark.md)
* [4.2 - Compression in Kafka](labs/04.2-compression.md)
* [4.3 - Kafka compression benchmarking](labs/04.3-producer-compress.md)

### 5 - Offset Management

* [5.1 - Manual offset](labs/05.1-manual-offset.md)
* [5.2 - Seeking Within Partition](labs/05.2-seek.md)

### 6 - Practice Lab - Domain Count

* [6.1 - Analyzing clickstream traffic data](labs/06-domain-count.md)

### 7 - Kafka Streams

* [7.1 - Streams - Intro](labs/07.1-streaming-intro.md)
* [7.2 - Streams - Foreach](labs/07.2-streaming-foreach.md)

optional (intermediate)

* [7.3 - Streams - Filter](labs/07.3-streaming-filter.md)
* [7.4 - Streams - Map](labs/07.4-streaming-map.md)
* [7.5 - Streams - GroupBy](labs/07.5-streaming-groupby.md)
* [7.6 - Streams - Window](labs/07.6-streaming-window.md)

### Practice Lab - Fraud Detection

* [Fraud detection](labs/practice-1-fraud-detection.md)

### Design Exercise 1 (Group Exercise)

* Instructor will provide details

## Intermediate Labs

### 8 - Confluent Platform

* [8.1 - Install Confluent](labs/08.1-install-confluent.md)
* [8.2 - Schema with Avro](labs/08.2-avro-schema.md)
* [8.3 - KSQL](labs/08.3-ksql-intro.md)

### 9 -  Metrics

* Optional - [9.1 - Metrics Intro](labs/09.1-metrics-intro.md) - This is an introduction to using Metrics library.  Provided as reference.
* [9.2 - Kafka Metrics](labs/09.2-kafka-metrics.md)
* BONUS - [9.3 - Kafka Metrics - traffic stats](labs/09.3-traffic-metrics.md)

### 10 - Kafka Connect

* [10.1 - Kafka Connect](labs/10.1-kafka-connect.md)

### 11 - Spark and Kafka

* [11.1 - Spark + Kafka: batch processing](labs/11.1-spark-kafka-batch-processing.md)
* [11.2 - Spark + Kafka: stream processing](labs/11.2-spark-kafka-stream-processing.md)
* [11.3 - Fraud detection with Spark and Kafka - batch mode](labs/11.3-spark-kafka-fraud-detection-batch.md)
* [11.4 - Fraud detection with Spark and Kafka - streaming](labs/11.4-spark-kafka-fraud-detection-streaming.md)

### Design Exercise 2 (Group Exercise)

* Instructor will provide details

## BONUS Labs

### 12 - Kafka in Docker

* [Running full kafka stack in docker](https://github.com/elephantscale/kafka-in-docker) - Check out this guide on how to run multiple kafka brokers + zookeeper + kafka manager using Docker

## Downloading Your Work from Lab Machine

On Jupyter terminal, execute these commands

```bash
$   cd ~/dev
$   zip -x '*.jar' -x '*.git*'  -x '*.ipynb_checkpoints*'  -r my-kafka-labs.zip    kafka-labs
```

Now right-click on `my-kafka-labs.zip` from Jupyter UI to download.