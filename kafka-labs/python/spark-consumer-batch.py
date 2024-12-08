"""
 running :
$   ~/apps/spark/bin/spark-submit  --master local[2] \
    --driver-class-path .  \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
    spark-consumer-batch.py
"""


import sys
from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
# from pyspark.sql.functions import from_json, col, min,max,mean,count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

topic = "clickstream"

## --- Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


## --- connect to Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()

## --- TODO-1: Go through Kafka data
## Fill in code from lab guide

## --- end: TODO-1 ------



## ---- TODO-2: extract kafka data into dataframe
## Fill in code from lab guide

## ----- end: TODO-2 ------



## ---- TODO-3: Run Spark SQL query to calculate cost per domain
## Fill in code from lab guide

## ----- end: TODO-3 ------




## ---- TODO-4: Run Spark SQL Query to calculste clicks/views/blocks per domain
## Fill in code from lab guide

## ----- end: TODO-4 ------

spark.stop()