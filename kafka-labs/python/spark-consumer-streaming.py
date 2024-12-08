"""
running :
$    ~/apps/spark/bin/spark-submit  --master local[2] \
     --driver-class-path .  \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
     spark-consumer-streaming.py
"""


import sys
from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
# from pyspark.sql.functions import from_json, col, min,max,mean,count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

topic = "clickstream"


spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


# Connect to Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic).option("startingOffsets", "latest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()

# Print out incoming data
query1 = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='3 seconds') \
    .queryName("reading from topic: " + topic) \
    .start()

# run query
query1.awaitTermination()


## ---- TODO-1: extract kafka data into dataframe
## Fill in code from lab guide


## ----- end: TODO-1 ------



## ---- TODO-2: Run a SQL query on streaming data!
## Fill in code from lab guide


## ----- end: TODO-2 ------


spark.stop()
