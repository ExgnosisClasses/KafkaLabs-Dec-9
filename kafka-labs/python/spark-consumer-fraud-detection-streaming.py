"""
running :
$    ~/apps/spark/bin/spark-submit  --master local[2] \
     --driver-class-path .  \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
     spark-consumer-fraud-detection-streaming.py
"""


import sys
from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
# from pyspark.sql.functions import from_json, col, min,max,mean,count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


# Fraud IP ranges
fraud_ips = {
        # '3.3',
        '4.4'
}

# --------------------------
def is_fraud_ip(ip):
        a_b = 'x.y'
        tokens  = ip.split('.')
        if len(tokens) == 4:
                a_b = '{}.{}'.format(tokens[0],  tokens[1])
        return a_b in fraud_ips
# --------------------------
# define a udf
is_fraud_ip_udf = udf(is_fraud_ip, BooleanType())
# --------------------------


topic = "clickstream"


spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])

sc = spark.sparkContext
sc.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic).option("startingOffsets", "latest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()

# # Print out incoming data
# query1 = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='3 seconds') \
#     .queryName("reading from topic: " + topic) \
#     .start()

# # run query
# query1.awaitTermination()

## ---- TODO-1: extract kafka data into dataframe
## Fill in code from lab guide
# code for TODO-1

# first we need a schema
schema = StructType(
    [
        StructField('timestamp', StringType(), True),
        StructField('ip', StringType(), True),
        StructField('user', StringType(), True),
        StructField('action', StringType(), True),
        StructField('domain', StringType(), True),
        StructField('campaign', StringType(), True),
        StructField('cost', IntegerType(), True)
    ]
)

# extract value from JSON string and populate into another df
df2 = df.withColumn("value", from_json("value", schema))\
    .select(col('key'), col('value.*'))

df2.printSchema()

# Print out incoming data
# query2 = df2.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='3 seconds') \
#     .queryName("reading from topic: " + topic) \
#     .start()

# # run query
# query2.awaitTermination()

## ----- end: TODO-1 ------




## ---- TODO-2:  detect fraud IPs
## Fill in code from lab guide
# code for TODO-2

fraud_df = df2.filter(is_fraud_ip_udf('ip'))

# TODO: try 'udpate' and 'complete' mode
query3 = fraud_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='3 seconds') \
    .queryName("fraud IP") \
    .start()

query3.awaitTermination()
## ----- end: TODO-2 ------


spark.stop()