"""
 running :
$   ~/apps/spark/bin/spark-submit  --master local[2] \
    --driver-class-path .  \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
    spark-consumer-fraud-detection-batch.py
"""


import sys
from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
# from pyspark.sql.functions import from_json, col, min,max,mean,count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


# Fraud IP ranges
fraud_ips = {
        '3.3' ,
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
spark.sparkContext.setLogLevel("ERROR")
print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()


print ("total count: ", df.count())
df.show()


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

# extract json into columns
df2 = df.withColumn("value", from_json("value", schema))\
    .select(col('key'), col('value.*'))

df2.printSchema()
df2.sample(0.1).show()


fraud_df = df2.filter(is_fraud_ip_udf('ip'))
fraud_df.show()

print ("Total record count: ", df2.count())
print ("Fraud record count: ", fraud_df.count())

spark.stop()
