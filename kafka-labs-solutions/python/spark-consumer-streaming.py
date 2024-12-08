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


# create spark
spark = SparkSession \
    .builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print('### Spark UI available on port : ' + spark.sparkContext.uiWebUrl.split(':')[2])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic).option("startingOffsets", "latest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)",
                       "CAST(partition AS INTEGER)", "CAST(offset AS LONG)", "CAST(timestamp AS TIMESTAMP)")

df.printSchema()


## Print out incoming data
# query1 = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='3 seconds') \
#     .queryName("reading from topic: " + topic) \
#     .start()
## run query
# query1.awaitTermination()

# extract json into columns

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

df2 = df.withColumn("value", from_json("value", schema))\
    .select(col('key'), col('value.*'))

df2.printSchema()

# # Print out incoming data
# query2 = df2.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='3 seconds') \
#     .queryName("reading from topic: " + topic) \
#     .start()

# # run query
# query2.awaitTermination()

## Let's do a SQL query on Kafka data!
df2.createOrReplaceTempView("clickstream_view")

# calculate avg spend per domain
sql_str = """
SELECT domain, count(*) as impressions, MIN(cost), AVG(cost), MAX(cost)
FROM clickstream_view
GROUP BY domain
"""

domain_cost = spark.sql(sql_str)

# TODO: try 'update' and 'complete' mode
query3 = domain_cost.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='3 seconds') \
    .queryName("domain cost") \
    .start()
query3.awaitTermination()

spark.stop()
