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
df2.sample(0.1).show()


## Let's do a SQL query on Kafka data!
df2.createOrReplaceTempView("clickstream_view")

# calculate avg spend per domain
sql_str = """
SELECT domain, count(*) as impressions, MIN(cost), AVG(cost), MAX(cost)
FROM clickstream_view
GROUP BY domain
"""
spark.sql(sql_str).show()


# count clicks/views/blocks
sql_str = """
SELECT domain, action, COUNT(action)
FROM clickstream_view
GROUP BY domain, action
ORDER BY domain, action
"""
spark.sql(sql_str).show()

# calculate avg spend per domain - using DF API
# spend_per_domain = df2.groupBy('domain').agg(min('cost'), mean('cost'), max('cost'))
# spend_per_domain.show()

# # count clicks/views/blocks - using DF API
# action_count_per_domain = df2.groupBy(['domain', 'action']).count().orderBy(['domain', 'action'])
# action_count_per_domain.show()

spark.stop()
