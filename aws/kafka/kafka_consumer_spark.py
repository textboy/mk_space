from pyspark import SparkContext
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import from_json
from pyspark.sql import Row
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("read write kafka") \
    .getOrCreate()

## read stream from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","b-1.mk-kafka-1.ggq3vp.c2.kafka.cn-north-1.amazonaws.com.cn:9092") \
    .option("subscribe","mk-topic-1") \
    .option("startingOffsets", "earliest") \
    .load()

## check stream (can NOT show on stream dataframe)
print(str(kafka_stream_df.dtypes))
print(str(kafka_stream_df.isStreaming))
kafka_stream_df.printSchema()

## parse stream
userSchema = StructType()  \
    .add("appsflyer_id", "string")  \
    .add("eventTime", "string")  \
    .add("advertising_id", "string")  \
    .add("eventName", "string")  \
    .add("eventValue", "string")
# convert binary to readable string (key & value are binary)
kafka_str_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
# kafka_df is still streaming dataframe
kafka_df = kafka_str_df.withColumn("value", from_json("value", userSchema))\
    .select(col('value.*'))

## write stream
# console
kafka_stream_df.writeStream \
    .format("console") \
    .start().awaitTermination()

# S3
# df_1=kafka_stream_df.selectExpr("CAST(value AS String)") \
#     .select(from_json("value",userSchema).alias("data")) \
#     .select("data.*") \
#     .writeStream \
#     .format("csv") \
#     .outputMode("append") \
#     .option("path","s3://mk-glue-367793726391/output/") \
#     .option("checkpointLocation","s3://mk-glue-367793726391/checkpoint") \
#     .start() \
#     .awaitTermination()

# in batch
def writeToS3(kafka_df, epochId):
    # kafka_df is batch dataframe now
    kafka_df.show()
    # mode: append, overwrite, update
    kafka_df.write \
        .format("parquet") \
        .option("path", "s3://mk-glue-367793726391/output/foreachBatch_sink.parquet") \
        .mode("append") \
        .save()
query = kafka_df.writeStream.foreachBatch(writeToS3).outputMode("append").start().awaitTermination()

# kafka
df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
   .writeStream
   .format("kafka")
   .outputMode("append")
   .option("kafka.bootstrap.servers", "192.168.1.100:9092")
   .option("topic", "josn_data_topic")
#     .option("checkpointLocation", "s3://mk-glue-367793726391/checkpoint") \
   .start()
   .awaitTermination()

# spark.streams.awaitAnyTermination()
