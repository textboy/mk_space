import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row, functions
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark import SparkFiles
from awsglue import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_raw = spark.readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers","10.196.100.33:6667") \
              .option("kafka.security.protocol","SSL") \
              .option("kafka.ssl.truststore.location","server.truststore.jks") \
              .option("kafka.ssl.truststore.password","Admin123") \
              .option("kafka.ssl.keystore.location","server.keystore.jks") \
              .option("kafka.ssl.keystore.password","Admin123") \
              .option("kafka.isolation.level","read_committed") \
              .option("subscribe","BBF464_IoT_Edge_Test_1") \
              .option("kafka.ssl.endpoint.identification.algorithm","") \
              .option("kafka.isolation.level","read_committed") \
              .option("kafka.ssl.protocol","TLSv1.2") \
              .load()

product_schema = StructType() \
        .add("action", StringType()) \
        .add("time", StringType()) 
  
df_1=df_raw.selectExpr("CAST(value AS String)") \
           .select(from_json("value",product_schema).alias("data")) \
           .select("data.*") \
           .writeStream \
           .format("parquet") \
           .outputMode("append") \
           .option("path","s3://kafka-tls-output/product/") \
           .option("checkpointLocation","s3://kafka-tls-output/checkpoint") \
           .start() \
           .awaitTermination()   

job.commit()
