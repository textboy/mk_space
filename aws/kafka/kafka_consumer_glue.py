import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
import logging

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Script generated for node Apache Kafka
data_frame_datasource0 = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options={
        "connectionName": "kafka_conn",
        "classification": "json",
        "startingOffsets": "earliest",
        "topicName": "mk-topic-1",
        "inferSchema": "true",
        "typeOfData": "kafka",
    },
    transformation_ctx="datasource0",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        data_frame.printSchema()
        data_frame.show()
        datasource0 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # datasource0 = ResolveChoice.apply(datasource0, specs=[("data.SampleValue", "cast:double")])
        relationalized_json = datasource0.relationalize(root_table_name="root",
                                                        staging_path="s3://mk-glue-367793726391/temp/")
        root_df = relationalized_json.select('root')
        root_df.printSchema()
        root_df = root_df.toDF()
        if 'eventValue.af_content_type' in root_df.columns:
            root_df = root_df.withColumn("af_content_type", root_df["`eventValue.af_content_type`"])
            # root_df = root_df.drop(col("`eventValue.af_content_type`"))
        root_df.printSchema()
        root_df.select("af_content_type").show()
    else:
        logger.info("lessThanZero~~~~")


glueContext.forEachBatch(
    frame=data_frame_datasource0,
    batch_function=processBatch,
    options={
        "windowSize": "3 seconds",
        "checkpointLocation": "s3://mk-glue-367793726391/kafka_output_checkpoint/",
    },
)
job.commit()
