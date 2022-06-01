import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp, col, split, length

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [stream_type = kafka, stream_batch_time = "100 seconds", database = "hisdata", additionalOptions = {"startingOffsets": "latest", "inferSchema": "true"}, table_name = "numericsamples-msk"]
## @return: datasource0
## @inputs: []
data_frame_datasource0 = glueContext.create_data_frame.from_catalog(database="hisdata", table_name="numericsamples-msk",
                                                                    transformation_ctx="datasource0",
                                                                    additional_options={"startingOffsets": "earliest",
                                                                                        "inferSchema": "true",
                                                                                        "failOnDataLoss": "false"})


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        datasource0 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        ## @type: DataSink
        ## @args: [database = "hisdata", table_name = "redshiftzbrldw_hisdata_numericsamples", stream_batch_time = "100 seconds", redshift_tmp_dir = TempDir, transformation_ctx = "datasink1"]
        ## @return: datasink1
        ## @inputs: [frame = datasource0]

        ### new start
        datasource0 = ResolveChoice.apply(datasource0, specs=[("data.SampleValue", "cast:double")])
        relationalized_json = datasource0.relationalize(root_table_name="root",
                                                        staging_path="s3://zbrl-bigdata-operation/aws-glue-staging-path/")
        root_df = relationalized_json.select('root')

        root_df = root_df.toDF()

        if 'data.TagID' in root_df.columns:
            root_df = root_df.filter(col('`data.TagID`').isNotNull()).filter(col('`metadata.operation`') == "insert")
            ## root_df = root_df.withColumn("tagid",root_df["`data.TagID`"])
            root_df = root_df.withColumn("tagid", col("`data.TagID`").cast("int"))
            if 'data.SampleValue' in root_df.columns:
                root_df = root_df.withColumn("samplevalue", root_df["`data.SampleValue`"])
                ## root_df = root_df.withColumn("sampledatetime",root_df["`data.SampleDateTime`"])
                root_df = root_df.withColumn("sampledatetime", col("`data.sampledatetime`").cast("Long"))
                root_df = root_df.withColumn("qualityid", root_df["`data.QualityID`"])
                ##root_df = root_df.withColumn("time1",root_df["`metadata.commit-timestamp`"])
                root_df = root_df.withColumn("time1", (
                            col("`metadata.commit-timestamp`").cast("timestamp") + F.expr("INTERVAL 8 hours")))
                root_df = root_df.drop(col("`data.QualityID`")).drop(col("`data.SampleDateTime`")).drop(
                    col("`data.SampleValue`")).drop(col("`data.TagID`")).drop(col("`metadata.timestamp`")).drop(
                    col("`metadata.prev-transaction-record-id`")).drop(col("`metadata.prev-transaction-id`")).drop(
                    col("`metadata.stream-position`")).drop(col("`metadata.transaction-record-id`")).drop(
                    col("`metadata.transaction-id`")).drop(col("`metadata.table-name`")).drop(
                    col("`metadata.schema-name`")).drop(col("`metadata.partition-key-type`")).drop(
                    col("`metadata.operation`")).drop(col("`metadata.record-type`")).drop(
                    col("`metadata.commit-timestamp`")).drop(col("`metadata.prev-transaction-id`")).drop(
                    col("`metadata.prev-transaction-record-id`"))
                root_df = DynamicFrame.fromDF(root_df, glueContext, "root_df")

                ## glueContext.write_dynamic_frame.from_options(frame = root_df, connection_type = "s3", connection_options = {"path": "s3://zbrl-bigdata-operation/redshift-tmp-dir/hisdata/"}, format = "csv")

                hisdatatags = glueContext.create_dynamic_frame.from_catalog(database="hisdata",
                                                                            table_name="redshiftzbrldw_hisdata_tags",
                                                                            redshift_tmp_dir="s3://zbrl-bigdata-operation/redshift-tmp-dir/",
                                                                            transformation_ctx="hisdatatags")

                energytags = glueContext.create_dynamic_frame.from_catalog(database="hisdata",
                                                                           table_name="redshiftzbrldw_energy_tags",
                                                                           redshift_tmp_dir="s3://zbrl-bigdata-operation/redshift-tmp-dir/",
                                                                           transformation_ctx="energytags")

                join1_result = Join.apply(root_df, hisdatatags, "tagid", "id").select_fields(
                    ["tagid", "sampledatetime", "samplevalue", "qualityid", "tagname", "time1"])

                energytags = DropFields.apply(frame=energytags,
                                              paths=["tagid", "id", "h_threshold", "l_threshold", "coefficient",
                                                     "domain", "device_type", "addtime", "updtime", "autocontroltypeid",
                                                     "tagenergytype"], transformation_ctx="Transform6")
                ##	energytags.select_fields(["tagid","tagname","stationid","stationname","partitionid","partitionname"])

                ## 转换为DF
                join1_result = join1_result.toDF()
                energytags = energytags.toDF()
                ## 添加新列
                energytags = energytags.withColumn("tagname_new", energytags.tagname)
                ## 删除tagname
                energytags = energytags.drop(energytags.tagname)

                join2_result = join1_result.join(energytags, join1_result.tagname == energytags.tagname_new, how='left')

                ## glueContext.write_dynamic_frame.from_options(frame = join2_result, connection_type = "s3", connection_options = {"path": "s3://zbrl-bigdata-operation/redshift-tmp-dir/hisdata/"}, format = "csv")

                join2_result = join2_result.withColumn('scada_station_id',
                                                       F.substring_index(F.split(join2_result.tagname, '\.').getItem(1),
                                                                         '_', -1))
                join2_result = join2_result.withColumn('tagname_type', F.array_join(
                    F.array_except(F.split(split(join2_result.tagname, '\.').getItem(1), '_'),
                                   F.array(F.substring_index(split(join2_result.tagname, '\.').getItem(1), '_', -1))),
                    '_'))

                join2_result = DynamicFrame.fromDF(join2_result, glueContext, "join2_result")

                join2_result = join2_result.select_fields(
                    ["tagid", "sampledatetime", "samplevalue", "qualityid", "tagname", "description", "time1",
                     "stationid", "stationname", "partitionid", "partitionname", "tagname_type", "scada_station_id"])

                ## applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("tagid", "int", "tagid", "int"), ("time1", "timestamp", "time1", "timestamp"), ("samplevalue", "double", "samplevalue", "double"), ("qualityid", "int", "qualityid", "int"), ("partitionname", "string", "partitionname", "string"), ("tagname_type", "string", "tagname_type", "string"), ("description", "string", "description", "string"), ("partitionid", "int", "partitionid", "int"), ("stationname", "string", "stationname", "string"), ("scada_station_id", "string", "scada_station_id", "string"), ("sampledatetime", "long", "sampledatetime", "long"), ("tagname", "string", "tagname", "string"), ("stationid", "int", "stationid", "int")], transformation_ctx = "applymapping0")

                ## applymapping0 = ApplyMapping.apply(frame=join2_result, mappings=[("tagid", "smallint", "tagid", "smallint"), ("sampledatetime", "bigint", "sampledatetime", "bigint"), ("samplevalue", "float", "samplevalue", "float"),("qualityid", "smallint", "qualityid", "smallint"),("tagname", "string", "tagname", "string"),("description", "string", "description", "string"), ("time1", "timestamp","time1", "timestamp"),("stationid", "smallint", "stationid", "smallint"), ("stationname", "string", "stationname", "string"),("partitionid", "smallint", "partitionid", "smallint"), ("partitionname", "string", "partitionname","string"), ("tagname_type", "string", "tagname_type","string"), ("scada_station_id", "string", "scada_station_id","string")], transformation_ctx="applymapping0")

                ### applymapping0 = ApplyMapping.apply(frame=join2_result, mappings=[("tagid", "smallint", "tagid", "smallint"), ("sampledatetime", "bigint", "sampledatetime", "bigint"), ("samplevalue", "float", "samplevalue", "float"),("qualityid", "smallint", "qualityid", "smallint"),("tagname", "varchar", "tagname", "varchar"), ("time1", "timestamp","time1", "timestamp"),("stationid", "smallint", "hstid", "smallint"), ("stationname", "varchar", "hstnname", "varchar"),("partitionid", "smallint", "ptnid", "smallint"), ("partitionname", "varchar", "ptnname","varchar")], transformation_ctx="applymapping0")

                ### applymapping0 = ApplyMapping.apply(frame=join2_result, mappings=[("tagid", "int", "tagid", "int"), ("sampledatetime", "long", "sampledatetime", "bigint"), ("samplevalue", "long", "samplevalue", "bigint"),("qualityid", "long", "qualityid", "bigint"),("tagname", "string", "tagname", "string"), ("time1", "string","time1", "timestamp"),("stationid", "int", "hstid", "int"), ("stationname", "string", "hstnname", "string"),("partitionid", "int", "ptnid", "int"), ("partitionname", "string", "ptnname","string")], transformation_ctx="applymapping0")

                ### new end

                ## glueContext.write_dynamic_frame.from_options(frame=join2_result, connection_type="s3", connection_options={"path": "s3://zbrl-bigdata-operation/redshift-tmp-dir/hisdata/"}, format="csv")
                datasink1 = glueContext.write_dynamic_frame.from_catalog(frame=join2_result, database="hisdata",
                                                                         table_name="redshiftzbrldw_hisdata_numericsamples",
                                                                         redshift_tmp_dir="s3://zbrl-bigdata-operation/redshift-tmp-dir/",
                                                                         transformation_ctx="datasink1")


glueContext.forEachBatch(frame=data_frame_datasource0, batch_function=processBatch, options={"windowSize": "10 seconds",
                                                                                             "checkpointLocation": "s3://zbrl-bigdata-operation/redshift-tmp-dir/checkpoint/"})
job.commit()
