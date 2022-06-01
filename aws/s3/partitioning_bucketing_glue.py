import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.job import Job
import datetime
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
datasink_path = "s3://cimcdl-hq-refine-mk/glue_partition_hq_155_iteration_csv"

## 准备测试数据
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://cimcdl-hq-refine-mk/raw/test1/tool_change/LOAD00000001_toolchange_mock_mk.csv"],
        "recurse": True,
    }
)
df = dynamic_frame.toDF()

## 自动分区分桶
# writing = df.coalesce(1).write.format("parquet").option("compression", "snappy") \
#                           .partitionBy('dSn') \
#                           .bucketBy(3, 'ID') \
#                           .saveAsTable('df', path = datasink_path, overwrite=True)


## 自动分区，手动分桶
# df.createOrReplaceTempView("test_df")
# df2_0=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 0")
# df2_1=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 1")
# df2_2=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 2")

# df2_0.coalesce(1).write.mode('overwrite') \
#                           .partitionBy('dSn') \
#                           .parquet(datasink_path)
# df2_1.coalesce(1).write.mode('append') \
#                           .partitionBy('dSn') \
#                           .parquet(datasink_path)
# df2_2.coalesce(1).write.mode('append') \
#                           .partitionBy('dSn') \
#                           .parquet(datasink_path)

## 手动分区，手动分桶
bucket_name = "cimcdl-hq-refine-mk"
now = datetime.datetime.now()
year = now.year
month = now.month
day = now.day
mid_path = "glue_partition_hq_155_iteration_csv"   \
    + "/ingest_year="  \
    + "{:0>4}".format(str(year))  \
    + "/ingest_month=" \
    + "{:0>2}".format(str(month))  \
    + "/ingest_day="   \
    + "{:0>2}".format(str(day))  \
    + "/"
datasink_path = "s3://" + bucket_name + '/' + mid_path
folder_name_1 = "sharp_01"
folder_name_2 = "sharp_02"
folder_name_3 = "sharp_03"

df.createOrReplaceTempView("test_df")
df2_0=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 0")
df2_1=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 1")
df2_2=spark.sql("select a.* from (select *, cast(ID as bigint) % 3 as id_type from test_df) a where a.id_type = 2")

df2_0.coalesce(1).write.parquet(datasink_path + folder_name_1)
df2_1.coalesce(1).write.parquet(datasink_path + folder_name_2)
df2_2.coalesce(1).write.parquet(datasink_path + folder_name_3)

## 重命名 (适用于：手动分区，手动分桶)
def rename_process(source_prefix, destination_path):
    try:
        client = boto3.client("s3")
        # # destination existance checking before coping
        # dest_list_resp_1 = client.list_objects_v2(Bucket=bucket_name, Prefix=destination_path)
        # if ('Contents' in dest_list_resp_1) & (len(dest_list_resp_1['Contents']) > 0):
        #     # remove destination before starting to copy in case destination was existed
        #     client.delete_object(Bucket=bucket_name, Key=destination_path)
            
        # get source key before coping
        src_list_resp = client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
        if ('Contents' in src_list_resp) & (len(src_list_resp['Contents']) > 0):
            # start to copy
            client.copy_object(Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': src_list_resp['Contents'][0]['Key']}, 
                Key=destination_path)
            # destination existance checking before deleting
            dest_list_resp_2 = client.list_objects_v2(Bucket=bucket_name, Prefix=destination_path)
            if ('Contents' in dest_list_resp_2) & (len(dest_list_resp_2['Contents']) > 0):
                # start to delete
                client.delete_object(Bucket=bucket_name, Key=src_list_resp['Contents'][0]['Key'])
            else:
                logger.error("Copy stage was not yet completed.")
        else:
            logger.error("Sources are not existed.")
    except Exception as e:
        logger.error(e)

file_name_1 = "sharp_01.snappy.parquet"
source_prefix_1 = mid_path + folder_name_1
destination_path_1 = mid_path + file_name_1
file_name_2 = "sharp_02.snappy.parquet"
source_prefix_2 = mid_path + folder_name_2
destination_path_2 = mid_path + file_name_2
file_name_3 = "sharp_03.snappy.parquet"
source_prefix_3 = mid_path + folder_name_3
destination_path_3 = mid_path + file_name_3

rename_process(source_prefix_1, destination_path_1)
rename_process(source_prefix_2, destination_path_2)
rename_process(source_prefix_3, destination_path_3)
