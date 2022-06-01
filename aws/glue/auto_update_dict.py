import boto3
import json
import awswrangler as wr
import re
import codecs
import os
import sys
import argparse
import logging

if sys.version_info >= (3, 6):
    import zipfile
else:
    import zipfile36 as zipfile


class AutoUpdateDict:
    def __init__(self, region_name, bucket, redshift_secret_id):
        self.region_name = region_name
        self.bucket = bucket
        self.redshift_secret_id = redshift_secret_id
        self.s3 = boto3.client('s3', region_name=region_name)
        self.con = wr.redshift.connect(secret_id=redshift_secret_id)

    def read_config_file(self):
        data = self.s3.get_object(Bucket=bucket, Key=config_file)
        contents = data['Body'].read().decode("utf-8")
        return json.loads(contents)

    def read_dim_table(self, table_name):
        df = wr.redshift.read_sql_query("SELECT * FROM  " + table_name, con=self.con)
        dim_list = df.to_numpy().tolist()
        return json.dumps(dim_list, ensure_ascii=False)

    def update_udf_dim(self):
        config_dict = self.read_config_file()
        for conf in config_dict:
            udf_file = conf["udf_file"]
            dim_data = "[]"
            dim2_data = "[]"
            if "dim_table" in conf.keys():
                dim_table = conf["dim_table"]
                dim_data = self.read_dim_table(dim_table)
                dim_data = dim_data.replace("\n", "")
                dim_data = dim_data.replace("\\n", "")
            # support 2nd dim table
            elif "dim2_table" in conf.keys():
                dim2_table = conf["dim2_table"]
                dim2_data = self.read_dim_table(dim2_table)
                dim2_data = dim2_data.replace("\n", "")
                dim2_data = dim2_data.replace("\\n", "")
            if (dim_data == "[]") & (dim2_data == "[]"):
                continue
            filenames = './dim/' + udf_file + '.py'
            with codecs.open(filenames, "r", encoding="utf-8") as f:
                file_data = f.read()
                file_data1 = f.read()
                if (dim_data != "[]"):
                    file_data1 = re.sub(r'dim_table.*]]', "dim_table=" + dim_data, file_data,flags=re.M)
                elif (dim2_data != "[]"):
                    file_data1 = re.sub(r'dim2_table.*]]', "dim2_table=" + dim2_data, file_data,flags=re.M)
                #print(file_data1)
            with codecs.open(filenames, "w", encoding="utf-8") as f:
                f.write(file_data1)


def make_zip(source_dir, output_filename):
    zipf = zipfile.ZipFile(output_filename, 'w')
    pre_len = len(os.path.dirname(source_dir))
    for parent, dirnames, filenames in os.walk(source_dir):
        for filename in filenames:
            pathfile = os.path.join(parent, filename)
            arcname = pathfile[pre_len:].strip(os.path.sep)  # 相对路径
            zipf.write(pathfile, arcname)
    zipf.close()


def upload_udf(local_file, path):
    wr.s3.upload(local_file=local_file, path=path)


def update_redshift_lib(udf_s3_path, iam_role):
    sql = """
    CREATE OR REPLACE LIBRARY meetyou_udf LANGUAGE plpythonu
        from '{0}'
        credentials '{1}';
    """.format(udf_s3_path, iam_role)
    con = wr.redshift.connect(secret_id=redshift_secret_id)
    con.rollback()
    con.autocommit = True
    con.run(sql)
    con.autocommit = False
    con.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='update redshift udf')
    parser.add_argument('--region_name', dest='region_name', required=True, type=str, help='redshift region')
    parser.add_argument('--bucket', dest='bucket', type=str,required=True,  help='s3 bucket name,stored config file')
    parser.add_argument('--redshift_secret_id', dest='redshift_secret_id',required=True,  type=str, help='redshift secret manager name')
    parser.add_argument('--config_file', dest='config_file',required=True,  type=str, help='config file s3 path, not contains bucket '
                                                                            'name')
    parser.add_argument('--udf_local_zip', dest='udf_local_zip', type=str, help='local zip file path', required=False)
    parser.add_argument('--udf_s3_path', dest='udf_s3_path', required=True, type=str, help='udf lib upload to s3 path, eg: '
                                                                              's3://bucket_name/meetyou-udf.zip')
    parser.add_argument('--iam_role', dest='iam_role', required=True, type=str, help='redshift iam role to access s3')

    args = parser.parse_args()
    region_name = args.region_name
    bucket =args.bucket
    config_file = args.config_file
    redshift_secret_id = args.redshift_secret_id
    udf_local_zip = args.udf_local_zip
    udf_s3_path = args.udf_s3_path
    iam_role = args.iam_role

    try:
        logging.warning("start update udf")
        AutoUpdateDict(region_name, bucket, redshift_secret_id).update_udf_dim()

        logging.warning("zip udf")
        make_zip("./", udf_local_zip)

        logging.warning("upload udf")
        upload_udf(udf_local_zip, udf_s3_path)

        logging.warning("update redshift lib")
        update_redshift_lib(udf_s3_path, iam_role)

        logging.warning("success")
    except Exception as e:
        logging.exception(e)
        raise e

# read_dim_data("dev.public.action_code")
