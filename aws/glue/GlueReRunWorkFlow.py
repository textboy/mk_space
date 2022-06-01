''' GlueReRunWorkFlow.py
param：
    --begin_ymd             rerun batch begin date (include), format date %Y-%m-%d, optional
    --end_ymd               rerun batch end date (include), format date %Y-%m-%d, optional
    --begin_hour            rerun batch begin hour, format integer, optional
    --end_hour              rerun batch end hour, format integer, optional
    --workflow_name         workflow name, format string, mandatory when rerun workflow
    --workflow_run_id       workflow run id, format string, mandatory when rerun workflow
    --node_name_list        node name list (keep workflow_name, format list seperate by space (e.g. a b), mandatory when rerun workflow
    --job_name              job name, format string, mandatory when rerun job
    --job_run_id            job run id, format string, mandatory when rerun job
    --replace_parameters    replace workflow run properties or job input arguments, format dict (e.g. {"a":"b"}), optional
return：
    （N/A, info or error please refer output log）
call:
    workflow rerun (ingest "Run properties" with "batch_date":<batch date>, batch date e.g. "2022-05-22 00:00:00") or 
    job run (ingest "Input arguments" with "batch_date":<batch date>)
created:
   2022-05-24:         Mike Kong
updated:
    ...
'''

import boto3
import sys
import logging
import datetime
import time
import botocore
import json

WORKFLOW_END_STATUS_LIST = ['COMPLETED', 'STOPPED', 'ERROR']
WORKFLOW_COMPLETED_STATUS = 'COMPLETED'
JOB_END_STATUS_LIST = ['STOPPED', 'SUCCEEDED', 'FAILED', 'TIMEOUT']
JOB_SUCCEEDED_STATUS = 'SUCCEEDED'
NO_BATCH_DATE_FLAG = 'NO BATCH DATE WAS ASSIGNED'

logging.basicConfig(level = logging.INFO)  # INFO for verification
logger = logging.getLogger()

class GlueReRunWorkFlow:
    def __init__(self):
        pass
    
    def parse_params(self): 
        param_dict = {}
        # 取从rerun job中取参数
        source_param_list=sys.argv #get the all input parameters

        if '--begin_ymd' in source_param_list:
            param_dict['begin_ymd'] = source_param_list[source_param_list.index('--begin_ymd') + 1].strip()
        if '--begin_hour' in source_param_list:
            param_dict['begin_hour'] = source_param_list[source_param_list.index('--begin_hour') + 1].strip()
        if '--end_ymd' in source_param_list:
            param_dict['end_ymd'] = source_param_list[source_param_list.index('--end_ymd') + 1].strip()
        if '--end_hour' in source_param_list:
            param_dict['end_hour'] = source_param_list[source_param_list.index('--end_hour') + 1].strip()
        if '--workflow_name' in source_param_list:
            param_dict['workflow_name'] = source_param_list[source_param_list.index('--workflow_name') + 1].strip()
        if '--workflow_run_id' in source_param_list:
            param_dict['workflow_run_id'] = source_param_list[source_param_list.index('--workflow_run_id') + 1].strip()
        if '--node_name_list' in source_param_list:
            node_name_list_str = source_param_list[source_param_list.index('--node_name_list') + 1]
            if node_name_list_str is not None:
                param_dict['node_name_list'] = node_name_list_str.split()
        if '--job_name' in source_param_list:
            param_dict['job_name'] = source_param_list[source_param_list.index('--job_name') + 1].strip()
        if '--job_run_id' in source_param_list:
            param_dict['job_run_id'] = source_param_list[source_param_list.index('--job_run_id') + 1].strip()
        if '--replace_parameters' in source_param_list:
            param_dict['replace_parameters'] = source_param_list[source_param_list.index('--replace_parameters') + 1].strip()
        return param_dict
        
    def get_batch_date_time_list(self, param_dict):

        logger.info(str(param_dict))
        begin_date_time = None
        end_date_time = None
        begin_date = None
        end_date = None
        batch_date_time_list = []
    
        # 取开始日期时间及结束日期时间
        if ('begin_ymd' in param_dict.keys()) & ('end_ymd' in param_dict.keys()):

            if (len(param_dict['begin_ymd']) > 0) & (len(param_dict['end_ymd']) > 0):
                try:
                    if ('begin_hour' in param_dict.keys()) & ('end_hour' in param_dict.keys()):
                        if param_dict['begin_hour'].isdigit() & param_dict['end_hour'].isdigit():
                            begin_date_time = datetime.datetime.strptime(param_dict['begin_ymd'],'%Y-%m-%d') + datetime.timedelta(hours=int(param_dict['begin_hour']))
                            end_date_time = datetime.datetime.strptime(param_dict['end_ymd'],'%Y-%m-%d') + datetime.timedelta(hours=int(param_dict['end_hour']))
                        else:
                            begin_date = datetime.datetime.strptime(param_dict['begin_ymd'],'%Y-%m-%d')
                            end_date = datetime.datetime.strptime(param_dict['end_ymd'],'%Y-%m-%d')
                    else:
                        begin_date = datetime.datetime.strptime(param_dict['begin_ymd'],'%Y-%m-%d')
                        end_date = datetime.datetime.strptime(param_dict['end_ymd'],'%Y-%m-%d')
                except ValueError as e1:
                    raise ValueError(str(e1))
        elif ('begin_ymd' not in param_dict.keys()) & ('end_ymd' not in param_dict.keys()):
            # get original "Run properties" of the workflow in the further module, bypass here
            pass
        else:
            raise ValueError('begin_ymd, end_ymd are required or empty at the same time.')
            
        # 取批次日期时间列表
        if (begin_date_time is not None) & (end_date_time is not None):
            batch_date_time_list = [begin_date_time + datetime.timedelta(hours=i) for i in range(int((end_date_time - begin_date_time).total_seconds() / 3600 + 1))]
        elif (begin_date is not None) & (end_date is not None):
            batch_date_time_list = [begin_date + datetime.timedelta(days=i) for i in range((end_date - begin_date).days + 1)]
        else:
            batch_date_time_list = [NO_BATCH_DATE_FLAG]
        
        return batch_date_time_list

    def rerun_job(self, client, job_name, job_arguments_dict):
        try:
            # update batch date & job rerun (ingest into "Input arguments" of the job)
            client.start_job_run(
                JobName=job_name,
                Arguments=job_arguments_dict
            )
        except botocore.exceptions.ClientError as e1:
            if e1.response['Error']['Code'] == 'ConcurrentModificationException':
                # 处理上一job status is succeeded, but still in processing in fact
                time.sleep(5)
                self.rerun_job(job_name, job_arguments_dict)
            else:
                raise ValueError(str(e1))
                return
    
    def workflow_waiter(self, client, param_dict):
        break_status = False
        # check running status
        workflow_status = ''
        while (workflow_status not in WORKFLOW_END_STATUS_LIST):
            workflow_resp = client.get_workflow_run(
                Name=param_dict['workflow_name'],
                RunId=param_dict['workflow_run_id'],
                IncludeGraph=True
            )
            logger.info('WorkflowRunProperties: {} \n'.format(str(workflow_resp['Run']['WorkflowRunProperties'])))
            
            workflow_status = workflow_resp['Run']['Status']
            logger.info('Workflow Status: {} \n'.format(workflow_status))
            time.sleep(5)
        # cancel running in case workflow was not completed successfully
        if (workflow_status != WORKFLOW_COMPLETED_STATUS):
            raise ValueError('Workflow {} was not completed successfully.'.format(param_dict['workflow_name']))
            break_status = True
        # # cancel running in case jobs in workflow were not completed successfully
        # for node in workflow_resp['Run']['Graph']['Nodes']:
        #     if (node['Type'] == 'JOB'):
        #         for run_attempt in node['JobDetails']['JobRuns']:
        #             if (run_attempt['JobRunState'] != JOB_SUCCEEDED_STATUS):
        #                 logger.error('Jobs in workflow {} was not completed successfully.'.format(param_dict['workflow_name']))
        #                 break_status = True
        time.sleep(5)
        return break_status
    
    def rerun(self, param_dict, batch_date_time_list):
        
        glue=boto3.client('glue')

        # rerun workflow
        if ('workflow_name' in param_dict.keys()) & ('workflow_run_id' in param_dict.keys()):

            # this logic is to 1) get node id into outlog for verification; 2) get node name id dict for further mapping
            workflow_resp = glue.get_workflow_run(
                Name=param_dict['workflow_name'],
                RunId=param_dict['workflow_run_id'],
                IncludeGraph=True
            )
            node_name_id_dict = {}
            node_id_info_list = []
            for node in workflow_resp['Run']['Graph']['Nodes']:
                node_name_id_dict[node['Name']] = node['UniqueId']
                node_id_info = 'Name:' + node['Name'] + ', node_id:' + node['UniqueId'] + ', type:' + node['Type']
                if (node['Type'] == 'JOB'):
                    if ('JobRuns' in node['JobDetails'].keys()):
                        node_id_info += ', status:' + str([run_attempt['JobRunState'] for run_attempt in node['JobDetails']['JobRuns']])
                node_id_info_list.append(node_id_info)
            logger.info(str(node_id_info_list))

            # get node id from node name
            node_id_list = []
            if ('node_name_list' in param_dict.keys()):
                for node_name in param_dict['node_name_list']:
                    if node_name in node_name_id_dict.keys():
                        node_id_list.append(node_name_id_dict[node_name])
            if (len(node_id_list) == 0):
                raise ValueError('node_name_list is required, cannot find node id from provided node name in the workflow run')
            else:
                # get original "Run properties" of the workflow
                workflow_resp = glue.get_workflow_run(
                    Name=param_dict['workflow_name'],
                    RunId=param_dict['workflow_run_id'],
                    IncludeGraph=True
                )
                workflow_run_properties_dict = {}
                if ('WorkflowRunProperties' in workflow_resp['Run'].keys()):
                    workflow_run_properties_dict = workflow_resp['Run']['WorkflowRunProperties']
                
                # 遍历需重跑的batch_date_time
                for batch_date_time in batch_date_time_list:
                    logger.info('Batch date time:{} \n'.format(str(batch_date_time)))

                    # overwrite batch date
                    if batch_date_time != NO_BATCH_DATE_FLAG:
                        workflow_run_properties_dict['batch_date'] = str(batch_date_time)

                    # replace run properties
                    if ('replace_parameters' in param_dict.keys()):
                        for key, value in json.loads(param_dict['replace_parameters']).items():
                            workflow_run_properties_dict[key] = value
                    logger.info('workflow_run_properties:' + str(workflow_run_properties_dict))
                        
                    # update batch date (ingest into "Run properties" of the workflow)
                    glue.put_workflow_run_properties(
                        Name=param_dict['workflow_name'],
                        RunId=param_dict['workflow_run_id'],
                        RunProperties=workflow_run_properties_dict
                    )
                    # workflow rerun
                    glue.resume_workflow_run(
                        Name=param_dict['workflow_name'],
                        RunId=param_dict['workflow_run_id'],
                        NodeIds=node_id_list
                    )
                    
                    break_status = self.workflow_waiter(glue, param_dict)
                    if break_status:
                        break
                    
        # rerun job
        # workflow_name, workflow_run_id 任一为空，且job_name, job_run_id 都不为空
        elif ('job_name' in param_dict.keys()) & ('job_run_id' in param_dict.keys()): 

            # 遍历需重跑的batch_date_time
            for batch_date_time in batch_date_time_list:
                logger.info('Batch date time:{} \n'.format(str(batch_date_time)))

                # get original "Input arguments" of the job
                job_resp = glue.get_job_run(
                    JobName=param_dict['job_name'],
                    RunId=param_dict['job_run_id']
                )
                job_arguments_dict = {}
                if ('Arguments' in job_resp['JobRun'].keys()):
                    job_arguments_dict = job_resp['JobRun']['Arguments']
                # overwrite batch date
                job_arguments_dict['batch_date'] = str(batch_date_time)
                # replace input arguments
                if ('replace_parameters' in param_dict.keys()):
                    for key, value in json.loads(param_dict['replace_parameters']).items():
                        job_arguments_dict[key] = value
                logger.info('job_arguments_dict:' + str(job_arguments_dict))
                
                self.rerun_job(glue, param_dict['job_name'], job_arguments_dict)
                    
                # check running status
                job_status = ''
                while (job_status not in JOB_END_STATUS_LIST):
                    job_resp = glue.get_job_run(
                        JobName=param_dict['job_name'],
                        RunId=param_dict['job_run_id']
                    )
                    job_status = job_resp['JobRun']['JobRunState']
                    logger.info('Job Status: {} \n'.format(job_status))
                    time.sleep(5)
                # # cancel running in case job was not completed successfully
                # if (job_status != JOB_SUCCEEDED_STATUS):
                #     logger.error('Job {} was not completed successfully.'.format(param_dict['job_name']))
                #     break
        
        else:
            raise ValueError('For workflow rerun, workflow_name and workflow_run_id are required, for job rerun, job_name and job_run_id are required.')
        
    def process(self):
        glueReRunWorkFlow = GlueReRunWorkFlow()
        param_dict = glueReRunWorkFlow.parse_params()
        batch_date_time_list = glueReRunWorkFlow.get_batch_date_time_list(param_dict)
        if (len(batch_date_time_list) > 0):
            glueReRunWorkFlow.rerun(param_dict, batch_date_time_list)


## Start processing
GlueReRunWorkFlow().process()
