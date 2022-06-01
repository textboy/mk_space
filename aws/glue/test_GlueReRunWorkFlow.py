import boto3

## unit test with different parameters
client=boto3.client('glue')

# # test 1: rerun multi-workflows by hour batch (bypass checking "cancel running in case jobs in workflow were not completed successfully")
# # test 2: rerun workflow by hour batch (include the checking)
# # Expect: rerun multi-workflows/workflow by hour batch
# job_arguments_dict = {
#     '--begin_ymd': '2022-05-21',
#     '--end_ymd': '2022-05-21',
#     '--begin_hour': '13',
#     '--end_hour': '23',
#     '--workflow_name': 'tFlow3',
#     '--workflow_run_id': 'wr_63e3e6b55eea6eac3c9fca85fae44d1d787b78ee580cc308ba68c5ce0b7b55c9',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b',
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 3: rerun workflow by day batch (end_hour is empty)
# # Expect: rerun workflow by day batch
# job_arguments_dict = {
#     '--begin_ymd': '2022-05-21',
#     '--end_ymd': '2022-05-22',
#     '--begin_hour': '13',
#     '--end_hour': '',
#     '--workflow_name': 'tFlow3',
#     '--workflow_run_id': 'wr_c2ea9036fec48c649e1d57988d61ca4aeccc8abb180402d15c3318264472b3e7',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b wnode_d8d428695efb2b5bec5d58b8b7f4e2d84f7c53dd8684e86b02654594130932e8',
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 4: rerun job (workflow_name is empty)
# # Expect: rerun job by hour batch
# job_arguments_dict = {
#     '--begin_ymd': '2022-05-21',
#     '--end_ymd': '2022-05-21',
#     '--begin_hour': '13',
#     '--end_hour': '15',
#     '--workflow_name': '',
#     '--workflow_run_id': 'wr_c2ea9036fec48c649e1d57988d61ca4aeccc8abb180402d15c3318264472b3e7',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b wnode_d8d428695efb2b5bec5d58b8b7f4e2d84f7c53dd8684e86b02654594130932e8',
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 5: exceptional case (begin_ymd is empty)
# # Expect: error in output log
# job_arguments_dict = {
#     '--begin_ymd': '',
#     '--end_ymd': '2022-05-21',
#     '--begin_hour': '13',
#     '--end_hour': '15',
#     '--workflow_name': '',
#     '--workflow_run_id': 'wr_63e3e6b55eea6eac3c9fca85fae44d1d787b78ee580cc308ba68c5ce0b7b55c9',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b',
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 6: exceptional case (workflow_name, job_name are empty)
# # Expect: error in output log
# job_arguments_dict = {
#     '--begin_ymd': '2022-05-21',
#     '--end_ymd': '2022-05-21',
#     '--begin_hour': '13',
#     '--end_hour': '15',
#     '--workflow_name': '',
#     '--workflow_run_id': 'wr_63e3e6b55eea6eac3c9fca85fae44d1d787b78ee580cc308ba68c5ce0b7b55c9',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b',
#     '--job_name': '',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 7: run workflow with empty ymd
# # Expect: run workflow
# job_arguments_dict = {
#     '--workflow_name': 'tFlow3',
#     '--workflow_run_id': 'wr_63e3e6b55eea6eac3c9fca85fae44d1d787b78ee580cc308ba68c5ce0b7b55c9',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b'
# }

# # test 8: run job with empty ymd
# # Expect: run job
# job_arguments_dict = {
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544'
# }

# # test 9: run workflow with succeed node
# # Expect: run workflow with succeed node
# job_arguments_dict = {
#     '--workflow_name': 'tFlow5',
#     '--workflow_run_id': 'wr_c75eaadd83f0eea6cceccdc0148075a6b8773ab7f5edf29636ed119553b33e66',
#     '--node_id_list': 'wnode_1192c76ac509b6406647b9ae96b4da6efdb3b87889ce8fc1c2759a25d9f12d59'
# }

# # test 10: run workflow with replaced properties
# # Expect: run workflow with replaced properties
# job_arguments_dict = {
#     '--workflow_name': 'tFlow5',
#     '--workflow_run_id': 'wr_dc0f10a0de74a358e33e9746d84c1e0b0d9f4020833b5db4b25f4b001c1cee63',
#     '--node_id_list': 'wnode_1192c76ac509b6406647b9ae96b4da6efdb3b87889ce8fc1c2759a25d9f12d59',
#     '--replace_parameters': '{"table_name":"test_table_1", "start_dt":"1999-01-23"}'
# }

# # test 11: run job with replaced parameters
# # Expect: run job with replaced parameters
# job_arguments_dict = {
#     '--job_name': 'wf_test_1',
#     '--job_run_id': 'jr_9a2b0fa7821475c62f6ca6fd7661f644a89d8dddf6b3922637bc88e49735a544',
#     '--replace_parameters': '{"table_name":"test_table_1", "start_dt":"1999-01-23"}'
# }

# # test 12: run workflow with the job before an ALL trigger (another job is FAILED before the ALL trigger)
# # Expect: ALL trigger 前没跑的job会记着之前的run state
# job_arguments_dict = {
#     '--workflow_name': 'tFlow5',
#     '--workflow_run_id': 'wr_6356dac90ea921f3b52078d4eed366b16b6f2da40c2dce58b1dfac094ed89b55',
#     '--node_id_list': 'wnode_1192c76ac509b6406647b9ae96b4da6efdb3b87889ce8fc1c2759a25d9f12d59'
# }

# # test 13: exceptional case: run workflow starting from trigger node
# # Expect: API error - xxx is a trigger and cannot be resumed
# job_arguments_dict = {
#     '--workflow_name': 'tFlow3',
#     '--workflow_run_id': 'wr_63e3e6b55eea6eac3c9fca85fae44d1d787b78ee580cc308ba68c5ce0b7b55c9',
#     '--node_id_list': 'wnode_762fbeff06a252e8dc82c68351e82fa21ad79ebbb99fda26310d1cd531c9fe8b'
# }

# test 14: exceptional case: run node of workflow without run attempt
# Expect: API error - xxx does not have a run attempt
job_arguments_dict = {
    '--workflow_name': 'tFlow5',
    '--workflow_run_id': 'wr_6356dac90ea921f3b52078d4eed366b16b6f2da40c2dce58b1dfac094ed89b55',
    '--node_name_list': 'pyspark_test02 pyTest01'
}

client.start_job_run(
    JobName='GlueReRunWorkFlow',
    Arguments=job_arguments_dict
)
