# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from sas_airflow_provider.operators.sas_jobexecution import SASJobExecutionOperator
from unittest.mock import patch


def mock_ret_headers(hdr):
    if hdr == 'X-Sas-Jobexec-Error':
        return None
    elif hdr == 'X-Sas-Jobexec-Id':
        return 1

class TestSasJobExecutionOperator:
    """
    Test class for SASJobExecutionOperator
    """

    @patch("sas_airflow_provider.operators.sas_jobexecution.dump_logs")
    @patch("sas_airflow_provider.operators.sas_jobexecution.SasHook")
    def test_execute_sas_job_execution_operator(self, session_mock, dump_logs_mock):
        """
        Test basic operation
        """
        session_mock.return_value.get_conn.return_value.post.return_value.status_code = 200
        session_mock.return_value.get_conn.return_value.get.return_value.status_code = 200
        session_mock.return_value.get_conn.return_value.post.return_value.headers.get=mock_ret_headers

        operator = SASJobExecutionOperator(task_id='test',
                                           connection_name="SAS", job_name='/Public/my_job',
                                           parameters={'a': 'b'},
                                           job_exec_log=True
                                           )

        operator.execute(context={})
        session_mock.assert_called_with('SAS')
        dump_logs_mock.assert_called()
        session_mock.return_value.get_conn.return_value.post.assert_called_with('/SASJobExecution/?_program=/Public/my_job&a=b',
                                                          headers={
                                                              'Accept': 'application/vnd.sas.job.execution.job+json'},
                                                          verify=False)
