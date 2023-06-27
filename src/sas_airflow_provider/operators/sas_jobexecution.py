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


from __future__ import annotations

import urllib.parse

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from sas_airflow_provider.hooks.sas import SasHook
from sas_airflow_provider.util.util import dump_logs

class SASJobExecutionOperator(BaseOperator):
    """
    Executes a SAS Job using /SASJobExecution endpoint. Job execution is documented here:
    https://go.documentation.sas.com/doc/en/pgmsascdc/default/jobexecug/p1ct9uzl5c7omun1t2zy0gxhlqlc.htm
    The specific endpoint /SASJobExecution is documented here:
    https://go.documentation.sas.com/doc/en/pgmsascdc/default/jobexecug/n06tcybrt9wdeun1ko9bkjn0ko0b.htm

    :param connection_name: Name of the SAS Viya connection stored as an Airflow HTTP connection
    :param job_name: Name of the SAS Job to be run
    :param parameters Dictionary of all the parameters that should be passed to the
        SAS Job as SAS Macro variables
    :param job_exec_log: boolean. whether or not to dump out the log
    """

    template_fields: Sequence[str] = ("parameters",)

    def __init__(self,
                 job_name: str,
                 parameters: dict,
                 connection_name: str = None,
                 job_exec_log: bool = False,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.connection_name = connection_name
        self.job_name = job_name
        self.parameters = parameters
        self.job_exec_log = job_exec_log

    def execute(self, context):
        h = SasHook(self.connection_name)
        session = h.get_conn()

        print(f"Executing SAS job: {self.job_name}")
        # url escape the program name
        program_name = urllib.parse.quote(self.job_name)
        url_string = ""
        for key, value in self.parameters.items():
            url_string += f"&{key}={urllib.parse.quote(value)}"

        url = f"/SASJobExecution/?_program={program_name}{url_string}"

        headers = {"Accept": "application/vnd.sas.job.execution.job+json"}
        response = session.post(url, headers=headers, verify=False)

        if response.status_code < 200 or response.status_code >= 300:
            raise AirflowFailException(f"SAS Job Execution HTTP status code {response.status_code}")

        error_code = response.headers.get('X-Sas-Jobexec-Error')
        if error_code:
            print(response.text)
            raise AirflowFailException(f"SAS Job Execution failed with code {error_code}")

        if self.job_exec_log:
            job_id = response.headers.get('X-Sas-Jobexec-Id')
            if job_id:
                job_status_url = f"/jobExecution/jobs/{job_id}"
                job = session.get(job_status_url, verify=False)
                if job.status_code >= 200:
                    dump_logs(session, job.json())
                else:
                    print(f"Failed to get job status for logs. /jobExecution/jobs returned {job.status_code}")
            else:
                print("Failed to get job id for logs. X-Sas-Jobexec-Id not found in response headers")

        return 1
