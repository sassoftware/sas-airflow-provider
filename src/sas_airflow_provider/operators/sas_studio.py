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

import os
import time

from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from sas_airflow_provider.hooks.sas import SasHook
from sas_airflow_provider.util.util import dump_logs, create_or_connect_to_session

# main API URI for Code Gen
URI_BASE = "/studioDevelopment/code"
# default context name
DEFAULT_COMPUTE_CONTEXT_NAME = "SAS Studio compute context"
# when creating a session it will be given this name
AIRFLOW_SESSION_NAME = "Airflow-Session"

JES_URI = "/jobExecution"
JOB_URI = f"{JES_URI}/jobs"


class SASStudioOperator(BaseOperator):
    """
    Executes a SAS Studio flow or a SAS program

    :param path_type: a type that indicates what the path parameter represents. valid values:
        content - the path parameter represents a path in SAS Content. For example /Public/myflow.flw
        compute - the path parameter represents a server file-system path that is accessible from a SAS session.
        raw - the path parameter itself is a string of SAS code, or JSON representation of a flow.
    :param path: path to the flow/program to execute, or actual flow/code. see above
    :param exec_log: boolean. Indicates whether to dump the execution log to the Airflow log
    :param exec_type: (optional) "flow" or "program" By default this operator will execute a Studio Flow. If you specify
        "program" then it will execute a program. ie. your path would either specify a path to the program
        like /Public/mycode.sas or would be the actual program itself
    :param codegen_init_code: (optional) boolean. Whether to generate init code
        (default value: False)
    :param codegen_wrap_code: (optional) boolean. Whether to generate wrapper code
        (default value: False)
    :param connection_name: (optional) name of the connection to use. The connection should be defined
        as an HTTP connection in Airflow. If not specified, the default is used (sas_default)
    :param compute_context: (optional) Name of the Compute context to use. If not provided, a
        suitable default is used (see DEFAULT_COMPUTE_CONTEXT NAME).
    :param env_vars: (optional) Dictionary of environment variables to set before running the flow.
    :param macro_vars: (optional) Dictionary of macro variables to set before running the flow.
    :param compute_session_id: (optional) Compute session id to use. If not specified, one will be created using the
        default session name (see AIRFLOW_SESSION_NAME). Note that the name and the id are not the same. The name
        will always be the value of AIRFLOW_SESSION_NAME, which means that if you don't supply a session id, then
        this named session will be created or re-used. The advantage is that the same session can be re-used between
        tasks. The disadvantage is that it offers less flexibility in terms of having multiple sessions.
    :param output_macro_var_prefix: (optional) string. If this has a value, then any macro variables which start
        with this prefix will be retrieved from the session after the code has executed and will be returned as XComs
    """

    ui_color = "#CCE5FF"
    ui_fgcolor = "black"

    template_fields: Sequence[str] = ("env_vars", "macro_vars", "compute_session_id", "path")

    def __init__(
            self,
            path_type: str,
            path: str,
            exec_log: bool,
            exec_type="flow",
            codegen_init_code=False,
            codegen_wrap_code=False,
            connection_name=None,
            compute_context=DEFAULT_COMPUTE_CONTEXT_NAME,
            env_vars=None,
            macro_vars=None,
            compute_session_id="",
            output_macro_var_prefix="",
            **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        if env_vars is None:
            env_vars = {}
        self.path_type = path_type
        self.exec_type = exec_type
        self.path = path
        self.exec_log = exec_log
        self.codegen_init_code = codegen_init_code
        self.codegen_wrap_code = codegen_wrap_code
        self.connection_name = connection_name
        self.compute_context_name = compute_context
        self.env_vars = env_vars
        self.macro_vars = macro_vars
        self.connection = None
        self.compute_session_id = compute_session_id
        self.output_macro_var_prefix = output_macro_var_prefix.upper()

    def execute(self, context):
        if self.path_type not in ['compute', 'content', 'raw']:
            raise AirflowFailException("Path type is invalid. Valid values are 'compute', 'content' or 'raw'")
        if self.exec_type not in ['flow', 'program']:
            raise AirflowFailException("Execution type is invalid. Valid values are 'flow' and 'program'")

        self._add_airflow_env_vars()

        try:
            self.log.info("Authenticate connection")
            h = SasHook(self.connection_name)
            self.connection = h.get_conn()

            if self.path_type == "raw":
                code = self.path
            else:
                self.log.info("Generate code for Studio object: %s", str(self.path))
                res = self._generate_object_code()
                code = res["code"]

            # add code for macros and env vars
            final_code = self._get_pre_code() + code

            # Create the job request for JES
            jr = {
                "name": f"Airflow_{self.task_id}",
                "jobDefinition": {"type": "Compute", "code": final_code}
            }

            # if we have a session id, we will use that, otherwise we'll use the context name
            if self.compute_session_id:
                jr['arguments'] = {"_sessionId": self.compute_session_id}
            else:
                jr['arguments'] = {"_contextName": self.compute_context_name}

            # Kick off the JES job
            job, success = self._run_job_and_wait(jr, 1)
            job_state = job["state"]

            # display logs if needed
            if self.exec_log is True:
                dump_logs(self.connection, job)

            # set output variables
            if success and self.output_macro_var_prefix:
                self._set_output_variables(context)

        # support retry if API-calls fails for whatever reason
        except Exception as e:
            raise AirflowException(f"SASStudioOperator error: {str(e)}")

        # raise exception in Airflow if SAS Studio Flow ended execution with "failed" "canceled" or "timed out" state
        # support retry for 'failed' (typically there is an ERROR in the log) and 'timed out'
        # do NOT support retry for 'canceled' (typically the SAS Job called ABORT ABEND)
        if job_state == "failed":
            raise AirflowException("SAS Studio Execution completed with an error.")

        elif job_state == "canceled":
            raise AirflowFailException("SAS Studio Execution was cancelled or aborted. See log for details ")

        elif job_state == "timed out":
            raise AirflowException("SAS Studio Execution has timed out. See log for details ")

        return 1

    def _add_airflow_env_vars(self):
        for x in ['AIRFLOW_CTX_DAG_OWNER',
                  'AIRFLOW_CTX_DAG_ID',
                  'AIRFLOW_CTX_TASK_ID',
                  'AIRFLOW_CTX_EXECUTION_DATE',
                  'AIRFLOW_CTX_TRY_NUMBER',
                  'AIRFLOW_CTX_DAG_RUN_ID', ]:
            v = os.getenv(x)
            if v:
                self.env_vars[x] = v

    def _get_pre_code(self):

        pre_code = ""
        if self.env_vars:
            self.log.info(f"Adding {len(self.env_vars)} environment variables to code")
            pre_code += "/** Begin environment variables **/\n"
            for k, v in self.env_vars.items():
                pre_code += f"OPTIONS SET={k}='{v}';\n"
            pre_code += "/** End environment variables **/\n\n"
        if self.macro_vars:
            self.log.info(f"Adding {len(self.macro_vars)} macro variables to code")
            pre_code += "/** Begin macro variables **/\n"
            for k, v in self.macro_vars.items():
                pre_code += f"%LET {k} = {v};\n"
            pre_code += "/** End macro variables **/\n\n"
        return pre_code

    def _generate_object_code(self):

        uri = URI_BASE

        if self.path_type == "compute":
            self.log.info("Code Generation for Studio object stored in Compute file system")

            # if session id is provided, use it, otherwise create a session
            if not self.compute_session_id:
                self.log.info("Create or connect to session")
                compute_session = create_or_connect_to_session(self.connection,
                                                               self.compute_context_name, AIRFLOW_SESSION_NAME)
                self.compute_session_id = compute_session["id"]
            else:
                self.log.info("Session ID was provided")

            uri = f"{URI_BASE}?sessionId={self.compute_session_id}"
        else:
            self.log.info("Code generation for Studio object stored in Content")

        media_type = "application/vnd.sas.dataflow"
        if self.exec_type == "program":
            media_type = "application/vnd.sas.program"
        req = {
            "reference": {
                "mediaType": media_type,
                "type": self.path_type,
                "path": self.path},
            "initCode": self.codegen_init_code,
            "wrapperCode": self.codegen_wrap_code,
        }

        response = self.connection.post(uri, json=req)
        if not response.ok:
            raise RuntimeError(f"Code generation failed: {response.text}")

        return response.json()

    def _run_job_and_wait(self, job_request: dict, poll_interval: int) -> (dict, bool):
        uri = JOB_URI
        response = self.connection.post(uri, json=job_request)
        # change to process non-standard codes returned from API (201, 400, 415)
        # i.e. situation when we were not able to make API call at all
        if response.status_code != 201:
            raise RuntimeError(f"Failed to create job request: {response.text}")

        job = response.json()
        job_id = job["id"]
        state = job["state"]
        self.log.info(f"Submitted job request with id {job_id}. Waiting for completion")
        uri = f"{JOB_URI}/{job_id}"
        while state in ["pending", "running"]:
            time.sleep(poll_interval)
            response = self.connection.get(uri)
            if not response.ok:
                raise RuntimeError(f"Failed to get job: {response.text}")
            job = response.json()
            state = job["state"]
        self.log.info("Job request has completed execution with the status: " + str(state))
        success = True
        if state in ['failed', 'canceled', 'timed out']:
            success = False
            if 'error' in job:
                self.log.error(job['error'])

        return job, success

    def _set_output_variables(self, context):
        # set Airflow variables from compute session variables

        # retrieve variables from compute session
        uri = f"/compute/sessions/{self.compute_session_id}/variables?limit=999&filter=startsWith(name,'{self.output_macro_var_prefix}')"
        response = self.connection.get(uri, headers={'Accept': '*/*'})
        if not response.ok:
            raise RuntimeError(f"get compute variables failed with {response.status_code}")
        v = response.json()["items"]

        # set Airflow variables
        for var in v:
            self.log.info(f"found output variable {var['name']}")
            self.xcom_push(context, var['name'], var['value'])
