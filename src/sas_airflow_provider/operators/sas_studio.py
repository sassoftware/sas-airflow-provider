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
from sas_airflow_provider.util.util import stream_log, create_or_connect_to_session, end_compute_session

# main API URI for Code Gen
URI_BASE = "/studioDevelopment/code"
# default context name
DEFAULT_COMPUTE_CONTEXT_NAME = "SAS Studio compute context"
# when creating a session it will be given this name
AIRFLOW_SESSION_NAME = "Airflow-Session"

JES_URI = "/jobExecution"
JOB_URI = f"{JES_URI}/jobs"

def on_success(context):
    # Only kill session when not reused or external managed
    context['task']._clean_up(also_kill_reused_session=False)

def on_failure(context):
    # Kill all sessions except external managed
    context['task']._clean_up(also_kill_reused_session=True)

def on_retry(context):
    # Kill all sessions except external managed
    context['task']._clean_up(also_kill_reused_session=True)


       
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
    :param allways_reuse_session: (optional) Specify true to always reuse the same Compute Session across all tasks. The name
        of the session will be the default session name (see AIRFLOW_SESSION_NAME), which means that if you don't supply a session id in compute_session_id,
        then this named session will be created and later re-used between tasks. The disadvantage is that it offers less flexibility in terms of 
        having multiple sessions (parallelisme). Default value is False meaning a new unnamed compute sessions will always be created 
        UNLESS a session id is specified in compute_session_id.
    :param compute_session_id: (optional) Compute Session id to use for the task. If a Session Id is specified, this will overide allways_reuse_session.
        Use SASComputeCreateSession Operator to define a task that will create the session. This gives full flexibility in how compue session are used. 
        The id of the session created by SASComputeCreateSession will be made avaliable as XCom variable 'compute_session_id' 
        for subsequent use by SASStudio Operator tasks. Tip: set the value to "{{ ti.xcom_pull(key='compute_session_id', task_ids=['<task_id>'])|first}}" to get the X-Com value.
    :param output_macro_var_prefix: (optional) string. If this has a value, then any macro variables which start
        with this prefix will be retrieved from the session after the code has executed and will be returned as XComs
    :param unknown_state_timeout: (optional) number of seconds to continue polling for the state of a running job if the state is 
        temporary unobtainable. When unknown_state_timeout is reached without the state being retrievable, the operator 
        will throw an AirflowFailException and the task will be marked as failed. 
        Default value is 0, meaning the task will fail immediately if the state could not be retrieved.
    :param http_timeout: (optional) Timeout for https requests. Default value is (30.05, 300), meaning a connect timeout sligthly above 30 seoconds and 
        a read timeout of 300 seconds where the operator will wait for the server to send a response.
    :param job_name_prefix: (optional) string. Specify a name that you want the compute session to identify as in SAS Workload Orchestrator (SWO). 
        If job_name_prefix is not specified the default prefix is determined by Viya (currently 'sas-compute-server-'). 
        If the value cannot be parsed by Viya to create a valid k8s pod name, the default value will be used as well.
        job_name_prefix is supported from Viya Stable 2024.07
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
            allways_reuse_session=False,
            compute_session_id="",
            output_macro_var_prefix="",
            unknown_state_timeout=0,
            job_name_prefix=None,
            http_timeout=(30.05, 300),
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
        self.allways_reuse_session = allways_reuse_session
        self.job_name_prefix = job_name_prefix

        self.external_managed_session = False
        self.compute_session_id = None
        if compute_session_id:
            self.compute_session_id = compute_session_id
            self.external_managed_session=True

        self.output_macro_var_prefix = output_macro_var_prefix.upper()
        self.unknown_state_timeout=max(unknown_state_timeout,0)

        # Use hooks to clean up
        self.on_success_callback=[on_success]
        
        if self.on_failure_callback == None:
           self.on_failure_callback=[on_failure]
        else:
            self.on_failure_callback=[on_failure, self.on_failure_callback]
        
        self.on_retry_callback=[on_retry]

        # Timeout
        self.http_timeout=http_timeout

        
    def execute(self, context):
        if self.path_type not in ['compute', 'content', 'raw']:
            raise AirflowFailException("Path type is invalid. Valid values are 'compute', 'content' or 'raw'")
        if self.exec_type not in ['flow', 'program']:
            raise AirflowFailException("Execution type is invalid. Valid values are 'flow' and 'program'")

        self._add_airflow_env_vars()

        try:
            self.log.info("Authenticate connection")
            h = SasHook(self.connection_name)
            self.connection = h.get_conn(http_timeout=self.http_timeout)

            # Create compute session
            if not self.compute_session_id:
                compute_session = create_or_connect_to_session(self.connection,
                                                            self.compute_context_name, 
                                                            AIRFLOW_SESSION_NAME if self.allways_reuse_session else None, 
                                                            http_timeout=self.http_timeout,
                                                            job_name_prefix=self.job_name_prefix
                                                            )
                self.compute_session_id = compute_session["id"]
            else:
                self.log.info(f"Compute Session {self.compute_session_id} was provided")

            # Generate SAS code
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
                # the jobExecution service will destroy the compute session if it was not passed in.
                if self.output_macro_var_prefix:
                    self.log.info("Output macro variables will not be available. To make them available please "
                                  "specify a compute session")

        # Support retry if API-calls fails for whatever reason as no harm is done
        except Exception as e:
            raise AirflowException(f"SASStudioOperator error: {str(e)}")

        # Kick off the JES job, wait to get the state
        # _run_job_and_wait will poll for new 
        # SAS log-lines and stream them in the DAG'-log
        job, success = self._run_job_and_wait(jr, 10)
        job_state= "unknown"
        if "state" in job:
            job_state = job["state"]
     
        # set output variables
        if success and self.output_macro_var_prefix and self.compute_session_id:
            try:
                self._set_output_variables(context)
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

        elif job_state == "timedOut":
            raise AirflowException("SAS Studio Execution has timed out. See log for details ")
      
        return 1

    def on_kill(self) -> None:
        self._clean_up(also_kill_reused_session=True)

    def _clean_up(self, also_kill_reused_session=False):
        # Always kill unnamed sessions (allways_reuse_session is false)
        # however is also_kill_reused_session is specified also kill the reuse session
        # newer kill external managed sessions, as this may prevent restart
        if self.compute_session_id and self.external_managed_session==False: 
            if (also_kill_reused_session and self.allways_reuse_session) or self.allways_reuse_session==False:
                try:
                    self.log.info(f"Deleting session with id {self.compute_session_id}")
                    success_end = end_compute_session(self.connection, self.compute_session_id, http_timeout=self.http_timeout)
                    if success_end:
                        self.log.info(f"Compute session succesfully deleted")
                    else:
                        self.log.info(f"Unable to delete compute session. You may need to kill the session manually")    
                    self.compute_session_id=None

                except Exception as e:
                    self.log.info(f"Unable to delete compute session. You may need to kill the session manually")
                    self.compute_session_id=None

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
        uri=URI_BASE

        if self.path_type == "compute":
            uri = f"{URI_BASE}?sessionId={self.compute_session_id}"
            self.log.info("Code Generation for Studio object stored in Compute file system")
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

        response = self.connection.post(uri, json=req, timeout=self.http_timeout)
        if not response.ok:
            raise RuntimeError(f"Code generation failed: {response.text}")

        return response.json()

    def _run_job_and_wait(self, job_request: dict, poll_interval: int) -> (dict, bool):
        uri = JOB_URI

        #Kick off job request. if failures, no harm is done.
        try:
            response = self.connection.post(uri, json=job_request, timeout=self.http_timeout)
        except Exception as e:
            raise AirflowException(f"Error when creating Job Request {e}") 

        # change to process non-standard codes returned from API (201, 400, 415)
        # i.e. situation when we were not able to make API call at all
        if response.status_code != 201:
            raise AirflowException(f"Failed to create job request. Repsonse status code was: {response.status_code}")
        
        # Job started succesfully, start waiting for the job to finish
        job = response.json()
        job_id = job["id"]
        self.log.info(f"Submitted job request with id {job_id}. Waiting for completion")
        uri = f"{JOB_URI}/{job_id}"

        # Poll for state of the job 
        # If ANY error occours set state to 'unknown', print the reason to the log, and continue polling until self.unknown_state_timeout
        state = "unknown"
        countUnknownState = 0
        log_location = None
        num_log_lines= 0
        while state in ["pending", "running"] or (state == "unknown" and ((countUnknownState*poll_interval) <= self.unknown_state_timeout)):
            time.sleep(poll_interval)

            try:
                response = self.connection.get(uri, timeout=self.http_timeout)
                if not response.ok:
                    countUnknownState = countUnknownState + 1
                    self.log.info(f'Invalid response code {response.status_code} from {uri}. Will set state=unknown and continue checking...')
                    state = "unknown"
                else:
                    countUnknownState = 0
                    job = response.json()
                    if "state" in job:
                        state = job["state"]
                    else:
                        self.log.info(f'Not able to determine state from {uri}. Will set state=unknown and continue checking...')
                        state = "unknown"

                    # Get the latest new log lines.
                    if self.exec_log and state != "unknown":
                        num_log_lines=stream_log(self.connection, job, num_log_lines, http_timeout=self.http_timeout)
                            
            except Exception as e:
                countUnknownState = countUnknownState + 1
                self.log.info(f'HTTP Call failed with error "{e}". Will set state=unknown and continue checking...')
                state = "unknown"
                
        if state == 'unknown':
            # Raise AirflowFailException as we don't know if the job is still running
            raise AirflowFailException(f'Unable to retrieve state of job after trying {countUnknownState} times. Will mark task as failed. Please check the SAS-log.')

        # Be sure to Get the latest new log lines after the job have finished.
        if self.exec_log:
            num_log_lines=stream_log(self.connection, job, num_log_lines, http_timeout=self.http_timeout)

        self.log.info("Job request has completed execution with the status: " + str(state))
        success = True
        if state in ['failed', 'canceled', 'timed out', 'timedOut']:
            success = False
            if 'error' in job:
                self.log.error(job['error'])

        return job, success

    def _set_output_variables(self, context):
        # set Airflow variables from compute session variables

        # retrieve variables from compute session
        uri = f"/compute/sessions/{self.compute_session_id}/variables?limit=999&filter=startsWith(name,'{self.output_macro_var_prefix}')"
        response = self.connection.get(uri, headers={'Accept': '*/*'}, timeout=self.http_timeout)
        if not response.ok:
            raise RuntimeError(f"get compute variables failed with {response.status_code}")
        v = response.json()["items"]

        # set Airflow variables
        for var in v:
            self.log.info(f"found output variable {var['name']}")
            self.xcom_push(context, var['name'], var['value'])
