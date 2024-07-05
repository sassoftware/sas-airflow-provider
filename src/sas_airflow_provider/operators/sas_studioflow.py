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

import copy
import json
import time
import warnings

import requests

from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from sas_airflow_provider.hooks.sas import SasHook
from sas_airflow_provider.util.util import dump_logs


class SASStudioFlowOperator(BaseOperator):
    """
    Executes a SAS Studio flow.
    Note that this operator is deprecated. Please use SASStudioOperator instead

    :param flow_path_type: valid values are content or compute
    :param flow_path: path to the flow to execute. eg /Public/myflow.flw
    :param flow_exec_log: whether or not to output the execution log
    :param flow_codegen_init_code: Whether or not to generate init code
        (default value: False)
    :param flow_codegen_wrap_code: Whether or not to generate wrapper code
        (default value: False)
    :param connection_name: name of the connection to use. The connection should be defined
        as an HTTP connection in Airflow.
    :param compute_context: (optional) Name of the compute context to use. If not provided, a
        suitable default is used.
    :param env_vars: (optional) Dictionary of environment variables to set before running the flow.
    """

    ui_color = "#CCE5FF"
    ui_fgcolor = "black"

    template_fields: Sequence[str] = ("env_vars",)

    def __init__(
            self,
            flow_path_type: str,
            flow_path: str,
            flow_exec_log: bool,
            flow_codegen_init_code=False,
            flow_codegen_wrap_code=False,
            connection_name=None,
            compute_context="SAS Studio compute context",
            env_vars=None,
            **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        warnings.warn("SASStudioFlowOperator is deprecated. Please use SASStudioOperator instead.")
        if env_vars is None:
            env_vars = {}
        self.flow_path_type = flow_path_type
        self.flow_path = flow_path
        self.flow_exec_log = flow_exec_log
        self.flow_codegen_initCode = flow_codegen_init_code
        self.flow_codegen_wrapCode = flow_codegen_wrap_code
        self.connection_name = connection_name
        self.compute_context = compute_context
        self.env_vars = env_vars

    def execute(self, context):
        try:
            self.log.info("Authenticate connection")
            h = SasHook(self.connection_name)
            session = h.get_conn()

            self.log.info("Generate code for Studio Flow: %s", str(self.flow_path))
            code = _generate_flow_code(
                session,
                self.flow_path_type,
                self.flow_path,
                self.flow_codegen_initCode,
                self.flow_codegen_wrapCode,
                None,
                self.compute_context,
            )

            if self.env_vars:
                # Add environment variables to pre-code
                self.log.info(f"Adding {len(self.env_vars)} environment variables to code")
                pre_env_code = "/** Setting up environment variables **/\n"
                for env_var in self.env_vars:
                    env_val = self.env_vars[env_var]
                    pre_env_code = pre_env_code + f"options set={env_var}='{env_val}';\n"
                pre_env_code = pre_env_code + "/** Finished setting up environment variables **/\n\n"
                code["code"] = pre_env_code + code["code"]

            # Create the job request for JES
            jr = {
                "name": f"Airflow_{self.task_id}",
                "jobDefinition": {"type": "Compute", "code": code["code"]},
                "arguments": {"_contextName": self.compute_context},
            }

            # Kick off the JES job
            job = _run_job_and_wait(session, jr, 1)
            job_state = job["state"]

        # support retry if API-calls fails for whatever reason
        except Exception as e:
            raise AirflowException(f"SASStudioFlowOperator error: {str(e)}")

        # display logs if needed
        if self.flow_exec_log is True:
            # Safeguard if we are unable to retreive the log. We will NOT throw any exceptions
            try:
                dump_logs(session, job)
            except Exception as e:
                self.log.info("Unable to retrieve log. Maybe the log is too large.")

        # raise exception in Airflow if SAS Studio Flow ended execution with "failed" "canceled" or "timed out" state
        # support retry for 'failed' (typically there is an ERROR in the log) and 'timed out'
        # do NOT support retry for 'canceled' (typically the SAS Job called ABORT ABEND)
        if job_state == "failed":
            raise AirflowException("SAS Studio Flow Execution completed with an error.")

        if job_state == "canceled":
            raise AirflowFailException("SAS Studio Flow Execution was cancelled or aborted. See log for details ")

        if job_state == "timed out":
            raise AirflowException("SAS Studio Flow Execution has timed out. See log for details ")

        if job_state == "timedOut":
            raise AirflowException("SAS Studio Flow Execution has timed out. See log for details ")

        return 1


def _generate_flow_code(
        session,
        artifact_type: str,
        path: str,
        init_code: bool,
        wrap_code: bool,
        session_id=None,
        compute_context="SAS Studio compute context",
):
    # main API URI for Code Gen
    uri_base = "/studioDevelopment/code"

    # if type == compute then Compute session should be created
    if artifact_type == "compute":
        print("Code Generation for Studio Flow with Compute session")

        # if session id is provided
        if session_id is not None:

            print("Session ID was provided")
            uri = f"{uri_base}?sessionId={session_id}"
        else:
            print("Create or connect to session")
            compute_session = _create_or_connect_to_session(session, compute_context, "Airflow-Session")
            uri = f'{uri_base}?sessionId={compute_session["id"]}'

        req = {
            "reference": {"mediaType": "application/vnd.sas.dataflow", "type": artifact_type, "path": path},
            "initCode": init_code,
            "wrapperCode": wrap_code,
        }

        response = session.post(uri, json=req)

        if response.status_code != 200:
            raise RuntimeError(f"Code generation failed: {response.text}")

        return response.json()

    # if type == content then Compute session is not needed
    elif artifact_type == "content":
        print("Code Generation for Studio Flow without Compute session")

        req = {
            "reference": {"mediaType": "application/vnd.sas.dataflow", "type": artifact_type, "path": path},
            "initCode": init_code,
            "wrapperCode": wrap_code,
        }

        uri = uri_base
        response = session.post(uri, json=req)

        if response.status_code != 200:
            raise RuntimeError(f"Code generation failed: {response.text}")

        return response.json()

    else:
        raise RuntimeError("invalid artifact_type was supplied")


def _create_or_connect_to_session(session: requests.Session, context_name: str, name: str) -> dict:
    # find session with given name
    response = session.get(f"/compute/sessions?filter=eq(name, {name})")
    if response.status_code != 200:
        raise RuntimeError(f"Find sessions failed: {response.text}")
    sessions = response.json()
    if sessions["count"] > 0:
        return sessions["items"][0]

    print(f"Compute session named '{name}' does not exist, a new one will be created")
    # find compute context
    response = session.get("/compute/contexts", params={"filter": f'eq("name","{context_name}")'})
    if response.status_code != 200:
        raise RuntimeError(f"Find context named {context_name} failed: {response.text}")
    context_resp = response.json()
    if not context_resp["count"]:
        raise RuntimeError(f"Compute context '{context_name}' was not found")
    sas_context = context_resp["items"][0]

    # create session with given context
    uri = f'/compute/contexts/{sas_context["id"]}/sessions'
    session_request = {"version": 1, "name": name}
    tmpheaders = copy.deepcopy(session.headers)
    tmpheaders["Content-Type"] = "application/vnd.sas.compute.session.request+json"

    req = json.dumps(session_request)
    response = session.post(uri, data=req, headers=tmpheaders)

    if response.status_code != 201:
        raise RuntimeError(f"Failed to create session: {response.text}")

    return response.json()


JES_URI = "/jobExecution"
JOB_URI = f"{JES_URI}/jobs"


def _run_job_and_wait(session, job_request: dict, poll_interval: int) -> dict:
    uri = JOB_URI
    response = session.post(uri, json=job_request)
    # change to process non standard codes returned from API (201, 400, 415)
    # i.e. sistuation when we were not able to make API call at all
    if response.status_code != 201:
        raise RuntimeError(f"Failed to create job request: {response.text}")
    job = response.json()
    job_id = job["id"]
    state = job["state"]
    print(f"Submitted job request with id {job_id}. Waiting for completion")
    uri = f"{JOB_URI}/{job_id}"
    while state in ["pending", "running"]:
        time.sleep(poll_interval)
        response = session.get(uri)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to get job: {response.text}")
        job = response.json()
        state = job["state"]
    print("Job request has completed execution with the status: " + str(state))
    return job
