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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from sas_airflow_provider.hooks.sas import SasHook
from sas_airflow_provider.util.util import \
    create_or_connect_to_session


class SASComputeCreateSession(BaseOperator):
    """
    Create a Compute session and push the session id as an XCom named 'compute_session_id'.
    This can be used as an input for the SASStudioOperator to give finer grained control over sessions

    :param connection_name: (optional) name of the connection to use. The connection should be defined
        as an HTTP connection in Airflow. If not specified then the default is used
    :param compute_context_name: (optional) Name of the Compute context to use. If not provided, a
        suitable default is used.
    :param session_name: (optional) name to give the created session. If not provided, a suitable default is used
    :param http_timeout: (optional) Timeout for https requests. Default value is (30.05, 300), meaning a connect timeout sligthly above 30 seoconds and 
        a read timeout of 300 seconds where the operator will wait for the server to send a response.
    :param job_name_prefix: (optional) string. Specify a name that you want the compute session to identify as in SAS Workload Orchestrator (SWO). 
        If job_name_prefix is not specified the default prefix is determined by Viya (currently 'sas-compute-server-'). 
        If the value cannot be parsed by Viya to create a valid k8s pod name, the default value will be used as well.
        job_name_prefix is supported from Viya Stable 2024.07
    """

    ui_color = "#CCE5FF"
    ui_fgcolor = "black"

    # template fields are fields which can be templated out in the Airflow task using {{ }}
    template_fields: Sequence[str] = ("compute_context_name", "session_name")

    def __init__(
            self,
            connection_name=None,
            compute_context_name="SAS Studio compute context",
            session_name="Airflow-Session",
            http_timeout=(30.05, 300),
            job_name_prefix=None,
            **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.connection = None
        self.connection_name = connection_name
        self.compute_context_name = compute_context_name
        self.session_name = session_name
        self.compute_session_id=""
        self.http_timeout=http_timeout
        self.job_name_prefix = job_name_prefix

    def execute(self, context):
        try:
            self.log.info("Authenticate connection")
            h = SasHook(self.connection_name)
            self.connection = h.get_conn()
            self._connect_compute()
            self.xcom_push(context, 'compute_session_id', self.compute_session_id)
        # support retry if API-calls fails for whatever reason
        except Exception as e:
            raise AirflowException(f"SASComputeCreateSession error: {str(e)}")

        return 1

    def _connect_compute(self):
        # connect to compute if we are not connected, and set our compute session id
        if not self.compute_session_id:
            self.log.info("Creating or connecting to compute session")
            sesh = create_or_connect_to_session(self.connection,self.compute_context_name,self.session_name,self.http_timeout,self.job_name_prefix)
            self.compute_session_id = sesh["id"]
            self.log.info(f"Created session with id {self.compute_session_id}")

