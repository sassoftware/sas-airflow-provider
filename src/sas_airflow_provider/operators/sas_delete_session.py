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
    create_or_connect_to_session, find_named_compute_session, end_compute_session


class SASComputeDeleteSession(BaseOperator):
    """
    Delete a Compute session. either a session_name or a session_id should be provided.
    The result is pushed as a True/False xcom named disconnect_succeeded

    :param connection_name: (optional) name of the connection to use. The connection should be defined
        as an HTTP connection in Airflow. If not specified then the default is used
    :param session_name: (optional) name of the session to delete
    :param session_id: (optiona) id of the session to delete
    """

    ui_color = "#CCE5FF"
    ui_fgcolor = "black"

    # template fields are fields which can be templated out in the Airflow task using {{ }}
    template_fields: Sequence[str] = ("compute_session_id", "compute_session_name")

    def __init__(
            self,
            connection_name=None,
            compute_session_name="",
            compute_session_id="",
            **kwargs,
    ) -> None:
        if not compute_session_id and not compute_session_name:
            raise AirflowException(f"Either session_name or session_id must be provided")
        super().__init__(**kwargs)
        self.connection = None
        self.connection_name = connection_name
        self.compute_session_name = compute_session_name
        self.compute_session_id = compute_session_id
        self.success=False

    def execute(self, context):
        try:
            self.log.info("Authenticate connection")
            h = SasHook(self.connection_name)
            self.connection = h.get_conn()
            self._delete_compute()
            self.xcom_push(context, 'disconnect_succeeded', self.success)
        # support retry if API-calls fails for whatever reason
        except Exception as e:
            raise AirflowException(f"SASComputeDeleteSession error: {str(e)}")

        return 1

    def _delete_compute(self):
        if self.compute_session_name:
            self.log.info(f"Find session named {self.compute_session_name}")
            sesh = find_named_compute_session(self.connection, self.compute_session_name)
            if sesh:
                self.compute_session_id = sesh["id"]
            else:
                self.log.info(f"Session named {self.compute_session_name} not found")
                return
        self.log.info(f"Delete session with id {self.compute_session_id}")
        self.success = end_compute_session(self.connection, self.compute_session_id)




