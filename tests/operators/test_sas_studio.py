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

from unittest.mock import ANY, Mock, patch

from sas_airflow_provider.operators.sas_studio import (
    SASStudioOperator,
    create_or_connect_to_session,
)

from sas_airflow_provider.util.util import dump_logs


class TestSASStudioOperator:
    """
    Test class for SASStudioOperator
    """


    @patch.object(SASStudioOperator, '_run_job_and_wait')
    @patch.object(SASStudioOperator, '_generate_object_code')
    @patch("sas_airflow_provider.operators.sas_studio.dump_logs")
    @patch("sas_airflow_provider.operators.sas_studio.SasHook")
    def test_execute_sas_studio_flow_operator_basic(
        self, session_mock, dumplogs_mock, mock_gen_flow_code, mock_run_job_and_wait
    ):
        mock_gen_flow_code.return_value = {"code": "test code"}
        mock_run_job_and_wait.return_value = { "id": "jobid1",
                                               "state": "completed",
                                               "links": [{"rel": "log", "uri": "log/uri"}],
                                               }, True
        environment_vars = {"env1": "val1", "env2": "val2"}
        operator = SASStudioOperator(
            task_id="demo_studio_flow_1.flw",
            path_type="content",
            path="/Public/Airflow/demo_studio_flow_1.flw",
            exec_log=True,
            connection_name="SAS",
            compute_context="SAS Studio compute context",
            codegen_init_code=False,
            codegen_wrap_code=False,
            env_vars=environment_vars,
        )

        operator.execute(context={})

        dumplogs_mock.assert_called()
        session_mock.assert_called_with("SAS")
        mock_gen_flow_code.assert_called()
        mock_run_job_and_wait.assert_called()

    def test_execute_sas_studio_flow_create_or_connect(self):
        session = Mock()
        req_ret = Mock()
        session.get.return_value = req_ret
        req_ret.json.return_value = {"count": 1, "items": ["dummy"]}
        req_ret.status_code = 200
        r = create_or_connect_to_session(session, "context", "name")
        assert r == "dummy"

    def test_execute_sas_studio_flow_create_or_connect_new(self):
        session = Mock()
        req_ret1 = Mock()
        req_ret2 = Mock()
        session.get.side_effect = [req_ret1, req_ret2]
        session.headers = {}
        post_ret = Mock()
        session.post.return_value = post_ret
        post_ret.status_code = 201
        post_ret.json.return_value = {"a": "b"}
        req_ret1.json.return_value = {"count": 0}
        req_ret1.status_code = 200
        req_ret2.json.return_value = {"count": 1, "items": [{"id": "10"}]}
        req_ret2.status_code = 200
        r = create_or_connect_to_session(session, "context", "name")
        assert r == {"a": "b"}

    def test_execute_sas_studio_flow_operator_gen_code(self):
        session = Mock()
        req_ret = Mock()
        session.post.return_value = req_ret
        req_ret.json.return_value = {"code": "code val"}
        req_ret.status_code = 200
        op = SASStudioOperator(task_id="demo_studio_flow_1.flw",
                                   path_type="content",
                                   path="/path",
                                   exec_log=True,
                                   codegen_init_code=True, codegen_wrap_code=True)
        op.connection = session
        r = op._generate_object_code()

        session.post.assert_called_with(
            "/studioDevelopment/code",
            json={
                "reference": {
                    "mediaType": "application/vnd.sas.dataflow",
                    "type": "content",
                    "path": "/path",
                },
                "initCode": True,
                "wrapperCode": True,
            },
        )
        assert r == {"code": "code val"}

    @patch("sas_airflow_provider.operators.sas_studio.create_or_connect_to_session")
    def test_execute_sas_studio_flow_operator_gen_code_compute(self, c_mock):
        session = Mock()
        req_ret = Mock()
        session.post.return_value = req_ret
        req_ret.json.return_value = {"code": "code val"}
        req_ret.status_code = 200
        c_mock.return_value = {"id": "abc"}

        op = SASStudioOperator(task_id="demo_studio_flow_1.flw",
                                   path_type="compute",
                                   path="/path",
                                   exec_log=True,
                                   codegen_init_code=True, codegen_wrap_code=True)
        op.connection = session
        r = op._generate_object_code()


        c_mock.assert_called_with(ANY, "SAS Studio compute context", "Airflow-Session")

        session.post.assert_called_with(
            "/studioDevelopment/code?sessionId=abc",
            json={
                "reference": {
                    "mediaType": "application/vnd.sas.dataflow",
                    "type": "compute",
                    "path": "/path",
                },
                "initCode": True,
                "wrapperCode": True,
            },
        )
        assert r == {"code": "code val"}

    def test_execute_sas_studio_flow_run_job(self):
        session_mock = Mock()
        session_mock.post.return_value.status_code = 201
        session_mock.post.return_value.json.return_value = {"id": "1", "state": "completed"}
        req = {"a": "b"}
        op = SASStudioOperator(task_id="demo_studio_flow_1.flw",
                                   path_type="compute",
                                   path="/path",
                                   exec_log=True,
                                   codegen_init_code=True, codegen_wrap_code=True)
        op.connection = session_mock
        r = op._run_job_and_wait(req, 1)
        session_mock.post.assert_called_with("/jobExecution/jobs", json={"a": "b"})
        assert r == ({"id": "1", "state": "completed"}, True)

    def test_execute_sas_studio_flow_get_logs(self):
        session_mock = Mock()
        session_mock.get.return_value.status_code = 200
        session_mock.get.return_value.text = """
            {"items": [{"type":"INFO", "line":"line value"}]}
            """
        req = {"links": [{"rel": "log", "uri": "log/uri"}]}
        dump_logs(session_mock, req)
        session_mock.get.assert_called_with("log/uri/content")
