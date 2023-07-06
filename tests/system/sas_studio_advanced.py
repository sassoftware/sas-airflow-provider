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

from datetime import datetime
from airflow import DAG
from sas_airflow_provider.operators.sas_studio import SASStudioOperator
from sas_airflow_provider.operators.sas_create_session import SASComputeCreateSession

dag = DAG('demo_advanced', description='Executing code',
          schedule="@once",
          start_date=datetime(2022, 6, 1), catchup=False)

environment_vars = {
    "env1": "val1",
    "env2": "val2"
}

task0 = SASComputeCreateSession(task_id="create_sess", dag=dag)

task1 = SASStudioOperator(task_id='demo_program',
                          path_type='compute',
                          exec_type='program',
                          path='/bigdisk/lnx/anshak/test.sas',
                          exec_log=True,
                          compute_session_id="{{ ti.xcom_pull(key='compute_session_id', task_ids=['create_sess'])|first }}",
                          compute_context="SAS Studio compute context",
                          codegen_init_code=False,
                          codegen_wrap_code=False,
                          env_vars=environment_vars,
                          output_macro_var_prefix="AF_",
                          dag=dag)

program2 = '''
%put value of one is &one;
%put value of two is &two;
%run;
'''

task2 = SASStudioOperator(task_id='demo_program_2',
                          path_type='raw',
                          exec_type='program',
                          path=program2,
                          exec_log=True,
                          compute_session_id="{{ ti.xcom_pull(key='compute_session_id', task_ids=['create_sess'])|first }}",
                          compute_context="SAS Studio compute context",
                          codegen_init_code=False,
                          codegen_wrap_code=False,
                          env_vars=environment_vars,
                          macro_vars={"one": "{{ti.xcom_pull(key='AF_1', task_ids=['demo_program'])|first}}",
                                      "two": "{{ti.xcom_pull(key='AF_2', task_ids=['demo_program'])|first}}"},
                          dag=dag)

task0 >> task1 >> task2
if __name__ == '__main__':
    dag.test()
