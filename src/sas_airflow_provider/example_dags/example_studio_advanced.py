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

# demonstrate executing code directly as well as passing input/output macro variables and explicitly
# creating a Compute session

dag = DAG('demo_studio_flow_advanced', description='Execute code and pass variables',
          schedule="@once",
          start_date=datetime(2022, 6, 1), catchup=False)

environment_vars = {
    "env1": "val1",
    "env2": "val2"
}

# Create a Compute session and make the session id available as XCom variable

task0 = SASComputeCreateSession(task_id="create_sess", dag=dag)

# execute a SAS program from a file in the Compute file system. The session created above is used (see the
# xcom_pull in the template). The path should point to an existing .sas file that is accessible from the
# session. For the purpose of the demonstration, the program should set two macro variables, AF_1 and AF_2.
# Here is a possible demo program:
#
# %let AF_1 = One;
# %let AF_2 = Two;
# %put This is a test;
# run;
#
# By setting output_macro_var_prefix, we are able to pull any macro variables that start with the prefix and make
# them available as XCom variables.

task1 = SASStudioOperator(task_id='demo_program',
                              path_type='compute',
                              exec_type='program',
                              path='/path/to/test.sas',
                              exec_log=True,
                              compute_session_id="{{ ti.xcom_pull(key='compute_session_id', task_ids=['create_sess'])|first }}",
                              compute_context="SAS Studio compute context",
                              codegen_init_code=False,
                              codegen_wrap_code=False,
                              env_vars=environment_vars,
                              output_macro_var_prefix="AF_",
                              dag=dag)

# The next task demonstrates the ability to directly execute code stored in a string parameter (see program2 below).
# it also demonstrates reading xcom variables that were set as outputs above.
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
