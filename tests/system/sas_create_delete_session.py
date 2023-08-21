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
from sas_airflow_provider.operators.sas_create_session import SASComputeCreateSession
from sas_airflow_provider.operators.sas_delete_session import SASComputeDeleteSession

dag = DAG('demo_create_delete', description='Create and delete sessions',
          schedule="@once",
          start_date=datetime(2022, 6, 1), catchup=False)

task0 = SASComputeCreateSession(task_id="create_sess", dag=dag)

task1 = SASComputeDeleteSession(task_id='delete_sess',
                                compute_session_id="{{ ti.xcom_pull(key='compute_session_id', task_ids=["
                                                   "'create_sess'])|first }}",
                                dag=dag)


task2 = SASComputeDeleteSession(task_id='delete_sess_named',
                                compute_session_name="Airflow-Session",
                                dag=dag)

task0 >> task1 >> task2
if __name__ == '__main__':
    dag.test()
