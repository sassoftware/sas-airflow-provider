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

dag = DAG('demo_studio_flow_1', description='Executing Studio Flow for demo purposes',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 6, 1), catchup=False)

environment_vars = {
    "env1": "val1",
    "env2": "val2"
}

task1 = SASStudioOperator(task_id='demo_studio_flow_1.flw',
                              path_type='content',
                              path='/Public/Airflow/demo_studio_flow_1.flw',
                              exec_log=True,
                              compute_context="SAS Studio compute context",
                              codegen_init_code=False,
                              codegen_wrap_code=False,
                              connection_name='sas_default',
                              env_vars=environment_vars,
                              dag=dag)
