#
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

"""
Compatibility helpers so this provider works on both Airflow 2.x and 3.x
without triggering deprecation warnings.

On Airflow 3, ``AirflowFailException`` and ``AirflowTaskTimeout`` moved from
``airflow.exceptions`` to ``airflow.sdk.exceptions``, and importing them from
the old location raises a UserWarning. ``AirflowException`` did not move.
"""

from __future__ import annotations

from airflow import __version__ as _airflow_version
from airflow.exceptions import AirflowException

_AIRFLOW_MAJOR_VERSION = int(_airflow_version.split('.')[0])

if _AIRFLOW_MAJOR_VERSION < 3:
    from airflow.exceptions import AirflowFailException, AirflowTaskTimeout
else:
    from airflow.sdk.exceptions import AirflowFailException, AirflowTaskTimeout

__all__ = ["AirflowException", "AirflowFailException", "AirflowTaskTimeout"]
