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

On later Airflow 3.x releases, ``AirflowFailException`` and
``AirflowTaskTimeout`` moved from ``airflow.exceptions`` to
``airflow.sdk.exceptions``, and importing them from the old location raises
a UserWarning. ``AirflowException`` did not move.

The move did not happen consistently across every Airflow 3.x sub-version
(e.g. Airflow 3.1.x still exposes them only from ``airflow.exceptions``,
while later releases moved them to ``airflow.sdk.exceptions``), so we try the
new location first and gracefully fall back to the old one instead of
branching on the Airflow major version number.
"""

from __future__ import annotations

from airflow.exceptions import AirflowException

try:
    from airflow.sdk.exceptions import AirflowFailException, AirflowTaskTimeout
except ImportError:
    from airflow.exceptions import AirflowFailException, AirflowTaskTimeout

__all__ = ["AirflowException", "AirflowFailException", "AirflowTaskTimeout"]
