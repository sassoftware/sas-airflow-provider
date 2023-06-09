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

import json


def _get_file_contents(session, file_uri) -> str:
    r = session.get(f"{file_uri}/content")
    if r.status_code != 200:
        raise RuntimeError(f"Failed to get file contents for {file_uri}: {r.text}")
    return r.text


def _get_uri(links, rel):
    link = next((x for x in links if x["rel"] == rel), None)
    if link is None:
        return None
    return link["uri"]


def dump_logs(session, job):
    # Get the log from the job
    log_uri = _get_uri(job["links"], "log")
    if not log_uri:
        print("Warning: failed to retrieve log uri from links. Log will not be displayed")
    else:
        log_contents = _get_file_contents(session, log_uri)
        # Parse the json log format and print each line
        jcontents = json.loads(log_contents)
        for line in jcontents["items"]:
            t = line["type"]
            if t != "title":
                print(f'{line["line"]}')
