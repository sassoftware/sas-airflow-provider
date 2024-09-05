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
import requests
import os
import logging


def get_folder_file_contents(session, path: str, http_timeout=None) -> str:
    """
    Fetch a file from folder service
    :param session:
    :param path:
    :param http_timeout: Timeout for http connection
    :return:
    """
    member = get_member_by_path(session, path)
    if member['contentType'] != 'file':
        raise RuntimeError(f"folder item is not a file: '{path}'")

    uri = member['uri'] + '/content'
    response = session.get(uri, timeout=http_timeout)
    if not response.ok:
        raise RuntimeError(f"File {path} was not found or could not be accessed. error code: {response.status_code}")

    return response.text


def get_folder_by_path(session, path: str, http_timeout=None) -> dict:
    """
    Get a folder given the path.
    Return a folder object, or raise an error
    """
    response = session.get('/folders/folders/@item', params={'path': path}, timeout=http_timeout)
    if response.ok:
        return response.json()
    raise RuntimeError(response.text)


def get_member_by_path(session, path: str, http_timeout=None) -> dict:
    """
    Get a folder member given the full path.
    Return a folder member (object), or an empty dict if not found
    """
    parts = os.path.split(path)
    if len(parts) < 2:
        raise RuntimeError(f"invalid path '{path}'")

    f = get_folder_by_path(session, parts[0], http_timeout=http_timeout)

    uri = get_uri(f['links'], 'members')
    if not uri:
        raise RuntimeError("failed to find members uri link")
    response = session.get(uri, params={'filter': f'eq("name","{parts[1]}")'}, timeout=http_timeout)

    if not response.ok:
        raise RuntimeError(f"failed to get folder members for '{path}'")

    members = response.json()['items']
    if not members:
        raise RuntimeError(f"failed to get folder path '{path}'")

    member = members[0]
    return member


def get_compute_session_file_contents(session, compute_session, path: str, http_timeout=None) -> str:
    """
    Fetch a file from the compute session file system
    :param session: the rest session that includes auth token
    :param compute_session: the compute session id
    :param path: full path to the file in the file system
    :param http_timeout: Timeout for http connection
    :return: contents of the file
    """
    p = f'{path.replace("/", "~fs~")}'
    uri = f'/compute/sessions/{compute_session}/files/{p}/content'

    response = session.get(uri, headers={"Accept": "application/octet-stream"}, timeout=http_timeout)
    if response.ok:
        return response.text
    raise RuntimeError(f"File {path} was not found or could not be accessed. error code: {response.status_code}")


def get_uri(links, rel):
    """
    Given a links object from a rest response, find the rel link specified and return the uri.
    Raise exception if not found
    """
    link = next((x for x in links if x["rel"] == rel), None)
    if link is None:
        return None
    return link["uri"]


def stream_log(session,job,start,limit=99999, http_timeout=None) -> int:
    current_line=start

    log_uri = get_uri(job["links"], "log")
    if not log_uri:
        logging.getLogger(name=None).warning("Warning: failed to retrieve log URI. Maybe the log is too large.")
    else:
        try:
            # Note if it is a files link (it will be that when the job have finished), this does not support the 'start' parameter, so we need to filter it by ourself.
            # We will ignore the limit parameter in that case
            is_files_link=log_uri.startswith("/files/")

            r = session.get(f"{log_uri}/content?start={start}&limit={limit}", timeout=http_timeout)
            if r.ok:
                # Parse the json log format and print each line
                log_contents = r.text            
                jcontents = json.loads(log_contents)
                lines=0;
                for line in jcontents["items"]:
                    if (is_files_link and lines>=start) or not is_files_link:
                        t = line["type"]
                        if t != "title":
                            logging.getLogger(name=None).info(f'{line["line"]}')
                        current_line=current_line+1      

                    lines=lines+1
            else:
                logging.getLogger(name=None).warning(f"Failed to retrieve parts of the log with status code {r.status_code} from URI: {log_uri}/content. Maybe the log is too large.")
        except Exception as e:
            logging.getLogger(name=None).warning(f"Unable to retrieve parts of the log: {e}. Maybe the log is too large.")
            
    return current_line
    


def dump_logs(session, job, http_timeout=None):
    """
    Get the log from the job object
    :param session: rest session
    :param job: job object that should contain links object
    :param http_timeout: Timeout for http connection
    """

    log_uri = get_uri(job["links"], "log")
    if not log_uri:
        print("Warning: failed to retrieve log uri from links. Log will not be displayed")
    else:
        r = session.get(f"{log_uri}/content", timeout=http_timeout)
        if not r.ok:
            print("Warning: failed to retrieve log content. Log will not be displayed")

        log_contents = r.text
        # Parse the json log format and print each line
        jcontents = json.loads(log_contents)
        for line in jcontents["items"]:
            t = line["type"]
            if t != "title":
                print(f'{line["line"]}')

def find_named_compute_session(session: requests.Session, name: str, http_timeout=None) -> dict:
    # find session with given name
    response = session.get(f"/compute/sessions?filter=eq(name, {name})", timeout=http_timeout)
    if not response.ok:
        raise RuntimeError(f"Find sessions failed: {response.status_code}")
    sessions = response.json()
    if sessions["count"] > 0:
        print(f"Existing compute session named '{name}' with id {sessions['items'][0]['id']} was found")
        return sessions["items"][0]
    return {}

def create_or_connect_to_session(session: requests.Session, context_name: str, name = None, http_timeout=None, job_name_prefix = None) -> dict:
    """
    Connect to an existing compute session by name. If that named session does not exist,
    one is created using the context name supplied
    :param session: rest session that includes oauth token
    :param context_name: the context name to use to create the session if the session was not found
    :param name: name of session to find
    :param http_timeout: Timeout for http connection
    :param job_name_prefix: (optional) string. The name that you want the compute session to identify as in SAS Workload Orchestrator (SWO). job_name_prefix is supported from Viya Stable 2024.07 and forward 
    :return: session object

    """
    if name != None:
        compute_session = find_named_compute_session(session, name, http_timeout=http_timeout)
        if compute_session:
            return compute_session
        
        print(f"Compute session named '{name}' does not exist, a new one will be created")
    else:
        print(f"A new unnamed compute session will be created")


    # find compute context
    response = session.get("/compute/contexts", params={"filter": f'eq("name","{context_name}")'},timeout=http_timeout)
    if not response.ok:
        raise RuntimeError(f"Find context named {context_name} failed: {response.status_code}")
    context_resp = response.json()
    if not context_resp["count"]:
        raise RuntimeError(f"Compute context '{context_name}' was not found")
    sas_context = context_resp["items"][0]

    # create session with given context
    uri = f'/compute/contexts/{sas_context["id"]}/sessions'
    if name != None:
        if job_name_prefix != None:
            session_request = {"version": 1, "name": name, "attributes":{"jobNamePrefix":job_name_prefix}}
        else:
            session_request = {"version": 1, "name": name}
    else:
        # Create a unnamed session
        if job_name_prefix != None:
            session_request = {"version": 1, "attributes":{"jobNamePrefix":job_name_prefix}}
        else:
            session_request = {"version": 1}

    headers = {"Content-Type": "application/vnd.sas.compute.session.request+json"}

    req = json.dumps(session_request)
    response = session.post(uri, data=req, headers=headers, timeout=http_timeout)

    if response.status_code != 201:
        raise RuntimeError(f"Failed to create session: {response.text}")

    json_response=response.json()
    print(f"Compute session {json_response['id']} created")

    return json_response

def end_compute_session(session: requests.Session, id, http_timeout=None):
    uri = f'/compute/sessions/{id}'
    response = session.delete(uri, timeout=http_timeout)
    if not response.ok:
        return False
    return True
