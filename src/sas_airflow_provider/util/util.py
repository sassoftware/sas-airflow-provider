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


def get_folder_file_contents(session, path: str) -> str:
    """
    Fetch a file from folder service
    :param session:
    :param path:
    :return:
    """
    member = get_member_by_path(session, path)
    if member['contentType'] != 'file':
        raise RuntimeError(f"folder item is not a file: '{path}'")

    uri = member['uri'] + '/content'
    response = session.get(uri)
    if not response.ok:
        raise RuntimeError(f"File {path} was not found or could not be accessed. error code: {response.status_code}")

    return response.text


def get_folder_by_path(session, path: str) -> dict:
    """
    Get a folder given the path.
    Return a folder object, or raise an error
    """
    response = session.get('/folders/folders/@item', params={'path': path})
    if response.ok:
        return response.json()
    raise RuntimeError(response.text)


def get_member_by_path(session, path: str) -> dict:
    """
    Get a folder member given the full path.
    Return a folder member (object), or an empty dict if not found
    """
    parts = os.path.split(path)
    if len(parts) < 2:
        raise RuntimeError(f"invalid path '{path}'")

    f = get_folder_by_path(session, parts[0])

    uri = get_uri(f['links'], 'members')
    if not uri:
        raise RuntimeError("failed to find members uri link")
    response = session.get(uri, params={'filter': f'eq("name","{parts[1]}")'})

    if not response.ok:
        raise RuntimeError(f"failed to get folder members for '{path}'")

    members = response.json()['items']
    if not members:
        raise RuntimeError(f"failed to get folder path '{path}'")

    member = members[0]
    return member


def get_compute_session_file_contents(session, compute_session, path: str) -> str:
    """
    Fetch a file from the compute session file system
    :param session: the rest session that includes auth token
    :param compute_session: the compute session id
    :param path: full path to the file in the file system
    :return: contents of the file
    """
    p = f'{path.replace("/", "~fs~")}'
    uri = f'/compute/sessions/{compute_session}/files/{p}/content'

    response = session.get(uri, headers={"Accept": "application/octet-stream"})
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


def dump_logs(session, job):
    """
    Get the log from the job object
    :param session: rest session
    :param job: job object that should contain links object
    """

    log_uri = get_uri(job["links"], "log")
    if not log_uri:
        print("Warning: failed to retrieve log uri from links. Log will not be displayed")
    else:
        r = session.get(f"{log_uri}/content")
        if not r.ok:
            print("Warning: failed to retrieve log content. Log will not be displayed")

        log_contents = r.text
        # Parse the json log format and print each line
        jcontents = json.loads(log_contents)
        for line in jcontents["items"]:
            t = line["type"]
            if t != "title":
                print(f'{line["line"]}')


def create_or_connect_to_session(session: requests.Session, context_name: str, name: str) -> dict:
    """
    Connect to an existing compute session by name. If that named session does not exist,
    one is created using the context name supplied
    :param session: rest session that includes oauth token
    :param context_name: the context name to use to create the session if the session was not found
    :param name: name of session to find
    :return: session object
    """
    # find session with given name
    response = session.get(f"/compute/sessions?filter=eq(name, {name})")
    if not response.ok:
        raise RuntimeError(f"Find sessions failed: {response.status_code}")
    sessions = response.json()
    if sessions["count"] > 0:
        print(f"Existing session named '{name}' was found")
        return sessions["items"][0]

    print(f"Compute session named '{name}' does not exist, a new one will be created")
    # find compute context
    response = session.get("/compute/contexts", params={"filter": f'eq("name","{context_name}")'})
    if not response.ok:
        raise RuntimeError(f"Find context named {context_name} failed: {response.status_code}")
    context_resp = response.json()
    if not context_resp["count"]:
        raise RuntimeError(f"Compute context '{context_name}' was not found")
    sas_context = context_resp["items"][0]

    # create session with given context
    uri = f'/compute/contexts/{sas_context["id"]}/sessions'
    session_request = {"version": 1, "name": name}

    headers = {"Content-Type": "application/vnd.sas.compute.session.request+json"}

    req = json.dumps(session_request)
    response = session.post(uri, data=req, headers=headers)

    if response.status_code != 201:
        raise RuntimeError(f"Failed to create session: {response.text}")

    return response.json()
