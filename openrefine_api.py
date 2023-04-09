
import requests
import json
import time
from urllib.parse import urlparse
from urllib.parse import parse_qs

ontorefine_server = 'http://127.0.0.1:3333'
headers = {'Accept': 'application/json'}


def connection_to_server_get(url, data={}):
    try:
        r = requests.get(url, data, timeout=3)
        r.raise_for_status()
        # Code here will only run if the request is successful
        return r
    except requests.ConnectionError as error:
        return error


def connection_to_server_post(url, data={}):
    try:
        r = requests.post(url, data, timeout=3)
        r.raise_for_status()
        # Code here will only run if the request is successful
        return r
    except requests.ConnectionError as error:
        return error


# GET CSRF TOKEN
def get_token():
    """Returns CSRF token

    Args:
        none

    Returns:
        string: token
    """

    token_url = "http://127.0.0.1:3333/command/core/get-csrf-token"
    r = (connection_to_server_get(token_url))
    data = r.json()
    return (data['token'])


# CREATE PROJECT FROM UPLOAD


def create_from_upload(project_file, onterefine_project_name):
    """Creates new project from uploaded file

    Args:
        project_file(data file): imported data file
        onterefine_project_name(string): project name


    Returns:
        string: project ID
    """

    ontorefine_data = {"project-name": onterefine_project_name,
                       "format": "text/line-based/*sv",
                       "options": {
                       }
                       }
    ontorefine_file = {
        'project-file': open(project_file, "r", encoding='utf-8-sig')}
    token = get_token()
    command = '/command/core/create-project-from-upload?csrf_token=' + token
    ontorefine_response = requests.post(
        ontorefine_server + command, data=ontorefine_data, files=ontorefine_file, headers={'Accept': 'application/json'})
    url = ontorefine_response.url
    parsed_url = urlparse(url)
    projekt_id = parse_qs(parsed_url.query)['project'][0]
    return projekt_id


def read_operation_file(filename):
    with open(filename) as f:
        json_data = json.load(f)
    return json_data


# GET STATUS AND WAIT

def get_status_wait(projekt_id, delay=0.5):
    """Waits operations to finish

    Args:
        project_id(string): project ID

    Returns:
        boolean
    """
    while True:
        token = get_token()
        command_apply_operations = '/command/core/get-processes?project=' + \
            projekt_id + '&csrf_token=' + token
        ontorefine_operations_response = requests.get(
            ontorefine_server + command_apply_operations, headers=headers)
        json_response = ontorefine_operations_response.json()
        if 'processes' in json_response and len(json_response['processes']) > 0:
            time.sleep(delay)
        else:
            return


# APPLY OPERATIONS


def apply_operations(project_id, filename, wait=True):
    """Apply operations from Openrefine *.json file

    Args:
        project_id(string): project ID
        filename(string): .json filename

    Returns:
        string: json status
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    dict1 = {}
    json_string = json.dumps([{"op": "core/column-rename", "oldColumnName": "DateTime",
                               "newColumnName": "DateTime_new", "description": "Rename column DateTime to DateTime_new"}])
    json_string = json.dumps(read_operation_file(filename))
    dict1["operations"] = json_string
    token = get_token()
    command_apply_operations = '/command/core/apply-operations?project=' + \
        project_id + '&csrf_token=' + token
    ontorefine_operations_response = requests.post(
        ontorefine_server + command_apply_operations, data=dict1, headers=headers)
    json_response = ontorefine_operations_response.json()
    if json_response['code'] == 'error':
        raise Exception(json_response['message'])
    elif json_response['code'] == 'pending':
        if wait:
            get_status_wait(project_id)
            return 'ok'


# EXPORT TO FILE


def export_to_file(projekt_id, fileformat):
    """Exports project to file

    Args:
        project_id(string): project ID
        fileformat(string): exported fail format (default cvs )

    Returns:
        none
    """
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    dict2 = {}
    dict2["engine"] = json.dumps({"facets": [], "mode": "row-based"})
    token = get_token()
    command_apply_operations = '/command/core/export-rows?project=' + \
        projekt_id + '&csrf_token=' + token + '&format=' + fileformat
    ontorefine_operations_response = requests.post(
        ontorefine_server + command_apply_operations, data=dict2, headers=headers)
    with open("exported_file." + fileformat, "w", encoding='utf-8-sig') as f:
        f.write(ontorefine_operations_response.text)


# DELETE PROJECT


def delete_project(projekt_id):
    """Deletes project

    Args:
        project_id(string): project ID

    Returns:
        none
    """
    token = get_token()
    command_apply_operations = '/command/core/delete-project?project=' + \
        projekt_id + '&csrf_token=' + token
    ontorefine_operations_response = requests.post(
        ontorefine_server + command_apply_operations, headers=headers)
