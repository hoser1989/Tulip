import requests
from requests.auth import HTTPBasicAuth

def sendToTulip(df, table_id):
    url = f'https://hiab.tulip.co/api/v3/tables/{table_id}/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        # Iterate and send requests
        for index, row in df.iterrows():
            payload = row.to_dict()  # Convert row to dictionary (JSON-like)
            print(payload)
            try:
                #print(payload)
                response = session.post(url, json=payload)
                response.raise_for_status()  # Raise exception for bad responses

                #print(f"Row {index} sent successfully: {response.json()}")

            except requests.exceptions.RequestException as e:
                print(f"Error sending row {index}: {e}")

def createSyncRecord(tableId, record_id):
    url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    # payload
    payload = {
        "id": record_id,
        "jrbjn_traveller": False ,
        "uazuq_bom": False,
    }

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        try:
            #print(f"Sending data: {payload}")
            response = session.post(url, json=payload)
            response.raise_for_status()  # Check error

            # print(f"Record {record_id} sent successfully: {response.json()}")

        except requests.exceptions.RequestException as e:
            print(f"Error sending record {record_id}: {e}")

def createTaskRecord(table_id, record_id, workstation, production_order, operator, order_status, task_status,  start_date, end_date, labour_hour, comment, lost_time, family, daily_team_id):
    url = f'https://hiab.tulip.co/api/v3/tables/{table_id}/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    # payload
    payload = {
        "id": record_id,
        "ivdic_operation_id": workstation,
        "jgrba_work_order_id": production_order,
        # "yaiob_operator": operator,
        "cdgld_status": order_status,
        "ustyz_task_status": task_status,
        "wevql_startdate": start_date,
        "sgkue_enddate": end_date,
        "snjnq_laborhour": labour_hour,
        "jjfve_comment": comment,
        "oddid_pause_reason": lost_time,
        "sjxkm_family": family,
        "gxvdj_daily_team_id": daily_team_id,
    }

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        try:
            # print(f"Sending data: {payload}")
            response = session.post(url, json=payload)
            response.raise_for_status()  # Check error

            # print(f"Record {record_id} sent successfully: {response.json()}")

        except requests.exceptions.RequestException as e:
            print(f"Error sending record {record_id}: {e}")

