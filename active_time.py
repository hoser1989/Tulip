from functools import total_ordering

import oracledb
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from dateutil import parser
from datetime import datetime, timezone

def updateStationActiveTime(record_id, fieldId, fieldValue):
    url_base = 'https://hiab.tulip.co/api/v3/tables/xhWSDpFY4LH34TXFe/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        url = f"{url_base}/{record_id}"  # Create URL for PUT

        payload = {
                f"{fieldId}" : fieldValue
            }

        try:
            print(f"Updating record {record_id}: {payload}")
            response = session.put(url, json=payload)
            response.raise_for_status()  # Sprawdzenie błędów

            print(f"Record {record_id} updated successfully: {response.json()}")

        except requests.exceptions.RequestException as e:
            print(f"Error updating record {record_id}: {e}")


def getTasks(order, workstation):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "jgrba_work_order_id", "functionType": "equal", "arg": order},
        {"field": "cdgld_status", "functionType": "notIsIn", "arg": ["Paused", "Lost Time", "Planned Lost Time"]},
        {"field": "ivdic_operation_id", "functionType": "equal", "arg": workstation}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/zoMehkj3DDaWcosvn/records?limit=100&filters={filters_str}'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()

    if not isinstance(data, list):  # Check if JSON is a list
        print("Error: List was expected, but received:", type(data))
        return None

    # If no records were found, return an empty DataFrame
    if not data:
        return pd.DataFrame()  # Return empty DataFrame if no data

    df = pd.DataFrame(data)  # Create DataFrame

    return df  # Dataframe result

def getActiveStations():
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "vpfdb_status", "functionType": "equal", "arg": 'In Progress'}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/xhWSDpFY4LH34TXFe/records?limit=100&filters={filters_str}'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()

    if not isinstance(data, list):  # Check if JSON is a list
        print("Error: List was expected, but received:", type(data))
        return None

    # If no records were found, return an empty DataFrame
    if not data:
        return pd.DataFrame()  # Return empty DataFrame if no data

    df = pd.DataFrame(data)  # Create DataFrame

    return df  # Dataframe result

active_stations = getActiveStations()
for as_index, as_row in active_stations.iterrows(): #get active stations (status: in progress)
    if as_row.onrzu_work_order:
        tasks = getTasks(as_row.onrzu_work_order, as_row.id) #get tasks by active production order and active workstation
        total_active_time = 0
        for t_index,t_row in tasks.iterrows(): #calculate active time (difference between end date and start date) for each task
            start_date = t_row.wevql_startdate
            end_date = t_row.sgkue_enddate
            if end_date is None:
                # start_date_object = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
                start_date_object = parser.isoparse(start_date)
                now = datetime.now(timezone.utc)
                active_time = (now - start_date_object).total_seconds()
                total_active_time += active_time # add active time to a total time
                print(as_row.id, t_row.id, start_date, start_date_object, now, active_time)
            else:
                # start_date_object = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
                # end_date_object = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                start_date_object = parser.isoparse(start_date)
                end_date_object = parser.isoparse(end_date)
                active_time = (end_date_object - start_date_object).total_seconds()
                total_active_time += active_time # add active time to a total time
                print(as_row.id, t_row.id, start_date, start_date_object, end_date, end_date_object, active_time)
        # print(f"{as_row.id} {as_row.onrzu_work_order}: {total_active_time}")
        updateStationActiveTime(as_row.id, 'qbteq_breaks', total_active_time)

