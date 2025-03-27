import requests
from requests.auth import HTTPBasicAuth

def updateTulipRecord(tableId, record_id, fieldId, fieldValue):
    url_base = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        url = f"{url_base}/{record_id}"  # Create URL for PUT

        payload = {
                f"{fieldId}" : fieldValue
            }

        try:
            #print(f"Updating record {record_id}: {payload}")
            response = session.put(url, json=payload)
            response.raise_for_status()  # Sprawdzenie błędów

            #print(f"Record {record_id} updated successfully: {response.json()}")

        except requests.exceptions.RequestException as e:
            print(f"Error updating record {record_id}: {e}")

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