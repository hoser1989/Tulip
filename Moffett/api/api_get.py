import requests
import pandas as pd
import json

def checkTulipIfSynced(tableId, order_number):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'
    url = 'https://hiab.tulip.co/api/v3/tables/' + tableId + '/records?filters=[{"field":"id","arg":"' + order_number + '","functionType":"equal"}]&limit=1'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        #print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()

    if not isinstance(data, list):  # Check, if JSON is a list
        print("Error: List was expected, but received:", type(data))
        return None

    df = pd.DataFrame(data)
    return df



def getProductionOrders(tableId, datetime_from, datetime_to, all = False):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "_createdAt", "functionType": "greaterThanOrEqual", "arg": datetime_from},
        {"field": "_createdAt", "functionType": "lessThan", "arg": datetime_to},
        # {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format
    if all == False:
        url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records?filters={filters_str}'
    else:
        url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        #print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()

    if not isinstance(data, list):  # Check if JSON is a list
        print("Error: List was expected, but received:", type(data))
        return None

    # If no records were found, return an empty DataFrame
    if not data:
        return pd.DataFrame()  # Return empty DataFrame if no data

    df = pd.DataFrame(data)  # Create DataFrame
    return df

    # production_order = df[['id']]
    # return production_order  # Dataframe result

def getActiveProductionOrders(tableId):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records?filters={filters_str}&limit=10'


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
    return df
    # production_order = df[['id']]

    # return production_order  # Dataframe result


def getActiveProductionOrdersALL(tableId):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    # filters = [
    #     {"field": "yqhhz_family", "functionType": "isIn", "arg": ['MFT02', 'MFT02A']},
    #     {"field": "levog_status", "functionType": "isIn", "arg": ['Completed', 'Production Completed']}
    # ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    base_url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records'
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    all_records = []
    offset = 0
    limit = 100  # Max Limit

    while True:
        url = f'{base_url}?filters={filters_str}&limit={limit}&offset={offset}'
        response = requests.get(url, auth=(user, pwd), headers=headers)

        if response.status_code != 200:
            print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
            break

        data = response.json()

        if not isinstance(data, list) or len(data) == 0:  # Check if list is empty
            break

        all_records.extend(data)
        offset += limit  # We shift the offset by the limit to fetch the next records

    df = pd.DataFrame(all_records)  # Create DataFrame

    return df  # Retrurn only "id"

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

def getLastTasks():
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    sort_options = [
        {"sortBy": "wevql_startdate", "sortDir": "desc"},
    ]

    sort_options_str = json.dumps(sort_options)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/zoMehkj3DDaWcosvn/records?limit=1&sortOptions={sort_options_str}'

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

    return df['id']# Dataframe result

def getActiveStations(filters, limit):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/xhWSDpFY4LH34TXFe/records?limit={limit}&filters={filters_str}'

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

def getSyncOrders(tableId):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    base_url = f'https://hiab.tulip.co/api/v3/tables/{tableId}/records'
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    all_records = []
    offset = 0
    limit = 100  # Max Limit

    while True:
        url = f'{base_url}?limit={limit}&offset={offset}'
        response = requests.get(url, auth=(user, pwd), headers=headers)

        if response.status_code != 200:
            print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
            break

        data = response.json()

        if not isinstance(data, list) or len(data) == 0:  # Check if list is empty
            break

        all_records.extend(data)
        offset += limit  # We shift the offset by the limit to fetch the next records

    df = pd.DataFrame(all_records)  # Create DataFrame

    return df  # Retrurn only "id"