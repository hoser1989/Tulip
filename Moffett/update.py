from numpy.testing.print_coercion_tables import print_coercion_table

import oms
import ln
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, timedelta

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

def createTulipRecord(tableId, record_id):
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


def checkIfTulipOrderExists(tableId, order_number):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'
    url = 'https://hiab.tulip.co/api/v3/tables/' + tableId + '/records?filters=[{"field":"id","arg":"' + order_number + '","functionType":"equal"}]&limit=1'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        #print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()
    return data

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

def getTulipProductionOrders(tableId, datetime_from, datetime_to, all = False):
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

def getActiveTulipProductionOrders(tableId):
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


def getActiveTulipProductionOrdersALL(tableId):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

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


#Execute
beginning_of_the_day = datetime.combine(datetime.today(), datetime.min.time())
beginning_of_the_day_str = beginning_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')
end_of_the_day = beginning_of_the_day + timedelta(days=1)
end_of_the_day_str = end_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')

# result = getTulipProductionOrders('Jii944sA7s3kS5pu8_DEFAULT', beginning_of_the_day_str, end_of_the_day_str)
#
production_orders_table_id = 'Jii944sA7s3kS5pu8_DEFAULT'
sync_table_id = 'F8msta7LuWpSHqXPz'
oms_conf_table_id = 'HpCsSgXsoWxPuQTit'
oms_sales_order_data_table_id = 'xkHFpjXYMbE2heyn5'
pss_table_id = 'JB6jTG755K3BFFAyS'
bom_table_id = 'K6y2f8AiFpK8SbscT'

result = getActiveTulipProductionOrdersALL('Jii944sA7s3kS5pu8_DEFAULT')

if not result.empty:
    for index,row in result.iterrows():
        sync_status = checkTulipIfSynced(sync_table_id,row['id'])
        if sync_status.empty:
            createTulipRecord(sync_table_id,row['id'])
            # update production specification summary
            pss = ln.M_productionSpecificationSummary(row['id'])
            sendToTulip(pss, pss_table_id)
            updateTulipRecord(sync_table_id, row['id'], 'jrbjn_traveller', True)
            # update bom
            bom = ln.M_billOfMaterial(row['id'])
            sendToTulip(bom, bom_table_id)
            updateTulipRecord(sync_table_id, row['id'], 'uazuq_bom', True)
            #update oms configuration
            if row['ntxzj_item_master_id']:
                oms_configuration = oms.M_OMSConfiguration(row['ntxzj_item_master_id'], row['id'])
                sendToTulip(oms_configuration,oms_conf_table_id)
                updateTulipRecord(sync_table_id,row['id'], 'gqlel_oms_configuration', True)
            #update oms sales order data
            if row['ntxzj_item_master_id']:
                oms_sales_order_data = oms.M_SalesOrderData(row['ntxzj_item_master_id'])
                if not oms_sales_order_data.empty:
                    sendToTulip(oms_sales_order_data.iloc[:1],oms_sales_order_data_table_id)
                    updateTulipRecord(sync_table_id, row['id'], 'bhutr_oms_sales_order_data', True)
        else:
            if not sync_status['jrbjn_traveller'].values:
                # update production specification summary
                pss = ln.M_productionSpecificationSummary(row['id'])
                sendToTulip(pss, pss_table_id)
                updateTulipRecord(sync_table_id, row['id'], 'jrbjn_traveller', True)
            if not sync_status['uazuq_bom'].values:
                # update bom
                bom = ln.M_billOfMaterial(row['id'])
                sendToTulip(bom, bom_table_id)
                updateTulipRecord(sync_table_id, row['id'], 'uazuq_bom', True)
            if not sync_status['gqlel_oms_configuration'].values:
                if row['ntxzj_item_master_id']:
                    # update OMS configuration
                    oms_configuration = oms.M_OMSConfiguration(row['ntxzj_item_master_id'], row['id'])
                    sendToTulip(oms_configuration,oms_conf_table_id)
                    updateTulipRecord(sync_table_id, row['id'], 'gqlel_oms_configuration', True)
            if not sync_status['bhutr_oms_sales_order_data'].values:
                if row['ntxzj_item_master_id']:
                    # update oms sales order data
                    oms_sales_order_data = oms.M_SalesOrderData(row['ntxzj_item_master_id'])
                    if not oms_sales_order_data.empty:
                        sendToTulip(oms_sales_order_data.iloc[:1], oms_sales_order_data_table_id)
                        updateTulipRecord(sync_table_id, row['id'], 'bhutr_oms_sales_order_data', True)
else:
    print('No data returned.')


#Tests
# results = getActiveTulipProductionOrdersALL()
# print(results)

# beginning_of_the_day = datetime.combine(datetime.today(), datetime.min.time())
# beginning_of_the_day_minus1 = beginning_of_the_day - timedelta(days=1)
# beginning_of_the_day_minus_1_str = beginning_of_the_day_minus1.strftime('%Y-%m-%dT%H:%M:%SZ')
#
# end_of_the_day_minus1 = beginning_of_the_day
# end_of_the_day_minus1_str = end_of_the_day_minus1.strftime('%Y-%m-%dT%H:%M:%SZ')
# print(beginning_of_the_day_minus1, end_of_the_day_minus1_str)
#
# result = getTulipProductionOrders(beginning_of_the_day_minus_1_str, end_of_the_day_minus1_str)

#Update one
# pss = ln.M_productionSpecificationSummary('D01036197')
# sendToTulip(pss, pss_table_id)
# updateTulipRecord(sync_table_id, 'D01036197', 'jrbjn_traveller', True)

# oms_sales_order_data = oms.M_SalesOrderData('D01035411')
# sendToTulip(oms_sales_order_data, oms_sales_order_data_table_id)
# updateTulipRecord(sync_table_id, 'D01027134', 'bhutr_oms_sales_order_data', True)

