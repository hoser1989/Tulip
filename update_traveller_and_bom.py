import oracledb
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, timedelta

# LN
def productionSpecificationSummary(order_number):
    # Connect to DB
    or_dns = oracledb.makedsn('134.37.15.96', 1522, service_name='erpln6c')
    con = oracledb.connect(user="tulip215db", password="tulip215f0rtulip215", dsn=or_dns)
    cursor = con.cursor()

    # SQL query
    sql = """select
         ticst001.t$pdno "Production Order",
         case to_char(tcibd001.t$cdf_popt)
            when 'PRT01' then 'Serialized engines'
            when 'PRT02' then 'Frame'
            when 'PRT03' then 'Mast'
            when 'PRT04' then 'Wheels'
            when 'PRT05' then 'PRT05'
            when 'PRT06' then 'Drive Motors'
            when 'PRT07' then 'Rollcage'
            when 'PRT08' then 'VB INFO'
            when 'PRT09' then 'PRT09'
            when 'PRT10' then 'Extras'
            when 'PRT11' then 'Print Flag'
        end "Print On Traveller Desc",
        ticst001.t$sitm "Item",
        tcibd001.t$dsca "Item Description",
        tisfc010.t$cwoc "Work Center",
        tisfc010.t$tano "Task",
        tirou003.t$dsca "Task Description"
    FROM ERPLN6C.tticst001215 ticst001
    left join ERPLN6C.ttisfc010215 tisfc010 on tisfc010.t$pdno = ticst001.t$pdno and tisfc010.t$opno = ticst001.t$opno
    left join ERPLN6c.ttirou003215 tirou003 on tirou003.t$tano = tisfc010.t$tano
    inner join ERPLN6C.ttcibd001215 tcibd001 on tcibd001.t$item = ticst001.t$sitm and tcibd001.t$cdf_popt in ('PRT01','PRT02','PRT03','PRT04','PRT05',
    'PRT06','PRT07','PRT08','PRT09','PRT10','PRT11')
    where ticst001.t$pdno = :production_order
    order by tcibd001.t$cdf_popt
    """

    # Execute query
    cursor.execute(sql, {'production_order': order_number})
    q_res = cursor.fetchall()

    # Convert to DataFrame
    columns = [desc[0] for desc in cursor.description]  # Pobranie nazw kolumn
    df = pd.DataFrame(q_res, columns=columns)
    df["ID"] = df["Production Order"] + "-" + df.index.astype(str)

    df = df.rename(columns={"ID": "id"})
    df = df.rename(columns={"Production Order": "gxuwe_production_order"})
    df = df.rename(columns={"Print On Traveller Desc": "vvytl_printontravellerdesc"})
    df = df.rename(columns={"Item": "zmgir_item"})
    df = df.rename(columns={"Item Description": "amadv_item_description"})
    df = df.rename(columns={"Work Center": "pqqtp_work_center"})
    df = df.rename(columns={"Task": "pssfa_task"})
    df = df.rename(columns={"Task Description": "tqsse_task_description"})

    # Zamknięcie połączenia
    cursor.close()
    con.close()
    return df

def billOfMaterial(order_number):
    # Connect to DB
    or_dns = oracledb.makedsn('134.37.15.96', 1522, service_name='erpln6c')
    con = oracledb.connect(user="tulip215db", password="tulip215f0rtulip215", dsn=or_dns)
    cursor = con.cursor()

    # SQL query
    sql = """select
                ticst001.t$pdno "Production Order",
                to_char(ticst001.t$pono) "Position",
                to_char(ticst001.t$opno) "Operation",
                ticst001.t$sitm "Item",
                tcibd001.t$dsca "Item Description",
                to_char(ticst001.t$ques) "Estimated Quantity",
                ticst001.t$revi "Revision",
                case ticst001.t$mcmd
                       when 1 then 'Print and Allocate'
                       when 2 then 'Print Only'
                end "Material Control Method",
                tisfc010.t$cwoc "Work Center",
                tcmcs065.t$dsca "Work Center Description"
            FROM ERPLN6C.tticst001215 ticst001
            left join ERPLN6C.ttisfc010215 tisfc010 on tisfc010.t$pdno = ticst001.t$pdno and tisfc010.t$opno = ticst001.t$opno
            left join ERPLN6C.ttcibd001215 tcibd001 on tcibd001.t$item = ticst001.t$sitm
            left join ERPLN6C.ttcmcs065215 tcmcs065 on tcmcs065.t$cwoc = tisfc010.t$cwoc
            where ticst001.t$pdno = :production_order
            order by tisfc010.t$cwoc"""

    # Execute query
    cursor.execute(sql, {'production_order': order_number})
    q_res = cursor.fetchall()

    # Convert to DataFrame
    columns = [desc[0] for desc in cursor.description]  # Pobranie nazw kolumn
    df = pd.DataFrame(q_res, columns=columns)
    df["ID"] = df["Production Order"] + "-" + df.index.astype(str)

    df = df.rename(columns={"ID": "id"})
    df = df.rename(columns={"Production Order": "mxcex_production_order"})
    df = df.rename(columns={"Position": "icxws_position"})
    df = df.rename(columns={"Operation": "yqonz_workcenter"})
    df = df.rename(columns={"Item": "zflau_item"})
    df = df.rename(columns={"Item Description": "hfhrj_description"})
    df = df.rename(columns={"Work Center": "xlqjo_work_center"})
    df = df.rename(columns={"Work Center Description": "blbfw_workcenterdiscription"})
    df = df.rename(columns={"Material Control Method": "aobvh_printand_allocate"})
    df = df.rename(columns={"Revision": "uxbzl_revision"})
    df = df.rename(columns={"Estimated Quantity": "lvmim_estimated_quantity"})

    # Zamknięcie połączenia
    cursor.close()
    con.close()
    return df
#---

def sendToTulip(df, table_id):
    url = f'https://hiab.tulip.co/api/v3/tables/{table_id}/records'
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(user, pwd)

        # Iterate and send requests
        for index, row in df.iterrows():
            payload = row.to_dict()  # Convert row to dictionary (JSON-like)

            try:
                #print(payload)
                response = session.post(url, json=payload)
                response.raise_for_status()  # Raise exception for bad responses

                #print(f"Row {index} sent successfully: {response.json()}")

            except requests.exceptions.RequestException as e:
                print(f"Error sending row {index}: {e}")

def updateTulipSyncRecord(record_id, fieldId, fieldValue):
    url_base = 'https://hiab.tulip.co/api/v3/tables/F8msta7LuWpSHqXPz/records'
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

def createTulipSyncRecord(record_id):
    url = 'https://hiab.tulip.co/api/v3/tables/F8msta7LuWpSHqXPz/records'
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

            #print(f"Record {record_id} sent successfully: {response.json()}")

        except requests.exceptions.RequestException as e:
            print(f"Error sending record {record_id}: {e}")


def checkIfTulipOrderExists(order_number):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'
    url = 'https://hiab.tulip.co/api/v3/tables/Jii944sA7s3kS5pu8_DEFAULT/records?filters=[{"field":"id","arg":"' + order_number + '","functionType":"equal"}]&limit=1'

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    response = requests.get(url, auth=(user, pwd), headers=headers)

    if response.status_code != 200:
        #print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    data = response.json()
    return data

def checkTulipIfSynced(order_number):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'
    url = 'https://hiab.tulip.co/api/v3/tables/F8msta7LuWpSHqXPz/records?filters=[{"field":"id","arg":"' + order_number + '","functionType":"equal"}]&limit=1'

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

def getTulipProductionOrders(datetime_from, datetime_to, all = False):
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "_createdAt", "functionType": "greaterThanOrEqual", "arg": datetime_from},
        {"field": "_createdAt", "functionType": "lessThan", "arg": datetime_to},
        # {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format
    if all == False:
        url = f'https://hiab.tulip.co/api/v3/tables/Jii944sA7s3kS5pu8_DEFAULT/records?filters={filters_str}'
    else:
        url = f'https://hiab.tulip.co/api/v3/tables/Jii944sA7s3kS5pu8_DEFAULT/records'

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

    production_order = df[['id']]

    return production_order  # Dataframe result

def getActiveTulipProductionOrders():
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    url = f'https://hiab.tulip.co/api/v3/tables/Jii944sA7s3kS5pu8_DEFAULT/records?filters={filters_str}&limit=100&offset=200'


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

    production_order = df[['id']]

    return production_order  # Dataframe result


def getActiveTulipProductionOrdersALL():
    user = 'apikey.2_YbECmsfBSjhGwYf3T'
    pwd = 'GO01NhVXEikyXQ-uksiQ4v6nPplEtoAW-sVklVtUAfs'

    filters = [
        {"field": "levog_status", "functionType": "isIn", "arg": ['Active', 'Released']}
    ]

    filters_str = json.dumps(filters)  # Convert to proper JSON format

    base_url = 'https://hiab.tulip.co/api/v3/tables/Jii944sA7s3kS5pu8_DEFAULT/records'
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

    return df[['id']]  # Retrurn only "id"


#Execute
beginning_of_the_day = datetime.combine(datetime.today(), datetime.min.time())
beginning_of_the_day_str = beginning_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')
end_of_the_day = beginning_of_the_day + timedelta(days=1)
end_of_the_day_str = end_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')

result = getTulipProductionOrders(beginning_of_the_day_str, end_of_the_day_str)

if not result.empty:
    for index,row in result.iterrows():
        sync_status = checkTulipIfSynced(row.id)
        if sync_status.empty:
            createTulipSyncRecord(row.id)
            # update production specification summary
            pss = productionSpecificationSummary(row.id)
            sendToTulip(pss, 'JB6jTG755K3BFFAyS')
            updateTulipSyncRecord(row.id, 'jrbjn_traveller', True)
            # update bom
            bom = billOfMaterial(row.id)
            sendToTulip(bom, 'K6y2f8AiFpK8SbscT')
            updateTulipSyncRecord(row.id, 'uazuq_bom', True)
        else:
            if not sync_status['jrbjn_traveller'].values:
                # update production specification summary
                pss = productionSpecificationSummary(row.id)
                sendToTulip(pss, 'JB6jTG755K3BFFAyS')
                updateTulipSyncRecord(row.id, 'jrbjn_traveller', True)
            if not sync_status['uazuq_bom'].values:
                # update bom
                bom = billOfMaterial(row.id)
                sendToTulip(bom, 'K6y2f8AiFpK8SbscT')
                updateTulipSyncRecord(row.id, 'uazuq_bom', True)
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
# print(result)