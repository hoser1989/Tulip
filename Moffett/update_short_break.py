from api import api_get, api_post
from dateutil import parser
from datetime import datetime


# Get todays date
today = datetime.today().strftime('%Y-%m-%d')

# Fixed hour
short_break_fixed_time_start = "T11:00:00Z"
short_break_fixed_time_end = "T11:15:00Z"

# Short break
dynamic_short_break_start_date = f"{today}{short_break_fixed_time_start}"
dynamic_short_break_start_end = f"{today}{short_break_fixed_time_end}"

task_table_id = 'zoMehkj3DDaWcosvn'
filter_active_stations = [
    {"field": "vpfdb_status", "functionType": "equal", "arg": 'In Progress'},
    {"field": "id", "functionType": "isIn", "arg": ['WC6010 - Line 2','WC6020 - Line 2','WC6030 - Line 2','WC6040 - Line 2','WC6050 - Line 2','WC6060 - Line 2','WC6070 - Line 2']},
]

active_stations = api_get.getActiveStations(filter_active_stations, 100)

for index, row in active_stations.iterrows():
    last_id = api_get.getLastTasks()
    last_id_str = str(int(last_id.iloc[0]) + 1)
    if last_id_str:
        api_post.createTaskRecord(task_table_id, last_id_str, row['id'], row['onrzu_work_order'],
                              'admin', 'Planned Lost Time', 'Closed',
                              dynamic_short_break_start_date, dynamic_short_break_start_end, 900,
                              'test', 'Planned Lost Time', row['dqzxf_family'], 'admin' )

