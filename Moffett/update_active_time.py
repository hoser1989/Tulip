from api import api_get, api_post, api_put
from dateutil import parser
from datetime import datetime, timezone

filters_active_stations = [
    {"field": "vpfdb_status", "functionType": "equal", "arg": 'In Progress'}
]

active_stations = api_get.getActiveStations(filters_active_stations, 100)
for as_index, as_row in active_stations.iterrows(): #get active stations (status: in progress)
    if as_row.onrzu_work_order:
        tasks = api_get.getTasks(as_row.onrzu_work_order, as_row.id) #get tasks by active production order and active workstation
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
                # print(as_row.id, t_row.id, start_date, start_date_object, now, active_time)
            else:
                # start_date_object = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
                # end_date_object = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
                start_date_object = parser.isoparse(start_date)
                end_date_object = parser.isoparse(end_date)
                active_time = (end_date_object - start_date_object).total_seconds()
                total_active_time += active_time # add active time to a total time
                # print(as_row.id, t_row.id, start_date, start_date_object, end_date, end_date_object, active_time)
        # print(f"{as_row.id} {as_row.onrzu_work_order}: {total_active_time}")
        api_put.updateStationActiveTime(as_row.id, 'qbteq_breaks', total_active_time)

