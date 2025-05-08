from numpy.testing.print_coercion_tables import print_coercion_table
from api import api_get, api_post, api_put
import oms
import ln
from datetime import datetime, timedelta

production_orders_table_id = 'Jii944sA7s3kS5pu8_DEFAULT'
sync_table_id = 'F8msta7LuWpSHqXPz'
oms_conf_table_id = 'HpCsSgXsoWxPuQTit'
oms_sales_order_data_table_id = 'xkHFpjXYMbE2heyn5'
pss_table_id = 'JB6jTG755K3BFFAyS'
bom_table_id = 'K6y2f8AiFpK8SbscT'
serialized_items_id = 'RZxzXt8qm9jLcejmY'


# result = api_get.getActiveProductionOrdersALL('Jii944sA7s3kS5pu8_DEFAULT')
# for index, row in result.iterrows():
#     if 'EMPTY' not in row['id'] and 'TEST' not in row['id']:
#         serialized_items = ln.M_SerializedItems(row['id'])
#         api_post.sendToTulip(serialized_items, serialized_items_id)
#         api_put.updateTulipRecord(sync_table_id, row['id'], 'tlmrb_serialized_items', True)

#Update one
# pss = ln.M_productionSpecificationSummary('D01036197')
# sendToTulip(pss, pss_table_id)
# updateTulipRecord(sync_table_id, 'D01036197', 'jrbjn_traveller', True)

# oms_sales_order_data = oms.M_SalesOrderData('502210')
# print(oms_sales_order_data)
# sendToTulip(oms_sales_order_data, oms_sales_order_data_table_id)
# updateTulipRecord(sync_table_id, 'D01027134', 'bhutr_oms_sales_order_data', True)

#update sync table
# syncedOrders = api_get.getSyncOrders(sync_table_id)
# for index, row in syncedOrders.iterrows():
#     api_put.updateTulipRecord(sync_table_id, row['id'], 'bhutr_oms_sales_order_data', False)