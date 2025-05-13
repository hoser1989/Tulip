from numpy.testing.print_coercion_tables import print_coercion_table
from api import api_get, api_post, api_put
import oms
import ln
from datetime import datetime, timedelta
import sys


#Execute
beginning_of_the_day = datetime.combine(datetime.today(), datetime.min.time())
beginning_of_the_day_str = beginning_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')
end_of_the_day = beginning_of_the_day + timedelta(days=1)
end_of_the_day_str = end_of_the_day.strftime('%Y-%m-%dT%H:%M:%SZ')

result = api_get.getProductionOrders('Jii944sA7s3kS5pu8_DEFAULT', beginning_of_the_day_str, end_of_the_day_str)

production_orders_table_id = 'Jii944sA7s3kS5pu8_DEFAULT'
sync_table_id = 'F8msta7LuWpSHqXPz'
oms_conf_table_id = 'HpCsSgXsoWxPuQTit'
oms_sales_order_data_table_id = 'xkHFpjXYMbE2heyn5'
pss_table_id = 'JB6jTG755K3BFFAyS'
bom_table_id = 'K6y2f8AiFpK8SbscT'
serialized_items_id = 'RZxzXt8qm9jLcejmY'

# result = api_get.getActiveProductionOrdersALL('Jii944sA7s3kS5pu8_DEFAULT')

if not result.empty:
    for index,row in result.iterrows():
        if 'EMPTY' not in row['id'] and 'TEST' not in row['id']:
            sync_status = api_get.checkTulipIfSynced(sync_table_id,row['id'])
            if sync_status.empty:
                api_post.createSyncRecord(sync_table_id,row['id'])
                # update production specification summary
                pss = ln.M_productionSpecificationSummary(row['id'])
                api_post.sendToTulip(pss, pss_table_id)
                api_put.updateTulipRecord(sync_table_id, row['id'], 'jrbjn_traveller', True)
                # update bom
                bom = ln.M_billOfMaterial(row['id'])
                api_post.sendToTulip(bom, bom_table_id)
                api_put.updateTulipRecord(sync_table_id, row['id'], 'uazuq_bom', True)
                #update oms configuration
                if row['ntxzj_item_master_id']:
                    if row['ntxzj_item_master_id'][:2] != 'RD':
                        oms_configuration = oms.M_OMSConfiguration(row['ntxzj_item_master_id'], row['id'])
                        api_post.sendToTulip(oms_configuration,oms_conf_table_id)
                        api_put.updateTulipRecord(sync_table_id,row['id'], 'gqlel_oms_configuration', True)
                #update oms sales order data
                if row['ntxzj_item_master_id']:
                    if row['ntxzj_item_master_id'][:2] != 'RD':
                        oms_sales_order_data = oms.M_SalesOrderData(row['ntxzj_item_master_id'])
                        if not oms_sales_order_data.empty:
                            api_post.sendToTulip(oms_sales_order_data.iloc[:1],oms_sales_order_data_table_id)
                            api_put.updateTulipRecord(sync_table_id, row['id'], 'bhutr_oms_sales_order_data', True)
                #update serialized item
                serialized_items = ln.M_SerializedItems(row['id'])
                api_post.sendToTulip(serialized_items, serialized_items_id)
                api_put.updateTulipRecord(sync_table_id, row['id'], 'tlmrb_serialized_items', True)

            else:
                if not sync_status['jrbjn_traveller'].values:
                    # update production specification summary
                    pss = ln.M_productionSpecificationSummary(row['id'])
                    api_post.sendToTulip(pss, pss_table_id)
                    api_put.updateTulipRecord(sync_table_id, row['id'], 'jrbjn_traveller', True)
                if not sync_status['uazuq_bom'].values:
                    # update bom
                    bom = ln.M_billOfMaterial(row['id'])
                    api_post.sendToTulip(bom, bom_table_id)
                    api_put.updateTulipRecord(sync_table_id, row['id'], 'uazuq_bom', True)
                if not sync_status['gqlel_oms_configuration'].values:
                    if row['ntxzj_item_master_id']:
                        # update OMS configuration
                        if row['ntxzj_item_master_id'][:2] != 'RD':
                            oms_configuration = oms.M_OMSConfiguration(row['ntxzj_item_master_id'], row['id'])
                            api_post.sendToTulip(oms_configuration,oms_conf_table_id)
                            api_put.updateTulipRecord(sync_table_id, row['id'], 'gqlel_oms_configuration', True)
                if not sync_status['bhutr_oms_sales_order_data'].values:
                    if row['ntxzj_item_master_id']:
                        if row['ntxzj_item_master_id'][:2] != 'RD':
                            # update oms sales order data
                            oms_sales_order_data = oms.M_SalesOrderData(row['ntxzj_item_master_id'])
                            if not oms_sales_order_data.empty:
                                api_post.sendToTulip(oms_sales_order_data.iloc[:1], oms_sales_order_data_table_id)
                                api_put.updateTulipRecord(sync_table_id, row['id'], 'bhutr_oms_sales_order_data', True)
                if not sync_status['tlmrb_serialized_items'].values:
                    # update serialized item
                    serialized_items = ln.M_SerializedItems(row['id'])
                    api_post.sendToTulip(serialized_items, serialized_items_id)
                    api_put.updateTulipRecord(sync_table_id, row['id'], 'tlmrb_serialized_items', True)
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

