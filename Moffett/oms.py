import pandas as pd
import pyodbc
import numpy as np

#OMS
def M_OMSConfiguration(project, order):

    server = 'fihel1-sp080SQL.mcint.local,50002'
    database = 'oms'
    username = 'oms_reader'
    password = '7yrVS2WYzFwgwYVs'

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          'Trusted_Connection=no;')

    cursor = conn.cursor()
    sql = """select substring(convert(varchar(10), global_order_counter), 1, 6)    as proj,
                               c.component_code,
                               c.family_group_name,
                               c.family_name,
                               c.component_name + ' ' + isnull(c.custom_component_value, ' ') as component_name,
                               CAST(sum(c.quantity) as varchar(10))                               as quantity,
                               CAST(isnull(p.[value], ' ') as varchar(10))                        as prop_val
                        from component c
                                 inner join sales_order_line sl on c.sales_order_line_id = sl.sales_order_line_id
                                 inner join sales_order s on s.sales_order_id = sl.sales_order_id
                                 left join property p on p.component_id = c.component_id and p.code = 'hi_colour_mb'
                        where s.company_id = 'D82845F8-B439-4C28-9877-CB2544CA12A8'
                          and s.global_order_no like 'CS-OR' + CAST(? as varchar(6)) + '%'
                          --and sl.line_number = 1
                          and c.visibility in (9, 5, 3)
                          and (c.nsc_state <> 'X' or c.nsc_state is null)
                          and c.component_code not like 'TECHDATA_DESC'
                        group by substring(convert(varchar(10), global_order_counter), 1, 6),
                                 c.component_code, c.family_group_name, c.family_name, c.component_name, c.custom_component_value, p.[value]
                        order by c.component_code"""

    cursor.execute(sql, [project])
    q_res = cursor.fetchall()

    np_data = np.array(q_res, dtype=object)

    #Convert to DataFrame
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(np_data, columns=columns)
    df["ID"] = order + "-" + df.index.astype(str)

    column_mapping = {
        "ID": "id",
        "proj": "xoojv_project",
        "component_code": "nahpn_component_code",
        "family_group_name": "vkekd_family_group_name",
        "family_name": "gsxyq_family_name",
        "component_name": "iopvy_component_name",
        "quantity": "jkxdu_quantity",
        "prop_val": "wiqig_prop_val"
    }
    df = df.rename(columns=column_mapping)

    # df = pd.DataFrame(q_res)

    # Zamknięcie połączenia
    cursor.close()
    conn.close()
    return df


def M_SalesOrderData(order):
    server = 'fihel1-sp080SQL.mcint.local,50002'
    database = 'oms'
    username = 'oms_reader'
    password = '7yrVS2WYzFwgwYVs'

    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          'Trusted_Connection=no;')

    cursor = conn.cursor()
    sql = """select
                so.global_order_no as [Global Order Number],
                substring(convert(varchar, so.order_date ,101),1,11) as [Created Date],
                a.name1 as [Delivery Name],
                a.address1 as [Delivery Address],
                a.zipcode as [Delivery Zip Code],
                a.city as [Delivery City],
                a.country_name as [Delivery Country],
                da.name1 as [Ord Name],
                da.address1 as [Ord Address],
                da.zipcode as [Ord Zip Code],
                da.city as [Ord City],
                da.country_name as [Ord Country],
                so.delivery_method_name as [Delivery Method],
                case when so.footer_remark like'' then '-' else so.footer_remark end as [Footer Remark],
                so.remark as Remark,
                so.order_type as "Order Type",
                so.ewr_no as 'EWR'
                from sales_order so
                    INNER JOIN [sales_order_line] sol ON sol.sales_order_id = so.sales_order_id
                    INNER JOIN [address] a ON so.sales_order_id = a.sales_order_id and a.address_type = 2
                    INNER JOIN [address] da ON so.sales_order_id = da.sales_order_id and da.address_type = 1
                where
                so.company_id = 'D82845F8-B439-4C28-9877-CB2544CA12A8'
                and sol.production_order_number = ?"""

    cursor.execute(sql, [order])
    q_res = cursor.fetchall()

    if q_res:
        np_data = np.array(q_res, dtype=object)

        # Convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(np_data, columns=columns)
        df["ID"] = order + "-" + df.index.astype(str)
        df["Production Order"] = order

        column_mapping = {
            "ID": "id",
            "Production Order": "ippgo_production_order",
            "Global Order Number": "udeys_global_order_number",
            "Created Date": "yxpjm_created_date",
            "Delivery Name": "ykzzy_delivery_name",
            "Delivery Address": "lnepj_delivery_address",
            "Delivery Zip Code": "fcbwj_delivery_zip_code",
            "Delivery City": "isqqo_delivery_city",
            "Delivery Country": "mzfxl_delivery_country",
            "Ord Name": "sqenp_ord_name",
            "Ord Address": "uryyh_ord_address",
            "Ord Zip Code": "mybgh_ord_zip_code",
            "Ord City": "ocewb_ord_city",
            "Ord Country": "emvds_ord_country",
            "Delivery Method": "vcedg_delivery_method",
            "Footer Remark": "jdqjk_footer_remark",
            "Remark": "fsxep_remark",
            "Order Type": "tkiml_order_type",
            "EWR": "sdhxe_ewr"
        }
        df = df.rename(columns=column_mapping)

        # df = pd.DataFrame(q_res)

        # Zamknięcie połączenia
        cursor.close()
        conn.close()
        return df
    else:
        cursor.close()
        conn.close()
        return pd.DataFrame()
#---