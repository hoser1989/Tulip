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

    # df = df.rename(columns={"ID": "id"})
    # df = df.rename(columns={"proj": "xoojv_project"})
    # df = df.rename(columns={"component_code": "nahpn_component_code"})
    # df = df.rename(columns={"family_group_name": "vkekd_family_group_name"})
    # df = df.rename(columns={"family_name": "gsxyq_family_name"})
    # df = df.rename(columns={"component_name": "iopvy_component_name"})
    # df = df.rename(columns={"quantity": "jkxdu_quantity"})
    # df = df.rename(columns={"prop_val": "wiqig_prop_val"})

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
#---