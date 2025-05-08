import oracledb
import pandas as pd

# LN
def M_productionSpecificationSummary(order_number):
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
            when 'PRT05' then 'Forks'
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

    #Convert to DataFrame
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

    # Close the connection
    cursor.close()
    con.close()
    return df

def M_billOfMaterial(order_number):
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


def M_SerializedItems(order_number):
    # Connect to DB
    or_dns = oracledb.makedsn('134.37.15.96', 1522, service_name='erpln6c')
    con = oracledb.connect(user="tulip215db", password="tulip215f0rtulip215", dsn=or_dns)
    cursor = con.cursor()

    # SQL query
    sql = """select
                ticst001.t$pdno "Production Order",
                ticst001.t$sitm "Item",
                tcibd001.t$dsca "Item Description"
            FROM ERPLN6C.tticst001215 ticst001
            left join ERPLN6C.ttisfc010215 tisfc010 on tisfc010.t$pdno = ticst001.t$pdno and tisfc010.t$opno = ticst001.t$opno
            inner join ERPLN6C.ttcibd001215 tcibd001 on tcibd001.t$item = ticst001.t$sitm
            where ticst001.t$pdno = :production_order and tcibd001.t$seri = 1
            order by tisfc010.t$cwoc
                """

    # Execute query
    cursor.execute(sql, {'production_order': order_number})
    q_res = cursor.fetchall()

    #Convert to DataFrame
    columns = [desc[0] for desc in cursor.description]  # Pobranie nazw kolumn
    df = pd.DataFrame(q_res, columns=columns)
    df["ID"] = df["Production Order"] + "-" + df.index.astype(str)

    df = df.rename(columns={"ID": "id"})
    df = df.rename(columns={"Production Order": "tseev_production_order"})
    df = df.rename(columns={"Item": "qhxpc_item"})
    df = df.rename(columns={"Item Description": "nigrc_item_description"})

    # Close the connection
    cursor.close()
    con.close()
    return df