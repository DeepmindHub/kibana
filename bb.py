import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
from elasticsearch import Elasticsearch
import time
import datetime as dt
import numpy as np
from math import radians, cos, sin, asin, sqrt

import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
import config.db_config_hl as db
import config.db_config_locationdump as dbl


time_interval='= curdate()'
def main():
    cnx = sqlcon.connect(user=db.USER, password=db.PWD, database='ebdb',
                                  host=db.HOST)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    es = Elasticsearch(['###############'],timeout=30, max_retries=10, retry_on_timeout=True)


    index_name="bigbasket"
    # mappings={
    #         "dashboard": {
    #             "properties": {
    #                 "cluster_name": {"type": "string", "index": "not_analyzed"},
    #                 "rider_name": {"type": "string", "index": "not_analyzed"},
    #                 "ATL/TH": {"type": "string", "index": "not_analyzed"},
    #                 "outlet_name": {"type": "string", "index": "not_analyzed"},
    #                 "allotted_phone": {"type": "string", "index": "not_analyzed"},
    #                 "slots": {"type": "string", "index": "not_analyzed"},
    #                 "order_status_str": {"type": "string", "index": "not_analyzed"}
    #                 }
    #         }
    # }
    # es.indices.create(index=index_name,ignore=400)
    # es.indices.put_mapping(index=index_name, doc_type="dashboard",body=mappings)
    last_updated = dt.date.today()
  
    while(1):
        bb_data = getData(cnx,last_updated)
        bb_data['scheduled_time'] = pd.to_datetime(bb_data.scheduled_time)
        bb_data['order_time'] = pd.to_datetime(bb_data.order_time)
        bb_data['curr_time'] = pd.to_datetime(bb_data.curr_time)
        bb_data['last_updated'] = pd.to_datetime(bb_data.last_updated)
        print index_name,'Current time:', dt.datetime.now(),'Dataframe shape:', bb_data.shape
        status_dict={'index':index_name,'Current time':dt.datetime.now(),'Dataframe shape':bb_data.shape}
        try:
          es.index(index="kibana_status", doc_type="dashboard", id=index_name, body=status_dict)
        except:
          print 'error1'
          continue
        cnt=0
        last_updated = bb_data.last_updated.max()
            
        for ind in bb_data.index:
            row = bb_data.loc[ind]
            order_id = str(row.order_id)
            ind = row.isnull()
            row.loc[ind] = None
            row = row.to_dict()  
            # print "pushing index "+str(ind)
            try:
              es.index(index=index_name, doc_type="dashboard", id=order_id, body=row)
              print cnt
              cnt=cnt+1
              # time.sleep(1)      
            except Exception as e:
              print 'error2 '+str(e)
              continue
        # exit(-1)
        time.sleep(60)


def getData(cnx,last_updated):
    query = '''
            SELECT o.id order_id,
            DATE(o.scheduled_time) order_date,
            o.scheduled_time,
            o.order_time,
            now() curr_time,
            coid,
            outlet_name,
            cl.city,
            cluster_name,
            '--' 'ATL/TH',
            sr.id rider_id,
            concat(rp.first_name,' ',rp.last_name) rider_name,
            sr.allotted_phone,
            o.pickup_flag,
            o.delivered_flag,
            o.last_updated,
            (CASE WHEN concat(rp.first_name,' ',rp.last_name) LIKE '%dummy%' THEN 'NO'
                  WHEN concat(rp.first_name,' ',rp.last_name) LIKE '%big%basket%' THEN 'NO'
                  WHEN concat(rp.first_name,' ',rp.last_name) LIKE '%shadow%' THEN 'NO'
                  ELSE 'YES' END) 'assigned',
            (CASE WHEN TIME(CONVERT_TZ(o.scheduled_time, 'GMT', 'ASIA/KOLKATA')) BETWEEN '07:00:00' AND '09:29:00' THEN 'S1 (07:00am-09:30am)'
                  WHEN time(CONVERT_TZ(o.scheduled_time, 'GMT', 'ASIA/KOLKATA')) BETWEEN '09:30:00' AND '12:00:00' THEN 'S2 (09:30am-12:00pm)'
                  WHEN time(CONVERT_TZ(o.scheduled_time, 'GMT', 'ASIA/KOLKATA')) BETWEEN '17:00:00' AND '19:29:00' THEN 'S3 (5:00pm-07:30pm)'
                  WHEN time(CONVERT_TZ(o.scheduled_time, 'GMT', 'ASIA/KOLKATA')) BETWEEN '19:30:00' AND '22:00:00' THEN 'S4 (7:30pm-10:00pm)'
                  ELSE 'other' END) 'slots',
            (CASE     
                  WHEN o.status=0 THEN 'Unassigned'
                  WHEN o.status=1 THEN 'Allotted' 
                  WHEN o.status=2 THEN 'Message received'
                  WHEN o.status=3 THEN 'Accepted'
                  WHEN o.status=4 THEN 'Collected'
                  WHEN o.status=5 THEN 'Delivered'
                  WHEN o.status=6 THEN 'Rejected By Customer'
                  WHEN o.status=7 THEN 'Undelivered'
                  WHEN o.status=8 THEN 'Arrived'
                  WHEN o.status=302 THEN 'Cancelled'
                  WHEN o.status=403 THEN 'Rider Deleted Order'
                  WHEN o.status=503 THEN 'Rider Rejected Order'
                  WHEN o.status=404 THEN 'No Rider Found'
                  WHEN o.status=111 THEN 'Customer initiated delay'
                  WHEN o.status=112 THEN 'Customer uncontactable'
                  WHEN o.status=113 THEN 'Order not accepted by customer'
                  END) order_status_str,
                  o.status,
                  (CASE WHEN o.status NOT IN (6,302) AND pickup_flag!=0 AND delivered_flag=0 THEN 'undelivered'
                   WHEN o.status NOT IN (0,302) AND pickup_flag=0 THEN 'unpicked'
                   WHEN o.status=0 THEN 'unassigned'
                   END) tracker
            FROM coreengine_order o
            LEFT JOIN coreengine_sfxseller ss
            ON o.seller_id=ss.id
            LEFT JOIN coreengine_sfxrider sr ON o.rider_id=sr.id
            LEFT JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
            LEFT JOIN coreengine_cluster cl ON o.cluster_id=cl.id
            -- LEFT JOIN (
            --         SELECT soc.cluster_id,au.username
            --         FROM coreengine_sfxoperation so 
            --         LEFT JOIN auth_user au ON au.id=so.user_id 
            --         LEFT JOIN coreengine_sfxoperation_cluster soc ON soc.sfxoperation_id=so.id 
            --         WHERE role=2 
            --         )t2 ON cl.id=t2.cluster_id 
            WHERE chain_id=85
            -- AND (t2.username NOT IN ('harish.naidu','nirupama.das','zia.shaikh','n.srikanth') OR t2.cluster_id IN (23,67))
            AND awb LIKE '%scheduled%'
            AND o.last_updated>="'''+str(last_updated)+'''"
            ;
            '''
    bb_data = sql.read_sql(query, cnx)
    # print bb_data.shape
    return bb_data

if __name__ == "__main__":
    main()
