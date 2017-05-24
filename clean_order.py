import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
from elasticsearch import Elasticsearch
import time
import datetime as dt
import numpy as np
import os
import sys


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
import config.db_config_hl as db


def main():
    cnx = sqlcon.connect(user=db.USER, password=db.PWD, database='ebdb',
                                  host=db.HOST)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])

    index_name="clean_order"
    mappings={
            "dashboard": {
                "properties": {
                    "outlet_name": {"type": "string", "index": "not_analyzed"},
                    "cluster_name": {"type": "string", "index": "not_analyzed"},
                    "rider_name": {"type": "string", "index": "not_analyzed"},
                    "address": {"type": "string", "index": "not_analyzed"}
                }
            }
    }

    es.indices.create(index=index_name,ignore=400)
    es.indices.put_mapping(index=index_name, doc_type="dashboard",body=mappings)
    last_updated = dt.date.today()
    
    while(1):
        clean_order_data = getData(cnx,last_updated)
        # last_timestamp = attendance_data.timestamp_updated.max()
        clean_order_data['order_date'] = pd.to_datetime(clean_order_data.order_date)
        print index_name,'Current time:', dt.datetime.now(),'Dataframe shape:', clean_order_data.shape
        status_dict={'index':index_name,'Current time':dt.datetime.now(),'Dataframe shape':clean_order_data.shape}
        try:
          es.index(index="kibana_status", doc_type="dashboard", id=index_name, body=status_dict)
        except:
          continue
        last_updated = clean_order_data.last_updated.max()
            
        for ind in clean_order_data.index:
            row = clean_order_data.loc[ind]
            order_id = str(row.order_id)
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            # print "pushing index "+str(ind)
            try:
              es.index(index=index_name, doc_type="dashboard", id=order_id, body=row)
            except:
              continue
        # exit(-1)
        time.sleep(60)


def getData(cnx,last_updated):
    query = '''SELECT k.*,
             (CASE WHEN (k.allotduration BETWEEN 0 AND 30) THEN 1 ELSE 0 END)allotclean,
             (CASE WHEN k.acceptduration BETWEEN 0 AND 30 THEN 1 ELSE 0 END)acceptclean,
             (CASE WHEN k.pickupduration BETWEEN 3 AND 50 THEN 1 ELSE 0 END)pickupclean,
             (CASE WHEN k.deliveredduration BETWEEN 10 AND 90 THEN 1 ELSE 0 END)deliveredclean,
             (CASE WHEN (k.allotduration BETWEEN 0 AND 30) AND (k.acceptduration BETWEEN 0 AND 30) AND (k.pickupduration BETWEEN 3 AND 50) AND (k.deliveredduration BETWEEN 10 AND 90) THEN 1 ELSE 0 END)cleanorders
             FROM
             (
              SELECT o.id order_id,
              o.status,
              o.source,
              o.seller_id,
              outlet_name,
              c.city,
              c.operational_city,
              c.id cluster_id,
              cluster_name,
              DATE(scheduled_time) 'order_date',
              dayname(DATE(scheduled_time)) DAY,
              o.rider_id,
              concat(first_name,' ',last_name) rider_name,
              sr.allotted_phone,
              concat(o.house_number,o.locality) address,
              o.amount,
              o.scheduled_time,
              o.allot_time,
              o.accept_time,
              o.pickup_time,
              o.delivered_time,
              o.last_updated,
              TIMESTAMPDIFF(MINUTE,scheduled_time,allot_time)allotduration,
              TIMESTAMPDIFF(MINUTE,scheduled_time,o.accept_time)acceptduration,
              TIMESTAMPDIFF(MINUTE,scheduled_time,o.pickup_time)pickupduration,
              TIMESTAMPDIFF(MINUTE,scheduled_time,o.delivered_time)deliveredduration
              FROM coreengine_order o 
              INNER JOIN coreengine_sfxseller sf ON sf.id=o.seller_id 
              INNER JOIN coreengine_cluster c ON c.id=o.cluster_id 
              INNER JOIN coreengine_chain ch ON ch.id=sf.chain_id 
              INNER JOIN coreengine_sfxrider sr ON sr.id=o.rider_id 
              INNER JOIN coreengine_riderprofile rp ON rp.id=sr.rider_id 
              WHERE DATE(scheduled_time) = curdate() 
              AND cluster_name NOT LIKE '%test%' 
              AND cluster_name NOT LIKE '%hub%' 
              AND cluster_name NOT LIKE '%snapdeal%' 
              AND o.status<10 
              AND o.source!=9 
              AND ch.id NOT IN (85,1662,1717)
             )k WHERE k.last_updated> "'''+str(last_updated)+'''";'''


    clean_order_data = sql.read_sql(query, cnx)
    # print clean_order_data.shape
    return clean_order_data

if __name__ == "__main__":
    main()
