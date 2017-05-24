import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
from elasticsearch import Elasticsearch
import time
import datetime as dt
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
    last_updated=dt.datetime.utcnow() - dt.timedelta(minutes=5)
    while(1):
        delivery_data = getData(cnx,last_updated)
        last_updated=delivery_data.last_updated.max()
        print 'delivery_service ','Current time:', dt.datetime.now(),' Dataframe shape:', delivery_data.shape
        
        status_dict={'index':'delivery_service','Current time':dt.datetime.now(),'Dataframe shape':delivery_data.shape}
        try:
          es.index(index="kibana_status", doc_type="document", id='delivery_service', body=status_dict)
        except:
          continue
        # print delivery_data.head()
        for ind in delivery_data.index:
            row = delivery_data.loc[ind]
            delivery_id = row.Order_id
            doc_type = str(row.order_time.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            #Sprint row
            try:
              es.index(index="delivery-service", doc_type="document", id=delivery_id, body=row)
            except:
              continue
        time.sleep(60)
               

def getData(cnx,last_updated):
    query = '''select seller_id,
               now() curr_time,
               outlet_name,
               a.source,
               a.amount,
               cluster_name,
               operational_city,
               a.Rider_id,
               a.locality,
               concat(first_name," ", last_name) as Rider_Name,
               allotted_phone,
               a.id as Order_id,
               a.last_updated,
               scheduled_time as order_time,
               allot_time as allot_time,
               pickUp_time as pickup_time,
               delivered_time as delivered_time,
               round(acos(sin(radians(c.latitude))*sin(radians(d.pickup_lat))+cos(radians(c.latitude))*cos(radians(d.pickup_lat))*cos(radians(c.longitude-d.pickup_long)))*6371000,2) as 'seller_to_pickup_distance_m',
               round(acos(sin(radians(a.latitude))*sin(radians(d.delivered_lat))+cos(radians(a.latitude))*cos(radians(d.delivered_lat))*cos(radians(a.longitude-d.delivered_lng)))*6371000,2) as 'locality_to_delivered_distance_m'
               from coreengine_order as a
               inner join coreengine_sfxseller as b on a.seller_id = b.id 
               inner join coreengine_pickupaddress as c on b.address_id = c.id
               inner join coreengine_ordervariables as d on a.id = d.order_id  
               inner join coreengine_cluster as e on e.id = a.cluster_id
               inner join coreengine_sfxrider as f on a.rider_id = f.id 
               inner join coreengine_riderprofile as g on f.rider_id = g.id
               where a.last_updated >"'''+str(last_updated)+'''"  and 
               b.status =1   and 
               (
                  cluster_name like "%domlur%" or 
                  cluster_name like "%btm%" or 
                  cluster_name like "%banner%" or 
                  cluster_name like "%BSK%" or 
                  cluster_name like "%jayana%"
               )and 
               cluster_name not like "%hub%" and 
               (
                  outlet_name not like "%basket%" or
                  awb like "%express%") and 
                  pickup_lat not in ('28.47','0','NULL') and 
                  delivered_lat not in ('28.47','0','NULL') and 
                  source = 0 and 
               (round(acos(sin(radians(c.latitude))*sin(radians(d.pickup_lat))+cos(radians(c.latitude))*cos(radians(d.pickup_lat))*cos(radians(c.longitude-d.pickup_long)))*6371000,2) > 200 or 
               round(acos(sin(radians(a.latitude))*sin(radians(d.delivered_lat))+cos(radians(a.latitude))*cos(radians(d.delivered_lat))*cos(radians(a.longitude-d.delivered_lng)))*6371000,2) > 1000)
               order by cluster_name, scheduled_time'''
    delivery_data = sql.read_sql(query, cnx)
    return delivery_data

if __name__ == "__main__":
    main()
