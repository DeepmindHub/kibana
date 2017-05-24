import pandas as pd
import mysql.connector as sqlcon
from pandas.io import sql
# import statsd
# from statsd import StatsdClient
from elasticsearch import Elasticsearch
import sys
import time
import json
import datetime as dt
import numpy as np
import os
import sys


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
import config.db_config_hl as db
import config.db_config_locationdump as db_ld



def main():
    cnx = sqlcon.connect(user=db.USER, password= db.PWD,
                         host=db.HOST, database='ebdb')
    cnx.start_transaction(isolation_level='READ COMMITTED')
    cnx2 = sqlcon.connect(user=db_ld.USER, password=db_ld.PWD,
                          host=db_ld.HOST, database='riderlocationdump')
    cnx2.start_transaction(isolation_level='READ COMMITTED')
    
    index_name="order-data"
    
    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])
    
    # mappings = {
    #       "dashboard": {
    #           "properties": {
    #               "cancel_reason_str": {"type": "string", "index": "not_analyzed"},
    #               "chain_name": {"type": "string", "index": "not_analyzed"},
    #               "cluster_name": {"type": "string", "index": "not_analyzed"},
    #               "order_issue_str": {"type": "string", "index": "not_analyzed"},
    #               "order_locality": {"type": "string", "index": "not_analyzed"},
    #               "order_payment_mode_str": {"type": "string", "index": "not_analyzed"},
    #               "order_source_str": {"type": "string", "index": "not_analyzed"},
    #               "order_status_str": {"type": "string", "index": "not_analyzed"},
    #               "outlet_name": {"type": "string", "index": "not_analyzed"},
    #               "outlet_status_str": {"type": "string", "index": "not_analyzed"},
    #               "rider_name": {"type": "string", "index": "not_analyzed"},
    #               "rider_state_str": {"type": "string", "index": "not_analyzed"},
    #               "rider_status_str": {"type": "string", "index": "not_analyzed"},
    #               "seller_city": {"type": "string", "index": "not_analyzed"},
    #               "seller_status_str": {"type": "string", "index": "not_analyzed"},
    #               "rider_location": {"type": "geo_point"},
    #               "rider_arrival_location": {"type": "geo_point"},
    #               "rider_accept_location": {"type": "geo_point"},
    #               "rider_pickup_location": {"type": "geo_point"},
    #               "rider_delivered_location": {"type": "geo_point"}
    #                         }
    #                   }
    #             }
    # es.indices.create(index=index_name,ignore=400)
    # es.indices.put_mapping(index=index_name, doc_type="dashboard",body=mappings)
    last_time = dt.datetime.utcnow() - dt.timedelta(minutes=5)
    while(1):
        order_data = getData(last_time, cnx)
        
        if len(order_data):
            # last_id = order_data.iloc[:, 0].max()
            last_time = order_data.last_updated.max()
            riders = order_data.rider_id.unique().astype(str)
            location_data = getLocation(cnx2, riders)
            location_data.columns = ['ld_rider_id', 'ld_update_timestamp','rider_location']
            order_data = order_data.merge(location_data, how='left', left_on='rider_id', right_on='ld_rider_id')
        # print 'Last update time:', str(last_time)
        print 'order_data ','Current time:', dt.datetime.now(),' Dataframe shape:', order_data.shape
        status_dict={'index':'order_data','Current time':dt.datetime.now(),'Dataframe shape':order_data.shape}
        try:
          es.index(index="kibana_status", doc_type="document", id='order_data', body=status_dict)
        except:
          continue
        
        # print "joined orderdata",order_data.shape
        for ind in order_data.index:
            row = order_data.loc[ind]
            order_id = row.order_id
            doc_type = str(row.order_time.date())
            ind = row.isnull()
            row.loc[ind] = None
            row = row.to_dict()  # json.dumps()
            #print "sending "+str(ind)
            # print row
            try:
              es.index(index=index_name, doc_type="dashboard", id=order_id, body=row)
            except Exception as e:
              print str(e)
              continue
        time.sleep(60)


def getData(last_time, cnx):
    query = '''SELECT now() curr_time,
                      o.id order_id,
                      o.locality order_locality,
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
                      o.status order_status,
                      (CASE WHEN o.payment_mode=0 THEN 'Online'
                            WHEN o.payment_mode=1 THEN 'Cash'
                            WHEN o.payment_mode=2 THEN  'Card'
                      END) order_payment_mode_str,
                      o.payment_mode order_payment_mode,
                      (CASE     WHEN o.issue=0 THEN  '-'
                                WHEN o.issue=1 THEN  'Rider_not_reachable' 
                                WHEN o.issue=2 THEN  'Rider_delayed' 
                                WHEN o.issue=3 THEN  'Rider_not_groomed_properly' 
                                WHEN o.issue=4 THEN  'Rider_not_carrying_float_money' 
                                WHEN o.issue=5 THEN  'Rider_Misbehaved' 
                                WHEN o.issue=6 THEN  'Delivery_delayed_by_rider' 
                                WHEN o.issue=7 THEN  'Rider_not_reached' 
                                WHEN o.issue=8 THEN  'Not_delivered' 
                                WHEN o.issue=9 THEN  'Rider_not_assigned' 
                                WHEN o.issue=10 THEN  'Rider_not_carrying_bag'  
                        END ) order_issue_str,
                      o.issue order_issue,
                      (CASE   WHEN o.cancel_reason=0 THEN  'Order Cancelled by Consumer'
                              WHEN o.cancel_reason=1 THEN  'No Rider Assigned'
                              WHEN o.cancel_reason=2 THEN  'Rider late for pickup'
                              WHEN o.cancel_reason=3 THEN  'Double Order Punched'
                              WHEN o.cancel_reason=4 THEN  'Other Reasons'
                              WHEN o.cancel_reason=-1 THEN  'No reason specified'
                      END) cancel_reason_str,
                      o.cancel_reason cancel_reason,
                      (CASE WHEN o.source=0 THEN  'seller' 
                            WHEN o.source=1 THEN  'manager_new' 
                            WHEN o.source=2 THEN  'manager_old' 
                            WHEN o.source=3 THEN  'bulk_upload' 
                            WHEN o.source=4 THEN  'API' 
                            WHEN o.source=5 THEN  'OneClick' 
                            WHEN o.source=6 THEN  'RequestRider' 
                            WHEN o.source=7 THEN  'PickupDrop' 
                            WHEN o.source=8 THEN  'PickupDropApis' 
                            WHEN o.source=9 THEN  'ecom' 
                            WHEN o.source=10 THEN 'Scheduled_later'
                      END) order_source_str,
                      o.source order_source,
                      o.amount order_amount,
                      o.locality order_locality,
                      o.order_time order_time,
                      o.allot_time allot_time,
                      o.accept_time accept_time,
                      o.pickup_time pickup_time,
                      o.delivered_time delivered_time,
                      sr.id rider_id,
                      o.seller_id seller_id,
                      o.accepted_flag accpeted_flag,
                      o.delivered_flag delivered_flag,
                      o.pickup_flag pickup_flag,
                      o.COID COID,
                      date(o.scheduled_time) order_date,
                      o.scheduled_time scheduled_time,
                      o.last_updated last_updated,
                      o.cluster_id cluster_id,
                      c.cluster_name cluster_name,
                      c.city seller_city,
                      (CASE   WHEN ss.status=0 THEN 'Inactive'
                                WHEN ss.status=1 THEN 'Active'
                      END) seller_status_str,
                      ss.status seller_status,
                      ss.chain_id chain_id,
                      ch.chain_name,
                      ss.category seller_category,
                      ss.city_id seller_city_id,
                      
                      ss.outlet_name outlet_name,
                      (CASE     WHEN ss.outlet_status=0 THEN  'pending'
                                WHEN ss.outlet_status=1 THEN  'approved'
                                WHEN ss.outlet_status=2 THEN  'rejected'
                       END) outlet_status_str,
                      ss.outlet_status outlet_status,
                      sr.allotted_phone rider_allotted_phone,
                      (CASE   WHEN sr.status=0 THEN 'Inactive'
                                WHEN sr.status=1 THEN 'Active'
                      END) rider_status_str,
                      sr.status rider_status,
                      (CASE   WHEN sr.rider_status=0 THEN 'returning free'
                                WHEN sr.rider_status=1 THEN 'received message'
                                WHEN sr.rider_status=2 THEN 'out for pickup'
                                WHEN sr.rider_status=3 THEN 'out for delivery'
                      END)rider_state_str,
                      sr.rider_status rider_state,
                      
                      concat(rp.first_name ," ",rp.last_name) rider_name,
                      sr.rider_id rider_profile_id,
                      concat(ov.pickup_lat,",",ov.pickup_long) rider_pickup_location,
                      concat(ov.delivered_lat,",",ov.delivered_lng) rider_delivered_location,
                      concat(ov.arrival_lat,",",ov.arrival_lng) rider_arrival_location,
                      concat(ov.acceptance_lat,",",ov.acceptance_long) rider_accept_location,
                      ov.arrival_time rider_arrival_time,
                      oi.final_charge order_final_charge,
                      oi.distance order_invoice_distance
              FROM 
                      coreengine_order o 
                      LEFT JOIN coreengine_sfxseller ss 
                        ON o.seller_id=ss.id 
                      RIGHT JOIN coreengine_sfxrider sr 
                        ON o.rider_id=sr.id 
                      LEFT JOIN coreengine_riderprofile rp 
                        ON sr.rider_id=rp.id 
                      LEFT JOIN coreengine_cluster c 
                        ON o.cluster_id=c.id 
                      LEFT JOIN coreengine_ordervariables ov 
                        ON o.id=ov.order_id 
                      LEFT JOIN payments_orderinvoicedata oi
                        ON o.id=oi.order_id 
                      LEFT JOIN coreengine_chain ch
                        ON ss.chain_id=ch.id
              WHERE 
                      o.last_updated > "'''+str(last_time)+'''"
                      AND c.cluster_name NOT LIKE '%test%';'''
    order_data = sql.read_sql(query, cnx)
    return order_data


def getLocation(cnx, riders):
    query = '''select ld.rider_id, ld.update_timestamp,
              concat(ld.latitude,",",ld.latitude) rider_location 
              from 
              coreengine_riderlocationdump ld,
              (
                select ld2.rider_id, max(ld2.update_timestamp) update_timestamp 
                from coreengine_riderlocationdump ld2 
                where ld2.rider_id in (''' + ','.join(riders) + ''') 
                group by 1 
                having update_timestamp>=now() - interval 15 minute
              ) t 
              where ld.rider_id=t.rider_id and 
              ld.update_timestamp=t.update_timestamp 
              group by 1,2;'''
    return sql.read_sql(query, cnx)

if __name__ == "__main__":
    main()
