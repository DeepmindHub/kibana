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
import config.db_config_ecom as db

def main():
    cnx = sqlcon.connect(user=db.USER, password=db.PWD, database=db.DATABASE,
                                  host=db.HOST)
    cnx.start_transaction(isolation_level='READ COMMITTED')
    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])
    last_updated=dt.datetime.utcnow() - dt.timedelta(days=1)
    
    while(1):
        ecom_data = getData(cnx,last_updated)
        last_updated=ecom_data.last_updated.max()
        
        # ecom_data['onduty_time'] = pd.to_datetime(ecom_data.onduty_time)
        print 'ecom_data ','Current time:', dt.datetime.now(),' Dataframe shape:', ecom_data.shape
        status_dict={'index':'ecom_data','Current time':dt.datetime.now(),'Dataframe shape':ecom_data.shape}
        try:
            es.index(index="kibana_status", doc_type="document", id='ecom_data', body=status_dict)
        except:
            continue
        
        for ind in ecom_data.index:
            row = ecom_data.loc[ind]
            ecom_id = row.delivery_request_id
            doc_type = "dashboard"
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            # print "pushing index "+str(ind)
            try:
                es.index(index="ecom_data", doc_type="dashboard", id=ecom_id, body=row)
            except:
                continue
        time.sleep(60)


def getData(cnx,last_updated):
    query = '''SELECT 
                dr.id delivery_request_id,
                dr.client_order_id,
                dr.awb_number,
                dr.customer_city,
                dr.customer_state,
                dr.cluster_id,
                dr.order_date,
                dr.scheduled_date,
                ( CASE   
                    WHEN dr.order_status= 0 THEN "NEW" 
                    WHEN dr.order_status= 1 THEN "TO_BE_ASSIGNED" 
                    WHEN dr.order_status= 2 THEN "ASSIGNED" 
                    WHEN dr.order_status= 3 THEN "OUT_FOR_DELIVERY" 
                    WHEN dr.order_status= 4 THEN "DELIVERED" 
                    WHEN dr.order_status= 5 THEN "TO_BE_RETURNED" 
                    WHEN dr.order_status= 6 THEN "CLOSED" 
                    WHEN dr.order_status= 7 THEN "ASSIGNED_TO_CLUSTER" 
                    WHEN dr.order_status= 8 THEN "LOST" 
                    WHEN dr.order_status= 9 THEN "RECEIVED_AT_DC" 
                    WHEN dr.order_status= 10 THEN "SCHEDULED" 
                    WHEN dr.order_status= 11 THEN "CID" 
                    WHEN dr.order_status= 12 THEN "CANCELLED" 
                    WHEN dr.order_status= 13 THEN "NOT_CONTACTABLE" 
                    WHEN dr.order_status= 14 THEN "NOT_ATTEMPTED" 
                    WHEN dr.order_status= 15 THEN "RECEIVED_AT_HUB" 
                    WHEN dr.order_status= 16 THEN "RETURNED_TO_CLIENT" 
                    WHEN dr.order_status= 17 THEN "RTO" 
                    WHEN dr.order_status= 18 THEN "RTD" 
                    WHEN dr.order_status= 19 THEN "NDR_DEL" 
                END) order_status_str,
                dr.order_status,
                ( CASE   
                    WHEN dr.order_source= 0 THEN "ecom"  
                    WHEN dr.order_source= 1 THEN "API" 
                END ) order_source_str,
                dr.order_source,
                dr.creation_date,
                dr.last_updated,
                dr.client_id,
                dr.dispatch_center,
                er.rider_id,
                dr.runsheet_id,
                hub.name hub_name,
                dc.name dispatch_center_name 
                FROM 
                ecommerce_deliveryrequest dr,
                ecommerce_rider er,
                ecommerce_hub hub,
                ecommerce_dispatchcenter dc
                WHERE dr.last_updated>"'''+str(last_updated)+'''" 
                AND dr.hub_id=hub.id 
                AND dr.rider_id=er.id
                and dr.dispatch_center=dc.id; 
                '''
    # print query
    ecom_data = sql.read_sql(query, cnx)
    # print ecom_data.shape
    drids=tuple(ecom_data['delivery_request_id'].tolist())
    query2= '''
                SELECT sl.new_state,(CASE   
                WHEN sl.new_state= 0 THEN "NEW"  
                WHEN sl.new_state= 1 THEN "TO_BE_ASSIGNED" 
                WHEN sl.new_state= 2 THEN "ASSIGNED" 
                WHEN sl.new_state= 3 THEN "OUT_FOR_DELIVERY" 
                WHEN sl.new_state= 4 THEN "DELIVERED" 
                WHEN sl.new_state= 5 THEN "TO_BE_RETURNED" 
                WHEN sl.new_state= 6 THEN "CLOSED" 
                WHEN sl.new_state= 7 THEN "ASSIGNED_TO_CLUSTER" 
                WHEN sl.new_state= 8 THEN "LOST" 
                WHEN sl.new_state= 9 THEN "RECEIVED_AT_DC" 
                WHEN sl.new_state= 10 THEN "SCHEDULED" 
                WHEN sl.new_state= 11 THEN "CID" 
                WHEN sl.new_state= 12 THEN "CANCELLED" 
                WHEN sl.new_state= 13 THEN "NOT_CONTACTABLE" 
                WHEN sl.new_state= 15 THEN "RECEIVED_AT_HUB" 
                WHEN sl.new_state= 14 THEN "NOT_ATTEMPTED" 
                WHEN sl.new_state= 16 THEN "RETURNED_TO_CLIENT" 
                WHEN sl.new_state= 17 THEN "RTO" 
                WHEN sl.new_state= 18 THEN "RTD" 
                WHEN sl.new_state= 19 THEN "NDR_DEL" 
                END) status_str,
                sl.action_time,
                sl.delivery_request_id
                FROM ecommerce_deliveryrequeststatelog sl 
                WHERE sl.delivery_request_id IN '''+str(drids)
    
    state_log_data=sql.read_sql(query2,cnx)
    state_log_pivot=pd.pivot_table(state_log_data,values='action_time',columns='status_str',index='delivery_request_id',aggfunc=np.max)
    state_log_pivot.fillna(0, inplace=True)
    state_log_pivot.reset_index(inplace=True)
    ecom_data=ecom_data.merge(state_log_pivot, how='left', on='delivery_request_id')
    return ecom_data

if __name__ == "__main__":
    main()
