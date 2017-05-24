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
    last_timestamp = dt.datetime.utcnow() - dt.timedelta(minutes=5)
    # mappings={
    #         "dashboard": {
    #             "properties": {
    #                 "outlet_name": {"type": "string", "index": "not_analyzed"},
    #                 "cluster_name": {"type": "string", "index": "not_analyzed"},
    #                 "rider_name": {"type": "string", "index": "not_analyzed"}
    #             }
    #         }
    # }

    # es.indices.create(index='otp-data-v1',ignore=400)
    # es.indices.put_mapping(index="otp-data", doc_type="dashboard",body=mappings)


    while(1):
        OTP_data = getData(cnx, last_timestamp)
        last_timestamp = OTP_data.timestamp_updated.max()
        print 'otp-data','Current time:', dt.datetime.now(),'Dataframe shape:', OTP_data.shape
        status_dict={'index':'otp-data','Current time':dt.datetime.now(),'Dataframe shape':OTP_data.shape}
        try:
            es.index(index="kibana_status", doc_type="document", id='otp-data', body=status_dict)
        except:
            continue
        
        OTP_data['time_past_scheduling'].fillna(0)
        OTP_data['time_to_allot'].fillna(0)
        OTP_data['time_to_accept'].fillna(0)
        OTP_data['time_to_pickup'].fillna(0)
        OTP_data['time_to_deliver'].fillna(0)

        # print OTP_data.head()
        for ind in OTP_data.index:
            row = OTP_data.loc[ind]
            order_id = row.order_id
            doc_type = str(row.curr_time.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            #print "pushing index "+str(ind)
            try:
                es.index(index="otp-data", doc_type="document", id=order_id, body=row)
            except:
                continue
        time.sleep(60)

                        # convert_tz(now(),"UTC","Asia/kolkata")  AS curr_time,
                        # convert_tz(scheduled_time,"UTC","Asia/kolkata")  AS scheduled_time,
                        # convert_tz(allot_time,"UTC","Asia/kolkata")  AS allot_time,
                        # convert_tz(accept_time,"UTC","Asia/kolkata") AS accept_time,
                        # convert_tz(pickup_time,"UTC","Asia/kolkata")  AS pickup_time,
                        # convert_tz(delivered_time,"UTC","Asia/kolkata")  AS delivered_time
                
def getData(cnx,last_timestamp):
    query = '''SELECT 
                        a.order_id,
                        outlet_name,
                        b.amount,
                        b.status,
                        b.source,
                        c.chain_id,
                        receiver_mobile,
                        (CASE is_sms_sent WHEN 1 THEN 'YES' ELSE 'NO' END) AS SMS_sent,
                        (CASE is_verified WHEN 1 THEN 'YES' ELSE 'NO' END) AS is_verified,                       
                        a.otp,
                        attempts_total,
                        attempts_resend,
                        b.rider_id,
                        concat(first_name,' ',last_name) rider_name,
                        allotted_phone,
                        personal_phone,
                        is_verified,
                        acknowledgement_recvd,
                        b.cluster_id,
                        g.cluster_name,
                        now()  AS curr_time,
                        scheduled_time,
                        allot_time,
                        accept_time,
                        pickup_time,
                        delivered_time,
                        a.timestamp_verified verified_time,
                        a.timestamp_updated,
                        d.version,
                        round(acos(sin(radians(a.latitude))*sin(radians(h.delivered_lat))+cos(radians(a.latitude))*cos(radians(h.delivered_lat))*cos(radians(a.longitude-h.delivered_lng)))*6371000,2) AS del_vrfy_dist_m,                        
                        round(time_to_sec(timediff(now(),scheduled_time))/60,0) time_past_scheduling,
                        round(time_to_sec(timediff(allot_time,scheduled_time))/60,0) time_to_allot,
                        round(time_to_sec(timediff(accept_time,allot_time))/60,0) time_to_accept,
                        round(time_to_sec(timediff(pickup_time,scheduled_time))/60,0) time_to_pickup,
                        round(time_to_sec(timediff(now(),scheduled_time))/60,0) time_to_verify,
                        round(time_to_sec(timediff(delivered_time,scheduled_time))/60,0) time_to_deliver
                FROM coreengine_orderstatusverification AS a 
                    INNER JOIN coreengine_order AS b 
                        ON a.order_id = b.id 
                    INNER JOIN coreengine_sfxseller AS c 
                        ON b.seller_id = c.id 
                    INNER JOIN coreengine_sfxrider AS d 
                        ON b.rider_id =d.id 
                    INNER JOIN coreengine_riderprofile AS f 
                        ON d.rider_id = f.id 
                    INNER JOIN coreengine_cluster AS g 
                        ON g.id = b.cluster_id 
                    INNER JOIN coreengine_ordervariables AS h 
                        ON h.order_id = b.id 
                WHERE timestamp_updated>"'''+str(last_timestamp)+'''" or ((is_verified = 0 and date(b.order_time)=curdate()) AND d.version between 18 and 29)
                or (d.version >=30 and b.status in (0,1,2,3,4,8) and date(b.order_time)=curdate() ); 
              '''
    OTP_data = sql.read_sql(query, cnx)
    return OTP_data

if __name__ == "__main__":
    main()
