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
    cnx2 = sqlcon.connect(user=dbl.USER, password=dbl.PWD, database=dbl.DATABASE,
                                  host=dbl.HOST)
    cnx2.start_transaction(isolation_level='READ COMMITTED')
    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])

    index_name="low_performance"
    mappings={
            "dashboard": {
                "properties": {
                    "cluster_name": {"type": "string", "index": "not_analyzed"},
                    "rider_name": {"type": "string", "index": "not_analyzed"},
                    "attendance_status": {"type": "string", "index": "not_analyzed"},
                    "performance": {"type": "string", "index": "not_analyzed"},
                    "shift_type": {"type": "string", "index": "not_analyzed"},
                    "Session": {"type": "string", "index": "not_analyzed"},
                    "ATL": {"type": "string", "index": "not_analyzed"},
                    "rider_location": {"type": "geo_point"},
                    "cluster_location": {"type": "geo_point"}
                }
            }
    }

    es.indices.create(index=index_name,ignore=400)
    es.indices.put_mapping(index=index_name, doc_type="dashboard",body=mappings)

    last_updated = dt.date.today()
    
    while(1):
        lp_data = getData(cnx,last_updated)
        riders=tuple(lp_data.rider_id.tolist())
        loc_data = get_locdata(cnx2,riders)
        lp_data = lp_data.merge(loc_data,how='left',on='rider_id')
        lp_data['rider_lat']=lp_data['rider_lat'].astype(float)
        lp_data['rider_lng']=lp_data['rider_lng'].astype(float)
        lp_data['cluster_lng']=lp_data['cluster_lng'].astype(float)
        lp_data['cluster_lat']=lp_data['cluster_lat'].astype(float)
  
        lp_data['rider_cluster_dist']=lp_data.apply(lambda row:haversine(row['rider_lng'],row['rider_lat'],row['cluster_lng'],row['cluster_lat']),axis=1)
        # last_timestamp = attendance_data.timestamp_updated.max()
        lp_data['order_date'] = pd.to_datetime(lp_data.order_date)
        print index_name,'Current time:', dt.datetime.now(),'Dataframe shape:', lp_data.shape
        status_dict={'index':index_name,'Current time':dt.datetime.now(),'Dataframe shape':lp_data.shape}
        try:
          es.index(index="kibana_status", doc_type="dashboard", id=index_name, body=status_dict)
        except:
          continue
        # last_updated = lp_data.last_updated.max()
            
        for ind in lp_data.index:
            row = lp_data.loc[ind]
            order_id = str(row.rider_id)
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
    query = '''
            SELECT *,now() curr_time FROM 
            (SELECT sr.id rider_id,
            concat(rp.first_name,' ',rp.last_name) rider_name,
            rp.role,
            sr.date_of_join,
            (CASE rp.shift_type WHEN 0 THEN '4_hour_shift'
            WHEN 1 THEN '9_hour_shift'
            WHEN 2 THEN '11_hour_shift'
            WHEN 3 THEN 'Break_shift'
            ELSE NULL end
            ) shift_type,
            sr.allotted_phone,
            cl.cluster_name,
            sr.cluster_id,
            (CASE cl.cluster_name 
                                        WHEN "Domlur" THEN 'Abdul' 
                                        WHEN "Indira Nagar" THEN 'Abdul'
                                        WHEN "Kammanahalli" THEN 'Abdul'
                                        WHEN "Koramangala" THEN 'Abdul'
                                        WHEN "Bannerghatta" THEN 'Harish'
                                        WHEN "BSK" THEN 'Harish'
                                        WHEN "BTM - Bangalore" THEN 'Harish'
                                        WHEN "Bellandur" THEN 'Naveed'
                                        WHEN "Marathahalli" THEN 'Naveed'
                                        WHEN "HSR Layout" THEN 'Naveed'
                                        WHEN "Whitefield" THEN 'Patrick'
                                        WHEN "mahadevpura" THEN 'Patrick' END) ATL,
          cl.city,
          cl.latitude cluster_lat,
          cl.longitude cluster_lng,
          concat(cl.latitude,",",cl.longitude) cluster_location,
          (CASE WHEN rp.role='FT' AND t.order_count<8 THEN 'low_performing'
                  WHEN rp.role='PRT' AND t.order_count<4 THEN 'low_performing' 
                WHEN rp.role='FT' AND t.order_count>7 THEN 'performing'
                WHEN rp.role='PRT' AND t.order_count>3 THEN 'performing'
                  ELSE 'no_orders' END) performance,
            (CASE WHEN rs.out_time IS NULL AND DATE(rs.in_time)'''+time_interval+''' THEN 'Session_ON'
            ELSE 'Session_OFF' END) 'Session',
            t.order_date,
            COALESCE(t.order_count, 0 ) order_count,
            COALESCE(t.cancelled_order, 0 ) cancelled_order,
            t1.attendance_status,
            t1.total_working_hours
            FROM coreengine_sfxrider sr 
            LEFT JOIN 
            (SELECT rider_id,
            attendancedate,
            total_working_hours,
            CASE ra.status WHEN 0 THEN 'Present' 
                                        WHEN 1 THEN 'LWA' 
                                        WHEN 2 THEN 'LWOA' 
                                        WHEN 3 THEN 'sick LEAVE WITH approval' 
                                        WHEN 4 THEN 'sick LEAVE without approval'  
                                        WHEN 5 THEN 'Weekly off' 
                                        WHEN -1 THEN 'NOT Marked'
                                        ELSE '-' END AS attendance_status        
             FROM coreengine_riderattendance ra WHERE attendancedate'''+time_interval+''') t1
             ON sr.id=t1.rider_id
            LEFT JOIN 
            (SELECT rider_id ,
            DATE(o.scheduled_time) order_date,
            count(id) order_count,
            sum(CASE WHEN o.status IN (302,403,503) THEN 1 ELSE 0 END) 
            cancelled_order  
            FROM coreengine_order o 
            WHERE DATE(o.scheduled_time)'''+time_interval+''' GROUP BY 1
            ) t
            ON sr.id=t.rider_id
            LEFT JOIN coreengine_cluster cl 
            ON sr.cluster_id=cl.id
            LEFT JOIN coreengine_riderprofile rp
            ON sr.rider_id=rp.id
            LEFT JOIN coreengine_ridersession rs
            ON rs.rider_id=sr.id AND rs.out_time IS NULL AND DATE(rs.in_time)'''+time_interval+'''
             WHERE cl.city ='BLR'
             AND cluster_name NOT LIKE '%test%'
             AND rp.first_name NOT LIKE '%dummy%'
             AND cluster_name NOT LIKE '%hub%'
             AND sr.status=1
             GROUP BY 1) rd
             LEFT JOIN 
             (
             SELECT 
             COALESCE(count(o.id), 0 ) 'cluster_orders',
             count(DISTINCT(o.rider_id)) 'cluster_riders',
             round(count(o.id)/count(DISTINCT(o.rider_id)),1) 'cluster_avg',
             cl.id cluster_id
             FROM coreengine_order o
             LEFT JOIN coreengine_cluster cl 
             ON o.cluster_id=cl.id 
             LEFT JOIN coreengine_sfxrider sr
             ON o.rider_id=sr.id
             LEFT JOIN coreengine_riderprofile rp
             ON sr.rider_id=rp.id
             LEFT JOIN coreengine_sfxseller ss
             ON o.seller_id=ss.id
             WHERE cl.city='BLR'
             AND cluster_name NOT LIKE '%test%'
             AND cluster_name NOT LIKE '%hub%'
             AND o.source!=9
             AND ss.outlet_name NOT LIKE '%reverse%'
             AND ss.chain_id!=85
             AND DATE(o.scheduled_time)'''+time_interval+'''
             AND o.status IN (0,1,2,3,4,5,6,8)
             GROUP BY cluster_name
             ) cd
             ON rd.cluster_id=cd.cluster_id
            ;
            '''
    lp_data = sql.read_sql(query, cnx)
    # print lp_data.shape
    return lp_data

def get_locdata(cnx2,riders):
  query='''SELECT rld.rider_id,
                  rld.latitude rider_lat,
                  rld.longitude rider_lng,
                  concat(rld.latitude,",",rld.longitude) rider_location
          FROM coreengine_riderlocationdump rld 
          INNER JOIN (
                  SELECT max(id) loc_id,
                          rider_id 
                  FROM coreengine_riderlocationdump 
                  WHERE DATE(update_timestamp)'''+time_interval+'''
                  and rider_id in '''+str(riders)+'''
                  GROUP BY 2
                    )t ON t.loc_id=rld.id;
        '''
  loc_data = sql.read_sql(query, cnx2)
  return loc_data

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    m = 6367000 * c
    return m  

if __name__ == "__main__":
    main()
