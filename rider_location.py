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
import config.db_config_locationdump as dbConfig



def main():
    cnx = sqlcon.connect(user=dbConfig.USER, password=dbConfig.PWD,
                         host=dbConfig.HOST, database=dbConfig.DATABASE)
    cnx.start_transaction(isolation_level='READ COMMITTED')

    cnx2 = sqlcon.connect(user=db.USER, password=db.PWD, database='ebdb',
                                  host=db.HOST)
    cnx2.start_transaction(isolation_level='READ COMMITTED')

    es = Elasticsearch(['http://ec2-107-23-40-43.compute-1.amazonaws.com:9200/'])
    update_timestamp=dt.datetime.utcnow() - dt.timedelta(minutes=5)
    while(1):
        rider_data = getData(cnx,update_timestamp)
        rider_data['update_timestamp']=pd.to_datetime(rider_data.update_timestamp)
        if len(rider_data):
          update_timestamp=rider_data.update_timestamp.max()
        rider_data.columns = ['current_time','rider_id','update_timestamp','latitude','longitude','rider_location_v1']
        # print 
        # print 'Dataframe shape:', rider_data.shape
        # print 'update_timestamp',update_timestamp
        if len(rider_data):
          riders=rider_data.rider_id.unique().astype(str)
          cluster_data=get_cluster(cnx2,riders)
          cluster_data.columns=['cl_rider_id','cl_cluster_id','cl_cluster_name','city','city_id','allotted_phone','status','app_version','in_time','out_time','rider_name','rider_session']
          cluster_data['in_time'] = pd.to_datetime(cluster_data.in_time)
          cluster_data['out_time'] = pd.to_datetime(cluster_data.out_time)
          
          rider_data=rider_data.merge(cluster_data, how='left', left_on='rider_id', right_on='cl_rider_id')
        print 'rider-location','Current time:', dt.datetime.now(),'Dataframe shape:', rider_data.shape
        status_dict={'index':'rider-location','Current time':dt.datetime.now(),'Dataframe shape':rider_data.shape}
        try:
          es.index(index="kibana_status", doc_type="document", id='rider-location', body=status_dict)
        except:
          continue
        
        for ind in rider_data.index:
            row = rider_data.loc[ind]
            rider_id = row.rider_id
            doc_type = str(row.update_timestamp.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            # print "pushing index ", row
            try:
              es.index(index="rider-location", doc_type="document", id=rider_id, body=row)
            except:
              continue
        time.sleep(60)

def getData(cnx,update_timestamp):
    query = '''select now() cur_time, ld.rider_id, ld.update_timestamp, ld.latitude, ld.longitude ,
              concat(ld.latitude,",",ld.longitude) rider_location1
              from 
              coreengine_riderlocationdump ld,
              (
                select ld2.rider_id, max(ld2.update_timestamp) update_timestamp 
                from coreengine_riderlocationdump ld2  
                group by 1 
              ) t 
              where ld.rider_id=t.rider_id and 
              ld.update_timestamp=t.update_timestamp 
              # and ld.update_timestamp between"'''+str(update_timestamp)+'''" and now()      
              group by 1,2;'''
    # and ld.update_timestamp>"'''+str(update_timestamp)+'''"
    rider_data = sql.read_sql(query, cnx)
    return rider_data


def get_cluster(cnx2,riders):
    query='''SELECT sf.id rider_id,
                    c.id cluster_id,
                    c.cluster_name,
                    c.city,
                    c.fk_city_id,
                    sf.allotted_phone,
                    sf.status,
                    sf.version,
                    p.in_time,
                    p.out_time,
                    concat(first_name," " ,last_name) AS rider_name,
                    (CASE WHEN rs2.out_time IS NULL THEN 1 ELSE 0 END) ridersession
              FROM coreengine_sfxrider sf INNER JOIN coreengine_cluster c 
                                  ON c.id=sf.cluster_id ,
                  (
                    SELECT rs.rider_id, 
                           max(rs.in_time) in_time,
                           max(rs.out_time) out_time
                    FROM coreengine_ridersession rs 
                    GROUP BY 1
                  )  p, coreengine_ridersession rs2,
                  coreengine_riderprofile rp
              WHERE p.rider_id = sf.id AND 
                    p.in_time=rs2.in_time AND 
                    p.rider_id=rs2.rider_id AND
                    rp.id=sf.rider_id AND 
                    sf.id IN  ('''+','.join(riders)+''')'''
    cluster_data=sql.read_sql(query,cnx2)
    # print cluster_data
    return cluster_data

if __name__ == "__main__":
    main()
