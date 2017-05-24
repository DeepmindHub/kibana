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
    last_timestamp = dt.datetime.utcnow() - dt.timedelta(minutes=5)
    
    while(1):
        attendance_data = getData(cnx)
        # last_timestamp = attendance_data.timestamp_updated.max()
        attendance_data['onduty_time'] = pd.to_datetime(attendance_data.onduty_time)
        print 'rider-attendance-v1','Current time:', dt.datetime.now(),'Dataframe shape:', attendance_data.shape
        status_dict={'index':'rider-attendance-v1','Current time':dt.datetime.now(),'Dataframe shape':attendance_data.shape}
        try:
            es.index(index="kibana_status", doc_type="document", id='rider-attendance-v1', body=status_dict)
        except:
            continue
        
        for ind in attendance_data.index:
            row = attendance_data.loc[ind]
            rider_id = str(row.rider_id)+str(row.order_date)
            doc_type = str(row.onduty_time.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            # print "pushing index "+str(ind)
            try:
                es.index(index="rider-attendance-v1", doc_type="document", id=rider_id, body=row)
            except:
                continue
        # exit(-1)
        time.sleep(60)


def getData(cnx):
    query = '''    SELECT  
                    now() cur_time,
                    operational_city,
                    e.city seller_city,
                    cluster_name,
                    a.id AS rider_id,
                    concat(first_name," " ,last_name) AS rider_name,
                    allotted_phone,
                    expected_intime AS expected_intime,
                    actual_intime AS actual_intime,
                    in_time AS onduty_time,
                    expected_outtime AS expected_outtime,
                    actual_outtime AS actual_outtime,
                    out_time AS offduty_time, 
                CASE c.status WHEN 0 THEN 'Present' 
                            WHEN 1 THEN 'LWA' 
                            WHEN 2 THEN 'LWOA' 
                            WHEN 3 THEN 'sick leave with approval' 
                            WHEN 4 THEN 'sick leave without approval'  
                            WHEN 5 THEN 'Weekly off' 
                            ELSE 'Not Marked' END AS Attendance_Status,
                IF( time_to_sec(timediff(expected_intime, in_time))/60 >= 15 ,'Early Intime',IF(time_to_sec(timediff(expected_intime, in_time))/60 <= -15 ,'Late Intime','')) AS 'Intime Adherence', 
                IF( time_to_sec(timediff(expected_outtime, out_time))/60 >= 15 ,'Early outtime',IF(time_to_sec(timediff(expected_outtime, out_time))/60 <= -15 ,'Late outtime','')) AS 'outtime Adherence',
                IF( time_to_sec(timediff(now(), expected_intime)) > 15 AND in_time IS NULL ,'Not marked yet','') AS 'Intime not marked',
                IF( time_to_sec(timediff(now(), expected_outtime)) > 15 AND out_time IS NULL ,'Not marked yet','') AS 'Outtime not marked',
                a.version appversion,
                d.geofence_flag,
                c.attendancedate order_date,
                c.attendancedate order_time,
                timestamp_updated
                
                FROM coreengine_sfxrider AS a
                LEFT OUTER JOIN coreengine_riderprofile AS b  
                    ON a.rider_id = b.id 
                LEFT OUTER JOIN coreengine_riderattendance AS c  
                    ON a.id = c.rider_id AND c.attendancedate = curdate() 
                LEFT OUTER JOIN coreengine_ridersession AS d 
                    ON d.rider_id = c.rider_id AND DATE(d.in_time) = c.attendancedate 
                LEFT OUTER JOIN coreengine_cluster AS e 
                    ON e.id = a.cluster_id
                LEFT OUTER JOIN (SELECT rider_id, max(in_time) max_intime FROM coreengine_ridersession GROUP BY rider_id)rs2
                    ON rs2.rider_id=d.rider_id AND DATE(rs2.max_intime)=in_time
                WHERE a.status =1 AND 
                      cluster_name NOT LIKE "%hub%" AND 
                      a.cluster_id NOT IN (1,19) AND
                      first_name NOT LIKE "%dummy%"
                      and c.attendancedate=curdate()
                ORDER BY 1,2,3,8,4,12;'''


    attendance_data = sql.read_sql(query, cnx)
    # print "attendance_data"
    # print attendance_data.shape
    riders=tuple(attendance_data['rider_id'].tolist())
    
    print "riders"
    print len(riders)
    query2='''      SELECT rider_id,(CASE     WHEN o.issue=0 THEN  '-'
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
                        END ) issue_description,DATE(o.scheduled_time) modified_at   
                    FROM coreengine_order o
                    WHERE rider_id IN '''+str(riders)+'''
                    and DATE(o.scheduled_time)=curdate()
                    '''
    issue_data = sql.read_sql(query2, cnx)
    # print "issue_data"
    # print issue_data.shape
    
    issue_time_pivot=pd.pivot_table(issue_data,values='modified_at',columns='issue_description',index='rider_id',aggfunc=np.max)
    issue_time_pivot.columns = issue_time_pivot.columns.map(lambda x: str(x) + '_time')
    issue_time_pivot[[]].apply(lambda x:pd.to_datetime(x),axis=0)
    
    # print "issue_time_pivot"
    # print issue_time_pivot.shape
    issue_desc_pivot=pd.pivot_table(issue_data,values='modified_at',columns='issue_description',index='rider_id',aggfunc='count')
    issue_desc_pivot.columns = issue_desc_pivot.columns.map(lambda x: str(x) + '_count')
    
    # print "issue_desc_pivot"
    # print issue_desc_pivot.shape
    # issue_time_pivot.fillna(0, inplace=True)
    issue_time_pivot.reset_index(inplace=True)
    # issue_desc_pivot.fillna(0, inplace=True)
    issue_desc_pivot.reset_index(inplace=True)
    attendance_data=attendance_data.merge(issue_time_pivot, how='left', on='rider_id')
    attendance_data=attendance_data.merge(issue_desc_pivot, how='left', on='rider_id')
    
    # print "attendance_data"
    # print attendance_data.shape
    return attendance_data

if __name__ == "__main__":
    main()
