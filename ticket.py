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
    # last_timestamp = dt.datetime.utcnow() - dt.timedelta(minutes=5)
    mappings={
            "dashboard": {
                "properties": {
                    "outlet_name": {"type": "string", "index": "not_analyzed"},
                    "cluster_name": {"type": "string", "index": "not_analyzed"},
                    "rider_name": {"type": "string", "index": "not_analyzed"},
                    "description": {"type": "string", "index": "not_analyzed"}
                }
            }
    }

    es.indices.create(index='ticket-data',ignore=400)
    es.indices.put_mapping(index="ticket-data", doc_type="dashboard",body=mappings)


    while(1):
        ticket_data = getData(cnx)
        # last_timestamp = OTP_data.timestamp_updated.max()
        print 'ticket_data ','Current time:', dt.datetime.now(),'Dataframe shape:', ticket_data.shape
        status_dict={'index':'ticket_data','Current time':dt.datetime.now(),'Dataframe shape':ticket_data.shape}
        try:
            es.index(index="kibana_status", doc_type="document", id='ticket_data', body=status_dict)
        except:
            continue
        
        # OTP_data['time_past_scheduling'].fillna(0)
        # OTP_data['time_to_allot'].fillna(0)
        # OTP_data['time_to_accept'].fillna(0)
        # OTP_data['time_to_pickup'].fillna(0)
        # OTP_data['time_to_deliver'].fillna(0)

        # print OTP_data.head()
        for ind in ticket_data.index:
            row = ticket_data.loc[ind]
            ticket_id = row.ticket_id
            # doc_type = str(row.curr_time.date())
            ind = row.isnull()
            row[ind] = None
            row = row.to_dict()  
            #print "pushing index "+str(ind)
            try:
                es.index(index="ticket-data", doc_type="document", id=ticket_id, body=row)
            except:
                continue
        time.sleep(180)

                       
def getData(cnx):
    query = '''SELECT 
                t.id ticket_id,
                t.date_raised,
                t.date_raised order_date,
                t.status,
                (CASE t.status 
                WHEN 1 THEN 'Pending'
                WHEN 2 THEN 'Approved'
                WHEN 3 THEN 'Revoked'
                WHEN 4 THEN 'SuperRevoked'
                END) status_str,
                t.approved_date,
                t.type_ticket,
                (CASE t.type_ticket WHEN 1 THEN 'Rider'
                WHEN 2 THEN 'Seller'
                END) type_ticket_str,
                t.flag,
                (CASE t.flag 
                WHEN 1 THEN 'Unresolved'
                WHEN 2 THEN 'Resolved'
                WHEN 3 THEN 'Closed'
                END) flag_str,
                t.closed_date,
                (CASE t.expired WHEN 1 THEN 'yes'
                WHEN 2 THEN 'No'
                END) expired,
                t.rider_id,
                concat(rp.first_name," ",rp.last_name) rider_name,
                t.seller_id,
                ss.outlet_name,
                t.amount,
                t.cluster_id,
                cl.cluster_name,
                cl.city,
                t.category_id,
                tc.description 
                FROM coreengine_tickets t 
                LEFT JOIN coreengine_ticketcategory tc ON t.category_id=tc.id
                LEFT JOIN coreengine_cluster cl ON t.cluster_id =cl.id
                LEFT JOIN coreengine_sfxseller ss ON t.seller_id=ss.id
                LEFT JOIN coreengine_sfxrider sr ON t.rider_id=sr.id
                LEFT JOIN coreengine_riderprofile rp ON sr.rider_id=rp.id
                WHERE closed_date > curdate() - INTERVAL 7 DAY

              '''
    ticket_data = sql.read_sql(query, cnx)
    return ticket_data

if __name__ == "__main__":
    main()
