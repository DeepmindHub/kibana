import pandas as pd
from pandas.io import sql
from elasticsearch import Elasticsearch
import elasticsearch
import time
import datetime as dt
import numpy as np
import os
import sys


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
import config.db_config_hl as db
from utility_scripts import utils

DEFAULT_PICKUP_SLA = 15
DEFAULT_DELIVERY_SLA = 45

def main():
    cnx = utils.get_cnx(db, read_committed=True)

    es = Elasticsearch(['################'])
    # create_kibana_index(es)

    last_updated = None
    
    while(1):
        clean_order_data = get_clean_order_data(cnx,last_updated)
        if not len(clean_order_data):
            continue
        last_updated = clean_order_data.last_updated.max()
        clean_order_data['order_time'] = pd.to_datetime(clean_order_data.order_time)
        clean_order_data['pickup_time'] = pd.to_datetime(clean_order_data.pickup_time)
        clean_order_data['delivered_time'] = pd.to_datetime(clean_order_data.delivered_time)
        clean_order_data['last_updated'] = pd.to_datetime(clean_order_data.last_updated)
        clean_order_data['last_called_at'] = pd.to_datetime(clean_order_data.last_called_at)
        clean_order_data.loc[:,'pickup_sla'].fillna(DEFAULT_PICKUP_SLA, inplace=True)
        clean_order_data.loc[:,'delivered_sla'].fillna(DEFAULT_DELIVERY_SLA, inplace=True)

        clean_order_data['met_pickup_sla'] = ((clean_order_data.pickup_time - clean_order_data.order_time) /
                                             np.timedelta64(1, 'm') < clean_order_data.pickup_sla).astype(int)
        clean_order_data['met_delivery_sla'] = ((clean_order_data.delivered_time - clean_order_data.order_time) /
                                             np.timedelta64(1, 'm') < clean_order_data.delivered_sla).astype(int)
        clean_order_data['met_sla'] = (clean_order_data.met_pickup_sla & clean_order_data.met_delivery_sla).astype(int)
        # print clean_order_data

        for ind in clean_order_data.index:
            order = clean_order_data.loc[ind]
            ind = order.isnull()
            order.loc[ind] = None
            row = order.to_dict()
            # print row
            try:
                es.index(index="clean_orders", doc_type="dashboard", id=order.order_id, body=row)
            except elasticsearch.exceptions.ConnectionTimeout, e:
              print e
              print row
              print len(clean_order_data), 'records in clean orders data'
            except elasticsearch.exceptions.RequestError, e:
                print row
                raise e
        time.sleep(60)


def get_clean_order_data(cnx,last_updated):
    query = '''select o.id order_id, date(o.order_time) order_date, o.seller_id, ss.outlet_name, ss.chain_id,
            o.cluster_id, c.cluster_name, c.city, o.rider_id, concat(rp.first_name, ' ', rp.last_name) rider_name,
            o.order_time, o.allot_time, o.pickup_time, o.delivered_time, sla.pickup_sla, sla.delivered_sla,
             o.last_updated, sum(if(ec.call_to='rider',1,0)) rider_calls, sum(if(ec.call_to='seller',1,0)) seller_calls,
             max(ec.created) last_called_at from coreengine_order o join coreengine_cluster c on o.cluster_id=c.id
            join coreengine_sfxseller ss on o.seller_id=ss.id left join coreengine_sfxrider sr on o.rider_id=sr.id
            left join coreengine_riderprofile rp on sr.rider_id=rp.id left join merchant_sla sla on
            ss.chain_id=sla.chain_id left join coreengine_exotelcalls ec on o.id=ec.order_id_id where
            date(o.order_time)=curdate() group by o.id having
    '''
    if last_updated:
        query += "last_updated>'{0}' or last_called_at>'{0}';".format(last_updated)
    else:
        query += "order_time>now() - interval 5 minute;"
    return sql.read_sql(query, cnx)


def create_kibana_index(es):
    mappings={
        "mappings": {
            "dashboard": {
                "properties": {
                    "outlet_name": {"type": "string", "index": "not_analyzed"},
                    "cluster_name": {"type": "string", "index": "not_analyzed"},
                    "rider_name": {"type": "string", "index": "not_analyzed"},
                    "address": {"type": "string", "index": "not_analyzed"}
                }
            }
        }
    }
    es.indices.create(index='clean_orders', body=mappings)


if __name__ == "__main__":
    main()
