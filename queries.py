from elasticsearch.helpers import scan
import helpers as hp
import pandas as pd

import logging
logging.basicConfig(format = '%(asctime)s %(message)s',
                    datefmt = '%m/%d/%Y %I:%M:%S %p',
                    filename = './progress.log',
                    level=logging.INFO)

def queryIndex(datefrom, dateto, idx):
    query = {
        "query": {
            "bool": {
                    "filter": [
                    {
                        "range": {
                            "timestamp": {
                            "gte": datefrom,
                            "lt": dateto
                            }
                        }
                    }
                ]
            }
        }
      }
    try:
        # print(idx, str(query).replace("\'", "\""))
        data = scan(client=hp.es,index=idx,query=query)
        ret_data = {}
        count = 0
        last_entry = 0

        for item in data:
            if not count%50000 and count>0:
                logging.info(idx,count)
            ret_data[count] = item
            ret_data[count] = ret_data[count]['_source']
            count+=1


        return ret_data
    except Exception as e:
        print(traceback.format_exc())
