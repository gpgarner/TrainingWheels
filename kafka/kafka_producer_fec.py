import random
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import json
import csv
import pickle

import config

def main():

    	producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVERS,value_serializer= lambda v: json.dumps(v).encode())
        
        headers = ['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP','ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT','TRANSACTION_AMT', 'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID']

        with open('/home/ubuntu/2016/by_date/itcont_2016_10151005_20150726.txt') as f:
               data = f.readlines()

        data = [x.split("|") for x in data]
        #producer = KafkaProducer(bootstrap_servers = '18.205.181.166:9092',value_serializer = lambda v: json.dumps(v).encode('utf-8'))
	
        for row in data:
               	row = {h:x for h,x in zip(headers,row)}
		#print row
		producer.send('data', row)
		#print row

	producer.flush()
	producer = KafkaProducer(retries=5)
                
if __name__ == "__main__":
    main()

