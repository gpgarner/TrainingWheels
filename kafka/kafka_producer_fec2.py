import random
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import json
import csv
import pickle
import glob
import config

def file_number(i):
	if (i < 10):
                str_counter = "0" + str(i)
        elif (i<90):
                str_counter = str(i)
        elif ((i-90)<10):
                str_counter = "900"+str(i-90)
        elif ((i-90)<100):
                str_counter = "90"+str(i-90)
        else:
                str_counter = "9"+str(i-90)

	return str_counter

def main():
    	producer = KafkaProducer(bootstrap_servers=["localhost:9092"],value_serializer= lambda v: json.dumps(v).encode())
        

	headers = ['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',       \
                   'TRANSACTION_TP','ENTITY_TP','NAME', 'CITY', 'STATE', 'ZIP_CODE',       \
                   'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT','TRANSACTION_AMT',           \
                   'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'     ]
	split_counter = len(glob.glob('/home/ubuntu/manip_data/split_*'))
	
	for j in range(1):
		for i in range(split_counter):
			with open('/home/ubuntu/manip_data/split_'+file_number(i)) as f:
				reader = csv.reader(f, delimiter='|')
			
				for row in reader:
					row = {h:x for h,x in zip(headers,row)}
					producer.send('datatwo', row)

	producer.flush()
	producer.close()
                
if __name__ == "__main__":
    main()

