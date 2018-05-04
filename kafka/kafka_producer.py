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

class Producer(object):
	
	def __init__(self,addr):
		self.producer = KafkaProducer(	bootstrap_servers=addr,					\
						 value_serializer= lambda v: json.dumps(v).encode()	)

	def produce_msgs(self, source_symbol):
        
		headers = [	'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',	\
				'TRANSACTION_TP','ENTITY_TP','NAME', 'CITY', 'STATE', 'ZIP_CODE',	\
				'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT','TRANSACTION_AMT',		\
 				'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'	]

		with open('/home/ubuntu/2016/by_date/itcont_2016_10151005_20150726.txt') as f:
			#data = f.readlines()
			reader = csv.reader(f, delimiter='|')
			
			for row in reader:
				row = {h:x for h,x in zip(headers,row)}
				print row
				self.producer.send('data', row)
        	#data = [x.split("|") for x in data]
	
        	#for row in data:
               	#	row = {h:x for h,x in zip(headers,row)}
		#	print row
		#	self.producer.send('data', row)
		

		producer.flush()
		producer = KafkaProducer(retries=5)
                
if __name__ == "__main__":
   	args 		= sys.argv
	ip_addr 	= str(args[1])
	partition_key 	= str(args[2])
	prod 		= Producer(ip_addr)
	
	prod.produce_msgs(partition_key)

