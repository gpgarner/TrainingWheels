import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'
from ast import literal_eval
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import struct
import sys
import datetime,time
import json

def parse_json(df):
        cmte_id         = str(json.loads(df[0])['CMTE_ID'])
        amndt_in        = str(json.loads(df[0])['AMNDT_IN'])
        rpt_tp          = str(json.loads(df[0])['RPT_TP'])
        transaction_pgi = str(json.loads(df[0])['TRANSACTION_PGI'])
        entity_tp       = str(json.loads(df[0])['ENTITY_TP'])
        name            = str(json.loads(df[0])['NAME'])
        city            = str(json.loads(df[0])['CITY'])
        state           = str(json.loads(df[0])['STATE'])
        zip_code        = str(json.loads(df[0])['ZIP_CODE'])
        employer        = str(json.loads(df[0])['EMPLOYER'])
        occupation      = str(json.loads(df[0])['OCCUPATION'])
        transaction_dt  = str(json.loads(df[0])['TRANSACTION_DT'])
        transaction_amt = str(json.loads(df[0])['TRANSACTION_AMT'])
        other_id        = str(json.loads(df[0])['other_id'])
        tran_id         = str(json.loads(df[0])['tran_id'])
        file_num        = str(json.loads(df[0])['FILE_NUM'])
        memo_cd         = str(json.loads(df[0])['MEMO_CD'])
        memo_text       = str(json.loads(df[0])['MEMO_TEXT'])
        return [cmte_id, anmndt_in, rpt_tp, transaction_pgi, entity_tp, name, city, state, zip_code, employer, occupation, transaction_dt, transaction_amt, other_id, tran_id, file_num, memo_cd, memo_text]


schema = StructType([   StructField('CMTE_ID', StringType(), True),
                                StructField('AMNDT_IND', StringType(), True),
                                StructField('RPT_TP', StringType(), True),
                                StructField('TRANSACTION_PGI', StringType(), True),
                                StructField('IMAGE_NUM', StringType(), True),
                                StructField('TRANSACTION_TP', StringType(), True),
                                StructField('ENTITY_TP', StringType(), True),
                                StructField('NAME', StringType(), True),
                                StructField('CITY', StringType(), True),
                                StructField('STATE', StringType(), True),
                                StructField('ZIP_CODE', StringType(), True),
                                StructField('EMPLOYER', StringType(), True),
                                StructField('OCCUPATION', StringType(), True),
                                StructField('TRANSACTION_DT', StringType(), True),
                                StructField('TRANSACTION_AMT', StringType(), True),
                                StructField('OTHER_ID', StringType(), True),
                                StructField('TRAN_ID', StringType(), True),
                                StructField('FILE_NUM', StringType(), True),
                                StructField('MEMO_CD', StringType(), True),
                                StructField('MEMO_TEXT', StringType(), True),
                                StructField('SUB_ID', StringType(), True),
                        ])

StructType.fromJson(schema.jsonValue())

spark = SparkSession.builder.appName("SSKafka").getOrCreate()#.enableHiveSupport().getOrCreate()

events = spark.readStream.format("kafka").option("kafka.bootstrap.servers","18.205.181.166:9092").option("subscribe","data").option("startingOffsets","earliest").load()
events.printSchema()

events_string = events.selectExpr("CAST(value AS STRING)")

events_table = events_string.select(from_json(col("value"),schema).alias("data")).select("data.*")
events_table.printSchema()

#events_table.show().writeStream.start()
#words = events_table.select(explode(split(events_table.CITY, " "))).alias("word")

#wordCounts = events_table.groupBy("STATE").count()
date_func = udf (lambda x: datetime.datetime.strptime(x,'%m%d%Y'), DateType())
zip_func = udf (lambda x: str(x)[0:5])

events_table = events_table.where(col("CMTE_ID").isNotNull())
events_table = events_table.where(col("NAME").isNotNull())
events_table = events_table.where(col("ZIP_CODE").isNotNull())
events_table = events_table.where(col("TRANSACTION_DT").isNotNull())
events_table = events_table.where(col("TRANSACTION_AMT").isNotNull())
events_table = events_table.where(col("OTHER_ID").isNull())

events_table = events_table.withColumn('ZIP_CODE',zip_func(col('ZIP_CODE')))
events_table = events_table.withColumn('TRANSACTION_DT',date_func(col('TRANSACTION_DT')))

events_table.printSchema()
#events_table = events_table[events_table["ZIP_CODE"]].notnull()

#events_window = events_table.window(10,10)

#groupies = events_table.groupBy("ZIP_CODE", window("TRANSACTION_DT", "5 days")).count()
groupies = events_table.groupBy("TRANSACTION_DT").count()
query=groupies \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

#spark.stop()

#streamingETLQuery = fec_events \
#  .withColumn("date", fec_events.timestamp.cast("date")) \
#  .writeStream \
#  .format("parquet") \
#  .option("path", parquetOutputPath) \
#  .partitionBy("date") \
#  .trigger(processingTime="10 seconds") \
#  .option("checkpointLocation", checkpointPath) \
#  .start()
