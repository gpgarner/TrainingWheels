package com.sandbox.src.main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._//{StringType, StructField, StructType, DateType}
import org.apache.commons.lang.StringUtils.substring
import java.text.SimpleDateFormat

import java.util.Properties
//import kafkashaded.org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter
import org.apache.kafka.clients.producer._
import org.apache.kafka.common._
import org.apache.spark.sql.streaming.ProcessingTime


/*class  KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)] {
   val kafkaProperties = new Properties()
   kafkaProperties.put("bootstrap.servers", servers)
   kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   val results = new scala.collection.mutable.HashMap[String, String]
   var producer: KafkaProducer[String, String] = _

   def open(partitionId: Long,version: Long): Boolean = {
     producer = new KafkaProducer(kafkaProperties)
     true
   }

   def process(value: (String, String)): Unit = {
       producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
   }

   def close(errorOrNull: Throwable): Unit = {
     producer.close()
   }
}*/

object FileStreamExample {

class  KafkaSink(topic:String, servers:String) extends ForeachWriter[(String, String)] {
   val kafkaProperties = new Properties()
   kafkaProperties.put("bootstrap.servers", servers)
   kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   val results = new scala.collection.mutable.HashMap[String, String]
   var producer: KafkaProducer[String, String] = _

   def open(partitionId: Long,version: Long): Boolean = {
     producer = new KafkaProducer(kafkaProperties)
     true
   }

   def process(value: (String, String)): Unit = {
       producer.send(new ProducerRecord(topic, value._1 + ":" + value._2))
   }

   def close(errorOrNull: Throwable): Unit = {
     producer.close()
   }
}

  def main(args: Array[String]): Unit = {
    //import sparkSession.implicits._
    //import sqlContext.implicits._
    //import spark.implicits._

    val sparkSession = SparkSession.builder
      .master("spark://18.205.181.166:7077")
      .appName("example")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("CMTE_ID", StringType),
                StructField("AMNDT_IND", StringType),
                StructField("RPT_TP", StringType),
                StructField("TRANSACTION_PGI", StringType),
                StructField("IMAGE_NUM", StringType),
                StructField("TRANSACTION_TP", StringType),
                StructField("ENTITY_TP", StringType),
                StructField("NAME", StringType),
                StructField("CITY", StringType),
                StructField("STATE", StringType),
                StructField("ZIP_CODE", StringType),
                StructField("EMPLOYER", StringType),
                StructField("OCCUPATION", StringType),
                StructField("TRANSACTION_DT", StringType),
                StructField("TRANSACTION_AMT", StringType),
                StructField("OTHER_ID", StringType),
                StructField("TRAN_ID", StringType),
                StructField("FILE_NUM", StringType),
                StructField("MEMO_CD", StringType),
                StructField("MEMO_TEXT", StringType),
		StructField("SUB_ID", StringType)))

    import sparkSession.implicits._

    //create stream from folder
    //val fileStreamDf = sparkSession.readStream
    //  .option("sep", "|")
    //  .schema(schema)
    //  .csv("/home/ubuntu/sandbox/src/main/scala/tmp/")
    
    //create stream from kafka topic data
    val fileStreamDf = sparkSession
	.readStream
	.format("kafka")
 	.option("kafka.bootstrap.servers", "18.205.181.166:9092")
 	.option("subscribe", "data")
        .option("startingOffsets","earliest")
 	.load()
    
    val df_string = fileStreamDf.selectExpr("CAST(key AS STRING)")
    
    var df = df_string.select(from_json(col("value"),schema).alias("data")).select("data.*")
    
    val dfFilter0 = df
      .filter(col("CMTE_ID")!=="")
      .filter(col("NAME")!=="")
      .filter(col("ZIP_CODE")!=="")
      .filter(col("TRANSACTION_DT")!=="")
      .filter(col("TRANSACTION_AMT")!=="")
      .filter(col("OTHER_ID")==="")
    

    def dateFunc: (String => String) = {s => substring(s,4)}
    val myDateFunc = udf(dateFunc)
    def zipFunc: (String => String) = {s => substring(s,0,5)}
    val myZipFunc = udf(zipFunc)
     
    val dfAlter1 = dfFilter0
      .withColumn("ZIP_CODE", myZipFunc(dfFilter0("ZIP_CODE")))
      .withColumn("YEAR", myDateFunc(dfFilter0("TRANSACTION_DT")).cast(IntegerType))
      .withColumn("TRANSACTION_AMT",dfFilter0("TRANSACTION_AMT").cast(IntegerType))

    val dfAlter2 = dfAlter1
      .filter(col("YEAR")<=2018)
      .filter(col("YEAR")>=1980)

    val dfAlter3 = dfAlter2
      .withColumn("timestamp",to_timestamp($"TRANSACTION_DT", "MMddyyyy"))
      .withColumn("TRANSACTION_DT",to_date($"TRANSACTION_DT", "MMddyyyy").alias("date"))
    
    dfAlter3.printSchema()
    
    val groupies = dfAlter3.withWatermark("timestamp", "15 days").groupBy(window($"timestamp", "2 days", "1 days"),$"STATE").count()
    
    //val query0 = dfAlter3.select("CMTE_ID","NAME","STATE","ZIP_CODE","TRANSACTION_DT","YEAR","timestamp")  
    //  .writeStream
    //  .format("console")
    //  .option("truncate","false")
    //  .outputMode(OutputMode.Append())
    //  .start()
    //query0.awaitTermination() 
    //val writer = new KafkaSink("dump","18.205.181.166:9092") 
    
    //val query1 = groupies
    //.writeStream
    //.foreach(writer)
    //.outputMode("update")
    //.trigger(ProcessingTime("25 seconds"))
    //.start()
    groupies.select(to_json(struct("window.start","window.end","STATE")).alias("key"),col("count").cast("string").alias("value")).printSchema()
    val query = groupies.select(to_json(struct("window.start","window.end","STATE")).alias("key"),col("count").cast("string").alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "dump")
      .option("checkpointLocation", "/home/ubuntu/sandbox/checkpoint")
      .outputMode("complete")
      .start()
    query.awaitTermination()
    //println(query.lastProgress)
  }
}
