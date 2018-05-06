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

import java.sql.{Array=>SQLArray,_}

object FileStreamExample {

  def main(args: Array[String]): Unit = {
    //import sparkSession.implicits._
    //import sqlContext.implicits._
    //import spark.implicits._

    val sparkSession = SparkSession.builder
      .master("spark://18.205.181.166:7077")
      .appName("example")
      .getOrCreate()
    
    class  JDBCSink(url:String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
      val driver = "org.postgresql.Driver"
      var connection:Connection = _
      var statement:Statement = _
      

      def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection =java.sql.DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: org.apache.spark.sql.Row): Unit = {
        statement.executeUpdate("INSERT INTO public.test(col1, col2) " + "VALUES ('" + value(0) + "','" + value(1) + ");")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    } 

    
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
 	.load()
    
    val df_string = fileStreamDf.selectExpr("CAST(value AS STRING)")
    
    var df = df_string.select(from_json(col("value"),schema).alias("data")).select("data.*")
 
    val dfFilter0 = df
      .filter(col("CMTE_ID")!=="")
      .filter(col("NAME")!=="")
      .filter(col("ZIP_CODE")!=="")
      .filter(col("TRANSACTION_DT")!=="")
      .filter(col("TRANSACTION_AMT")!=="")
      .filter(col("OTHER_ID")==="")
      .filter($"AMNDT_IND" rlike "[N]|[A]|[T]")
      .filter($"RPT_TP" rlike "[1][2][C]|[1][2][G]|[1][2][P]|[1][2][R]|[1][2][S]|[3][0][D]|[3][0][G]|[3][0][P]|[3][0][R]|[3][0][S]|[6][0][D]|[A][D][J]|[C][A]|[M][1][0]|[M][1][1]|[M][2]|[M][3]|[M][4]|[M][5]|[M][6]|[M][7]|[M][8]|[M][9]|[M][Y]|[Q][1]|[Q][2]|[Q][3]|[T][E][R]|[Y][E]|[9][0][S]|[9][0][D]|[4][8][H]|[2][4][H]")
      .filter(col("FILE_NUM")!=="")

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
    
    val url="jdbc:postgresql://postgresgpgsql.c0npzf7zoofq.us-east-1.rds.amazonaws.com:5432/postgreMVP"
    val user="gpgarner8324"
    val pwd="carmenniove84!"
    val writer = new JDBCSink(url, user, pwd)


    //val dfGroup1 = dfAlter3.groupBy("STATE").count()
    
    /*val query0 = dfGroup1
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()*/

    val query0 = dfAlter3.select("CITY","STATE")
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(ProcessingTime("25 seconds"))
      .start()
    query0.awaitTermination()

    // val groupies = dfAlter3.groupBy(window($"timestamp", "10 days", "5 days"),$"STATE").count()
    //val groupies = dfAlter3.withWatermark("timestamp", "10 days").groupBy(window($"timestamp", "10 days", "5 days"),$"STATE").count()
    //groupies.printSchema()
    /*val query0 = dfAlter3.select("CMTE_ID","AMNDT_IND","RPT_TP","FILE_NUM","NAME","STATE","ZIP_CODE","TRANSACTION_DT","YEAR","timestamp")  
      .writeStream
      .format("console")
      .option("truncate","false")
      .outputMode(OutputMode.Append())
      .start()
    query0.awaitTermination()*/
    //val writer = new KafkaSink("dump","18.205.181.166:9092") 
    
    //val query1 = groupies
    //.writeStream
    //.foreach(writer)
    //.outputMode("update")
    //.trigger(ProcessingTime("25 seconds"))
    //.start()
    //groupies.select(to_json(struct("window.start","window.end","STATE")).alias("key"),col("count").cast("string").alias("value")).printSchema()
    /*val query = groupies.select(to_json(struct("window.start","window.end","STATE")).alias("key"),col("count").cast("string").alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "dump")
      .option("checkpointLocation", "/home/ubuntu/repo/sandbox/checkpoint")
      .outputMode("complete")
      .start()
    query.awaitTermination()*/
    //println(query.lastProgress)
  }
}
