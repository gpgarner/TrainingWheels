
package com.report.src.main.scala


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

import org.apache.spark.sql.functions.monotonically_increasing_id


object ReportExample {

  def main(args: Array[String]): Unit = {
    //import sparkSession.implicits._
    //import sqlContext.implicits._
    //import spark.implicits._

    val sparkSession = SparkSession.builder
      .master("spark://18.205.181.166:7077")
      .appName("example")
      .getOrCreate()
    
    import sparkSession.implicits._
  val report = sparkSession
  .read
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "dump")
  .load()
val schemaCount = 
	StructType(
		Array(
			StructField("count", StringType)
			)
		)
val schemaWindow =         
	StructType(
                Array(
			StructField("start",StringType),
                        StructField("end",StringType),
                        StructField("STATE", StringType)))

val df1 = 
	report.selectExpr("CAST(key AS STRING)").select(from_json(col("key"),schemaWindow).alias("data")).select("data.*")

//val df2 = report.selectxpr("CAST(value AS STRING)").select(from_json(col("value"),schemaCount).alias("data2")).select("data2.*")
val df2 = report.select(col("value").cast("String"))


val df_complete = df1.withColumn("id", monotonically_increasing_id())
    .join(df2.withColumn("id", monotonically_increasing_id()), Seq("id"))
    .drop("id")

//val df_transform = df_complete.withColumn("start",to_timestamp($"start","yyyy-MM-ddThh:mm:ss.SSS"))
 val df_transform =  df_complete.withColumn("start",$"start".cast("timestamp"))
				.withColumn("end",$"end".cast("timestamp"))
				.withColumn("value", $"value".cast("long"))
 
val df_output = df_transform.filter(col("STATE") === "GA").sort(desc("start"))

}}
