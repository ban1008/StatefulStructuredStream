package org.statefulstructuredstreaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.NoTimeout
import org.apache.spark.sql.streaming.GroupState

object StatefulStructuredStreamingKafka {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9092")
    .option("subscribe", "demo")
    .load()

  case class WordCount(word:String, count:Long)

  case class OnlyCount(count:Long)

  def mappingFunc(key : String, values : Iterator[WordCount],state: GroupState[Long]) = {
    val temp= values.toList
    val valueCount = temp.map(wc=>wc.count).sum
    val newState = valueCount +  state.getOption.getOrElse(0.toLong)
    state.update(newState)
    WordCount(key, state.getOption.getOrElse(0))
  }

  def dataProcessing(df:DataFrame):Dataset[WordCount]={
    val lines = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]
    val words = lines
      .flatMap(_.split(" "))
    val query = words
      .map(k => (k, 1.toLong))
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      .as[WordCount]

    query
  }

  def callMappingFunc(temp:Dataset[WordCount]): Dataset[WordCount] ={

    val res = temp
      .groupByKey(_.word)
      .mapGroupsWithState(NoTimeout)(mappingFunc)

    res

  }

  def main(args:Array[String]){


    val temp = dataProcessing(df)

    val result = callMappingFunc(temp)

    result.writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()

    spark.stop()
  }

}
