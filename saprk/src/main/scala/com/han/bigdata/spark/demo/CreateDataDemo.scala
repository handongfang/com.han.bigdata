package com.han.bigdata.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java._
import java.lang.Thread.sleep

object CreateDataDemo {
  def main(args: Array[String]): Unit = {
    //    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
    //    logger.error("错误" * 10)

    val spark = SparkSession.builder().master("local[*]").appName("sparkDemo").getOrCreate()

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = SparkContext.getOrCreate(conf)

    //隐式转换
    import spark.implicits._

    val rdd: RDD[String] = sc.emptyRDD[String]

    val rdd1: RDD[String] = sc.parallelize(Seq("1", "2", "3"))

    val ds: Dataset[String] = spark.createDataset(Seq("1", "2", "3"))

    val ds1: Dataset[String] = spark.emptyDataset[String]

    //元素必须是row或元组
    val df = spark.createDataFrame(Seq(("1", 1), ("2", 2), ("3", 3))).toDF("name", "age")

    //    val schema = StructType(List(
    //      StructField("name", StringType, true),
    //      StructField("age", IntegerType, true),
    //      StructField("phone", LongType, true)
    //    ))
    //    val dataList = new util.ArrayList[Row]()
    //    dataList.add(Row("ming",20,15552211521L))
    //    dataList.add(Row("hong",19,13287994007L))
    //    dataList.add(Row("zhi",21,15552211523L))
    //
    //    spark.createDataFrame(dataList,schema).show()

    val df1: DataFrame = spark.emptyDataFrame


    //wordcount
    val wordDS: RDD[String] = sc.textFile("data/test/wordcount/input/wordstr.txt")

    wordDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)

    sleep(Int.MaxValue.toLong)

    spark.stop()
  }
}
