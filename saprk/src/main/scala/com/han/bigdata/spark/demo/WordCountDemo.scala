package com.han.bigdata.spark.demo

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-04-13 15:56
 * @Version: 1.0
 * @Company: 58集团
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName.stripSuffix("$"))
      .config("spark.default.parallelise", "3")
      .getOrCreate()

    val rdd1: RDD[String] = spark.sparkContext.parallelize(Seq("You can do this quiz online or print it on paper",
      "It tests comprehension of common English expressions that include the word VOICE",
      "The supreme happiness of life is the conviction that we are loved",
      "Life is just a series of trying to make up your mind"))

    val rs = rdd1.flatMap(_.split("\\s+"))
      .filter(StringUtils.isNotBlank(_)).map((_, 1))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false)

    rs.foreach(println)

    spark.stop()
  }
}
