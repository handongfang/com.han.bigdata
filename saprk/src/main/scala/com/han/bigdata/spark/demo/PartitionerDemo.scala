package com.han.bigdata.spark.demo

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName.stripSuffix("$"))
      .setMaster("local")
      //.setMaster("local[*]") //这里可以指定全局核数
    val gsc = SparkContext.getOrCreate(conf)
    //无分区器
    //val rdd = gsc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 6)
    //不指定就用defaultParallelism（local是默认1，cluser默认是Executor总核数但至少要2）
    val rdd = gsc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    println(rdd.partitioner.getOrElse("None") + ":" + rdd.partitions.size.toString)
    rdd.count()
    // textFile也可以指定最小分区数，不指定将使用math.min(defaultParallelism, 2)
    //val rdd1 = gsc.textFile("data/test/wordcount/input/", 6)
    val rdd1 = gsc.textFile("data/test/wordcount/input/")
    println(rdd1.partitioner.getOrElse("None") + ":" + rdd1.partitions.size.toString)
    //无分区器
    val rdd2 = rdd1.flatMap(_.split(" ")).map((_, "1")).repartition(4)
    println(rdd2.partitioner.getOrElse("None") + ":" + rdd2.partitions.size.toString)
    //HashPartitioner分区器
    val rdd3 = rdd2.mapValues(_.toInt).partitionBy(new HashPartitioner(4))
    println(rdd3.partitioner.getOrElse("None") + ":" + rdd3.partitions.size.toString)
    //HashPartitioner分区器
    val rdd4 = rdd3.reduceByKey(_ + _)
    println(rdd4.partitioner.getOrElse("None") + ":" + rdd4.partitions.size.toString)
    //RangePartitioner分区器
    val rdd4_2 = rdd3.sortByKey()
    println(rdd4_2.partitioner.getOrElse("None") + ":" + rdd4_2.partitions.size.toString)
    //HashPartitioner分区器
    val rdd4_3 = rdd3.groupByKey()
    println(rdd4_3.partitioner.getOrElse("None") + ":" + rdd4_3.partitions.size.toString)
    //无分区器
    val rdd5 = rdd4.map(_._1).flatMap(_.split("")).coalesce(6, false)
    println(rdd5.partitioner.getOrElse("None") + ":" + rdd5.partitions.size.toString)
    //无分区器
    val rdd6 = rdd5.filter(StringUtils.isNotBlank(_))
    println(rdd6.partitioner.getOrElse("None") + ":" + rdd6.partitions.size.toString)
    rdd6.count()

    Thread.sleep(Long.MaxValue)
    gsc.stop()
  }
}
