package com.han.bigdata.spark.demo

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.lib.CombineFileInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object StagePartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName.stripSuffix("$"))
      .setMaster("local[2]")
      .set("spark.default.parallelism", "4")

    //val gsc: SparkContext = SparkContext.getOrCreate(conf)
    val sc: SparkContext = new SparkContext(conf)
    //val ss = SparkSession.builder().getOrCreate()
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7))
    println(rdd1.partitions.length)

    /**
     * 给定最小分区数，但分区数据量要满足 最小值(1byte)<= 数据量/Math.max(最小分区数,1) <=标准文件块大小
     * 未给定最小分区数则默认值和'2'取最小，def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
     */
    //    val rdd2 = sc.textFile("data/test/wordcount/input", 250) // 太多只要248（byte）
    //    val rdd2 = sc.textFile("data/test/wordcount/input", 5) // 1,2,3,...分区数正好
    val rdd2 = sc.textFile("data/test/wordcount/input", 101) // ...不好划分
    println(rdd2.partitions.length)

    /*val rdd3 = sc.hadoopFile("hdfs://a-hdfs-path/",
      classOf[CombineFileInputFormat[LongWritable, Text]],
      classOf[LongWritable],
      classOf[Text], 2)*/

    println("=" * 50)

    //重分区划分Stage
    val rdd3 = rdd2.flatMap(_.split(" ")).map((_, "1")).repartition(4)
      .partitionBy(new HashPartitioner(4)) //（使得与下游分区相同或不同来判断影响）
    println(rdd3.partitioner.getOrElse("None") + ":" + rdd3.partitions.length.toString)
    //HashPartitioner分区器重分区（与上游分区不同）
    val rdd4 = rdd3.mapValues(_.toInt) .partitionBy(new HashPartitioner(5))
    println(rdd4.partitioner.getOrElse("None") + ":" + rdd4.partitions.length.toString)
    //HashPartitioner分区器非重分区（上游分区满足当前）
    val rdd5 = rdd4.reduceByKey(_ + _)
    println(rdd5.partitioner.getOrElse("None") + ":" + rdd5.partitions.length.toString)
    //RangePartitioner分区器重分区（与上游分区不同）
    val rdd6 = rdd5.sortBy(_._2, ascending = false)
    println("=" * 50)
    rdd6.collect.foreach(println)

    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
