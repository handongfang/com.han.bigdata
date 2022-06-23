package com.han.bigdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.SamplingUtils
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

import java.lang.Thread.sleep
import scala.util.hashing.byteswap32

object DenpendencyDemo {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
    // logger.error("错误" * 10)

    val conf = new SparkConf().setAppName(this.getClass.getName.stripSuffix("$")).setMaster("local[4]")
    val sc = SparkContext.getOrCreate(conf)

    val rdd1 = sc.parallelize(Seq("1", "2", "3", "1", "2", "3", "1", "2", "3", "4"))

    val rdd2 = rdd1.map((_, 1)) //变成二元组模拟K-V数据

    //    rdd2.mapPartitionsWithIndex((index, iter) => {
    //      iter.map((_, index))
    //    }).foreach(println)
    //
    //    println("=" * 30)
    //
    //    rdd2.mapPartitionsWithIndex { (idx, iter) =>
    //      iter.map((_, idx))
    //    }.foreach(println)
    //
    //    println("=" * 30)

    val wordcount: RDD[(String, Int)] = rdd1.map((_, 1)) //OneToOneDE
      .keyBy(_._1) //OneToOneDE
      //.groupBy(_._1, 2) //ShuffleDE
      //重分区涉及shuffle的采用HashPartitioner分区器
      //.repartition(4) //ShuffleDE
//      // reduceByKey指定分区数时用HashPartitioner分区器，上下一致分区器时不会shuffle
      .partitionBy(new HashPartitioner(2)) //ShuffleDE
      .reduceByKey((t1, t2) => (t1._1, t2._2 + t1._2), 2) //OneToOneDE
//      .partitionBy(new RangePartitioner(3, rdd2)) //ShuffleDE
//      .reduceByKey((t1, t2) => (t1._1, t2._2 + t1._2), 3) //ShuffleDE
      /**
       * reduceByKey不指定分区数时选择匹配上游父算子具有分区器且最大分区数的分区器，匹配条件是
       * 上游父算子最大分区数不能超过分区器最大分区数的10倍。不匹配时构建HashPartitioner分区器，
       * 分区数是在spark.default.parallelism设定的分区数和上游父算子的最大分区数两者中取最大。
       */
      //.reduceByKey((t1, t2) => (t1._1, t2._2 + t1._2)) //ShuffleDE
      //.coalesce(2, shuffle = false) //窄依赖（但非RangeDE，这是union算子分区器）它是一个匿名窄依赖实现
      //.coalesce(6, shuffle = false) //ShuffleDE
      //.coalesce(2, shuffle = true) //ShuffleDE
      .map(_._2) //OneToOneDE

    wordcount.foreach(s => {
      println(s + " => partitionId:" + TaskContext.get.partitionId.toString)
    })

    sleep(Int.MaxValue.toLong)

    sc.stop()
  }
}
