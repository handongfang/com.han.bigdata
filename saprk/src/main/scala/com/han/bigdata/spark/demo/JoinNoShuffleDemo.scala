package com.han.bigdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object JoinNoShuffleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName.stripSuffix("$"))
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array((1, "1"), (2, "2"),
      (3, "3"), (4, "4"), (5, "5"), (6, "6"), (7, "7")))

    val rdd2 = sc.parallelize(Array((1, "Mon"), (2, "Tues"),
      (3, "Wed"), (4, "Thur"), (5, "Fri"), (6, "Sat"), (7, "Sun")))

    //ShuffleDE
    val joinRdd1: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    joinRdd1.foreach(println)
    println("=" * 30)
    val rdd1_1 = rdd1.partitionBy(new HashPartitioner(7))
    val rdd2_1 = rdd2.partitionBy(new HashPartitioner(7))
    //OneToOneDE
    //    val joinRdd2: RDD[(Int, (String, String))] = rdd1_1.join(rdd2_1, new HashPartitioner(7))
    //OneToOneDE(join传分区数就是采用HashPartitioner)
    //    val joinRdd2: RDD[(Int, (String, String))] = rdd1_1.join(rdd2_1, 7)
    /**
     * OneToOneDE
     * 不传分区数则使用默认分区器：先从父rdd找匹配分区器，不匹配再HashPartitioner(defaultParallelism)
     * defaultParallelism取决于spark.default.parallelism或父rdd最大分区数
     */
    val joinRdd2: RDD[(Int, (String, String))] = rdd1_1.join(rdd2_1)
    //ShuffleDE(分区器变化了分区数量)
    //val joinRdd2: RDD[(Int, (String, String))] = rdd1_1.join(rdd2_1, new HashPartitioner(3))
    joinRdd2.foreach(println)

    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
