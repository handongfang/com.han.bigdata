package com.han.bigdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * @Description:
 * @Author: HanDongFang
 * @CreateTime: 2022-04-12 11:20
 * @Version: 1.0
 * @Company: 58集团
 */

object GroupReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName.stripSuffix("$"))
    val gsc = SparkContext.getOrCreate(conf)

    val rdd1 = gsc.textFile("data/test/wordcount/input/")
    // 二元组默认是(k,v)对
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(_.split("\\s+")).map(w => (w.trim, 1))

    // 依据key标签变成(k,iterable(v))对:(直接依据key分组)
    val groupByKeyRdd1: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    // 打上key标签变成(k,iterable(elem))对:(流程先map出(k,elem)后groupByKey分组)
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = rdd2.groupBy(_._1)

    // 打上key标签变成(k,elem)对
    val keyRdd: RDD[(String, (String, Int))] = rdd2.keyBy(_._1)
    // 等效groupBy:(由此知有键用groupByKey,无键用groupBy)
    val groupByKeyRdd2: RDD[(String, Iterable[(String, Int)])] = keyRdd.groupByKey()

    // 依据key标签分组聚合:(相对group它能提前在分区内聚合数据,减少数据IO次数)
    val reduceByKeyRdd1: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)
    val reduceByKeyRdd2: RDD[(String, (String, Int))] = keyRdd.reduceByKey((t1, t2) => (t1._1, t1._2 + t2._2))
    //reduceByKeyRdd1.foreach(println)

    // mapValues可以对键的值进行改变,对分组数据可以模仿reduceByKey聚合操作
    val mapValuesRdd1: RDD[(String, Int)] = groupRdd.mapValues(iter => {
      val iterator: Iterator[(String, Int)] = iter.toIterator
      var sum: Int = 0
      while (iterator.hasNext) {
        sum += iterator.next()._2
      }
      sum
    })
    //mapValuesRdd1.foreach(println)
    val mapValuesRdd2: RDD[(String, Int)] = groupByKeyRdd1.mapValues(iter => {
      iter.toList.sum
    })
    //mapValuesRdd2.foreach(println)

    // sortBy排序:(底层调用keyBy后再sortByKey取元素值)
    val sortByRdd: RDD[(String, Int)] = reduceByKeyRdd1.sortBy(_._2, ascending = false)
    sortByRdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(t => (index, t._1, t._2))
    }).collect.foreach(println)
    println("=" * 50)
    // 等效于
    val sortByKeyRdd: RDD[(String, Int)] = mapValuesRdd1.keyBy(_._2).sortByKey(ascending = false).values
    sortByKeyRdd.mapPartitions(iter => {
      val id = TaskContext.getPartitionId()
      iter.map(t => (id, t._1, t._2))
    }).collect.foreach(println)
    println("=" * 50)

    // 分区内聚合
    val seqOP = (U: String, T: Int) => ((U.toInt + T).toString)
    // 分区间聚合
    val combOP = (U1: String, U2: String) => ((U1.toInt + U2.toInt).toString)
    //aggregateByKey可以指定起始值并按其类型返回最终聚合值
    val aggregateByKeyRdd: RDD[(String, String)] = rdd2.aggregateByKey("100")(seqOP, combOP)
    //aggregateByKeyRdd.foreach(println)

    Thread.sleep(Long.MaxValue)
    gsc.stop()
  }
}
