package com.han.bigdata.spark.demo

import org.apache.spark.util.Utils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext, TaskContext}

//自定义的分区器
class MyCustomPartitioner(numPar: Int) extends Partitioner {
  assert(numPar > 0)

  // 返回分区数, 必须要大于0.
  override def numPartitions: Int = numPar

  //返回指定键的分区编号(0到numPartitions-1)
  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode.abs % numPar
  }
}

//自定义的Hash分区器
class MyHashPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode % partitions + (if (key.hashCode % partitions < 0) partitions else 0)
    //case _ => key.hashCode.abs % partitions
  }
}

object MyCustomPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyCustomPartitioner").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array((10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")))
    println(rdd1.partitioner)
    //使用HashPartitioner重新分区
    //val rdd2 = rdd1.partitionBy(new MyCustomPartitioner(2))
    val rdd2 = rdd1.partitionBy(new MyHashPartitioner(2))
    println(rdd2.partitioner)
    //所有的数据都分到0分区，因为key都是偶数
    rdd2.glom().map(_.toList).foreach(s =>
      println(s + " => partitionId:" + TaskContext.get.partitionId.toString)
    )
    rdd2.glom().map(_.toList).foreachPartition(itre => {
      val id = TaskContext.get.partitionId.toString
      itre.foreach(s => println(s + " => partitionId:" + id))
    })

    sc.stop()
  }
}