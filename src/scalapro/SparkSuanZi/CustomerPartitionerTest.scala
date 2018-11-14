package scalapro.SparkSuanZi

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

import java.net.URL

/**
  * 自定义partitioner分区函数
  */

object CustomerPartitionerTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("E:\\LearnFile\\BigData\\Spark\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })

    val ints = rdd3.map(_._1).distinct().collect()

    //在这里实例化自定义分区类对象
    val hostParitioner = new HostParitioner(ints)

    //    val rdd4 = rdd3.partitionBy(new HashPartitioner(ints.length))

    //在这里使用自定义分区类
    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    rdd4.saveAsTextFile("E:\\IdeaProjects\\SparkTest\\output")

    //println(rdd4.collect().toBuffer)
    sc.stop()

  }

}

class HostParitioner(ins: Array[String]) extends Partitioner {
  val parMap = new mutable.HashMap[String, Int]()

  var count = 0
  for(i <- ins){
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString, 0)
  }
}
