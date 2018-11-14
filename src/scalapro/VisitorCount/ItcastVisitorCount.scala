package scalapro.VisitorCount

import org.apache.spark.{SparkConf, SparkContext}
import java.net.URL

/**
  *
  * 统计网站的访问次数
  *
  */

object ItcastVisitorCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ItcastVisitorCount")
    val sc = new SparkContext(conf)

    val rdd0 = sc.textFile("E:\\IdeaProjects\\SparkTest\\ItcastLog").map(line => {
      val fields = line.split("\t")
      val time = fields(0)
      val url = fields(1)
      (url, 1)
    })

    val rdd1 = rdd0.reduceByKey(_+_)

    val rdd2 = rdd1.map(x => {
      val url = x._1
      val host = new URL(url).getHost
      (host, url, x._2)
    })

    val arr = Array("java.itcast.cn", "php.itcast.cn", "java.itcast.cn")

//    val rddjava = rdd2.filter(_._1 == "java.itcast.cn")
//    val sortedjava = rddjava.sortBy(_._3, false).take(3)

    for(ins <- arr){
      val rdd = rdd2.filter(_._1 == ins)
      val sortedrdd = rdd.sortBy(_._3, false).take(3)
      print(sortedrdd.toBuffer)
    }

    sc.stop()

  }
}
