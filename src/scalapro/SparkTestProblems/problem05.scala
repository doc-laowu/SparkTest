package scalapro.SparkTestProblems

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object problem05 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("problem05")
    val sc = new SparkContext(conf)

    val mgr = new mutable.HashMap[String, Int]()

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      mgr.put(arr(0), arr(5).toInt)
      (arr(3), (arr(1), arr(5).toInt))
    })

    var rdd2 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      (arr(0), arr(5).toInt)
    })

    val rdd3 = rdd1.join(rdd2).filter(x=>{
      x._2._1._2 > x._2._2
    }).map(x=>{
      (x._2._1._1, x._2._1._2)
    }).foreach(println)

    sc.stop()
  }
}
