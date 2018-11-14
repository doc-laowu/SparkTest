package scalapro.SparkTestProblems

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object problem02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("problem02").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      val sal = arr(5).toInt
      (arr(arr.length-1), sal)
    }).groupByKey().map(x=>{
      (x._1, x._2.size+"\t"+x._2.sum/x._2.size.toDouble)
    })

    val rdd2 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\dept.txt").map(line=>{

      val arr = line.split(",")
      (arr(0), arr(1))
    })

    rdd1.join(rdd2).map(x=>{
      (x._2._2, x._2._1)
    }).foreach(println)

    sc.stop()
  }
}
