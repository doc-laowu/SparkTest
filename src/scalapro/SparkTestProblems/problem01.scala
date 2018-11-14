package scalapro.SparkTestProblems

import org.apache.spark.{SparkConf, SparkContext}

object problem01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("problem01")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      val sal = arr(5).toInt
      (arr(arr.length-1), sal)
    }).reduceByKey(_+_)

    val rdd3 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\dept.txt").map(line=>{

      val arr = line.split(",")
      (arr(0), arr(1))
    })

    rdd1.join(rdd3).map(x=>{
      (x._2._2, x._2._1)
    }).foreach(println)

    sc.stop()
  }
}
