package scalapro.SparkTestProblems

import org.apache.spark.{SparkConf, SparkContext}

object problem04 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("problem04")
    val sc= new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      (arr(arr.length-1), arr(5).toInt)
    }).reduceByKey(_+_)

    val rdd2 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\dept.txt").map(line=>{
      val arr = line.split(",")
      (arr(0), arr(2))
    })

    rdd1.join(rdd2).map(x=>{
      (x._2._2, x._2._1)
    }).foreach(println)

    sc.stop()
  }
}
