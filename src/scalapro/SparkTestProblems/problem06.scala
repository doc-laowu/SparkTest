package scalapro.SparkTestProblems

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 6) 列出工资比公司平均工资要高的员工姓名及其工资
  */

object problem06 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("problem06")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      (arr(1), arr(5).toInt)
    })

    sc.stop()
  }
}
