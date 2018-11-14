package scalapro.SparkTestProblems

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object problem03 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("problem03")
    val sc = new SparkContext(conf)

    val sdf = new SimpleDateFormat("dd-MM月-yy")

    val rdd1 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\emp.txt").map(line=>{
      val arr = line.split(",")
      val time:Long = sdf.parse(arr(4)).getTime
      (arr(arr.length-1), (arr(1), time))
    })

    val rdd2 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\exam\\dept.txt").map(line=>{
      val arr = line.split(",")
      (arr(0), arr(1))
    })
    //(20,((SMITH,345830400000),RESEARCH))
    rdd1.join(rdd2).map(iter=>{
      (iter._2._2, iter._2._1._1, iter._2._1._2)
    }).groupBy(_._1).map(it=>{
      it._2.toList.sortBy(_._3).reverse.take(1).map(x=>{
        (x._1, x._2)
      })
    }).foreach(println)

    sc.stop()
  }
}
