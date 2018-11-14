package scalapro.UserLocation

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计用户在基站的停留的最长时间
  */


object MobileLocation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("MobileLocation")
    val sc = new SparkContext(conf)

    //读取基站信息
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

    val rdd0 = sc.textFile("E:\\IdeaProjects\\SparkTest\\LacLog").map(line => {
      val fields = line.split(",")
      val eventType = fields(3)
      val time= sdf.parse(fields(1)).getTime
      val timelong = if(eventType == 1) -time else time
      //((手机号, 基站id), 时间)
      ((fields(0), fields(2)), timelong)
    })

    val rdd1 = rdd0.reduceByKey(_+_).map(x => {

      val mobile = x._1._1
      val lac = x._1._2
      val time = x._2
      (lac, (mobile, time))
    })

    val rdd2 = sc.textFile("E:\\IdeaProjects\\SparkTest\\LacInfo").map(line => {
      val fields = line.split(",")
      //(基站id, (精度， 维度))
      (fields(0), (fields(1), fields(2)))
    })

    val rdd3 = rdd1.join(rdd2).map(it => {
      val lac = it._1
      val mobile = it._2._1._1
      val time = it._2._1._2
      val x = it._2._2._1
      val y = it._2._2._2
      (mobile, lac, time, x, y)
    })

    val rdd4 = rdd3.groupBy(_._1)

    val rdd5 = rdd4.mapValues(itr => {
      itr.toList.sortBy(_._3).reverse.take(2)
    })

    println(rdd5.collect().toBuffer)

    sc.stop()
  }
}
