package scalapro.CleanMovie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 各年电影产出数
  */

object MovieNumsByYear {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieNumsByYear")
    val sc = new SparkContext(conf)

    sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\doubanmovie").map(line=>{
      val arr = line.split("\t")
      (arr(1), 1)
    }).foreachPartition(insertMysql)

    
    //.reduceByKey(_+_).saveAsTextFile("E:\\IdeaProjects\\SparkTest\\MovieNumsByYear")

    sc.stop()
  }

  /**
    * 将结果写入数据库中
    *
    * @param iterator
    */
  def insertMysql(iterator: Iterator[(String, Int)])={
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/exam", "root", "123456")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into movienumsbyyear(year, number) values (?, ?)")
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.executeUpdate()
    })
  }

}
