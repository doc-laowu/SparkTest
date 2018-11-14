package scalapro.CleanMovie

import org.apache.spark.{SparkConf, SparkContext}

object MovieNumsByCategory {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieNumsByYear")
    val sc = new SparkContext(conf)

    val rdd0 = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\doubanmovie").map(line=>{
      //整个行按\t切分
      val arr = line.split("\t")
      //Array(中国大陆,香港)
      var arr2 = arr(3).split("-")
      arr2.map(x=>{
        (x, 1)
      })
    }).flatMap(x=>{x.map(x=>(x._1, x._2))})
      .reduceByKey(_+_).foreachPartition(insertMysql)
    //.saveAsTextFile("E:\\IdeaProjects\\SparkTest\\MovieNumsByCity")
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
      val ps = conn.prepareStatement("insert into movienumsbycategory(category, number) values (?, ?)")
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.executeUpdate()
    })
  }

}
