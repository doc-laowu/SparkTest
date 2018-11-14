package scalapro.SparkSuanZi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 向mysql写入数据
  */

object WriteToMysql {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List((50, "研发部", "深圳"), (60, "制造部", "东莞")))
    data.foreachPartition(insertData)
    sc.stop()
  }

  def insertData(iterator: Iterator[(Int, String, String)]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3306/exam", "root", "123456")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into dept(deptno, dname, loc) values (?, ?, ?)")
      ps.setInt(1, data._1)
      ps.setString(2, data._2)
      ps.setString(3, data._3)
      ps.executeUpdate()
    })
  }
}
