package scalapro.SparkSuanZi

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadFromMysql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkConnectMysql").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //从mysql里面读取数据
    val rdd = new org.apache.spark.rdd.JdbcRDD (
      sc,
      () => {
        Class.forName ("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection ("jdbc:mysql://localhost:3306/exam", "root", "123456")
      },
      "select * from stu where sid >= ? and sid <= ?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    rdd.foreach (println (_) )

    sc.stop()
  }
}
