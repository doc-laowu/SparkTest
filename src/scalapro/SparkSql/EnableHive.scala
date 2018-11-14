package scalapro.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 在sparksql上整合hive
  */

object EnableHive {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("EnableHive").setMaster("local")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from student").show()

    spark.stop()
  }
}
