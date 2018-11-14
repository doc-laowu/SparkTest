package scalapro.SparkSql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object sparkDF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("sparkDF")
    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.txt")
      .map(_.split("\t"))

    val personRDD = lineRDD.map(x => Person(x(0).toString, x(1).toInt))

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val personDF = personRDD.toDF()

    personDF.show()

    sc.stop()
    spark.stop()
  }
}