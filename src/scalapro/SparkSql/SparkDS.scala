package scalapro.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 创建DataSets数据集
  */

case class Person(name: String, age: Long)

object SparkDS {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkDS")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }
}