package scalapro.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InferringSchema {
  def main(args: Array[String]): Unit = {

    // For implicit conversions from RDDs to DataFrames
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.txt")
      .map(_.split("\t"))
      .map(attributes => Person(attributes(0), attributes(1).toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 22 AND 40")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()


    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))

  }
}
