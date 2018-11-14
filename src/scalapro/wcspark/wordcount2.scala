package scalapro.wcspark

import org.apache.spark.sql.SparkSession


/**
  * 使用sparksession完成WordCount
  */

object wordcount2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
         .master("local")
         .appName("WordCount")
//         .config("spark.some.config.option", "some-value")
         .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val result = spark.read.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\input")
      .flatMap(_.split("\t"))
      .groupBy($"value" as "word")
      .agg(count("*") as "counts")
      .orderBy(desc("counts"))

//    result.mapPartition // 写入mysql
    result.show()
    spark.stop()

  }
}
