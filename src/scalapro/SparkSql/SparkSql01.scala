package scalapro.SparkSql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql01 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSql01").setMaster("local")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df = spark.read.json("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.json")

    df.printSchema()

    //显示所有的行
//    df.show()

    //值过滤
//    df.filter($"age" > 10).show()

    //创建一个本地的临时视图
    df.createOrReplaceTempView("people")

    df.show()

//    spark.sql(" select * from people where age > 10").map(x=>{
//      x.getLong(0).+(10)
//    })

    spark.stop()
  }
}
