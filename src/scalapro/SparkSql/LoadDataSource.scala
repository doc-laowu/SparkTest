package scalapro.SparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadDataSource {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LoadDataSource").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //读取parquet类型的文件
//    val usersDF = spark.read.load("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\users.parquet")
//
//    usersDF.select("name", "favorite_color").show()

    //将DF存成parquest的格式
    //同时还可以写成以下这些格式的文件：json、parquet、jdbc、orc、libsvm、csv、text
//    usersDF.select("name", "favorite_color").write.save("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\namesAndFavColors.parquet")

    //读取json格式的数据源
    val peopleDf = spark.read.format("json").load("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.json")
    peopleDf.show()
//    peopelDf.select("name", "age").write.format("parquet").save("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.parquet")

    //读取csv格式的数据源
//    val peopleDFCsv = spark.read.format("csv")
//      .option("sep", ";")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .load("E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\people.csv")
//    peopleDFCsv.show()

    //使用sql直接查询文件
//    val sqlDF = spark.sql("SELECT * FROM parquet.`E:\\IdeaProjects\\SparkTest\\SrcInput\\SparkSqlInput\\users.parquet`")

    //将结果保存到mysql中
//    sqlDF.write.mode(SaveMode.Append).jdbc("mysql:url", "users", new Properties())

    //将DF结果可以在分桶、分区和排序后将结果写入到表中
    // （这里会在输出目录下边创建SparkWareHouse存储我们的数据，然后将元数据存储在derby中，其本质是对hive的继承）
//    peopelDf.write.bucketBy(4, "name").sortBy("age")
//      .saveAsTable("people_bucketed")

    //或者将DF进行分区之后再持久化成表
//    peopelDf.write.partitionBy("city").format("parquet")
//      .saveAsTable("peopleByCity.parquet")


      //当写入和读出parquet格式的数据的时候
//      peopleDf.write.parquet("people.parquet")
//      spark.read.parquet("people.parquet")



  }

}
