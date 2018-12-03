package scalapro.SparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStream2Db {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DStream2Db").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "vvvv",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    //写入数据库的代码
//    stream.foreachRDD(rdd=>{
//
//      rdd.foreachPartition(partitionOfRecords => {
//        val connection = ConnectionPool.getConnection()
//        partitionOfRecords.foreach(record => connection.send(record))
//        ConnectionPool.returnConnection(connection)
//      })
//
//    })

    //在DStream上使用DF or SparkSql

    stream.flatMap(_.value().split(" ")).foreachRDD(rdd => {

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()

      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()

    })

    //当想查询之前的数据时（由于到下一个时间断的时候，之前的数据会被删除），比如：可以使用
    //streamingContext.remember(Minutes(5)) 记住前五分钟内的数据

    ssc.start()
    ssc.awaitTermination()

  }

}
