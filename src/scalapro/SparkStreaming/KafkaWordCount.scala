package scalapro.SparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

/**
  * 从kafka消费数据  这个存在于kafka 0.8及以前的版本
  */
//object KafkaWordCount {
//
//  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
//    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
//    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
//  }
//
//
//  def main(args: Array[String]) {
////    LoggerLevels.setStreamingLogLevels()
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//    ssc.checkpoint("c://ck2")
//    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
//    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
//    val words = data.map(_._2).flatMap(_.split(" "))
//    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}


/**
  * 通过直接流的方式消费数据
  *
  * 存在于kafka0.10版本及之后
  */

object KafkaWordCount {

  //对流式计算的结果进行累加
  val updateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) =>{
    iterator.flatMap { case(x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m)) }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    /**
      *默认有cache，可以禁用set spark.streaming.kafka.consumer.cache.enabled to false
      * The cache for consumers has a default maximum size of 64
      * change this setting via spark.streaming.kafka.consumer.cache.maxCapacity.
      */
    ssc.checkpoint("E:\\IdeaProjects\\SparkTest\\ck")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "vvvv",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,

      /**
        * LocationStrategies PreferBrokers PreferFixed 有着三种选择，区别见于官网
        */
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val results = stream.flatMap(_.value().split(" ")).map((_, 1))
      .updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)

    results.print()

    ssc.start()
    ssc.awaitTermination()

    sc.stop()
  }
}











