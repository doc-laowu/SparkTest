package scalapro.SparkStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag


object KafkaRdd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaRdd")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "vvvv",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

    //这是实验中的Api现在还没有正式的加入
//    val rdd = KafkaUtils.createRDD[String, String](
//      sc,
//      kafkaParams,
//      offsetRanges
//    )

    ssc.start()
    ssc.awaitTermination()

    sc.stop()

  }
}
