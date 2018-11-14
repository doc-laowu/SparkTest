package scalapro.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 以flume推数据的方式到spark端消费
  */
object FlumePushWordCount {

  def main(args: Array[String]) {
    val host = "192.168.73.1"//args(0)
    val port = 8888//args(1).toInt
    val conf = new SparkConf().setAppName("FlumeWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //推送方式: flume向spark发送数据
    val flumeStream = FlumeUtils.createStream(ssc, host, port)
    //flume中的数据通过event.getBody()才能拿到真正的内容
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(",")).map((_, 1))

    val results = words.reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
