package scalapro.SparkStreaming

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 以拉去数据的方式从flume消费数据
  */
object FlumePollWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从flume中拉取数据(flume的地址)
    val address = Seq(new InetSocketAddress("node01", 8888))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_,1))
    val results = words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
