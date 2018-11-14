package scalapro.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingByNc {
  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.

    /**
      * 在conf中配置app运行的资源时，分配的核心数（或者线程数）
      * 要大于输入输入流的数量，否则只是接收到数据而没有进行处理。
      */

    /**
      * Points to remember
      * When running a Spark Streaming program locally, do not use “local” or “local[1]” as the master URL.
      * Either of these means that only one thread will be used for running tasks locally. If you are using
      * an input DStream based on a receiver (e.g. sockets, Kafka, Flume, etc.), then the single thread will
      * be used to run the receiver, leaving no thread for processing the received data. Hence, when running
      * locally, always use “local[n]” as the master URL, where n > number of receivers to run (see Spark
      * Properties for information on how to set the master).
      *
      * Extending the logic to running on a cluster, the number of cores allocated to the Spark Streaming
      * application must be more than the number of receivers. Otherwise the system will receive data, but
      * not be able to process it.
      */

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingByNc")
    val ssc = new StreamingContext(conf, Seconds(5))
    // Create a DStream that will connect to hostname:port, like localhost:9999
//    val lines = ssc.socketTextStream("192.168.49.131", 9999)
    val customReceiverStream = ssc.receiverStream(new CustomReceiver("192.168.49.131", 9999))
    val words = customReceiverStream.flatMap(_.split(" "))
    // Split each line into words
//    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
