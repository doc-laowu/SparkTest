package scalapro.SparkStreaming

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFullWordCount {

  val updateFunc = (iter:(Iterator[(String, Seq[Int], Option[Int])])) => {
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m)) }
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingByNc")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("192.168.49.131", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch

//    sc.setCheckpointDir("E://ck")

    //使用updateStateByKey必须checkpoint操作。
    val results = words.map(word => (word, 1)).updateStateByKey(updateFunc,
    new HashPartitioner(sc.defaultParallelism), true)

    // Print the first ten elements of each RDD generated in this DStream to the console
    results.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    sc.stop()
    ssc.stop()
  }
}
