package scalapro.SparkSuanZi

import org.apache.spark.{SparkConf, SparkContext}

object SparkAccumulator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkAccumulator").setMaster("local")
    val sc = new SparkContext(conf)

    var sum = sc.longAccumulator("getSum")
    val arr = Array(1,2,3,4,5)
    val rdd = sc.makeRDD(arr).map(x=>{
      sum.add(x)
    })
    rdd.collect()
    println(sum.value)
    //打印分区数
    println(rdd.partitions.size)
    sc.stop()
  }
}
