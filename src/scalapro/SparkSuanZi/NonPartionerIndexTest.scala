package scalapro.SparkSuanZi

import org.apache.spark.{SparkConf, SparkContext}

object NonPartionerIndexTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NonPartionerIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(arr, 5).mapPartitionsWithIndex((index,iter)=>{
      Iterator(index.toString+" : "+iter.mkString("|"))
    }).collect()

    rdd.foreach(x=>println(x))

    sc.stop()

  }
}
