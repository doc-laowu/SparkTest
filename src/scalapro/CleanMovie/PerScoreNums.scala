package scalapro.CleanMovie

import org.apache.spark.SparkConf

object PerScoreNums {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("PerScoreNums")

  }
}
