package scalapro.SparkSuanZi

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2
import scala.collection.JavaConversions._

object CustomeAccumulatorTest {

    def main(args: Array[String]) {
      val conf=new SparkConf().setAppName("CustomeAccumulator").setMaster("local[*]")
      val sc=new SparkContext(conf)

      val accum = new CustomeAccumulator
      sc.register(accum, "logAccum")
      val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2)
        .filter(line => {
        val pattern = """^-?(\d+)"""
        val flag = line.matches(pattern)
        if (!flag) {
          accum.add(line)
        }
        flag
      }).map(_.toInt).reduce(_ + _)

      println("sum: " + sum)
      for (v <- accum.value) print(v + " ")
      println()
      sc.stop()
    }
}


class CustomeAccumulator extends AccumulatorV2[String, java.util.Set[String]] {

  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: CustomeAccumulator => _logArray.addAll(o.value)
    }

  }

  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy():org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new CustomeAccumulator()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}