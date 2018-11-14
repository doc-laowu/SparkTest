package scalapro.wcspark

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    //包含有关应用程序信息的SparkConf对象。
    val conf = new SparkConf().setAppName("wc").setMaster("local")

    //Spark程序必须做的第一件事是创建一个SparkContext对象，它告诉Spark如何访问集群。
    val sc = new SparkContext(conf)

    sc.textFile("E:\\IdeaProjects\\SparkTest\\input").flatMap(_.split("\t")).map((_, 1))
      .reduceByKey(_ + _).saveAsTextFile("E:\\IdeaProjects\\SparkTest\\output")

//    sc.parallelize(Array(("asf", 1243124), ("12sf", 345), ("wqr", 2332), ("wqr", 2332), ("wqr", 2332)))
//      .combineByKey(v=>(v,1),
//        (acc1:(Int, Int), v)=>(acc1._1 + v, acc1._2+1),
//        (acc1:(Int, Int), acc2:(Int,Int))=>(acc1._1+acc2._1, acc1._2+acc2._2))
//        .map(k=>(k._1, k._2._1/k._2._2.toDouble)


//    val arr = Array(1,2,3,4,5)
//    val rdd = sc.makeRDD(arr)
//    val sum = rdd.map(x=>(0, x)).reduceByKey(_+_).collect().foreach(x=>println(x._2))

    //使用RDD.foreach(print)打印时，会将结果打印在每个集群的节点上，
    // 而需要打印在一台上，需要先collect操作，将结果集传到driver上。但是collect操作可能会导致内存不足
    //更安全的方法是使用take(): RDD .take(100).foreach(println)。

    sc.stop()

  }
}
