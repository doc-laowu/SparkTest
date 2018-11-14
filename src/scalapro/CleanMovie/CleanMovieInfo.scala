package scalapro.CleanMovie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 清洗电影数据
  *
  * 超时空同居	(2018)	苏伦	喜剧-爱情-奇幻	中国大陆	汉语普通话	101	6.9	251223	8.7%-37.7%-45.1%-7.4%-1.1%	爱情-喜剧-搞笑-国产-奇幻-2018-中国大陆-穿越
  *
  * 去掉时间、国家、语言之间的间隔符的不一致
  *
  */

object CleanMovieInfo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("CleanMovieInfo")
    val sc = new SparkContext(conf)

    sc.textFile("E:\\IdeaProjects\\SparkTest\\SrcInput\\doubanmovie\\DouBanMovies1.txt").map(line=>{
      val arr = line.split("\t")
      val year = arr(1).replace("(", "").replace(")", "")
      var city = arr(4).replaceAll(" / ", "-")
      var language = arr(5).replaceAll(" / ", "-")
      arr(0)+"\t"+year+"\t"+arr(2)+"\t"+arr(3)+"\t"+city+"\t"+language+"\t"+arr(7)+"\t"+arr(8)+"\t"+arr(9)+"\t"+arr(10)
    }).saveAsTextFile("E:\\IdeaProjects\\SparkTest\\moviesinfo\\")

    sc.stop()
  }
}
