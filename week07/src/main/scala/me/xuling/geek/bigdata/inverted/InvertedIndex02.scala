package me.xuling.geek.bigdata.inverted

import org.apache.spark.sql.SparkSession


/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/12
 * */
object InvertedIndex02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("InvertedIndex")
      .getOrCreate()
    val files = spark.sparkContext.wholeTextFiles(args(0))
    val fileInput = files.map { x => (x._1.split("/").last, x._2) }
    val words = fileInput.flatMap { f =>
      val lines = f._2.split("\n");
      lines.flatMap{line => line.split("\\s+").map{
        v => ((v, f._1), 1)
      }}
    }.reduceByKey(_+_)
      .map(x=> (x._1._1, (x._1._2, x._2)))

    words
      .sortByKey()
      .aggregateByKey(List.empty[(String, Int)])(
        _ :+ _,
        _++_)
      .sortByKey()
      .map(v => (v._1, v._2.mkString("{", ", ", "}")))
      .map(word=>s"${word._1}:${word._2}")
      .saveAsTextFile(args(1))
  }
}
