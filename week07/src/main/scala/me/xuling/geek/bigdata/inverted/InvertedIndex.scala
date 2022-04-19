package me.xuling.geek.bigdata.inverted

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/12
 * */
object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("InvertedIndex")
      .getOrCreate()
    val files = spark.sparkContext.wholeTextFiles(args(0))
    val fileInput = files.map { x => (x._1.split("/").takeRight(1), x._2) }
    val words = fileInput.flatMap { f =>
      val lines = f._2.split("\n");
      lines.flatMap{line => line.split("\\s+").map{
        v => (v, f._1)
      }}
    }.distinct()

    words.sortByKey()
      .aggregateByKey(new mutable.HashSet[String]())(_+_, _++_)
      .sortByKey()
      .map(word=> s"${word._1}:${word._2}")
      .saveAsTextFile(args(1))
  }
}
