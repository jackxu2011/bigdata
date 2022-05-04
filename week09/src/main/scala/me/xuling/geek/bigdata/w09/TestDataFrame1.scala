package me.xuling.geek.bigdata.w09

import org.apache.spark.sql.SparkSession

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/5/1
 * */
object TestDataFrame1 {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("RDDToDataFrame")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val peopleRDD = sc.textFile(getClass.getClassLoader.getResource("people.txt").getPath)
      .map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    import spark.implicits._
    val df = peopleRDD.toDF()
    df.createTempView("people")
    spark.sql("select * from people").show()
  }
}
