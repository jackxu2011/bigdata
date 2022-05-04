package me.xuling.geek.bigdata.w09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/5/1
 * */
object TestDataFrame2 {

  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder()
      .appName("TestDataFrame2")
      .master("local")
      .getOrCreate()
    val fileRDD = spark.sparkContext.textFile(getClass.getClassLoader.getResource("people.txt").getPath)

    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })

    val structType: StructType = StructType(
      StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil
    )

    var df: DataFrame = spark.createDataFrame(rowRDD, structType)
    df.createTempView("people")
    spark.sql("select * from people").show()

  }

}
