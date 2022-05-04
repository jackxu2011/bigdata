package me.xuling.geek.bigdata.w09

import org.apache.spark.sql.SparkSessionExtensions

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/5/4
 * */
class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule(session => MyPushDown(session))
  }
}
