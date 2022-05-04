package me.xuling.geek.bigdata.w09

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/5/4
 * */
case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] with Logging{
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case command: Command => {
      logWarning("MyPushDown do nothing")
      command
    }
  }
}
