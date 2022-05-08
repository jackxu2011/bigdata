## 作业一

1. 修改SqlBase.g4增加二行
```
| SHOW VERSION                                                     #showVersion
   
VERSION : 'VERSION';
```

2. 增加 ShowVersionCommand.scala
```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData

/**
 * @author jack
 * @since 2022/5/3
 */
case class ShowVersionCommand() extends LeafRunnableCommand with IgnoreCachedData {

  //返回数据的structType在这里指定
  override def output: Seq[Attribute] = Seq(AttributeReference("version", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val javaVersion = System.getProperty("java.version")
    // scalastyle:off println
    println(s"java version: $javaVersion")
    println(s"spark version: ${sparkSession.version}")
    // scalastyle:on println
    val output = "Spark Version: %s, Java Version: %s".format(sparkSession.version, javaVersion);
    Seq(Row(output))
  }

}
```

3. 修改 SparkSqlParser.scala 
```scala
/**
   * Create a [[ShowVersion]] command.
   */
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }
```
4. 编译代码
需要增加`-Phive-thriftserver` 参数，否则无法运行 spark-sql

```shell
 mvn clean package -DskipTests -Phive-thriftserver
```
5. 运行./bin/spark-sql 执行

```shell
spark-sql> show version;
java version: 1.8.0_333
spark version: 3.2.1
Spark Version: 3.2.1, Java Version: 1.8.0_333
Time taken: 0.023 seconds, Fetched 1 row(s)
```
* 也可以通过./bin/spark-shell 执行
```shell
scala> spark.sql("show version").show
java version: 1.8.0_333
spark version: 3.2.1
+--------------------+
|             version|
+--------------------+
|Spark Version: 3....|
+--------------------+
```

## 作业二
1. 构建一条 SQL，同时 apply 下面三条优化规则：
* CombineFilters
* CollapseProject
* BooleanSimplification
```sql
    select amount 
    from (
        select id, amount //CollapseProject id
        from sales
        where 
          (status = 1 or status = 2) //CombineFilters status in (1 , 2)
          and id is not null) a
    where 1 = 1 // BooleanSimplification 1 = 1 true prune
```
2. 构建一条 SQL，同时 apply 下面五条优化规则：
* ConstantFolding
* PushDownPredicates
* ReplaceDistinctWithAggregate
* ReplaceExceptWithAntiJoin
* FoldablePropagation

```sql

SELECT 1+2 x, a1, a2  //ConstantFolding
from (
  SELECT a1, a2 FROM tab1 EXCEPT SELECT b1, b2 FROM tab2 //ReplaceExceptWithAntiJoin, ReplaceDistinctWithAggregate 
    where a1 > 100 //PushDownPredicates
) tab3
order by x // FoldablePropagation

```

## 作业三
简单实现MyPushDown, 还不清楚这个到底要怎么转换， 只是简单的为每个Command打印一个日志

```scala
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
```

执行结果: 打开spark-sql
```shell
set spark.sql.planChangeLog.level=WARN
spark-sql --jars week09-1.0.0-SNAPSHOT.jar --conf spark.sql.extensions=me.xuling.geek.bigdata.w09.MySparkSessionExtension
```
执行 show databases;

```shell
spark-sql> show databases;
22/05/04 16:04:07 WARN [main] MyPushDown: MyPushDown do nothing
22/05/04 16:04:07 WARN [main] MyPushDown: MyPushDown do nothing
afa
caixiao
caizhen
caojingwei
chaicq
chchang
cheechuen
chengwb
chenxi
cxp_movie
damon
db_test
.....
```
