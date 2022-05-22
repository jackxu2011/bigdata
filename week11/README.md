# 作业
1. 修改SqlBase.g4增加二行
```
    | COMPACT TABLE target=tableIdentifier partitionSpec?
    (INTO fileNum=INTEGER_VALUE identifier)?                           #compactTable
```

2. 增加 CompactTableCommand.scala
```scala
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier


/**
 * Analyzes the given table to generate statistics, which will be used in query optimizations.
 */
case class CompactTableCommand(
    tableIdent: TableIdentifier,
    fileNum: Int,
    partitionSpec: Map[String, Option[String]]) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val db = tableIdent.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = TableIdentifier(tableIdent.table, Some(db))
    val tableMeta = sessionState.catalog.getTableMetadata(tableIdentWithDB)

    sparkSession.table(tableIdentWithDB).repartition(fileNum).write.mode(SaveMode.Overwrite)
      .saveAsTable(tableIdentWithDB.identifier)

    Seq.empty[Row]
  }
}

```

3. 修改 SparkSqlParser.scala

```scala
  /**
   * Create an [[CompactTable]].
   * Example SQL for analyzing a table or a set of partitions :
   * {{{
   *   COMPACT TABLE multi_part_name [PARTITION (partcol1[=val1], partcol2[=val2], ...)]
   *   [INTO fileNum FILES];
   * }}}
   *
   */
  override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val target = visitTableIdentifier(ctx.target)
    val fileNum = if (ctx.fileNum != null) {
      ctx.fileNum.getText.toInt
    } else {
      0
    }

    val partitionSpec = if (ctx.partitionSpec() != null) {
      visitPartitionSpec(ctx.partitionSpec())
    } else {
      Map.empty[String, Option[String]]
    }

    CompactTableCommand(target, fileNum, partitionSpec)
  }
```

4. 编译代码
   需要增加`-Phive-thriftserver` 参数，否则无法运行 spark-sql

```shell
 mvn clean package -DskipTests -Phive-thriftserver
```

5. 运行./bin/spark-sql 执行

暂时还没有完成，看下周能不能完善它