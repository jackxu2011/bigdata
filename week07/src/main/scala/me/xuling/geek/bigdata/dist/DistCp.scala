package me.xuling.geek.bigdata.dist

import me.xuling.geek.bigdata.dist.info.{Accumulators, DistCpResult}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import me.xuling.geek.bigdata.dist.util.{CopyUtils, FileListUtils, Logging, PathUtils}
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.RDD

import java.net.URI

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/13
 * */
object DistCp extends Logging{

  type KeyedCopyDefinition = (URI, CopyDefinitionWithParentDirs)

  def main(args: Array[String]): Unit = {

    val distCpOptions = OptionsParser.parser(args)
    val spark = SparkSession.builder()
      .appName("distCp")
      .getOrCreate()
    val (src, dest) = distCpOptions.sourceAndDestPaths
    run(spark, src, dest, distCpOptions)
  }

  /** Main entry point for programmatic access to the application.
   *
   * @param sparkSession
   *   Active Spark Session
   * @param sourcePaths
   *   Source paths to copy from
   * @param destinationPath
   *   Destination path to copy to
   * @param options
   *   Options to use in the application
   */
  def run(
           sparkSession: SparkSession,
           sourcePaths: Seq[Path],
           destinationPath: Path,
           options: DistCpOptions
         ): Unit = {

    val qualifiedSourcePaths = sourcePaths.map(
      PathUtils
        .pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, _)
    )
    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(
      sparkSession.sparkContext.hadoopConfiguration,
      destinationPath
    )

    val sourceRDD = FileListUtils.getSourceFiles(
      sparkSession.sparkContext,
      qualifiedSourcePaths.map(_.toUri),
      qualifiedDestinationPath.toUri,
      options
    )

    val destinationRDD = FileListUtils.getDestinationFiles(
      sparkSession.sparkContext,
      qualifiedDestinationPath,
      options
    )

    val joined = sourceRDD.fullOuterJoin(destinationRDD)

    val toCopy = joined.collect { case (_, (Some(s), _)) => s }

    val accumulators = new Accumulators(sparkSession)

    val copyResult: RDD[DistCpResult] = doCopy(toCopy, accumulators, options)

    copyResult.foreach(_ => ())

    info("DistCp Run Statistics\n" + accumulators.getOutputText)

  }

  /** DistCP helper implicits on iterators
   */
  private[dist] implicit class DistCpIteratorImplicit[B](
                   iterator: Iterator[B]
                 ) {

    /** Scan over an iterator, mapping as we go with `action`, but making a
     * decision on which objects to actually keep using a set of what objects
     * have been seen and the `skip` function. Similar to a combining `collect`
     * and `foldLeft`.
     *
     * @param skip
     *   Should a mapped version of this element not be included in the output
     * @param action
     *   Function to map the element
     * @return
     *   An iterator
     */
    def collectMapWithEmptyCollection(
                                       skip: (B, Set[B]) => Boolean,
                                       action: B => DistCpResult
                                     ): Iterator[DistCpResult] = {

      iterator
        .scanLeft((Set.empty[B], None: Option[DistCpResult])) {
          case ((z, _), d) if skip(d, z) => (z, None)
          case ((z, _), d) =>
            (z + d, Some(action(d)))
        }
        .collect { case (_, Some(r)) => r }
    }

  }

  /** Perform the copy portion of the DistCP
   */
  private[dist] def doCopy(
                            sourceRDD: RDD[CopyDefinitionWithParentDirs],
                            accumulators: Accumulators,
                            options: DistCpOptions
                                 ): RDD[DistCpResult] = {

    val serConfig = new ConfigSerDeser(
      sourceRDD.sparkContext.hadoopConfiguration
    )
    batchAndPartitionFiles(
      sourceRDD,
      options.maxMaps,
      options.maxBytesPerTask
    )
      .mapPartitions { iterator =>
        val hadoopConfiguration = serConfig.get()
        val attemptID = TaskContext.get().taskAttemptId()
        val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

        iterator
          .flatMap(_._2.getAllCopyDefinitions)
          .collectMapWithEmptyCollection(
            (d, z) => z.contains(d),
            d => {
              val r = CopyUtils.handleCopy(
                fsCache.getOrCreate(d.source.uri),
                fsCache.getOrCreate(d.destination),
                d,
                options,
                attemptID
              )
              accumulators.handleResult(r)
              r
            }
          )
      }
  }

  /** Batch the given RDD into groups of files depending on
   * [[DistCpOptions.maxMaps]]
   * the same batches are in the same partitions
   */
  private[dist] def batchAndPartitionFiles(
                                            rdd: RDD[CopyDefinitionWithParentDirs],
                                            maxFilesPerTask: Int,
                                            maxBytesPerTask: Long
                                                 ): RDD[((Int, Int), CopyDefinitionWithParentDirs)] = {
    val partitioner =
      rdd.partitioner.getOrElse(new HashPartitioner(rdd.partitions.length))
    val sorted = rdd
      .map(v => (v.source.uri.toString, v))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(_._2)
    val batched = sorted.mapPartitionsWithIndex(
      generateBatchedFileKeys(maxFilesPerTask, maxBytesPerTask)
    ) // sorted

    batched.partitionBy(CopyPartitioner(batched))
  }

  /** Key the RDD within partitions based on batches of files based on
   * [[DistCpOptions.maxMaps]] and
   * [[DistCpOptions.maxBytesPerTask]] thresholds
   */
  private[dist] def generateBatchedFileKeys(
                                                    maxFilesPerTask: Int,
                                                    maxBytesPerTask: Long
                                                  ): (Int, Iterator[CopyDefinitionWithParentDirs]) => Iterator[
    ((Int, Int), CopyDefinitionWithParentDirs)
  ] = { (partition, iterator) =>
    iterator
      .scanLeft[(Int, Int, Long, CopyDefinitionWithParentDirs)](
        0,
        0,
        0,
        null
      ) { case ((index, count, bytes, _), definition) =>
        val newCount = count + 1
        val newBytes = bytes + definition.source.getLen
        if (newCount > maxFilesPerTask || newBytes > maxBytesPerTask) {
          (index + 1, 1, definition.source.getLen, definition)
        } else {
          (index, newCount, newBytes, definition)
        }
      }
      .drop(1)
      .map { case (index, _, _, file) => ((partition, index), file) }
  }

}
