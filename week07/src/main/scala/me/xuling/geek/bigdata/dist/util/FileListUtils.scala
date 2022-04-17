package me.xuling.geek.bigdata.dist.util

import me.xuling.geek.bigdata.dist.DistCp.KeyedCopyDefinition
import me.xuling.geek.bigdata.dist.{CopyDefinitionWithParentDirs, CopyListingFileStatus, DistCpOptions, CopyDefinition}
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.collection.JavaConverters.{asScalaIteratorConverter, seqAsJavaListConverter}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object FileListUtils extends Logging {

  /** Turn a [[RemoteIterator]] into a Scala [[Iterator]]
   */
  private implicit class ScalaRemoteIterator[T](underlying: RemoteIterator[T])
    extends Iterator[T] {
    override def hasNext: Boolean = underlying.hasNext

    override def next(): T = underlying.next()
  }

  /** Recursively list files in a given directory on a given FileSystem. This
   * will be done in parallel depending on the value of `threads`. An optional
   * list of regex filters to filter out files can be given.
   *
   * @param fs
   * FileSystem to search
   * @param path
   * Root path to search from
   * @param distCpOptions
   * distCp options
   */
  def listFiles(
                 fs: FileSystem,
                 path: Path,
                 distCpOptions: DistCpOptions
               ): Seq[(CopyListingFileStatus, Seq[CopyListingFileStatus])] = {

    val maybePathRoot = Some(CopyListingFileStatus(fs.getFileStatus(path)))

    val processed = new java.util.concurrent.LinkedBlockingQueue[
      (CopyListingFileStatus, Seq[CopyListingFileStatus])
    ](maybePathRoot.map((_, Seq.empty)).toSeq.asJava)

    val toProcess = new java.util.concurrent.LinkedBlockingDeque[
      (Path, Seq[CopyListingFileStatus])
    ](List((path, maybePathRoot.toSeq)).asJava)

    val exceptions = new java.util.concurrent.ConcurrentLinkedQueue[Exception]()

    val threadsWorking = new ConcurrentHashMap[UUID, Boolean]()

    class FileLister extends Runnable {

      private val localFS = FileSystem.get(fs.getUri, fs.getConf)

      private val uuid = UUID.randomUUID()
      threadsWorking.put(uuid, true)

      override def run(): Unit = {
        while (threadsWorking.containsValue(true)) {
          Try(
            Option(toProcess.pollFirst(50, TimeUnit.MILLISECONDS))
          ).toOption.flatten match {
            case None =>
              threadsWorking.put(uuid, false)
            case Some(p) =>
              threadsWorking.put(uuid, true)
              try {
                localFS
                  .listLocatedStatus(p._1)
                  .foreach {
                    case l if l.isSymlink =>
                      throw new RuntimeException(s"Link [$l] is not supported")
                    case d if d.isDirectory =>
                      val s = CopyListingFileStatus(d)
                      toProcess.addFirst((d.getPath, p._2 :+ s))
                      processed.add((s, p._2))

                    case f =>
                      processed.add((CopyListingFileStatus(f), p._2))
                  }
              } catch {
                case e: Exception => exceptions.add(e)
              }
          }
        }
      }
    }

    val pool = Executors.newFixedThreadPool(distCpOptions.numListStatusThreads)

    val tasks: Seq[Future[Unit]] = List
      .fill(distCpOptions.numListStatusThreads)(new FileLister)
      .map(pool.submit)
      .map(j =>
        Future {
          j.get()
          ()
        }(scala.concurrent.ExecutionContext.global)
      )

    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(Future.sequence(tasks), Duration.Inf)
    pool.shutdown()

    if (!toProcess.isEmpty)
      throw new RuntimeException(
        "Exception listing files, toProcess queue was not empty"
      )

    if (!exceptions.isEmpty) {
      val collectedExceptions = exceptions.iterator().asScala.toList
      collectedExceptions
        .foreach { e =>
          error("Exception during file listing", e)
        }
      throw collectedExceptions.head
    }

    processed.iterator().asScala.toSeq // Lazy streamify?

  }

  /** List all files in the given source URIs. This function will throw an
   * exception if any source files collide on identical destination locations
   * and any collisions on any cases where a source files is the same as the
   * destination file (copying between the same FileSystem)
   */
  def getSourceFiles(
                      sparkContext: SparkContext,
                      sourceURIs: Seq[URI],
                      destinationURI: URI,
                      options: DistCpOptions
                    ): RDD[KeyedCopyDefinition] = {
    val sourceRDD = sourceURIs
      .map { sourceURI =>
        val sourceFS = new Path(sourceURI).getFileSystem(sparkContext.hadoopConfiguration)
        sparkContext
          .parallelize(
            FileListUtils.listFiles(
              sourceFS,
              new Path(sourceURI),
              options
            )
          )
          .map { case (f, d) =>
            val dependentFolders = d.map { dl =>
              val udl = PathUtils.sourceURIToDestinationURI(
                dl.uri,
                sourceURI,
                destinationURI
              )
              CopyDefinition(dl, udl)
            }
            val fu = PathUtils.sourceURIToDestinationURI(
              f.uri,
              sourceURI,
              destinationURI
            )
            CopyDefinitionWithParentDirs(f, fu, dependentFolders)
          }
      }
      .reduce(_ union _)
      .map(_.toKeyedDefinition)

    handleSourceCollisions(sourceRDD)

    handleDestCollisions(sourceRDD)

    sourceRDD
  }

  /** List all files at the destination path
   */
  def getDestinationFiles(
                           sparkContext: SparkContext,
                           destinationPath: Path,
                           options: DistCpOptions
                         ): RDD[(URI, CopyListingFileStatus)] = {
    val destinationFS =
      destinationPath.getFileSystem(sparkContext.hadoopConfiguration)
    sparkContext
      .parallelize(
        FileListUtils.listFiles(
          destinationFS,
          destinationPath,
          options
        )
      )
      .map { case (f, _) => (f.getPath.toUri, f) }
  }

  /** Throw an exception if any source files collide on identical destination
   * locations
   */
  def handleSourceCollisions(source: RDD[KeyedCopyDefinition]): Unit = {
    val collisions = source
      .groupByKey()
      .filter(_._2.size > 1)

    collisions
      .foreach { case (f, l) =>
        error(
          s"The following files will collide on destination file [$f]: ${l.map(_.source.getPath).mkString(", ")}"
        )
      }

    if (!collisions.isEmpty())
      throw new RuntimeException(
        "Collisions found where multiple source files lead to the same destination location; check executor logs for specific collision detail."
      )
  }

  /** Throw an exception for any collisions on any cases where a source files is
   * the same as the destination file (copying between the same FileSystem)
   */
  def handleDestCollisions(source: RDD[KeyedCopyDefinition]): Unit = {

    val collisions = source
      .collect {
        case (_, CopyDefinitionWithParentDirs(s, d, _)) if s.uri == d => d
      }

    collisions
      .foreach { d =>
        error(
          s"The following file has the same source and destination location: [$d]"
        )
      }

    if (!collisions.isEmpty())
      throw new RuntimeException(
        "Collisions found where a file has the same source and destination location; check executor logs for specific collision detail."
      )
  }

}
