package me.xuling.geek.bigdata.dist.util

import me.xuling.geek.bigdata.dist.info.{CopyActionResult, DirectoryCopyResult, DistCpResult, FileCopyResult}
import me.xuling.geek.bigdata.dist.{CopyDefinition, CopyListingFileStatus, DistCpOptions}
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import java.io.FileNotFoundException
import java.net.URI
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps

object CopyUtils extends Logging {

  /** Handle the copy of a file/folder
    *
    * @param sourceFS
    *   Source FileSystem object
    * @param destFS
    *   Destination FileSystem object
    * @param copyDefinition
    *   Definition of the copy
    * @param options
    *   SparkDistCP options
    * @param taskAttemptID
    *   Spark task attempt ID (used to create a unique temporary file)
    */
  def handleCopy(
                  sourceFS: FileSystem,
                  destFS: FileSystem,
                  copyDefinition: CopyDefinition,
                  options: DistCpOptions,
                  taskAttemptID: Long
  ): DistCpResult = {

    val r = {
      if (copyDefinition.source.isDirectory) {
        CopyUtils.createDirectory(destFS, copyDefinition, options)
      } else {
        CopyUtils.copyFile(sourceFS, destFS, copyDefinition, options, taskAttemptID)
      }
    }

    info(r.getMessage)
    r
  }


  /** Internal create directory function
    */
  private[CopyUtils] def createDirectory(
                                      destFS: FileSystem,
                                      copyDefinition: CopyDefinition,
                                      options: DistCpOptions
  ): DirectoryCopyResult = {
    val destPath = new Path(copyDefinition.destination)
    if (destFS.exists(destPath)) {
      DirectoryCopyResult(
        copyDefinition.source.getPath.toUri,
        copyDefinition.destination,
        CopyActionResult.SkippedAlreadyExists
      )
    } else {
      val result = Try {
        if (destFS.exists(destPath.getParent)) {
          destFS.mkdirs(destPath)
          DirectoryCopyResult(
            copyDefinition.source.getPath.toUri,
            copyDefinition.destination,
            CopyActionResult.Created
          )
        } else
          throw new FileNotFoundException(
            s"Parent folder [${destPath.getParent}] does not exist."
          )
      }
        .recover { case _: FileAlreadyExistsException =>
          DirectoryCopyResult(
            copyDefinition.source.getPath.toUri,
            copyDefinition.destination,
            CopyActionResult.SkippedAlreadyExists
          )
        }
      result match {
        case Success(v) => v
        case Failure(e) if options.ignoreFailures =>
          error(
            s"Exception whilst creating directory [${copyDefinition.destination}]",
            e
          )
          DirectoryCopyResult(
            copyDefinition.source.getPath.toUri,
            copyDefinition.destination,
            CopyActionResult.Failed(e)
          )
        case Failure(e) =>
          throw e
      }
    }
  }

  /** Internal copy file function
    */
  private[CopyUtils] def copyFile(
    sourceFS: FileSystem,
    destFS: FileSystem,
    definition: CopyDefinition,
    options: DistCpOptions,
    taskAttemptID: Long
  ): FileCopyResult = {
    val destPath = new Path(definition.destination)
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) =>
        performCopy(
          sourceFS,
          definition.source,
          destFS,
          definition.destination,
          removeExisting = false,
          ignoreErrors = options.ignoreFailures,
          taskAttemptID
        )
      case Failure(e) if options.ignoreFailures =>
        error(
          s"Exception whilst getting destination file information [${definition.destination}]",
          e
        )
        FileCopyResult(
          definition.source.getPath.toUri,
          definition.destination,
          definition.source.len,
          CopyActionResult.Failed(e)
        )
      case Failure(e) =>
        throw e
      case Success(_) =>
        FileCopyResult(
          definition.source.getPath.toUri,
          definition.destination,
          definition.source.len,
          CopyActionResult.SkippedAlreadyExists
        )
    }
  }

  /** Check whether two files match, based on length and checksum. If either of
    * the checksums are None, then checksums are not used for comparison.
    */
  private[CopyUtils] def filesAreIdentical(
    f1: CopyListingFileStatus,
    mc1: => Option[FileChecksum],
    f2: CopyListingFileStatus,
    mc2: => Option[FileChecksum]
  ): Boolean = {
    if (f1.getLen != f2.getLen) {
      debug(
        s"Length [${f1.getLen}] of file [${f1.uri}] was not the same as length [${f2.getLen}] of file [${f2.uri}]. Files are not identical."
      )
      false
    } else {
      val c1 = mc1
      val c2 = mc2
      val same = mc1.flatMap(c1 => mc2.map(c1 ==)).getOrElse(true)
      if (same) {
        debug(
          s"CRC [$c1] of file [${f1.uri}] was the same as CRC [$c2] of file [${f2.uri}]. Files are identical."
        )
        true
      } else {
        debug(
          s"CRC [$c1] of file [${f1.uri}] was not the same as CRC [$c2] of file [${f2.uri}]. Files are not identical."
        )
        false
      }

    }

  }

  /** Internal copy function Only pass in true for removeExisting if the file
    * actually exists
    */
  def performCopy(
                   sourceFS: FileSystem,
                   sourceFile: CopyListingFileStatus,
                   destFS: FileSystem,
                   dest: URI,
                   removeExisting: Boolean,
                   ignoreErrors: Boolean,
                   taskAttemptID: Long
  ): FileCopyResult = {

    val destPath = new Path(dest)

    val tempPath = new Path(
      destPath.getParent,
      s".sparkdistcp.$taskAttemptID.${destPath.getName}"
    )

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourceFile.getPath))
        if (!destFS.exists(tempPath.getParent))
          throw new RuntimeException(
            s"Destination folder [${tempPath.getParent}] does not exist"
          )
        out = Some(destFS.create(tempPath, false))
        IOUtils.copyBytes(
          in.get,
          out.get,
          sourceFS.getConf.getInt("io.file.buffer.size", 4096)
        )

      } catch {
        case e: Throwable => throw e
      } finally {
        in.foreach(_.close())
        out.foreach(_.close())
      }
    }.map { _ =>
      val tempFile = destFS.getFileStatus(tempPath)
      if (sourceFile.getLen != tempFile.getLen)
        throw new RuntimeException(
          s"Written file [${tempFile.getPath}] length [${tempFile.getLen}] did not match source file [${sourceFile.getPath}] length [${sourceFile.getLen}]"
        )

      if (removeExisting) {
        val res = destFS.delete(destPath, false)
        if (!res)
          throw new RuntimeException(
            s"Failed to clean up existing file [$destPath]"
          )
      }
      if (destFS.exists(destPath))
        throw new RuntimeException(
          s"Cannot create file [$destPath] as it already exists"
        )
      val res = destFS.rename(tempPath, destPath)
      if (!res)
        throw new RuntimeException(
          s"Failed to rename temporary file [$tempPath] to [$destPath]"
        )
    } match {
      case Success(_) if removeExisting =>
        FileCopyResult(
          sourceFile.getPath.toUri,
          dest,
          sourceFile.len,
          CopyActionResult.OverwrittenOrUpdated
        )
      case Success(_) =>
        FileCopyResult(
          sourceFile.getPath.toUri,
          dest,
          sourceFile.len,
          CopyActionResult.Copied
        )
      case Failure(e) if ignoreErrors =>
        error(
          s"Failed to copy file [${sourceFile.getPath}] to [$destPath]",
          e
        )
        FileCopyResult(
          sourceFile.getPath.toUri,
          dest,
          sourceFile.len,
          CopyActionResult.Failed(e)
        )
      case Failure(e) =>
        throw e
    }

  }

}
