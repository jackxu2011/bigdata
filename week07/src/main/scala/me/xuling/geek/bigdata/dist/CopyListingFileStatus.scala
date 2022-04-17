package me.xuling.geek.bigdata.dist

import org.apache.hadoop.fs.{FileStatus, Path}

import java.net.URI

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/15
 * */
case class CopyListingFileStatus(uri: URI, len: Long, isDir: Boolean) extends Serializable {

  def getPath: Path = new Path(uri)

  def getLen: Long = len

  def isDirectory: Boolean = isDir
}

object CopyListingFileStatus {

  /** Create a [[CopyListingFileStatus]] from a [[FileStatus]] object
   */
  def apply(fileStatus: FileStatus): CopyListingFileStatus = {
    CopyListingFileStatus(
      fileStatus.getPath.toUri,
      fileStatus.getLen,
      fileStatus.isDirectory
    )
  }
}