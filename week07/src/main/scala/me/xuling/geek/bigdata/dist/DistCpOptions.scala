package me.xuling.geek.bigdata.dist

import org.apache.hadoop.fs.Path

import java.net.URI

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/13
 * */
case class DistCpOptions(
      ignoreFailures: Boolean = DistCpOptions.Defaults.ignoreFailures,
      maxMaps: Int = DistCpOptions.Defaults.maxMaps,
      maxBytesPerTask: Long = DistCpOptions.Defaults.maxMaps,
      files: Seq[URI] = Seq.empty,
      numListStatusThreads: Int = DistCpOptions.Defaults.numListStatusThreads) {

  def validateOptions(): Unit = {
    assert(maxMaps > 0, "maxMaps must be positive")
    assert(numListStatusThreads > 0, "numListStatusThreads must be positive")
  }

  def sourceAndDestPaths: (Seq[Path], Path) = {
    files.reverse match {
      case d :: ts =>
        (ts.reverse.map(u => new Path(u)), new Path(d))
      case _ => throw new RuntimeException("Incorrect number of URIs")
    }
  }

}

object DistCpOptions {
  object Defaults {
    val ignoreFailures: Boolean = false
    val maxMaps: Int = 20
    val numListStatusThreads: Int = 10
    val maxBytesPerTask: Long = 1073741824L
  }
}