package me.xuling.geek.bigdata.dist.info

/** Result of the DistCP action (copy/delete) used for both logging to a logger
 * and a file.
 */
trait DistCpResult extends Serializable {

  def getMessage: String

}
