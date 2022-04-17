package me.xuling.geek.bigdata.dist

import java.net.URI
import me.xuling.geek.bigdata.dist.DistCp.KeyedCopyDefinition

/** Definition of a single copy
  *
  * @param source
  *   Source file/folder to copy
  * @param destination
  *   Destination to copy to
  */
case class CopyDefinition(
  source: CopyListingFileStatus,
  destination: URI
)

/** Definition of a copy that includes any copying of parent folders this
  * file/folder depends on
  *
  * @param source
  *   Source file/folder to copy
  * @param destination
  *   Destination to copy to
  * @param parentDirs
  *   Any parent directory up to source copies this file/folder
  */
case class CopyDefinitionWithParentDirs(
                                         source: CopyListingFileStatus,
                                         destination: URI,
                                         parentDirs: Seq[CopyDefinition]
) {

  def toKeyedDefinition: KeyedCopyDefinition = (destination, this)

  def getAllCopyDefinitions: Seq[CopyDefinition] =
    parentDirs :+ CopyDefinition(source, destination)
}
