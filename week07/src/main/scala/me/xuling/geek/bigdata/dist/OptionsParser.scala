package me.xuling.geek.bigdata.dist

import scopt.OptionParser

import java.net.URI

/**
 * ${todo}
 *
 * @author jack
 * @since 2022/4/15
 * */
object OptionsParser {

  def parser(args: Array[String]): DistCpOptions = {
    val parser = new OptionParser[DistCpOptions]("") {
      opt[Unit]("i")
        .action((_, c) => c.copy(ignoreFailures = true))
        .text("Ignore failures")
      opt[Int]("m")
        .action((m, c) => c.copy(maxMaps=m))
        .text("Max number of concurrent maps to use for copy")
      help("help").text("prints this usage text")
      arg[URI]("[source_path...] <target_path")
        .minOccurs(2)
        .minOccurs(1024)
        .action((f, c) => c.copy(files =c.files :+ f))
    }
    parser.parse(args, DistCpOptions()) match {
      case Some(config) =>
        config
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }

}


