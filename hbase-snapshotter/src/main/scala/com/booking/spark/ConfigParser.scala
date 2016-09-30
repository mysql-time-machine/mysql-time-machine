package com.booking.spark

import java.nio.file.{Paths, Files}
import java.util
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._

case class IllegalFormatException(
  message: String,
  cause: Throwable
) extends RuntimeException(message, cause)

/**
 * Simple utility class for loading configuration from yml file
 * This is the structure of the config file:
 *
 * hbase:
 * zookeeper_quorum:  ['hbase-zk1-host', 'hbase-zkN-host']
 */
class ConfigParser(configPath: String) {
  val yaml = new Yaml()
  val in = Files.newInputStream(Paths.get(configPath))
  val config = yaml.load(in).asInstanceOf[util.Map[String, util.Map[String, Object]]]

  try {
    getZooKeeperQuorum
  }
  catch {
    case e: Exception =>
      throw IllegalFormatException(
        "The yaml config file is not formatted correctly." +
          "Check readme.md file for more information.",
        e
      )
  }

  def getZooKeeperQuorum(): String = {
    return config.get("hbase")
      .get("zookeeper_quorum")
      .asInstanceOf[util.ArrayList[String]]
      .asScala.mkString(", ")
  }
}
