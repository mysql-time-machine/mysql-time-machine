package com.booking.spark

case class Arguments(
  timestamp: Long = -1,
  configPath: String = null,
  mySQLTableName: String = null,
  hbaseTableName: String = null,
  schemaTableName: String = null,
  hiveTableName: String = null
)

/**
  * Parses command line arguments into an Argument object
  * @param args command line arguments
  */
object ArgumentParser {
  val parser = new scopt.OptionParser[Arguments]("hbase-snapshotter") {
    note("Options:")

    opt[Long]('t', "timestamp")
      .valueName("<TIMESTAMP>")
      .action((timestamp_, c) => c.copy(timestamp = timestamp_))
      .text("Takes a snapshot of the latest HBase version available before the given timestamp (exclusive). " +
        "If this option is not specified, the latest timestamp will be used.")

    help("help")
      .text("Prints this usage text")

    note("\nArguments:")

    arg[String]("<config file>")
      .action((configPath_, c) => c.copy(configPath = configPath_))
      .text("The path of a yaml config file.")

    arg[String]("<original table>")
      .action((mySQLTableName_, c) => c.copy(mySQLTableName = mySQLTableName_))
      .text("The original MySQL table you are copying from. " +
        "It should be in the format Tablename")

    arg[String]("<source table>")
      .action((hbaseTableName_, c) => c.copy(hbaseTableName = hbaseTableName_))
      .text("The source HBase table you are copying from. " +
        "It should be in the format NAMESPACE:TABLENAME")

    arg[String]("<schema table>")
      .action((schemaTableName_, c) => c.copy(schemaTableName = schemaTableName_))
      .text("The HBase schema table you are converting against. " +
        "It should be in the format NAMESPACE:TABLENAME")

    arg[String]("<dest table>")
      .action((hiveTableName_, c) => c.copy(hiveTableName = hiveTableName_))
      .text("The destination Hive table you are copying to. " +
        "It should be in the format database.tablename")
  }

  def apply(args: Array[String]): Arguments = {
    parser.parse(args, Arguments()) match {
      case Some(config) => {
        config
      }
      case None => {
        System.exit(1)
        return(Arguments())
      }
    }
  }
}
