package com.booking.spark

import java.util.NavigableMap

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

case class Arguments(pit: Long = -1, configPath: String = null, hbaseTableName: String = null, hiveTableName: String = null)

case class IllegalFormatException(message: String, cause: Throwable) extends RuntimeException(message, cause)

object HBaseSnapshotter {
  //FamilyMap = Map[FamilyName, Map[ColumnName, Map[Timestamp, Value]]]
  type FamilyMap = NavigableMap[Array[Byte], NavigableMap[Array[Byte], NavigableMap[java.lang.Long, Array[Byte]]]]

  /**
    * Parses command line arguments into an Argument object
    * @param args command line arguments
    */
  def parseArguments(args: Array[String]): Arguments = {
    val parser = new scopt.OptionParser[Arguments]("hbase-snapshotter") {
      note("Options:")

      opt[Long]('t', "pit")
        .valueName("<TIMESTAMP>")
        .action( (pit_, c) => c.copy(pit = pit_) )
        .text("Takes a snapshot of the latest HBase version available before the given timestamp (exclusive). " +
          "If this option is not specified, the latest timestamp will be used.")

      help("help")
        .text("Prints this usage text")

      note("\nArguments:")

      arg[String]("<config file>")
        .action( (configPath_, c) => c.copy(configPath = configPath_) )
        .text("The path of a yaml config file.")

      arg[String]("<source table>")
        .action( (hbaseTableName_, c) => c.copy(hbaseTableName = hbaseTableName_) )
        .text("The source HBase table you are copying from. " +
          "It should be in the format NAMESPACE:TABLENAME")

      arg[String]("<dest table>")
        .action ( (hiveTableName_, c) => c.copy(hiveTableName = hiveTableName_) )
        .text("The destination Hive table you are copying to. " +
          "It should be in the format DATABASE.TABLENAME")
    }

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

  def parseConfig(configPath: String): ConfigParser = {
    val configParser = new ConfigParser(configPath)

    try {
      configParser.getZooKeeperQuorum
      configParser.getSchema
      configParser.getDefaultNull
    }
    catch {
      case e: Exception =>
        throw IllegalFormatException(
          "The yaml config file is not formatted correctly." +
            "Check readme.md file for more information.",
          e
        )
    }
    configParser
  }

  def main(cmdArgs: Array[String]): Unit ={
    val args = parseArguments(cmdArgs)
    val config = parseConfig(args.configPath)

    val sc = new SparkContext( new SparkConf().setAppName("HBaseSnapshotter") )
    println(s"SparkApp Id = ${sc.applicationId}")

    val hbaseConfig = HBaseConfiguration.create()
    val zookeeperQuorum: String = config.getZooKeeperQuorum()
    hbaseConfig.set("hbase.zookeeper.quorum", zookeeperQuorum)

    val hbaseContext = new HBaseContext(sc, hbaseConfig)
    val hiveContext = new HiveContext(sc)
    val schema: StructType = config.getSchema

    val scan = new Scan()
    if(args.pit > -1 )
      scan.setTimeRange(0, args.pit)

    // Scans the given HBase table into an RDD.
    val hbaseRDD = hbaseContext.hbaseRDD(args.hbaseTableName, scan, {r: (ImmutableBytesWritable, Result) => r._2})
    val defaultNull: String = config.getDefaultNull

    // Mapping every row in HBase to a Row object in a Spark Dataframe
    // Note: familyMap has a custom comparator. The entries are sorted from newest to oldest.
    // map.firstEntry() is the newest entry (with largest timestamp). This is different than the default behaviour
    // of firstEntry() and lastEntry().
    val rowRDD = hbaseRDD.map(hbaseRow => {
      val rowKey = Bytes.toStringBinary(hbaseRow.getRow)
      val familyMap = hbaseRow.getMap
      transformMapToRow(rowKey, familyMap, schema, defaultNull)
    })
    val hiveSchema = StructType(schema.fieldNames.map(columnName => {
      StructField(columnName.split(':')(1), StringType, false)
    }))
    val dataFrame = hiveContext.createDataFrame(rowRDD, hiveSchema)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(args.hiveTableName)
  }

  /**
    * Transforms the data in a hashmap into a Row object.
    * The data of the current HBase row is stored in a hash map. To store them into Hive,
    * we need to feed them to an object of type Row. The elements should be in the same order
    * as the columns are written in the schema.
    *
    * @param rowKey The key of the HBase row.
    * @param familyMap A hashmap holding the values of the current row.
    * @param schema a struct that specifies how the schema would look like in Hive table.
    * @param defaultNull The value to be used in Hive table, if the cell value was missing from the source HBase table.
    * @return an object of type Row holding the row data.
    */
  def transformMapToRow(
    rowKey: String,
    familyMap: FamilyMap,
    schema: StructType,
    defaultNull: String): Row = {

    Row.fromSeq(for(fieldName <- schema.fieldNames) yield {
      try {
        val Array(familyName, qualifierName) = fieldName.split(':')
        Bytes.toStringBinary(familyMap
          .get(Bytes.toBytes(familyName))
          .get(Bytes.toBytes(qualifierName))
          .firstEntry().getValue)
      }
      catch { case e: Exception => defaultNull }
    })
  }
}
