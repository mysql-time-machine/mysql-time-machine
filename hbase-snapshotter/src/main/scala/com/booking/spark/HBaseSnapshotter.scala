package com.booking.spark

import java.util.NavigableMap

import scala.collection.JavaConversions._

// TODO: replace with scala.util.parsing.json.JSON
import com.google.gson.{JsonParser,JsonObject};
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

case class Arguments(timestamp: Long = -1, configPath: String = null, hbaseTableName: String = null, hiveTableName: String = null)

case class IllegalFormatException(message: String, cause: Throwable) extends RuntimeException(message, cause)

object HBaseSnapshotter {
  private var hc: HiveContext = null
  private var hbc: HBaseContext = null
  private var scan: Scan = null

  //FamilyMap = Map[FamilyName, Map[ColumnName, Map[Timestamp, Value]]]
  type FamilyMap = NavigableMap[Array[Byte], NavigableMap[Array[Byte], NavigableMap[java.lang.Long, Array[Byte]]]]

  /**
   * Parses command line arguments into an Argument object
   * @param args command line arguments
   */
  def parseArguments(args: Array[String]): Arguments = {
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

      arg[String]("<source table>")
        .action((hbaseTableName_, c) => c.copy(hbaseTableName = hbaseTableName_))
        .text("The source HBase table you are copying from. " +
          "It should be in the format NAMESPACE:TABLENAME")

      arg[String]("<dest table>")
        .action((hiveTableName_, c) => c.copy(hiveTableName = hiveTableName_))
        .text("The destination Hive table you are copying to. " +
          "It should be in the format DATABASE.TABLENAME")
    }

    parser.parse(args, Arguments()) match {
      case Some(config) => {
        config
      }
      case None => {
        System.exit(1)
        return (Arguments())
      }
    }
  }

  def parseConfig(configPath: String): ConfigParser = {
    val configParser = new ConfigParser(configPath)

    try {
      configParser.getZooKeeperQuorum
      configParser.getSchema
      configParser.getDefaultNull
    } catch {
      case e: Exception =>
        throw IllegalFormatException(
          "The yaml config file is not formatted correctly." +
            "Check readme.md file for more information.",
          e
        )
    }
    configParser
  }

  def init(args: Arguments, config: ConfigParser): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "64k")
    conf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.client.Result]))
    val sc = new SparkContext(conf.setAppName("HBaseSnapshotter"))
    val hbaseConfig = HBaseConfiguration.create()

    hbaseConfig.set("hbase.zookeeper.quorum", config.getZooKeeperQuorum())
    hbc = new HBaseContext(sc, hbaseConfig)
    hc = new HiveContext(sc)
    scan = new Scan()
    if (args.timestamp > -1) scan.setTimeRange(0, args.timestamp)
  }

  def getSchema(tableName: String): StructType = {
    val filters = new FilterList(
      FilterList.Operator.MUST_PASS_ALL,
      new FirstKeyOnlyFilter()// ,
      // new KeyOnlyFilter()
    )
    val schemaScan = new Scan()

    schemaScan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"))
    schemaScan.setFilter(filters)
    val rdd = hbc.hbaseRDD("schema_history:dw", schemaScan, { r: (ImmutableBytesWritable, Result) => r._2 })

    def transformSchema(table: String, r: Result): Seq[StructField] = {
      val k = Bytes.toString(r.getRow())
      val v = Bytes.toString(r.getFamilyMap(Bytes.toBytes("d")).get(Bytes.toBytes("schemaPostChange")))
      val o = new JsonParser().parse(v).getAsJsonObject().getAsJsonObject(table)
      o.getAsJsonObject("columnIndexToNameMap").entrySet().toSeq.map({ x => {
        val columnIndex: Int = x.getKey().toInt
        val columnName: String = x.getValue().getAsString()
        val columnType: String = o.getAsJsonObject("columnsSchema")
          .getAsJsonObject(columnName)
          .getAsJsonPrimitive("columnType")
          .getAsString()
        (columnIndex, columnName, columnType)
      }}).sortBy(_._1).map({ x => StructField(x._2, DataTypeParser.parse(x._3), true) })
    }

    StructType(transformSchema(tableName, rdd.first()))
  }

  def main(cmdArgs: Array[String]): Unit = {
    val args = parseArguments(cmdArgs)
    val config = parseConfig(args.configPath)

    init(args, config)

    val schema: StructType = getSchema("Reservation")

    // Scans the given HBase table into an RDD.
    val hbaseRDD = hbc.hbaseRDD(args.hbaseTableName, scan, { r: (ImmutableBytesWritable, Result) => r._2 })

    // Mapping every row in HBase to a Row object in a Spark Dataframe
    // Note: familyMap has a custom comparator. The entries are sorted from newest to oldest.
    // map.firstEntry() is the newest entry (with largest timestamp). This is different than the default behaviour
    // of firstEntry() and lastEntry().
    val defaultNull = config.getDefaultNull

    val rowRDD = hbaseRDD.map(hbaseRow => {
      val familyMap = hbaseRow.getMap
      transformMapToRow(familyMap, schema, defaultNull)
    })

    val dataFrame = hc.createDataFrame(rowRDD, schema)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(args.hiveTableName)
  }

  /**
   * Transforms the data in a hashmap into a Row object.
   * The data of the current HBase row is stored in a hash map. To store them into Hive,
   * we need to feed them to an object of type Row. The elements should be in the same order
   * as the columns are written in the schema.
   *
   * @param familyMap A hashmap holding the values of the current row.
   * @param schema a struct that specifies how the schema would look like in Hive table.
   * @param defaultNull The value to be used in Hive table, if the cell value was missing from the source HBase table.
   * @return an object of type Row holding the row data.
   */
  def transformMapToRow(
    familyMap: FamilyMap,
    schema: StructType,
    defaultNull: String
  ): Row = {

    Row.fromSeq(for (field: StructField <- schema.fields) yield {
      try {
        val fieldValue: String = Bytes.toStringBinary(familyMap
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes(field.name))
          .firstEntry().getValue)

        field.dataType match {
          case IntegerType => fieldValue.toInt
          case LongType => fieldValue.toLong
          case DoubleType => fieldValue.toDouble
          case _ => fieldValue
        }
      }
      catch {
        case e: Exception => null
      }
    })
  }
}
