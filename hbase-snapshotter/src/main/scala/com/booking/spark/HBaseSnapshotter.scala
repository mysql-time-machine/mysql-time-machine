package com.booking.spark

import java.util.NavigableMap

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

case class Arguments(pit: Long = -1, configPath: String = null, hbaseTableName: String = null, hiveTableName: String = null)
case class IllegalFormatException(message: String, cause: Throwable) extends RuntimeException(message, cause)
object HBaseSnapshotter {
  var cmdArgs: Arguments = null
  //FamilyMap = Map[FamilyName, Map[ColumnName, Map[Timestamp, Value]]]
  type FamilyMap = NavigableMap[Array[Byte],NavigableMap[Array[Byte], NavigableMap[java.lang.Long, Array[Byte]]]]
  /**
    * Parses command line arguments into an Argument object
    * @param args command line arguments
    */
  def parseArguments(args: Array[String]): Unit ={
    val parser = new scopt.OptionParser[Arguments]("hbase-snapshotter") {
      note("Options:")
      opt[Long]("pit") valueName("<TIMESTAMP>") action { (pit_, c) =>
        c.copy(pit = pit_) } text("Takes a snapshot of the latest HBase version available before the given timestamp (exclusive). " +
        "If this option is not specified, the latest timestamp will be used.")

      opt[String]("config") valueName("<CONFIG-PATH>") action { (configPath_, c) =>
        c.copy(configPath = configPath_) } text("The path of a yaml config file.")

      help("help") text("Prints this usage text")
      note("\nArguments:")
      arg[String]("<source table>")  action { (hbaseTableName_, c) =>
        c.copy(hbaseTableName = hbaseTableName_) } text("The source HBase table you are copying from. " +
        "It should be in the format NAMESPACE:TABLENAME")
      arg[String]("<dest table>")  action { (hiveTableName_, c) =>
        c.copy(hiveTableName = hiveTableName_) } text("The destination Hive table you are copying to. " +
        "It should be in the format DATABASE.TABLENAME")
    }
    parser.parse(args, Arguments()) match {
      case Some(config) => {
        if (config.configPath == null) {
          println("Error: Missing option --config <CONFIG-PATH>")
          println("Try --help for more information.")
          System.exit(1)
        }
        cmdArgs = config
      }
      case None =>{
        // arguments are bad, error message will have been displayed
        System.exit(1)
      }
    }
  }
  def validateConfigFile(configParser: ConfigParser):Unit = {
    try{
      configParser.getZooKeeperQuorum
      configParser.getSchema
      configParser.getDefaultNull
    }catch{
      case e:Exception =>
        throw IllegalFormatException("The yaml config file is not formatted correctly. Check readme.md file for more information.", e)
    }
  }
  def main(args: Array[String]): Unit ={
    parseArguments(args)
    val configParser = new ConfigParser(cmdArgs.configPath)
    validateConfigFile(configParser)
    val sc = new SparkContext( new SparkConf().setAppName("HBaseSnapshotter") )
    println(s"SparkApp Id = ${sc.applicationId}")
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", configParser.getZooKeeperQuorum())
    val hbaseContext = new HBaseContext(sc, hbaseConfig)
    val hiveContext = new HiveContext(sc)
    val schema:StructType = configParser.getSchema
    val scan = new Scan()
    if(cmdArgs.pit > -1 )
      scan.setTimeRange(0, cmdArgs.pit)
    // Scans the given HBase table into an RDD.
    val hbaseRDD = hbaseContext.hbaseRDD(cmdArgs.hbaseTableName, scan, {r: (ImmutableBytesWritable, Result) => r._2})
    val defaultNull:String = configParser.getDefaultNull
    // Mapping every row in HBase to a Row object in a Spark Dataframe
    // Note: familyMap has a custom comparator. The entries are sorted from newest to oldest.
    // map.firstEntry() is the newest entry (with largest timestamp). This is different than the default behaviour
    // of firstEntry() and lastEntry().
    val rowRDD = hbaseRDD.map(hbaseRow => {
      val rowKey = Bytes.toStringBinary(hbaseRow.getRow)
      val familyMap = hbaseRow.getMap
      transformMapToRow(rowKey, familyMap, schema, defaultNull)
    })
    val dataFrame = hiveContext.createDataFrame(rowRDD, schema)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(cmdArgs.hiveTableName)
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
  def transformMapToRow(rowKey: String,
                        familyMap: FamilyMap,
                        schema:StructType, defaultNull: String   ) = {
    // An array that will hold the row values in the same order as the schema's fields.
    val rowValues = new Array[String](schema.length)

    schema.fieldNames.foreach(fieldName => {
      val splitIndex = fieldName.indexOf('_')
      if(splitIndex == -1)
        throw IllegalFormatException(s"Missing '_' in the schema column '$fieldName'. Schema columns should be formatted as FamilyName_QualifierName", null)
      val (familyName, qualifierName) = (fieldName.take(splitIndex), fieldName.drop(splitIndex+1))
      val fieldIndex = schema.fieldIndex(fieldName)
      if(fieldName == "k_hbase_key")
        rowValues(fieldIndex) = rowKey
      else
        try{
          val value = familyMap.get(Bytes.toBytes(familyName))
            .get(Bytes.toBytes(qualifierName))
            .firstEntry().getValue
          rowValues(fieldIndex) = Bytes.toStringBinary(value)
        }
        catch{
          case e:Exception => rowValues(fieldIndex) = defaultNull
        }
    })
    // Expanding the array values as arguments to the constructor of Row object
    Row(rowValues:_*)
  }
}

