package com.booking.spark

import com.booking.sql.{DataTypeParser, MySQLDataType}

import java.util.NavigableMap
import scala.collection.JavaConversions._

import com.google.gson.{JsonObject, JsonParser}
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan, Get, HTable}
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.
  {
    StructField,
    StructType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType
  }

/**
  * Spark application that takes a snapshot of an HBase table at a
  * given point in time and stores it to a Hive table.
  */
object HBaseSnapshotter {
  private var _hc: HiveContext = null
  private var _hbc: HBaseContext = null
  private var _args: Arguments = null
  private var _config: ConfigParser = null


  /* Readable type structure returned by the hbase client */
  private type FamilyName = Array[Byte]
  private type ColumnName = Array[Byte]
  private type Timestamp = java.lang.Long
  private type Value = Array[Byte]
  private type FamilyMap = NavigableMap[FamilyName, NavigableMap[ColumnName, NavigableMap[Timestamp, Value]]]


  /** Initialize HiveContext, HBaseContext, arguments, and configuration options
    * @param args command line arguments
    * @param config configuration file options
    */
  def init(args: Arguments, config: ConfigParser): Unit = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "64k")
    conf.registerKryoClasses(Array(classOf[Result]))
    conf.setAppName("HBaseSnapshotter")

    val sc = new SparkContext(conf)
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", config.getZooKeeperQuorum())

    _hbc = new HBaseContext(sc, hbaseConfig)
    _hc = new HiveContext(sc)
    _args = args
    _config = config
  }


  /** Convert MySQL datatype strings to Spark datatypes
    * @param MySQL datatype
    * @return Spark datatype
    */
  def mySQLToSparkSQL(s: String): DataType = {
    val dt: MySQLDataType = DataTypeParser(s)
    dt.typename match {
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => IntegerType
      case "BIGINT" => LongType
      case "NUMERIC" | "DECIMAL" | "FLOAT" | "DOUBLE" | "REAL" => DoubleType
      case _ => StringType
    }
  }


  /** Parse schema information from MySQL to HBase dump
    * @param table original MySQL table name
    * @param value json object with following structure:
    *     {table => { "columnIndexToNameMap": { index: name, ... },
    *                 "columnsSchema": { name: { "columnType": "sometype", ... }}}}
    * @return Spark schema
    */
  def transformSchema(table: String, value: String): StructType = {
    val schemaObject = new JsonParser().parse(value)
    val tableSchema = schemaObject.getAsJsonObject().getAsJsonObject(table)
    val columnIndexToNameMap = tableSchema.getAsJsonObject("columnIndexToNameMap")
    val columnsSchema = tableSchema.getAsJsonObject("columnsSchema")

    val sortedSchema: Seq[(Int, String, String)] =
      columnIndexToNameMap.entrySet().toSeq.map({ idx => {
        val columnIndex = idx.getKey().toInt
        val columnName = idx.getValue().getAsString()
        val columnType = columnsSchema
          .getAsJsonObject(columnName)
          .getAsJsonPrimitive("columnType")
          .getAsString()
        (columnIndex, columnName, columnType)
      }}).sortBy(_._1)

    return StructType(sortedSchema.map({ field =>
      StructField(field._2, mySQLToSparkSQL(field._3), true)
    }))
  }


  /** Extract Spark schema from MySQL to HBase dump
    * @param tableName Original MySQL table name
    * @param schemaTableName HBase schema table
    * @param timestamp Closest epoch to desired version of the schema
    * @return Spark schema
    */
  def getSchema(tableName: String, schemaTableName: String, timestamp: Long): StructType = {

    /* Replicator schema dumps are keyed on timestamp, except for the
     * initial snapshot, which is keyed on "initial-snapshot".
     *  We therefore define an explicit ordering that takes it into account
     */
    val keyOrdering: Ordering[Result] = Ordering.by[Result, Long]({ x: Result =>
      val key = Bytes.toStringBinary(x.getRow())
      key match {
        case "initial-snapshot" => 0
        case _ => key.toLong
      }
    })

    /* get correct schema row: top(1) returns greatest (latest) key in [0,
     * timestamp/now()].
     */
    val scan = new Scan()
    if (timestamp > -1) scan.setTimeRange(0, timestamp)
    scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"))
    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, new FirstKeyOnlyFilter(), new KeyOnlyFilter()))

    val rdd = _hbc.hbaseRDD(schemaTableName, scan, { r: (ImmutableBytesWritable, Result) => r._2 })
    val row = rdd.top(1)(keyOrdering).last.getRow()

    /* get correct schema json dump: since we want to use
     * HBaseContext.hbaseRDD, we need to use a Scan object. However,
     * we can't pass (row, row), because the latter is exclusive,
     * which means we get the empty set. Therefore, we start the scan
     * at the (inclusive) row, and grab the top(1).reverse, which
     * returns the smallest (earliest) row. */
    val fullScan = new Scan(row)
    fullScan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("schemaPostChange"))
    fullScan.setFilter(new FirstKeyOnlyFilter())

    val fullRDD = _hbc.hbaseRDD(schemaTableName, fullScan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val value =
      Bytes.toStringBinary(
        fullRDD.top(1)(keyOrdering).reverse.last.getMap()
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes("schemaPostChange"))
          .lastEntry()
          .getValue()
      )

    transformSchema(tableName, value)
  }


  /**
    * Transforms the data in a hashmap into a Row object.
    * The data of the current HBase row is stored in a hash map. To store them into Hive,
    * we need to feed them to an object of type Row. The elements should be in the same order
    * as the columns are written in the schema.
    *
    * @param familyMap A hashmap holding the values of the current row.
    * @param schema a struct that specifies how the schema would look like in Hive table.
    * @return an object of type Row holding the row data.
    */
  def transformMapToRow(familyMap: FamilyMap, schema: StructType): Row = {
    Row.fromSeq(for (field: StructField <- schema.fields) yield {
      try {
        val fieldValue: String = Bytes.toStringBinary(familyMap
          .get(Bytes.toBytes("d"))
          .get(Bytes.toBytes(field.name))
          .lastEntry().getValue)

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

  def main(cmdArgs: Array[String]): Unit = {
    val args = ArgumentParser(cmdArgs)
    val config = new ConfigParser(args.configPath)

    init(args, config)

    val schema: StructType = getSchema(args.mySQLTableName, args.schemaTableName, args.timestamp)

    val scan = new Scan()
    if (args.timestamp > -1) scan.setTimeRange(0, args.timestamp)
    val hbaseRDD = _hbc.hbaseRDD(args.hbaseTableName, scan, { r: (ImmutableBytesWritable, Result) => r._2 })

    val rowRDD = hbaseRDD.map({ r => transformMapToRow(r.getMap(), schema) })
    val dataFrame = _hc.createDataFrame(rowRDD, schema)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(args.hiveTableName)
  }
}
