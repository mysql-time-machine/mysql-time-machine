package com.booking.sql

import org.scalatest._

class DataTypeParserSpec extends FlatSpec with Matchers {

  "A DataTypeParser" should "return a MySQLDataType" in {
    val dt: MySQLDataType = DataTypeParser("INT")
    assert(dt.isInstanceOf[MySQLDataType])
  }

  it should "parse typename" in {
    val dt: MySQLDataType = DataTypeParser("INT")
    assert(dt.typename == "INT")
  }

  it should "parse enums" in {
    val dt: MySQLDataType = DataTypeParser("enum('ok','cancelled_by_hotel','cancelled_by_guest','fraudulent','test','no_show','unknown','overbooking','cancelled_by_booking')")
    assert(dt.typename == "ENUM")
  }

  it should "parse enum values" in {
    val dt: MySQLDataType = DataTypeParser("enum('ok','cancelled_by_hotel','cancelled_by_guest','fraudulent','test','no_show','unknown','overbooking','cancelled_by_booking')")
    assert(dt.enumeration == Seq("ok","cancelled_by_hotel","cancelled_by_guest","fraudulent","test","no_show","unknown","overbooking","cancelled_by_booking"))
  }

  it should "parse precision" in {
    val dt: MySQLDataType = DataTypeParser("DOUBLE(10)")
    assert(dt.precision.get == MySQLPrecision(10, None))
  }

  it should "parse scale" in {
    val dt: MySQLDataType = DataTypeParser("DOUBLE(10, 2)")
    assert(dt.precision.get == MySQLPrecision(10, Some(2)))
  }

  it should "parse qualifiers" in {
    val dt: MySQLDataType = DataTypeParser("INTEGER UNSIGNED ZEROFILL")
    assert(dt.qualifiers == Seq("UNSIGNED", "ZEROFILL"))
  }

  it should "parse attributes" in {
    val dt: MySQLDataType = DataTypeParser("INTEGER CHARACTER SET \"utf-8\" COLLATE \'latin1_bin\'")
    assert(dt.attributes == Map(("COLLATE", "latin1_bin"), ("CHARACTER SET", "utf-8")))
  }
}
