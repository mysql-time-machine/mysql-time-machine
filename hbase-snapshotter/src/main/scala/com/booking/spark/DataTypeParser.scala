package com.booking.spark

import org.apache.spark.sql.types._

object DataTypeParser {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def parse(spec: String): DataType = {
    spec.toLowerCase().trim().split("""\W""")(0)  match {
      case "tinyint"
         | "smallint"
         | "mediumint"
         | "int"
         | "integer"
         | "timestamp" => IntegerType
      case "bigint" => LongType
      case "numeric"
         | "decimal"
         | "float"
         | "double"
         | "real" => DoubleType
      case _ => StringType
    }
  }
}
