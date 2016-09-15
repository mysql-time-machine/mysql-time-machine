package com.booking.spark

import org.scalatest._

class DataTypeParserSpec extends FlatSpec with Matchers[] {
  val queries: Seq[String] = Seq("INT",
      "INT(128)",
      "LONG UNSIGNED",
      "INT UNSIGNED BINARY",
      "INT (128) UNSIGNED",
      "INT (128) UNSIGNED BINARY",
      "INT (128, 1) UNSIGNED",
      "INT (128, 6) UNSIGNED BINARY")
}
