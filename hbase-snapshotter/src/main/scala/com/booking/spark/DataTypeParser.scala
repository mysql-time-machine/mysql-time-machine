package com.booking.spark

import scala.language.postfixOps
import scala.util.matching._
import scala.util.parsing.combinator._
import org.apache.spark.sql.types._

case class MySQLPrecision(precision: Int, scale: Option[Int])
case class MySQLDataType(typename: String, precision: Option[MySQLPrecision], qualifiers: Seq[String])

object DataTypeParser extends JavaTokenParsers {

  val typenames = Seq("MEDIUMBLOB", "MEDIUMTEXT", "MEDIUMINT", "TIMESTAMP", "VARBINARY", "DATETIME", "LONGBLOB", "LONGTEXT", "SMALLINT", "TINYBLOB", "TINYTEXT", "DECIMAL", "INTEGER", "NUMERIC", "TINYINT", "VARCHAR", "BIGINT", "BINARY", "DOUBLE", "FLOAT", "TIME", "BLOB", "CHAR", "DATE", "ENUM", "JSON", "REAL", "TEXT", "YEAR", "BIT", "INT", "SET")
  val qualifiers = Seq("UNSIGNED", "ZEROFILL", "BINARY", "CHARACTER SET", "COLLATE")

  def datatypeSpec: Parser[MySQLDataType] = typename ~ (precision?) ~ (qualifier*) ^^ {
    case t ~ p ~ Seq(q) => new MySQLDataType(t, p, Seq(q))
    case t ~ p ~ q => new MySQLDataType(t, p, q)
  }

  def typename: Parser[String] = makeRegex(typenames, true)

  def precision: Parser[MySQLPrecision] = "(" ~> wholeNumber ~ ("," ~> wholeNumber?) <~ ")" ^^ {
    case l ~ Some(d) => new MySQLPrecision(l.toInt, Some(d.toInt))
    case l ~ None => new MySQLPrecision(l.toInt, None)
  }

  def qualifier: Parser[String] = makeRegex(qualifiers, true)

  def makeRegex(tokens: Seq[String], caseInsensitive: Boolean): Regex = {
    val start = if (caseInsensitive) "(?i)" else ""
    (start + tokens.mkString("(", "|", ")")).r
  }

  def apply(exprString: String): DataType = {
     parseAll(datatypeSpec, exprString) match {
       case Success(dt, _) => mySQLToSparkSQL(dt)
       case Failure(msg, _) => NullType
       case Error(msg, _) => NullType
     }
  }

  def mySQLToSparkSQL(dt: MySQLDataType): DataType = {
    dt.typename match {
      case "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => IntegerType
      case "BIGINT" => LongType
      case "NUMERIC" | "DECIMAL" | "FLOAT" | "DOUBLE" | "REAL" => DoubleType
      case _ => StringType
    }
  }
}
