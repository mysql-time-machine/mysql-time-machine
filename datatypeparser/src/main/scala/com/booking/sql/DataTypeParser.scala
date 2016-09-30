package com.booking.sql

import scala.language.postfixOps
import scala.util.matching.Regex
import scala.util.parsing.combinator.JavaTokenParsers

case class MySQLPrecision(precision: Int, scale: Option[Int])
case class MySQLDataType(typename: String, enumeration: Seq[String], precision: Option[MySQLPrecision], qualifiers: Seq[String], attributes: Map[String, String])

object DataTypeParser extends JavaTokenParsers {

  val typenames = Seq("MEDIUMBLOB", "MEDIUMTEXT", "MEDIUMINT", "TIMESTAMP", "VARBINARY", "DATETIME", "LONGBLOB", "LONGTEXT", "SMALLINT", "TINYBLOB", "TINYTEXT", "DECIMAL", "INTEGER", "NUMERIC", "TINYINT", "VARCHAR", "BIGINT", "BINARY", "DOUBLE", "FLOAT", "TIME", "BLOB", "CHAR", "DATE", "ENUM", "JSON", "REAL", "TEXT", "YEAR", "BIT", "INT", "SET")
  val qualifiers = Seq("UNSIGNED", "ZEROFILL", "BINARY")
  val attributes = Seq("CHARACTER SET", "COLLATE")

  def datatypeSpec: Parser[MySQLDataType] =
    "enum" ~> enumeration ^^ {
      case e => new MySQLDataType("ENUM", e, None, Seq(), Map())
    } |
  typename ~ (precision?) ~ (qualifier*) ~ (attribute*) ^^ {
    case t ~ p ~ Seq(q) ~ a => new MySQLDataType(t.toUpperCase(), Seq(), p, Seq(q), a.toMap)
    case t ~ p ~ q ~ a => new MySQLDataType(t.toUpperCase(), Seq(), p, q, a.toMap)
  }

  def typename: Parser[String] = makeRegex(typenames, true)

  def enumeration: Parser[Seq[String]] = "(" ~> repsep(stringLiteral, ",") <~ ")"

  def precision: Parser[MySQLPrecision] = "(" ~> wholeNumber ~ ("," ~> wholeNumber?) <~ ")" ^^ {
    case l ~ Some(d) => new MySQLPrecision(l.toInt, Some(d.toInt))
    case l ~ None => new MySQLPrecision(l.toInt, None)
  }

  def qualifier: Parser[String] = makeRegex(qualifiers, true)

  def attribute: Parser[(String, String)] = characterSetName | collationName

  override def stringLiteral: Parser[String] = {
    ("\'" ~> """([^'\p{Cntrl}\\]|\\[0'"bnrtZ\\%_]|\\u[a-fA-F0-9]{4})*""".r <~ "\'") |
    ("\"" ~> """([^"\p{Cntrl}\\]|\\[0'"bnrtZ\\%_]|\\u[a-fA-F0-9]{4})*""".r <~ "\"")
  }

  def characterSetName: Parser[(String, String)] = "CHARACTER SET" ~ stringLiteral ^^ {
    case a ~ b => (a, b)
  }

  def collationName: Parser[(String, String)] = "COLLATE" ~ stringLiteral ^^ {
    case a ~ b => (a, b)
  }

  def makeRegex(tokens: Seq[String], caseInsensitive: Boolean): Regex = {
    val start = if (caseInsensitive) "(?i)" else ""
    (start + tokens.mkString("(", "|", ")")).r
  }

  def apply(exprString: String): MySQLDataType = {
     parseAll(datatypeSpec, exprString) match {
       case Success(dt, _) => dt
       case Failure(msg, _) => sys.error(msg)
       case Error(msg, _) => sys.error(msg)
     }
  }
}
