package com.criteo.dev.cluster.utils.ddl

import scala.util.parsing.combinator.RegexParsers

trait BaseParser extends RegexParsers {
  def validName: Parser[String] = "[A-Za-z0-9_]+".r

  def hiveStringLiteral: Parser[String] = ("'" | "\"") ~> "[^'\"]*".r <~ ("'" | "\"")

  def properties(delimiter: String = "="): Parser[Map[String, String]] = "(" ~> repsep(hiveStringLiteral ~ delimiter ~ hiveStringLiteral, ",") <~ ")" ^^ {
    _.map { case k ~ _ ~ v =>
      (k, v)
    }.toMap
  }

  def comment: Parser[String] = "comment" ~> hiveStringLiteral

  def int: Parser[Int] = "\\d+".r ^^ (_.toInt)

  // parser for case insensitive string literal
  implicit def caseInsensitiveLiteral(s: String): Parser[String] = new Parser[String] {
    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = handleWhiteSpace(source, offset)
      var i = 0
      var j = start
      while (i < s.length && j < source.length && s.charAt(i).toLower == source.charAt(j).toLower) {
        i += 1
        j += 1
      }
      if (i == s.length)
        Success(source.subSequence(start, j).toString, in.drop(j - offset))
      else {
        val found = if (start == source.length()) "end of source" else "`" + source.charAt(start) + "'"
        Failure("`" + s + "' expected but " + found + " found", in.drop(start - offset))
      }
    }
  }
}
