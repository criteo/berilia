package com.criteo.dev.cluster.utils.ddl

import scala.util.parsing.input.Position

object DDLParser extends BaseParser with CreateTableParser {

  /**
    * Parse a DDL statement
    *
    * @param in input string
    * @return Either a tuple of (error message, error position), or parsed statement
    */
  def apply(in: String): Either[(String, Position), Statement] = {
    parse(statement, in) match {
      case Success(result, _) => Right(result)
      case Failure(msg, input) => Left((msg, input.pos))
      case Error(msg, input) => Left((msg, input.pos))
    }
  }

  def statement: Parser[Statement] = createTable
}
