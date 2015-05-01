package com.twitter.scalding.db

import java.sql.PreparedStatement
import scala.util.Try

/**
 * Case class to JDBC statement setter used for database writes
 */
trait JdbcStatementSetter[T] extends java.io.Serializable { self =>
  def apply(t: T, s: PreparedStatement): Try[PreparedStatement]
}
