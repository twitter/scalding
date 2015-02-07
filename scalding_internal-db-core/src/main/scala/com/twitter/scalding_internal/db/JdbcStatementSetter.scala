package com.twitter.scalding_internal.db

import java.sql.PreparedStatement
import scala.util.Try

trait JdbcStatementSetter[T] extends java.io.Serializable { self =>
  def apply(t: T, s: PreparedStatement): Try[Unit]
}
