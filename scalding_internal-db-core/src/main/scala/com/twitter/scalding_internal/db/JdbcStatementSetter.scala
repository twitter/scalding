package com.twitter.scalding_internal.db

import java.sql.PreparedStatement

trait JdbcStatementSetter[T] extends java.io.Serializable { self =>
  def apply(t: T, s: PreparedStatement): Unit
}
