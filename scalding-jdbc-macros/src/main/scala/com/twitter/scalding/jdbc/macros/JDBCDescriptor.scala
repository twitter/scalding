package com.twitter.scalding.jdbc.macros

import scala.language.experimental.macros
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.scalding.jdbc.macros.impl._

@scala.annotation.meta.getter
class size(val size: Int) extends annotation.StaticAnnotation

sealed trait JdbcColumn[T] {
  def defaultValue: Option[Any]
}

case class StringColumn(name: String, length: Int, defaultValue: Option[String] = None) extends JdbcColumn[StringColumn]
case class IntColumn(name: String, length: Int, defaultValue: Option[Int] = None) extends JdbcColumn[IntColumn]
case class OptionalColumn[T](innerColumn: JdbcColumn[T]) extends JdbcColumn[OptionalColumn[T]] {
  override val defaultValue = None
}

trait JDBCDescriptor[T] {
  def columns: Iterable[JdbcColumn[_]]
}

object JDBCDescriptor {
  implicit def toJDBCDescriptor[T: IsCaseClass]: JDBCDescriptor[T] = macro JdbcMacroImpl.caseClassJdbcPayloadImpl[T]
}
