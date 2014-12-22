package com.twitter.scalding.jdbc.macros

import scala.language.experimental.macros
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.scalding.jdbc.macros.impl._

@scala.annotation.meta.getter
class size(val size: Int) extends annotation.StaticAnnotation

sealed trait JdbcColumn

sealed trait PrimitiveJdbcColumn[T] extends JdbcColumn {
  def defaultValue: Option[T]
}
case class StringColumn(name: String, length: Int, defaultValue: Option[String] = None) extends PrimitiveJdbcColumn[String]
case class IntColumn(name: String, length: Int, defaultValue: Option[Int] = None) extends PrimitiveJdbcColumn[Int]

case class OptionalColumn[T](innerColumn: PrimitiveJdbcColumn[T]) extends JdbcColumn

trait JDBCDescriptor[T] {
  def columns: Iterable[JdbcColumn]
}

object JDBCDescriptor {
  implicit def toJDBCDescriptor[T: IsCaseClass]: JDBCDescriptor[T] = macro JdbcMacroImpl.caseClassJdbcPayloadImpl[T]
}
