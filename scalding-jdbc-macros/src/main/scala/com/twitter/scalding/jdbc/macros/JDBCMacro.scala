package com.twitter.scalding.jdbc.macros

import scala.language.experimental.macros
import com.twitter.scalding.jdbc.macros.impl._
import com.twitter.scalding.jdbc.{ ColumnDefinitionProvider, JDBCTypeInfo }

sealed trait ScaldingJdbcAnnotation
// This is the size in characters for a string field
// For integers its really for display purposes
@scala.annotation.meta.getter
class size(val size: Int) extends annotation.StaticAnnotation with ScaldingJdbcAnnotation

@scala.annotation.meta.getter
class text() extends annotation.StaticAnnotation with ScaldingJdbcAnnotation

@scala.annotation.meta.getter
class varchar() extends annotation.StaticAnnotation with ScaldingJdbcAnnotation

@scala.annotation.meta.getter
class date() extends annotation.StaticAnnotation with ScaldingJdbcAnnotation

object JDBCMacro {
  implicit def toColumnDefinitionProvider[T]: ColumnDefinitionProvider[T] = macro JDBCMacroImpl.caseClassJDBCPayloadImpl[T]
  implicit def toJDBCTypeInfo[T]: JDBCTypeInfo[T] = macro JDBCTypeInfoImpl.caseClassJDBCTypeInfoImpl[T]
}
