package com.twitter.scalding.db.macros

import scala.language.experimental.macros
import com.twitter.scalding.db.macros.impl._
import com.twitter.scalding.db.{ ColumnDefinitionProvider, DBTypeDescriptor }

// This is the sealed base trait for scala runtime annotiations used by the JDBC macros.
// These will read from these macros as a means to annotate fields to make up for the missing
// extra type information JDBC wants but is not in the jvm types.
sealed trait ScaldingDBAnnotation

// This is the size in characters for a char field
// For integers its really for display purposes
@scala.annotation.meta.getter
final class size(val size: Int) extends annotation.StaticAnnotation with ScaldingDBAnnotation

// JDBC TEXT type, this forces the String field in question to be a text type
@scala.annotation.meta.getter
final class text() extends annotation.StaticAnnotation with ScaldingDBAnnotation

// JDBC VARCHAR type, this forces the String field in question to be a text type
@scala.annotation.meta.getter
final class varchar() extends annotation.StaticAnnotation with ScaldingDBAnnotation

// JDBC DATE type, this toggles a java.util.Date field to be JDBC Date.
// It will default to DATETIME to preserve the full resolution of java.util.Date
@scala.annotation.meta.getter
final class date() extends annotation.StaticAnnotation with ScaldingDBAnnotation

// This is the entry point to explicitly calling the JDBC macros.
// Most often the implicits will be used in the package however
object DBMacro {
  def toColumnDefinitionProvider[T]: ColumnDefinitionProvider[T] = macro ColumnDefinitionProviderImpl[T]
  def toDBTypeDescriptor[T]: DBTypeDescriptor[T] = macro DBTypeDescriptorImpl[T]
}
