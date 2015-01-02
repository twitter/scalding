package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.Success

import com.twitter.algebird.Monoid
import com.twitter.scalding.jdbc.ColumnDefinition
import com.twitter.scalding.jdbc.macros.impl.FieldName

object DateTypeHandler {

  def apply[T](c: Context)(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[List[c.Expr[ColumnDefinition]]] = {
    import c.universe._

    val opts = scala.util.Try[Boolean](Monoid.sum(annotationInfo.collect {
      case (tpe, _) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.date] => (true)
      case tpe => sys.error(s"Hit annotation ${tpe} which is not supported on field ${fieldName.toStr} of type Int")
    }))

    opts.flatMap { useDateType =>
      val jdbcType = if (useDateType) "DATE" else "DATETIME"
      Success(ColFormatter(c)(jdbcType, None))
    }
  }
}
