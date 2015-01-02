package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Success, Failure }

import com.twitter.algebird.Monoid
import com.twitter.scalding._
import com.twitter.scalding.jdbc.ColumnDefinition
import com.twitter.scalding.jdbc.macros.impl.FieldName

object NumericTypeHandler {
  def apply[T](c: Context)(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean,
    numericType: String): scala.util.Try[List[c.Expr[ColumnDefinition]]] = {
    import c.universe._

    val opts = scala.util.Try[Option[Int]](Monoid.sum(annotationInfo.collect {
      case (tpe, Some(siz)) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.size] => (Some(siz))
      case tpe => sys.error(s"Hit annotation ${tpe} which is not supported on field ${fieldName.toStr} of type $numericType")
    }))

    opts.flatMap { size =>
      if ((!size.isDefined || size.get > 0)) {
        Success(ColFormatter(c)(numericType, size))
      } else {
        Failure(new Exception(s"Int field $fieldName, has a size defined that is <= 0."))
      }
    }
  }
}
