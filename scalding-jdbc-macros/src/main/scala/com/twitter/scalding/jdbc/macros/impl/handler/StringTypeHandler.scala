package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Success, Failure }

import com.twitter.algebird.Monoid
import com.twitter.scalding._
import com.twitter.bijection.macros.IsCaseClass
import com.twitter.scalding.jdbc.{ ColumnDefinition, ColumnDefinitionProvider }
import com.twitter.scalding.jdbc.macros._
import com.twitter.scalding.jdbc.macros.impl.FieldName
/**
 * This class contains the core macro implementations. This is in a separate module to allow it to be in
 * a separate compilation unit, which makes it easier to provide helper methods interfacing with macros.
 */
object StringTypeHandler {

  def apply[T](c: Context)(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[c.Expr[ColumnDefinition]] = {
    import c.universe._

    val opts = scala.util.Try[(Option[Int], Boolean, Boolean)](Monoid.sum(annotationInfo.collect {
      case (tpe, Some(siz)) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.size] => (Some(siz), false, false)
      case (tpe, _) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.varchar] => (None, true, false)
      case (tpe, _) if tpe =:= typeOf[com.twitter.scalding.jdbc.macros.text] => (None, false, true)
      case tpe => sys.error(s"Hit annotation ${tpe} which is not supported on field ${fieldName.toStr} of type String")
    }))

    opts.flatMap {
      case (size, forceVarChar, forceText) =>
        (size, forceVarChar, forceText) match {
          case (_, true, true) => Failure(new Exception(s"String field $fieldName, has mutually exclusive annotations @text and @varchar"))
          case (None, true, false) => Failure(new Exception(s"String field $fieldName, is forced varchar but has no size annotation. size is required in the presence of varchar."))
          case (None, false, false) => Failure(new Exception(s"String field $fieldName, at least one of size, varchar, text must be present."))
          case (Some(siz), _, _) if siz <= 0 => Failure(new Exception(s"String field $fieldName, has a size $siz which is <= 0. Doesn't make sense for a string."))
          case (Some(siz), false, false) if siz <= 255 => Success(ColFormatter(c)("VARCHAR", Some(siz)))
          case (Some(siz), false, false) if siz > 255 => Success(ColFormatter(c)("TEXT", None))
          case (Some(siz), true, false) => Success(ColFormatter(c)("VARCHAR", Some(siz)))
          case (_, false, true) => Success(ColFormatter(c)("TEXT", None))
        }
    }
  }
}
