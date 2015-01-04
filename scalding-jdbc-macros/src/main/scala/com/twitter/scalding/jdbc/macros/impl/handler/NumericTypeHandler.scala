package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Success, Failure }

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

    val helper = new {
      val ctx: c.type = c
      val cfieldName = fieldName
      val cannotationInfo = annotationInfo
    } with AnnotationHelper

    val extracted = for {
      (nextHelper, sizeAnno) <- helper.sizeAnnotation
      _ <- nextHelper.validateFinished
    } yield (sizeAnno)

    extracted.flatMap { t =>
      t match {
        case WithSize(s) if s > 0 => Success(ColFormatter(c)(numericType, Some(s)))
        case WithSize(s) => Failure(new Exception(s"Int field $fieldName, has a size defined that is <= 0."))
        case WithoutSize => Success(ColFormatter(c)(numericType, None))
      }
    }

  }
}
