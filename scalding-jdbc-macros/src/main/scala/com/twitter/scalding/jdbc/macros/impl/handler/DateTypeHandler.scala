package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.Success

import com.twitter.scalding.jdbc.ColumnDefinition
import com.twitter.scalding.jdbc.macros.impl.FieldName

object DateTypeHandler {

  def apply[T](c: Context)(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[List[c.Expr[ColumnDefinition]]] = {
    import c.universe._

    val helper = new {
      val ctx: c.type = c
      val cfieldName = fieldName
      val cannotationInfo = annotationInfo
    } with AnnotationHelper

    val extracted = for {
      (nextHelper, dateAnno) <- helper.dateAnnotation
      _ <- nextHelper.validateFinished
    } yield (dateAnno)

    extracted.flatMap { t =>
      t match {
        case WithDate => Success(ColFormatter(c)("DATE", None))
        case WithoutDate => Success(ColFormatter(c)("DATETIME", None))
      }
    }
  }
}
