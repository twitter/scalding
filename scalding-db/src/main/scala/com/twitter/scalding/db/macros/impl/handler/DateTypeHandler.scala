package com.twitter.scalding.db.macros.impl.handler

import scala.reflect.macros.Context
import scala.util.Success

import com.twitter.scalding.db.macros.impl.FieldName

object DateTypeHandler {

  def apply[T](c: Context)(implicit accessorTree: List[c.universe.MethodSymbol],
    fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[List[ColumnFormat[c.type]]] = {

    val helper = new {
      val ctx: c.type = c
      val cfieldName = fieldName
      val cannotationInfo = annotationInfo
    } with AnnotationHelper

    val extracted = for {
      (nextHelper, dateAnno) <- helper.dateAnnotation
      _ <- nextHelper.validateFinished
    } yield (dateAnno)

    extracted.flatMap {
      case WithDate => Success(List(ColumnFormat(c)(accessorTree, "DATE", None)))
      case WithoutDate => Success(List(ColumnFormat(c)(accessorTree, "DATETIME", None)))
    }
  }
}
