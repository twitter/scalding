package com.twitter.scalding.db.macros.impl.handler

import scala.reflect.macros.Context
import scala.util.{ Success, Failure }

import com.twitter.scalding.db.macros.impl.FieldName

object NumericTypeHandler {
  def apply[T](c: Context)(implicit accessorTree: List[c.universe.MethodSymbol],
    fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean,
    numericType: String): scala.util.Try[List[ColumnFormat[c.type]]] = {

    val helper = new {
      val ctx: c.type = c
      val cfieldName = fieldName
      val cannotationInfo = annotationInfo
    } with AnnotationHelper

    val extracted = for {
      (nextHelper, sizeAnno) <- helper.sizeAnnotation
      _ <- nextHelper.validateFinished
    } yield (sizeAnno)

    extracted.flatMap {
      case WithSize(s) if s > 0 => Success(List(ColumnFormat(c)(accessorTree, numericType, Some(s))))
      case WithSize(s) => Failure(new Exception(s"Int field $fieldName, has a size defined that is <= 0."))
      case WithoutSize => Success(List(ColumnFormat(c)(accessorTree, numericType, None)))
    }
  }
}
