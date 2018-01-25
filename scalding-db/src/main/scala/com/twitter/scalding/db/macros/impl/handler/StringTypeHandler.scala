package com.twitter.scalding.db.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context
import scala.reflect.runtime.universe._
import scala.util.{ Success, Failure }

import com.twitter.scalding.db.macros.impl.FieldName

object StringTypeHandler {
  def apply[T](c: Context)(implicit accessorTree: List[c.universe.MethodSymbol],
    fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[List[ColumnFormat[c.type]]] = {
    import c.universe._

    val helper = new {
      val ctx: c.type = c
      val cfieldName = fieldName
      val cannotationInfo = annotationInfo
    } with AnnotationHelper

    val extracted = for {
      (nextHelper, sizeAnno) <- helper.sizeAnnotation
      (nextHelper, varcharAnno) <- nextHelper.varcharAnnotation
      (nextHelper, textAnno) <- nextHelper.textAnnotation
      _ <- nextHelper.validateFinished
    } yield (sizeAnno, varcharAnno, textAnno)

    extracted.flatMap {
      case (_, WithVarchar, WithText) => Failure(new Exception(s"String field $fieldName, has mutually exclusive annotations @text and @varchar"))
      case (WithoutSize, WithVarchar, WithoutText) => Failure(new Exception(s"String field $fieldName, is forced varchar but has no size annotation. size is required in the presence of varchar."))
      case (WithoutSize, WithoutVarchar, WithoutText) => Failure(new Exception(s"String field $fieldName, at least one of size, varchar, text must be present."))
      case (WithSize(siz), _, _) if siz <= 0 => Failure(new Exception(s"String field $fieldName, has a size $siz which is <= 0. Doesn't make sense for a string."))
      case (WithSize(siz), WithoutVarchar, WithoutText) if siz <= 255 => Success(List(ColumnFormat(c)(accessorTree, "VARCHAR", Some(siz))))
      case (WithSize(siz), WithoutVarchar, WithoutText) if siz > 255 => Success(List(ColumnFormat(c)(accessorTree, "TEXT", None)))
      case (WithSize(siz), WithVarchar, WithoutText) => Success(List(ColumnFormat(c)(accessorTree, "VARCHAR", Some(siz))))
      case (_, WithoutVarchar, WithText) => Success(List(ColumnFormat(c)(accessorTree, "TEXT", None)))
    }
  }
}
