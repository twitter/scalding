package com.twitter.scalding.db.macros.impl.handler

import scala.reflect.macros.Context
import scala.util.Success

import com.twitter.scalding.db.macros.impl.FieldName

object BlobTypeHandler {
  def apply[T](c: Context)(implicit accessorTree: List[c.universe.MethodSymbol],
    fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): scala.util.Try[List[ColumnFormat[c.type]]] = {

    assert(defaultValue.isEmpty)
    assert(annotationInfo.isEmpty)

    Success(List(ColumnFormat(c)(accessorTree, "BLOB", None)))
  }
}
