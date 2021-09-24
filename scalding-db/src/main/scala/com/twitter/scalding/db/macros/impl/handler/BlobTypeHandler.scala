package com.twitter.scalding.db.macros.impl.handler

import com.twitter.scalding.db.macros.impl.FieldName
import scala.reflect.macros.Context
import scala.util.{Failure, Success}

object BlobTypeHandler {
  def apply[T](c: Context)(implicit
      accessorTree: List[c.universe.MethodSymbol],
      fieldName: FieldName,
      defaultValue: Option[c.Expr[String]],
      annotationInfo: List[(c.universe.Type, Option[Int])],
      nullable: Boolean
  ): scala.util.Try[List[ColumnFormat[c.type]]] =
    if (defaultValue.nonEmpty || annotationInfo.nonEmpty)
      Failure(
        new Exception(
          s"Default values and annotation info are not supported: defaultValue = $defaultValue annotationInfo = $annotationInfo"
        )
      )
    else
      Success(List(ColumnFormat(c)(accessorTree, "BLOB", None)))
}
