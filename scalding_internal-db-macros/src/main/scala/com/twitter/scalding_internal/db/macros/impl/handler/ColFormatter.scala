package com.twitter.scalding_internal.db.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context

import com.twitter.scalding_internal.db.ColumnDefinition
import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.FieldName

object ColFormatter {
  def apply(c: Context)(fieldType: String, sizeOpt: Option[Int])(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): List[c.Expr[ColumnDefinition]] = {
    import c.universe._
    val nullableVal = if (nullable)
      q"com.twitter.scalding_internal.db.Nullable"
    else
      q"com.twitter.scalding_internal.db.NotNullable"

    val fieldTypeSelect = Select(q"com.twitter.scalding_internal.db", newTermName(fieldType))

    List(c.Expr[ColumnDefinition](q"""new com.twitter.scalding_internal.db.ColumnDefinition(
        $fieldTypeSelect,
        com.twitter.scalding_internal.db.ColumnName(${fieldName.toStr}),
        $nullableVal,
        $sizeOpt,
        ${defaultValue})
        """))
  }
}
