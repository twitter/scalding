package com.twitter.scalding.jdbc.macros.impl.handler

import scala.language.experimental.macros

import scala.reflect.macros.Context

import com.twitter.scalding.jdbc.ColumnDefinition
import com.twitter.scalding.jdbc.macros._
import com.twitter.scalding.jdbc.macros.impl.FieldName

object ColFormatter {
  def apply(c: Context)(fieldType: String, sizeOpt: Option[Int])(implicit fieldName: FieldName,
    defaultValue: Option[c.Expr[String]],
    annotationInfo: List[(c.universe.Type, Option[Int])],
    nullable: Boolean): List[c.Expr[ColumnDefinition]] = {
    import c.universe._
    val nullableVal = if (nullable)
      q"com.twitter.scalding.jdbc.Nullable"
    else
      q"com.twitter.scalding.jdbc.NotNullable"

    val fieldTypeSelect = Select(q"com.twitter.scalding.jdbc", newTermName(fieldType))

    List(c.Expr[ColumnDefinition](q"""new com.twitter.scalding.jdbc.ColumnDefinition(
        $fieldTypeSelect,
        com.twitter.scalding.jdbc.ColumnName(${fieldName.toStr}),
        $nullableVal,
        $sizeOpt,
        ${defaultValue})
        """))
  }
}
