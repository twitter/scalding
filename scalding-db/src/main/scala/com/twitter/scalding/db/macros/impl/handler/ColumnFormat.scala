package com.twitter.scalding.db.macros.impl.handler

import scala.reflect.macros.Context

import com.twitter.scalding.db.macros.impl.FieldName

object ColumnFormat {
  def apply(c: Context)(fAccessor: List[c.universe.MethodSymbol], fType: String, size: Option[Int])(implicit fName: FieldName,
    isNullable: Boolean, defaultV: Option[c.Expr[String]]): ColumnFormat[c.type] = {

    new ColumnFormat[c.type](c) {
      val fieldAccessor = fAccessor
      val fieldType = fType
      val fieldName = fName
      val nullable = isNullable
      val sizeOpt = size
      val defaultValue = defaultV
    }
  }
}

/**
 * Contains data format information for a column as defined in the case class.
 *
 * Used by the ColumnDefinitionProvider macro too generate columns definitions and
 * JDBC ResultSet extractor.
 */
abstract class ColumnFormat[C <: Context](val ctx: C) {
  def fieldAccessor: List[ctx.universe.MethodSymbol]
  def fieldType: String
  def fieldName: FieldName
  def nullable: Boolean
  def sizeOpt: Option[Int]
  def defaultValue: Option[ctx.Expr[String]]
}

