package com.twitter.scalding_internal.db.macros.impl.handler

import scala.reflect.macros.Context

import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.FieldName

// TODO: remove this in favor of companion object below
object ColFormatter {
  def apply(c: Context)(fieldType: String, sizeOpt: Option[Int])(implicit fieldName: FieldName,
    nullable: Boolean, defaultValue: Option[c.Expr[String]]): ColumnFormat[c.type] = {
    ColumnFormat(c)(
      fieldType,
      fieldName.toStr,
      nullable,
      sizeOpt,
      defaultValue)
  }
}

object ColumnFormat {
  def apply(c: Context)(fType: String, fName: String, isNullable: Boolean,
    size: Option[Int], defaultV: Option[c.Expr[String]]): ColumnFormat[c.type] = {

    new ColumnFormat[c.type](c) {
      val fieldType = fType
      val fieldName = fName
      val nullable = isNullable
      val sizeOpt = size
      val defaultValue = defaultV
    }
  }
}

abstract class ColumnFormat[C <: Context](val ctx: C) {
  def fieldType: String
  def fieldName: String
  def nullable: Boolean
  def sizeOpt: Option[Int]
  def defaultValue: Option[ctx.Expr[String]]
}

