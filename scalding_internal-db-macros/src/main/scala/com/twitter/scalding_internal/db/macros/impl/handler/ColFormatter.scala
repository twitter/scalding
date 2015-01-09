package com.twitter.scalding_internal.db.macros.impl.handler

import com.twitter.scalding_internal.db.macros._
import com.twitter.scalding_internal.db.macros.impl.FieldName

object ColFormatter {
  def apply(fieldType: String, sizeOpt: Option[Int])(implicit fieldName: FieldName,
    nullable: Boolean): ColumnFormat = {
    ColumnFormat(
      fieldType,
      fieldName.toStr,
      nullable,
      sizeOpt)
  }
}

case class ColumnFormat(
  fieldType: String,
  fieldName: String,
  nullable: Boolean,
  sizeOpt: Option[Int])
