package com.twitter.scalding_internal.db.macros

import com.twitter.scalding_internal.db.macros._

case class ExternalCaseClass(
  date_id: Int,
  @size(64) user_name: String,
  age: Option[Int],
  @size(22) gender: String = "male")
