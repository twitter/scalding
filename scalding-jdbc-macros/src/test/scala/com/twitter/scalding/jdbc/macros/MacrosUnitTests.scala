package com.twitter.scalding.jdbc.macros

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.jdbc.macros._

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }

case class User(
  date_id: Int,
  @size(64) user_name: String,
  @size(64) email: String,
  @size(64) password: String,
  age: Option[Int],
  @size(22) gender: String = "male")

class JdbcMacroUnitTests extends WordSpec with Matchers {

  "Produces the JdbcDescriptor" should {
    val expectedColumns = List(
      IntColumn("date_id", 32, None),
      StringColumn("user_name", 64, None),
      StringColumn("email", 64, None),
      StringColumn("password", 64, None),
      OptionalColumn(IntColumn("age", 32, None)),
      StringColumn("gender", 22, Some("male")))

    assert(JDBCDescriptor.toJDBCDescriptor[User].columns.toList === expectedColumns)

  }
}
