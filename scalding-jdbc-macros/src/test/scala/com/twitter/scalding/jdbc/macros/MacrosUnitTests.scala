package com.twitter.scalding.jdbc.macros

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import com.twitter.scalding.jdbc.macros._
import org.scalatest.exceptions.TestFailedException

import com.twitter.bijection.macros.{ IsCaseClass, MacroGenerated }

case class User(
  date_id: Int,
  @size(64) user_name: String,
  @size(64) email: String,
  @size(64) password: String,
  age: Option[Int],
  @size(22) gender: String = "male")

class JdbcMacroUnitTests extends WordSpec with Matchers {

  val dummy = new JDBCDescriptor[Nothing] {
    override val columns = Nil
  }

  def isDescriptorAvailable[T](implicit proof: JDBCDescriptor[T] = dummy.asInstanceOf[JDBCDescriptor[T]]) {
    proof shouldBe a[MacroGenerated]
    proof.columns.isEmpty shouldBe false
  }

  "String field missing annotation" in {
    case class BadUser(user_name: String, age: Int)
    a[TestFailedException] should be thrownBy isDescriptorAvailable[BadUser]
  }

  "Option field with default" in {
    case class BadUser(user_name: Option[String] = Some("bob"), age: Int)
    a[TestFailedException] should be thrownBy isDescriptorAvailable[BadUser]
  }

  "Unknown field type" in {
    case class BadUser(user_names: List[String])
    a[TestFailedException] should be thrownBy isDescriptorAvailable[BadUser]
  }

  "Produces the JdbcDescriptor" should {
    isDescriptorAvailable[User]

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
