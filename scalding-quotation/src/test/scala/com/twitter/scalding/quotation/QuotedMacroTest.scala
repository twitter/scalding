package com.twitter.scalding.quotation

import org.scalatest.MustMatchers
import org.scalatest.FreeSpec

class QuotedMacroTest extends Test {

  val test = new TestClass

  val nullary = test.nullary
  val parametrizedNullary = test.parametrizedNullary[Int]
  val withParam = test.withParam[Person, String](_.name)._1

  val quotedFunction =
    Quoted.function[Person, Contact](_.contact)

  val nestedQuotedFuction =
    Quoted.function[Person, Contact](p => quotedFunction(p))

  val person = Person("John", Contact("33223"), None)

  class TestClass {
    def nullary(implicit q: Quoted) = q
    def parametrizedNullary[T](implicit q: Quoted) = q
    def withParam[T, U](f: T => U)(implicit q: Quoted) = (q, f)
  }

  "quoted method" - {

    "nullary" in {
      nullary.position.toString mustEqual "QuotedMacroTest.scala:10"
      nullary.projections.set mustEqual Set.empty
      nullary.text mustEqual Some("nullary")
    }

    "parametrizedNullary" in {
      parametrizedNullary.position.toString mustEqual "QuotedMacroTest.scala:11"
      parametrizedNullary.projections.set mustEqual Set.empty
      parametrizedNullary.text mustEqual Some("parametrizedNullary[Int]")
    }

    "withParam" in {
      withParam.position.toString mustEqual "QuotedMacroTest.scala:12"
      withParam.projections.set mustEqual Set(Person.nameProjection)
      withParam.text mustEqual Some("withParam[Person, String](_.name)")
    }
  }

  "quoted function" - {
    "simple" in {
      val q = quotedFunction.quoted
      q.position.toString mustEqual "QuotedMacroTest.scala:15"
      q.projections.set mustEqual Set(Person.contactProjection)
      q.text mustEqual Some("[Person, Contact](_.contact)")

      quotedFunction(person) mustEqual person.contact
    }
    "nested" in {
      val q = nestedQuotedFuction.quoted
      q.position.toString mustEqual "QuotedMacroTest.scala:18"
      q.projections.set mustEqual Set(Person.contactProjection)
      q.text mustEqual Some("[Person, Contact](p => quotedFunction(p))")

      nestedQuotedFuction(person) mustEqual person.contact
    }
  }

  "invalid quoted method call" in {
    "Quoted.method" mustNot compile
  }
}