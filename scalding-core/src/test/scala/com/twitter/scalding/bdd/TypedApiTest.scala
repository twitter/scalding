package com.twitter.scalding.bdd

import com.twitter.scalding.typed.TDsl
import com.twitter.scalding.{ TypedPipe, TypedTsv, Job, Args }
import org.specs.Specification

import scala.collection.mutable

case class TestType(name: String, sex: String, age: Int)
case class TestTypeOut(name: String, suggestedPensionContributionPerMonth: Double)

class TypedApiTest extends Specification with TBddDsl {

  "A test with a single source" should {

    "accept an operation from single typed pipe" in {
      Given {
        List(TestType("Joe", "M", 40), TestType("Sarah", "F", 22))
      } When {
        in: TypedPipe[TestType] =>
          in.map[TestTypeOut] { person =>
            person match {
              case TestType(name, "M", age) => TestTypeOut(name, 1000 / (72 - age))
              case TestType(name, _, age) => TestTypeOut(name, 1000 / (80 - age))
            }
          }
      } Then {
        buffer: mutable.Buffer[TestTypeOut] =>
          buffer.toList mustEqual List(TestTypeOut("Joe", 1000 / 32), TestTypeOut("Sarah", 1000 / 58))
      }
    }
  }

  "A test with a two sources" should {

    "accept an operation from two typed pipes" in {
      Given {
        List(("Joe", "M"), ("Sarah", "F"))
      } And {
        List(("Joe", 40), ("Sarah", 22))
      } When {
        (sex: TypedPipe[(String, String)], age: TypedPipe[(String, Int)]) =>
          sex
            .groupBy(_._1)
            .join(age.groupBy(_._1))
            .mapValues { value: ((String, String), (String, Int)) =>
              val (withSex, withAge) = value
              TestType(withSex._1, withSex._2, withAge._2)
            }
            .values
      } Then {
        buffer: mutable.Buffer[TestType] =>
          buffer.toList mustEqual List(TestType("Joe", "M", 40), TestType("Sarah", "F", 22))
      }
    }
  }
}
