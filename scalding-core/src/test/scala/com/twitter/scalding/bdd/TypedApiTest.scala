package com.twitter.scalding.bdd

import com.twitter.scalding.typed.TDsl
import com.twitter.scalding.{ TypedPipe, TypedTsv, Job, Args }
import org.specs.Specification

import scala.collection.mutable

case class TestType(name: String, sex: String, age: Int)
case class TestTypeOut(name: String, suggestedPensionContributionPerMonth: Double)

class TypedApiTest extends Specification with TBddDsl {

  "A test with a single source" should {

    "accept an operation from single typed pipe - basic tuple type" in {
      Given {
        List(("Joe", "M", 40), ("Sarah", "F", 22))
      } When {
        in: TypedPipe[(String, String, Int)] =>
          in.map[(String, Double)] { person =>
            person match {
              case (name, "M", age) => (name, (1000.0 / (72 - age)).toDouble)
              case (name, _, age) => (name, (1000.0 / (80 - age)).toDouble)
            }
          }
      } Then {
        buffer: mutable.Buffer[(String, Double)] =>
          buffer.toList mustEqual List(("Joe", 1000.0 / 32), ("Sarah", 1000.0 / 58))
      }
    }
  }

  "A test with a two sources" should {

    "accept an operation from two typed pipes - basic tuple type" in {
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
              (withSex._1, withSex._2, withAge._2)
            }
            .values
      } Then {
        buffer: mutable.Buffer[(String, String, Int)] =>
          buffer.toList mustEqual List(("Joe", "M", 40), ("Sarah", "F", 22))
      }
    }
  }
}
