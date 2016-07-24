package com.twitter.scalding.bdd

import cascading.flow.FlowException
import com.twitter.scalding.typed.TDsl
import com.twitter.scalding._
import org.scalatest.{ Matchers, WordSpec }
import scala.math._

import scala.collection.mutable

case class UserWithGender(name: String, gender: String)
case class UserWithAge(name: String, age: Int)
case class UserInfo(name: String, gender: String, age: Int)
case class EstimatedContribution(name: String, suggestedPensionContributionPerMonth: Double)

class TypedApiTest extends WordSpec with Matchers with TBddDsl {
  "A test with a single source" should {
    "accept an operation from working with a single tuple-typed pipe" in {
      Given {
        List(("Joe", "M", 40), ("Sarah", "F", 22))
      } When {
        in: TypedPipe[(String, String, Int)] =>
          in.map[(String, Double)] {
            case (name, "M", age) => (name, (1000.0 / (72 - age)))
            case (name, _, age) => (name, (1000.0 / (80 - age)))
          }
      } Then {
        buffer: mutable.Buffer[(String, Double)] =>
          buffer.toList shouldBe List(("Joe", 1000.0 / 32), ("Sarah", 1000.0 / 58))
      }
    }

    "accept an operation from single case class-typed pipe" in {
      Given {
        List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      } When {
        in: TypedPipe[UserInfo] =>
          in.map {
            case UserInfo(name, "M", age) => EstimatedContribution(name, (1000.0 / (72 - age)))
            case UserInfo(name, _, age) => EstimatedContribution(name, (1000.0 / (80 - age)))
          }
      } Then {
        buffer: mutable.Buffer[EstimatedContribution] =>
          buffer.toList shouldBe List(EstimatedContribution("Joe", 1000.0 / 32), EstimatedContribution("Sarah", 1000.0 / 58))
      }
    }
  }

  "A test with a two sources" should {

    "accept an operation from two tuple-typed pipes" in {
      Given {
        List(("Joe", "M"), ("Sarah", "F"))
      } And {
        List(("Joe", 40), ("Sarah", 22))
      } When {
        (gender: TypedPipe[(String, String)], age: TypedPipe[(String, Int)]) =>
          gender
            .group
            .join(age.group)
            .toTypedPipe
            .map { value: (String, (String, Int)) =>
              val (name, (gender, age)) = value
              (name, gender, age)
            }
      } Then {
        buffer: mutable.Buffer[(String, String, Int)] =>
          buffer.toList shouldBe List(("Joe", "M", 40), ("Sarah", "F", 22))
      }
    }

    "accept an operation from two case classes-typed pipes" in {
      Given {
        List(UserWithGender("Joe", "M"), UserWithGender("Sarah", "F"))
      } And {
        List(UserWithAge("Joe", 40), UserWithAge("Sarah", 22))
      } When {
        (gender: TypedPipe[UserWithGender], age: TypedPipe[UserWithAge]) =>
          gender
            .groupBy(_.name)
            .join(age.groupBy(_.name))
            .mapValues { value: (UserWithGender, UserWithAge) =>
              val (withGender, withAge) = value
              UserInfo(withGender.name, withGender.gender, withAge.age)
            }
            .values
      } Then {
        buffer: mutable.Buffer[UserInfo] =>
          buffer.toList shouldBe List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      }
    }
  }

  "A test with a list of sources" should {
    "Work as if combining the sources with the And operator but requires explicit cast of the input pipes" in {
      GivenSources {
        List(
          List(UserWithGender("Joe", "M"), UserWithGender("Sarah", "F")),
          List(UserWithAge("Joe", 40), UserWithAge("Sarah", 22)))
      } When {
        pipes: List[TypedPipe[_]] =>
          val gender = pipes(0).asInstanceOf[TypedPipe[UserWithGender]] // linter:ignore
          val age = pipes(1).asInstanceOf[TypedPipe[UserWithAge]] // linter:ignore

          gender
            .groupBy(_.name)
            .join(age.groupBy(_.name))
            .mapValues { value: (UserWithGender, UserWithAge) =>
              val (withGender, withAge) = value
              UserInfo(withGender.name, withGender.gender, withAge.age)
            }
            .values
      } Then {
        buffer: mutable.Buffer[UserInfo] =>
          buffer.toList shouldBe List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      }
    }

    "not checking the types of the sources and fail if any error occurs" in {
      an[FlowException] should be thrownBy {
        GivenSources {
          List(
            List(UserWithGender("Joe", "M"), UserWithGender("Sarah", "F")),
            List(("Joe", 40), ("Sarah", 22)))
        } When {
          pipes: List[TypedPipe[_]] =>
            val gender = pipes(0).asInstanceOf[TypedPipe[UserWithGender]] // linter:ignore
            val age = pipes(1).asInstanceOf[TypedPipe[UserWithAge]] // linter:ignore

            gender
              .groupBy(_.name)
              .join(age.groupBy(_.name))
              .mapValues { value: (UserWithGender, UserWithAge) =>
                val (withGender, withAge) = value
                UserInfo(withGender.name, withGender.gender, withAge.age)
              }
              .values
        } Then {
          buffer: mutable.Buffer[UserInfo] =>
            buffer.toList shouldBe List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
        }
      }
    }

    "be created when adding a source to four sources" in {
      Given {
        List(("Joe", "user1"), ("Sarah", "user2"))
      } And {
        List(("user1", "M"), ("user2", "F"))
      } And {
        List(("user1", 40), ("user2", 22))
      } And {
        List(("user1", 1000l), ("user2", 800l))
      } And {
        List(("user1", true), ("user2", false))
      } When {
        pipes: List[TypedPipe[_]] =>
          val withUserID = pipes(0).asInstanceOf[TypedPipe[(String, String)]] // linter:ignore
          val withGender = pipes(1).asInstanceOf[TypedPipe[(String, String)]]
          val withAge = pipes(2).asInstanceOf[TypedPipe[(String, Int)]]
          val withIncome = pipes(3).asInstanceOf[TypedPipe[(String, Long)]]
          val withSmoker = pipes(4).asInstanceOf[TypedPipe[(String, Boolean)]]

          withUserID
            .swap.group
            .join(withGender.group)
            .join(withAge.group)
            .join(withIncome.group)
            .join(withSmoker.group)
            .flatMapValues {
              case ((((name: String, gender: String), age: Int), income: Long), smoker) =>
                val lifeExpectancy = (gender, smoker) match {
                  case ("M", true) => 68
                  case ("M", false) => 72
                  case (_, true) => 76
                  case (_, false) => 80
                }

                Some(EstimatedContribution(name, floor(income / (lifeExpectancy - age))))
              case _ => None
            }
            .values
      } Then {
        buffer: mutable.Buffer[EstimatedContribution] =>
          buffer.toList shouldBe List(EstimatedContribution("Joe", 35.0), EstimatedContribution("Sarah", 13.0))
      }
    }
  }
}
