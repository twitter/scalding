package com.twitter.scalding.bdd

import cascading.flow.FlowException
import com.twitter.scalding.typed.TDsl
import com.twitter.scalding._
import org.specs.Specification

import scala.collection.mutable

case class UserWithGender(name: String, gender: String)
case class UserWithAge(name: String, age: Int)
case class UserInfo(name: String, gender: String, age: Int)
case class EstimatedContribution(name: String, suggestedPensionContributionPerMonth: Double)

class TypedApiTest extends Specification with TBddDsl {

  "A test with a single source" should {

    "accept an operation from working with a single tuple-typed pipe" in {
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

    "accept an operation from single case class-typed pipe" in {
      Given {
        List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      } When {
        in: TypedPipe[UserInfo] =>
          in.map { person =>
            person match {
              case UserInfo(name, "M", age) => EstimatedContribution(name, (1000.0 / (72 - age)))
              case UserInfo(name, _, age) => EstimatedContribution(name, (1000.0 / (80 - age)))
            }
          }
      } Then {
        buffer: mutable.Buffer[EstimatedContribution] =>
          buffer.toList mustEqual List(EstimatedContribution("Joe", 1000.0 / 32), EstimatedContribution("Sarah", 1000.0 / 58))
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
            .groupBy(_._1)
            .join(age.groupBy(_._1))
            .mapValues { value: ((String, String), (String, Int)) =>
              val (withGender, withAge) = value
              (withGender._1, withGender._2, withAge._2)
            }
            .values
      } Then {
        buffer: mutable.Buffer[(String, String, Int)] =>
          buffer.toList mustEqual List(("Joe", "M", 40), ("Sarah", "F", 22))
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
          buffer.toList mustEqual List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      }
    }
  }

  "A test with a list of sources" should {
    "Work as if combining the sources with the And operator" in {
      GivenSources {
        List(
          List(UserWithGender("Joe", "M"), UserWithGender("Sarah", "F")),
          List(UserWithAge("Joe", 40), UserWithAge("Sarah", 22)))
      } When {
        pipes: List[TypedPipe[_]] =>
          val gender = pipes(0).asInstanceOf[TypedPipe[UserWithGender]]
          val age = pipes(1).asInstanceOf[TypedPipe[UserWithAge]]

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
          buffer.toList mustEqual List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      }
    }

    "not checking the types of the sources and fail if any error occurs" in {
      GivenSources {
        List(
          List(UserWithGender("Joe", "M"), UserWithGender("Sarah", "F")),
          List(("Joe", 40), ("Sarah", 22)))
      } When {
        pipes: List[TypedPipe[_]] =>
          val gender = pipes(0).asInstanceOf[TypedPipe[UserWithGender]]
          val age = pipes(1).asInstanceOf[TypedPipe[UserWithAge]]

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
          buffer.toList mustEqual List(UserInfo("Joe", "M", 40), UserInfo("Sarah", "F", 22))
      } must throwA[FlowException]
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
          val withUserID = pipes(0).asInstanceOf[TypedPipe[(String, String)]]
          val withGender = pipes(1).asInstanceOf[TypedPipe[(String, String)]]
          val withAge = pipes(2).asInstanceOf[TypedPipe[(String, Int)]]
          val withIncome = pipes(3).asInstanceOf[TypedPipe[(String, Long)]]
          val withSmoker = pipes(4).asInstanceOf[TypedPipe[(String, Boolean)]]

          withUserID
            .groupBy(_._2)
            .join(withGender.groupBy(_._1))
            .join(withAge.groupBy(_._1))
            .join(withIncome.groupBy(_._1))
            .join(withSmoker.groupBy(_._1))
            .mapValues { value =>
              val name = value._1._1._1._1._1
              val gender = value._1._1._1._2._2
              val age = value._1._1._2._2
              val income = value._1._2._2
              val smoker = value._2._2

              val lifeExpectancy = (gender, smoker) match {
                case ("M", true) => 68
                case ("M", false) => 72
                case (_, true) => 76
                case (_, false) => 80
              }

              val contribution = Math.floor(income / (lifeExpectancy - age))
              EstimatedContribution(name, contribution)
            }
            .values
            .debug
      } Then {
        buffer: mutable.Buffer[EstimatedContribution] =>
          buffer.toList mustEqual List(EstimatedContribution("Joe", 35.0), EstimatedContribution("Sarah", 13.0))
      }
    }
  }
}
