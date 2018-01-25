package com.twitter.scalding.quotation

import org.scalatest.Matchers
import org.scalatest.WordSpec
import org.scalatest.FreeSpec
import org.scalatest.MustMatchers

class TextMacroTest extends Test {

  class TestClass {
    def nullary(implicit m: Quoted) = m
    def parametrizedNullary[T](implicit m: Quoted) = m
    def primitiveParam(v: Int)(implicit m: Quoted) = (m, v)
    def parametrized[T](v: T)(implicit m: Quoted) = (m, v)
    def paramGroups(a: Int, b: Int)(c: Int)(implicit m: Quoted) = (m, a, b, c)
    def parametrizedParamGroups[T](a: T, b: Int)(c: T)(implicit m: Quoted) = (m, a, b, c)
    def paramGroupsWithFunction(a: Int)(b: Int => Int)(implicit m: Quoted) = (m, a, b)
    def function(f: Int => Int)(implicit m: Quoted) = (m, f)
    def multipleFunctions[T, U, V](f1: T => U, f2: U => V)(implicit m: Quoted) = (m, f1, f2)
    def tupleParam(t: (Int, Int))(implicit m: Quoted) = (m, t)
  }

  val test = new TestClass

  "nullary" in {
    test.nullary.text mustEqual
      Some("nullary")
  }

  "parametrizedNullary" - {
    "inferred type param" in {
      test.parametrizedNullary.text mustEqual
        Some("parametrizedNullary")
    }
    "explicit type param" in {
      test.parametrizedNullary[Int].text mustEqual
        Some("parametrizedNullary[Int]")
    }
  }

  "primitiveParam" in {
    test.primitiveParam(22)._1.text mustEqual
      Some("primitiveParam(22)")
  }

  "parametrized" - {
    "inferred type param" in {
      test.parametrized(42)._1.text mustEqual
        Some("parametrized(42)")
    }
    "explicit type param" in {
      test.parametrized[Int](42)._1.text mustEqual
        Some("parametrized[Int](42)")
    }
  }

  "paramGroups" - {
    "primitives" in {
      test.paramGroups(1, 2)(3)._1.text mustEqual
        Some("paramGroups(1, 2)(3)")
    }
    "parametrized" - {
      "explicit type param" in {
        test.parametrizedParamGroups[Int](1, 2)(3)._1.text mustEqual
          Some("parametrizedParamGroups[Int](1, 2)(3)")
      }
      "inferred type param" in {
        test.parametrizedParamGroups(1, 2)(3)._1.text mustEqual
          Some("parametrizedParamGroups(1, 2)(3)")
      }
    }
    "with function" in {
      (test.paramGroupsWithFunction(1) {
        case 1 => 2
        case _ => 3
      })._1.text mustEqual
        Some("""paramGroupsWithFunction(1) {
        case 1 => 2
        case _ => 3
      }""")
    }
  }

  "function" - {
    "underscore" in {
      test.function(_ + 1)._1.text mustEqual
        Some("function(_ + 1)")
    }
    "pattern matching" in {
      test.function { case _ => 4 }._1.text mustEqual Some("function { case _ => 4 }")
    }
    "curly braces" in {
      test.function { _ + 1 }._1.text mustEqual Some("function { _ + 1 }")
    }
  }

  "complex tree" in {
    val c = test.function {
      def test = 1
      _ + 1
    }
    c._1.text mustEqual
      Some(
        """function {
      def test = 1
      _ + 1
    }""")
  }

  "tuple param" in {
    test.tupleParam((1, 2))._1.text mustEqual
      Some("tupleParam((1, 2))")
  }
}