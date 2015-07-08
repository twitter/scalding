package com.twitter.scalding.source.typedtext

import com.twitter.scalding._

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Test1(a: Int, b: Long, c: Option[Double])
case class Test2(one: Test1, d: String)

class TypedTextTest extends FunSuite {
  test("Test with a flat tuple") {
    val source = TypedText.tsv[Test1]("myPath")
    assert(source.sourceFields.size == 3)
  }

  test("Test with a nested tuple") {
    val source = TypedText.tsv[Test2]("myPath")
    assert(source.sourceFields.size == 4)
  }

  test("Test with a raw type") {
    descriptor[String]
    val source = TypedText.tsv[String]("myPath")
    assert(source.sourceFields.size == 1)
  }

  test("Test with a tuple") {
    val source = TypedText.tsv[(Int, Int)]("myPath")

    assert(source.sourceFields.size == 2)
  }
}
