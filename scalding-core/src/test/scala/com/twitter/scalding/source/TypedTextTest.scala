package com.twitter.scalding.source

import org.scalatest.FunSuite

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
    val source = TypedText.tsv[String]("myPath")
    assert(source.sourceFields.size == 1)
  }

  test("Test with a tuple") {
    val source = TypedText.tsv[(Int, Int)]("myPath")
    assert(source.sourceFields.size == 2)
  }

  test("Test with an Optional Int") {
    val source = TypedText.tsv[Option[Int]]("myPath")
    assert(source.sourceFields.size == 1)
  }

  test("Test with an Int") {
    val source = TypedText.tsv[Int]("myPath")
    assert(source.sourceFields.size == 1)
  }
}
