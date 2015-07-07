package com.twitter.scalding.source.typedtext

import com.twitter.scalding._

//import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Test1(a: Int, b: Long, c: Option[Double])
case class Test2(one: Test1, d: String)

//@RunWith(classOf[JUnitRunner])
class TypedTextTest extends FunSuite {
  test("Test with a flat tuple") {
    val source = TypedText.tsv[Test1]("myPath")
    assert(source.sourceFields.size == 3)
  }
  test("Test with a nested tuple") {
    val source = TypedText.tsv[Test2]("myPath")
    assert(source.sourceFields.size == 4)
  }
  test("Test with a nested tuple: Daily") {
    val source = TypedText.dailyLzoTsv[Test2]("myPath")(DateRange(RichDate.now, RichDate.now + Hours(1)), implicitly)
    assert(source.sourceFields.size == 4)
  }
}
