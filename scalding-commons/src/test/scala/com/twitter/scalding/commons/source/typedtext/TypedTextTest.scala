package com.twitter.scalding.commons.source.lzotypedtext

import com.twitter.scalding._
import com.twitter.scalding.source.typedtext._

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Test1(a: Int, b: Long, c: Option[Double])
case class Test2(one: Test1, d: String)

class TypedTextTest extends FunSuite {
  test("Test with a nested tuple: Daily") {
    val source = LzoTypedText.dailyLzoTsv[Test2]("myPath")(DateRange(RichDate.now, RichDate.now + Hours(1)), implicitly)
    assert(source.sourceFields.size == 4)
  }
}
