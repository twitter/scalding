package com.twitter.scalding

import org.scalatest.{ PropSpec, Matchers, WordSpec }
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import org.scalacheck.Gen

class StringUtilityTest extends WordSpec with Matchers {
  "fastSplitTest" should {
    "be able to split white space" in {
      val text1 = "this is good time"
      val res1 = StringUtility.fastSplit(text1, " ") // split single white space
      res1 should be {
        Seq("this", "is", "good", "time")
      }
    }
  }
  "be able to split other separators" in {
    val text2 = "a:b:c:d:"
    val res2 = StringUtility.fastSplit(text2, ":")
    res2 should be {
      Seq("a", "b", "c", "d", "")
    }
  }
  "be able to split only one separators" in {
    val text2 = "a@"
    val res2 = StringUtility.fastSplit(text2, "@")
    res2 should be {
      Seq("a", "")
    }
  }
  "be able to split when separator doesn't show up" in {
    val text2 = "a"
    val res2 = StringUtility.fastSplit(text2, "@")
    res2 should be {
      Seq("a")
    }
  }
}

class StringUtilityPropertyTest extends PropSpec with Checkers {
  val randomStringGen = for {
    s <- Gen.pick(5, List.fill(100)(List("k", "l", "m", "x", "//.", "@")).flatten)

  } yield s

  // test for one separator and two
  val randomSeparator = for {
    s <- Gen.oneOf("@@", "@", "x", "//.")
  } yield s

  property("fastSplit(s, sep) should match s.split(sep, -1) for non-regex sep") {
    check {
      forAll(randomStringGen, randomSeparator) {
        (str, separator) =>
          val t = str.mkString("")
          val r1 = t.split(separator, -1).toList
          val r2 = StringUtility.fastSplit(t, separator)
          r1 == r2
      }
    }
  }

}
