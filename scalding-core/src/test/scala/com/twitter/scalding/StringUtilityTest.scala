package com.twitter.scalding

import org.scalatest.{ Matchers, WordSpec }

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
      Seq("a", "b", "c", "d")
    }
  }
  "be able to split only one separators" in {
    val text2 = "a@"
    val res2 = StringUtility.fastSplit(text2, "@")
    res2 should be {
      Seq("a")
    }
  }
  "be able to split when separator doesn't show up" in {
    val text2 = "a"
    val res2 = StringUtility.fastSplit(text2, "@")
    res2 should be {
      Seq("a")
    }
  }
  "be able to generate the same result with Java's split for random strings and separators" in {
    def randomString(length: Int) = {
      val possibleChars = "abcdefgABCDEFG #@:!$%^&*()+-_"
      val nPossibleChar = possibleChars.length
      val r = new scala.util.Random
      val sb = new StringBuilder
      for (i <- 1 to length) {
        sb.append(possibleChars(r.nextInt(nPossibleChar)))
      }
      sb.toString
    }
    // randomly test to make sure the fastSplit function works the same (with high probability) with java's impl
    // for loop is to test for different separator it works exactly the same with Java split
    for (i <- 1 to 100) {
      val randomStrings: List[String] = (1 to 1000).map {
        x =>
          randomString(20)
      }.toList
      val randomSeparatorIndex = scala.util.Random.nextInt(5)
      val separator = "#@/: "(randomSeparatorIndex).toString
      val splittedByRegex = randomStrings.map { s => s.split(separator).toList }
      val splittedByFastSpliter = randomStrings.map { s => StringUtility.fastSplit(s, separator).toList }
      splittedByRegex should be(splittedByFastSpliter)
    }
  }

}
