package com.twitter.scalding

import org.scalatest.{ PropSpec, Matchers, WordSpec }
import org.scalacheck.{ Arbitrary, Properties }
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers
import org.scalacheck.Gen

import scala.collection.mutable.ListBuffer

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
  /*
  "be able to be faster than java's split function" in {
    // helper function to time
    def time[R](block: => R): Double = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      val timeDiff = (t1 - t0)
      timeDiff
    }

    def randomString(length: Int) = {
      val possibleChars = "abcdefg|"
      val nPossibleChar = possibleChars.length
      val r = new scala.util.Random
      val sb = new StringBuilder
      for (i <- 1 to length) {
        sb.append(possibleChars(r.nextInt(nPossibleChar)))
      }
      sb.toString
    }

    // randomly test
    // for loop is to run the functions multiple times
    var javaRunTimeList = List[Double]()
    var fastSplitRunTimeList = List[Double]()
    for (i <- 1 to 100) {
      val randomStrings: List[String] = (1 to 100000).map {
        x =>
          randomString(50)
      }.toList
      val randomSeparatorIndex = scala.util.Random.nextInt(1)
      val separator = "|"(randomSeparatorIndex).toString

      val fastSplitRunTime = time {
        val splittedByFastSpliter = randomStrings.map { s => StringUtility.fastSplit(s, separator).toList }
      }
      fastSplitRunTimeList = fastSplitRunTime :: fastSplitRunTimeList

      val javaRunTime = time {
        val splittedByRegex = randomStrings.map { s => s.split(separator).toList }
      }

      javaRunTimeList = javaRunTime :: javaRunTimeList

    }

    def meanAndStd(list: List[Double]): (Double, Double, Double, Double) = {
      val s = list.sum
      val mean = s / list.size
      val std = math.sqrt(list.map{ x => x * x }.sum / list.size - mean * mean)
      val sorted = list.sorted
      val median = sorted(list.length / 2)
      (mean, std, median, s)
    }

  }
  */
}

class StringUtilityPropertyTest extends PropSpec with Checkers {
  val randomStringGen = for {
    s <- Gen.pick(5, List.fill(100)(List("k", "l", "m", "x", "//.")).flatten)

  } yield s

  // test for one separator and two
  val randomSeparator = for {
    s <- Gen.oneOf("@@", "@", "x", "//.")
  } yield s

  property("blah") {
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
