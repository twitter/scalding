/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/* split from the original CoreTest.scala */
package com.twitter.scalding

import org.scalatest.{ WordSpec, Matchers }

import cascading.tuple.Fields
import cascading.tuple.TupleEntry
import java.util.concurrent.TimeUnit
import com.twitter.scalding.source.DailySuffixTsv

import java.lang.{ Integer => JInt }


class NumberJoinerJob(args: Args) extends Job(args) {
  val in0 = TypedTsv[(Int, Int)]("input0").read.rename((0, 1) -> ('x0, 'y0))
  val in1 = Tsv("input1").read.mapTo((0, 1) -> ('x1, 'y1)) { input: (Long, Long) => input }
  in0.joinWithSmaller('x0 -> 'x1, in1)
    .write(Tsv("output"))
}

class NumberJoinTest extends WordSpec with Matchers {
  import Dsl._
  "A NumberJoinerJob" should {
    //Set up the job:
    "not throw when joining longs with ints" in {
      JobTest(new NumberJoinerJob(_))
        .source(TypedTsv[(Int, Int)]("input0"), List((0, 1), (1, 2), (2, 4)))
        .source(Tsv("input1"), List(("0", "1"), ("1", "3"), ("2", "9")))
        .sink[(Int, Int, Long, Long)](Tsv("output")) { outBuf =>
        val unordered = outBuf.toSet
        unordered should have size 3
        unordered should contain (0, 1, 0L, 1L)
        unordered should contain (1, 2, 1L, 3L)
        unordered should contain (2, 4, 2L, 9L)
      }
        .run
        .runHadoop
        .finish()
    }
  }
}

class SpillingJob(args: Args) extends Job(args) {
  TypedTsv[(Int, Int)]("input").read.rename((0, 1) -> ('n, 'v))
    .groupBy('n) { group =>
      group.spillThreshold(3).sum[Int]('v).size
    }.write(Tsv("output"))
}

class SpillingTest extends WordSpec with Matchers {
  import Dsl._
  "A SpillingJob" should {
    val src = (0 to 9).map(_ -> 1) ++ List(0 -> 4)
    val result = src.groupBy(_._1)
      .mapValues { v => (v.map(_._2).sum, v.size) }
      .map { case (a, (b, c)) => (a, b, c) }
      .toSet

    //Set up the job:
    "work when number of keys exceeds spill threshold" in {
      JobTest(new SpillingJob(_))
        .source(TypedTsv[(Int, Int)]("input"), src)
        .sink[(Int, Int, Int)](Tsv("output")) { outBuf =>
        outBuf.toSet shouldBe result
      }.run
        .runHadoop
        .finish()
    }
  }
}

class TinyJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .read
    .mapTo((0, 1) -> ('k1, 'v1)) { v: (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .read
    .mapTo((0, 1) -> ('k2, 'v2)) { v: (String, Int) => v }
  p1.joinWithTiny('k1 -> 'k2, p2)
    .project('k1, 'v1, 'v2)
    .write(Tsv(args("output")))
}

class TinyJoinTest extends WordSpec with Matchers {
  "A TinyJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map("b" -> (2, -1), "c" -> (3, 5))
    var idx = 0
    JobTest(new TinyJoinJob(_))
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String, Int, Int)](Tsv("fakeOutput")) { outBuf =>
      val actualOutput = outBuf.map {
        case (k: String, v1: Int, v2: Int) =>
          (k, (v1, v2))
      }.toMap
      (idx + ": join tuples with the same key") in {
        actualOutput shouldBe correctOutput
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class TinyThenSmallJoin(args: Args) extends Job(args) {
  val pipe0 = Tsv("in0", ('x0, 'y0)).read
  val pipe1 = Tsv("in1", ('x1, 'y1)).read
  val pipe2 = Tsv("in2", ('x2, 'y2)).read

  pipe0.joinWithTiny('x0 -> 'x1, pipe1)
    .joinWithSmaller('x0 -> 'x2, pipe2)
    .map(('y0, 'y1, 'y2) -> ('y0, 'y1, 'y2)) { v: (TC, TC, TC) =>
      (v._1.n, v._2.n, v._3.n)
    }
    .project('x0, 'y0, 'x1, 'y1, 'x2, 'y2)
    .write(Tsv("out"))
}

case class TC(val n: Int)

class TinyThenSmallJoinTest extends WordSpec with Matchers with FieldConversions {
  "A TinyThenSmallJoin" should {
    val input0 = List((1, TC(2)), (2, TC(3)), (3, TC(4)))
    val input1 = List((1, TC(20)), (2, TC(30)), (3, TC(40)))
    val input2 = List((1, TC(200)), (2, TC(300)), (3, TC(400)))
    val correct = List((1, 2, 1, 20, 1, 200),
      (2, 3, 2, 30, 2, 300), (3, 4, 3, 40, 3, 400))
    var idx = 0
    JobTest(new TinyThenSmallJoin(_))
      .source(Tsv("in0", ('x0, 'y0)), input0)
      .source(Tsv("in1", ('x1, 'y1)), input1)
      .source(Tsv("in2", ('x2, 'y2)), input2)
      .sink[(Int, Int, Int, Int, Int, Int)](Tsv("out")) { outBuf =>
      (idx + ": join tuples with the same key") in {
        outBuf.toList.sorted shouldBe correct
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class LeftJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .mapTo((0, 1) -> ('k1, 'v1)) { v: (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .mapTo((0, 1) -> ('k2, 'v2)) { v: (String, Int) => v }
  p1.leftJoinWithSmaller('k1 -> 'k2, p2)
    .project('k1, 'v1, 'v2)
    // Null sent to TSV will not be read in properly
    .map('v2 -> 'v2) { v: AnyRef => Option(v).map { _.toString }.getOrElse("NULL") }
    .write(Tsv(args("output")))
}

class LeftJoinTest extends WordSpec with Matchers {
  "A LeftJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map[String, (Int, AnyRef)]("a" -> (1, "NULL"), "b" -> (2, "-1"),
      "c" -> (3, "5"))
    var idx = 0
    JobTest(new LeftJoinJob(_))
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String, Int, JInt)](Tsv("fakeOutput")) { outBuf =>
      val actualOutput = outBuf.map { input: (String, Int, AnyRef) =>
        println(input)
        val (k, v1, v2) = input
        (k, (v1, v2))
      }.toMap
      (idx + ": join tuples with the same key") in {
        correctOutput shouldBe actualOutput
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class LeftJoinWithLargerJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .mapTo((0, 1) -> ('k1, 'v1)) { v: (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .mapTo((0, 1) -> ('k2, 'v2)) { v: (String, Int) => v }
  // Note i am specifying the joiner explicitly since this did not work properly before (leftJoinWithLarger always worked)
  p1.joinWithLarger('k1 -> 'k2, p2, new cascading.pipe.joiner.LeftJoin)
    .project('k1, 'v1, 'v2)
    // Null sent to TSV will not be read in properly
    .map('v2 -> 'v2) { v: AnyRef => Option(v).map { _.toString }.getOrElse("NULL") }
    .write(Tsv(args("output")))
}

class LeftJoinWithLargerTest extends WordSpec with Matchers {
  "A LeftJoinWithLargerJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map[String, (Int, AnyRef)]("a" -> (1, "NULL"), "b" -> (2, "-1"),
      "c" -> (3, "5"))
    var idx = 0
    JobTest(new LeftJoinWithLargerJob(_))
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String, Int, JInt)](Tsv("fakeOutput")) { outBuf =>
      val actualOutput = outBuf.map { input: (String, Int, AnyRef) =>
        println(input)
        val (k, v1, v2) = input
        (k, (v1, v2))
      }.toMap
      s"$idx: join tuples with the same key" in {
        correctOutput shouldBe actualOutput
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class ForceReducersJob(args: Args) extends Job(args) {
  TextLine("in").read
    .rename((0, 1) -> ('num, 'line))
    .flatMap('line -> 'words){ l: String => l.split(" ") }
    .groupBy('num){ _.toList[String]('words -> 'wordList).forceToReducers }
    .map('wordList -> 'wordList){ w: List[String] => w.mkString(" ") }
    .project('num, 'wordList)
    .write(Tsv("out"))
}

class ForceReducersTest extends WordSpec with Matchers {
  "A ForceReducersJob" should {
    var idx = 0
    JobTest(new ForceReducersJob(_))
      .source(TextLine("in"), List("0" -> "single test", "1" -> "single result"))
      .sink[(Int, String)](Tsv("out")) { outBuf =>
      (idx + ": must get the result right") in {
        //need to convert to sets because order
        outBuf(0)._2.split(" ").toSet shouldBe Set("single", "test")
        outBuf(1)._2.split(" ").toSet shouldBe Set("single", "result")
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}


class CrossJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0, 1) -> ('x, 'y)) { tup: (Int, Int) => tup }
  val p2 = Tsv(args("in2")).read
    .mapTo(0 -> 'z) { (z: Int) => z }
  p1.crossWithTiny(p2).write(Tsv(args("out")))
}

class CrossTest extends WordSpec with Matchers {
  "A CrossJob" should {
    var idx = 0
    JobTest(new com.twitter.scalding.CrossJob(_))
      .arg("in1", "fakeIn1")
      .arg("in2", "fakeIn2")
      .arg("out", "fakeOut")
      .source(Tsv("fakeIn1"), List(("0", "1"), ("1", "2"), ("2", "3")))
      .source(Tsv("fakeIn2"), List("4", "5").map { Tuple1(_) })
      .sink[(Int, Int, Int)](Tsv("fakeOut")) { outBuf =>
      (idx + ": must look exactly right") in {
        outBuf should have size 6
        outBuf.toSet shouldBe (Set((0, 1, 4), (0, 1, 5), (1, 2, 4), (1, 2, 5), (2, 3, 4), (2, 3, 5)))
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class GroupAllCrossJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0, 1) -> ('x, 'y)) { tup: (Int, Int) => tup }
    .groupAll { _.max('x) }
    .map('x -> 'x) { x: Int => List(x) }

  val p2 = Tsv(args("in2")).read
    .mapTo(0 -> 'z) { (z: Int) => z }
  p2.crossWithTiny(p1)
    .map('x -> 'x) { l: List[Int] => l.size }
    .project('x, 'z)
    .write(Tsv(args("out")))
}

class GroupAllCrossTest extends WordSpec with Matchers {
  "A GroupAllCrossJob" should {
    var idx = 0
    JobTest(new GroupAllCrossJob(_))
      .arg("in1", "fakeIn1")
      .arg("in2", "fakeIn2")
      .arg("out", "fakeOut")
      .source(Tsv("fakeIn1"), List(("0", "1"), ("1", "2"), ("2", "3")))
      .source(Tsv("fakeIn2"), List("4", "5").map { Tuple1(_) })
      .sink[(Int, Int)](Tsv("fakeOut")) { outBuf =>
      (idx + ": must look exactly right") in {
        outBuf should have size 2
        outBuf.toSet shouldBe Set((1, 4), (1, 5))
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class SmallCrossJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0, 1) -> ('x, 'y)) { tup: (Int, Int) => tup }
  val p2 = Tsv(args("in2")).read
    .mapTo(0 -> 'z) { (z: Int) => z }
  p1.crossWithSmaller(p2).write(Tsv(args("out")))
}

class SmallCrossTest extends WordSpec with Matchers {
  "A SmallCrossJob" should {
    var idx = 0
    JobTest(new SmallCrossJob(_))
      .arg("in1", "fakeIn1")
      .arg("in2", "fakeIn2")
      .arg("out", "fakeOut")
      .source(Tsv("fakeIn1"), List(("0", "1"), ("1", "2"), ("2", "3")))
      .source(Tsv("fakeIn2"), List("4", "5").map { Tuple1(_) })
      .sink[(Int, Int, Int)](Tsv("fakeOut")) { outBuf =>
      (idx + ": must look exactly right") in {
        outBuf should have size 6
        outBuf.toSet shouldBe Set((0, 1, 4), (0, 1, 5), (1, 2, 4), (1, 2, 5), (2, 3, 4), (2, 3, 5))
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}


class ScanJob(args: Args) extends Job(args) {
  Tsv("in", ('x, 'y, 'z))
    .groupBy('x) {
      _.sortBy('y)
        .scanLeft('y -> 'ys)(0) { (oldV: Int, newV: Int) => oldV + newV }
    }
    .project('x, 'ys, 'z)
    .map('z -> 'z) { z: Int => z } //Make sure the null z is converted to an int
    .write(Tsv("out"))
}

class ScanTest extends WordSpec with Matchers {
  import Dsl._

  "A ScanJob" should {
    var idx = 0
    JobTest(new ScanJob(_))
      .source(Tsv("in", ('x, 'y, 'z)), List((3, 0, 1), (3, 1, 10), (3, 5, 100)))
      .sink[(Int, Int, Int)](Tsv("out")) { outBuf =>
      val correct = List((3, 0, 0), (3, 0, 1), (3, 1, 10), (3, 6, 100))
      (idx + ": have a working scanLeft") in {
        outBuf.toList shouldBe correct
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

class IterableSourceJob(args: Args) extends Job(args) {
  val list = List((1, 2, 3), (4, 5, 6), (3, 8, 9))
  val iter = IterableSource(list, ('x, 'y, 'z))
  Tsv("in", ('x, 'w))
    .joinWithSmaller('x -> 'x, iter)
    .write(Tsv("out"))

  Tsv("in", ('x, 'w))
    .joinWithTiny('x -> 'x, iter)
    .write(Tsv("tiny"))
  //Now without fields and using the implicit:
  Tsv("in", ('x, 'w))
    .joinWithTiny('x -> 0, list).write(Tsv("imp"))
}

class IterableSourceTest extends WordSpec with Matchers with FieldConversions {
  val input = List((1, 10), (2, 20), (3, 30))
  "A IterableSourceJob" should {
    var idx = 0
    JobTest(new IterableSourceJob(_))
      .source(Tsv("in", ('x, 'w)), input)
      .sink[(Int, Int, Int, Int)](Tsv("out")) { outBuf =>
      s"$idx: Correctly joinWithSmaller" in {
        outBuf.toList.sorted shouldBe List((1, 10, 2, 3), (3, 30, 8, 9))
      }
      idx += 1
    }
      .sink[(Int, Int, Int, Int)](Tsv("tiny")) { outBuf =>
      s"$idx: correctly joinWithTiny" in {
        outBuf.toList.sorted shouldBe List((1, 10, 2, 3), (3, 30, 8, 9))
      }
      idx += 1
    }
      .sink[(Int, Int, Int, Int, Int)](Tsv("imp")) { outBuf =>
      s"$idx: correctly implicitly joinWithTiny" in {
        outBuf.toList.sorted shouldBe List((1, 10, 1, 2, 3), (3, 30, 3, 8, 9))
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}

// TODO make a Product serializer that clean $outer parameters
case class V(v: Int)
class InnerCaseJob(args: Args) extends Job(args) {
  val res = TypedTsv[Int]("input")
    .mapTo(('xx, 'vx)) { x => (x * x, V(x)) }
    .groupBy('xx) { _.head('vx) }
    .map('vx -> 'x) { v: V => v.v }
    .project('x, 'xx)
    .write(Tsv("output"))
}

class InnerCaseTest extends WordSpec with Matchers {
  import Dsl._

  val input = List(Tuple1(1), Tuple1(2), Tuple1(2), Tuple1(4))
  "An InnerCaseJob" should {
    JobTest(new InnerCaseJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](Tsv("output")) { outBuf =>
      "Correctly handle inner case classes" in {
        outBuf.toSet shouldBe Set((1, 1), (2, 4), (4, 16))
      }
    }
      .runHadoop
      .finish()
  }
}


class ForceToDiskJob(args: Args) extends Job(args) {
  val x = Tsv("in", ('x, 'y))
    .read
    .filter('x) { x: Int => x > 0 }
    .rename('x -> 'x1)
  Tsv("in", ('x, 'y))
    .read
    .joinWithTiny('y -> 'y, x.forceToDisk)
    .project('x, 'x1, 'y)
    .write(Tsv("out"))
}

class ForceToDiskTest extends WordSpec with Matchers {
  import Dsl._

  "A ForceToDiskJob" should {
    var idx = 0
    val input = (1 to 1000).flatMap { i => List((-1, i), (1, i)) }.toList
    JobTest(new ForceToDiskJob(_))
      .source(Tsv("in", ('x, 'y)), input)
      .sink[(Int, Int, Int)](Tsv("out")) { outBuf =>
      (idx + ": run correctly when combined with joinWithTiny") in {
        outBuf should have size 2000
        val correct = (1 to 1000).flatMap { y => List((1, 1, y), (-1, 1, y)) }.sorted
        outBuf.toList.sorted shouldBe correct
      }
      idx += 1
    }
      .run
      .runHadoop
      .finish()
  }
}


class GroupAllToListTestJob(args: Args) extends Job(args) {
  TypedTsv[(Long, String, Double)]("input")
    .mapTo('a, 'b) { case (id, k, v) => (id, Map(k -> v)) }
    .groupBy('a) { _.sum[Map[String, Double]]('b) }
    .groupAll {
      _.toList[(Long, Map[String, Double])](('a, 'b) -> 'abList)
    }
    .map('abList -> 'abMap) {
      list: List[(Long, Map[String, Double])] => list.toMap
    }
    .project('abMap)
    .map('abMap -> 'abMap) { x: AnyRef => x.toString }
    .write(Tsv("output"))
}

class GroupAllToListTest extends WordSpec with Matchers {
  import Dsl._

  "A GroupAllToListTestJob" should {
    val input = List((1L, "a", 1.0), (1L, "b", 2.0), (2L, "a", 1.0), (2L, "b", 2.0))
    val output = Map(2L -> Map("a" -> 1.0, "b" -> 2.0), 1L -> Map("a" -> 1.0, "b" -> 2.0))
    JobTest(new GroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String, Double)]("input"), input)
      .sink[String](Tsv("output")) { outBuf =>
      "must properly aggregate stuff into a single map" in {
        outBuf should have size 1
        outBuf(0) shouldBe output.toString
      }
    }
      .runHadoop
      .finish()
  }
}


class ToListGroupAllToListTestJob(args: Args) extends Job(args) {
  TypedTsv[(Long, String)]("input")
    .mapTo('b, 'c) { case (k, v) => (k, v) }
    .groupBy('c) { _.toList[Long]('b -> 'bList) }
    .groupAll {
      _.toList[(String, List[Long])](('c, 'bList) -> 'cbList)
    }
    .project('cbList)
    .write(Tsv("output"))
}

class ToListGroupAllToListSpec extends WordSpec with Matchers {
  import Dsl._

  val expected = List(("us", List(1)), ("jp", List(3, 2)), ("gb", List(3, 1)))

  "A ToListGroupAllToListTestJob" should {
    JobTest(new ToListGroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String)]("input"), List((1L, "us"), (1L, "gb"), (2L, "jp"), (3L, "jp"), (3L, "gb")))
      .sink[String](Tsv("output")) { outBuf =>
      "must properly aggregate stuff in hadoop mode" in {
        outBuf should have size 1
        outBuf.head shouldBe (expected.toString)
        println(outBuf.head)
      }
    }
      .runHadoop
      .finish()

    JobTest(new ToListGroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String)]("input"), List((1L, "us"), (1L, "gb"), (2L, "jp"), (3L, "jp"), (3L, "gb")))
      .sink[List[(String, List[Long])]](Tsv("output")) { outBuf =>
      "must properly aggregate stuff in local model" in {
        outBuf should have size 1
        outBuf.head shouldBe expected
        println(outBuf.head)
      }
    }
      .run
      .finish()
  }
}

// TODO: HangingTest is very flaky now because we enabled multi-thread testing. Need to be fixed later.
/*
class HangingJob(args : Args) extends Job(args) {
  val x = Tsv("in", ('x,'y))
    .read
    .filter('x, 'y) { t: (Int, Int) =>
      val (x, y) = t
      timeout(Millisecs(2)) {
        if (y % 2 == 1) Thread.sleep(1000)
        x > 0
      } getOrElse false
    }
    .write(Tsv("out"))
}

class HangingTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  "A HangingJob" should {
    val input = (1 to 100).flatMap { i => List((-1, i), (1, i)) }.toList
    JobTest(new HangingJob(_))
      .source(Tsv("in",('x,'y)), input)
      .sink[(Int,Int)](Tsv("out")) { outBuf =>
        "run correctly when task times out" in {
          //outBuf.size must_== 100
          //val correct = (1 to 100).map { i => (1, i) }
          outBuf.size must_== 50
          val correct = (1 to 50).map { i => (1, i*2) }
          outBuf.toList.sorted must_== correct
        }
      }
      .run
      .runHadoop
      .finish()
  }
}
*/

