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
package com.twitter.scalding

import org.scalatest.{FunSuite, Matchers, WordSpec}
import com.twitter.scalding.source.{FixedTypedText, TypedText}
import scala.collection.mutable
// Use the scalacheck generators
import org.scalacheck.Gen
import scala.collection.mutable.Buffer

import TDsl._

import typed.MultiJoin

object TUtil {
  def printStack(fn: => Unit): Unit = {
    try { fn } catch { case e: Throwable => e.printStackTrace; throw e }
  }
}

class TupleAdderJob(args: Args) extends Job(args) {

  TypedText.tsv[(String, String)]("input")
    .map{ f =>
      (1 +: f) ++ (2, 3)
    }
    .write(TypedText.tsv[(Int, String, String, Int, Int)]("output"))
}

class TupleAdderTest extends WordSpec with Matchers {
  "A TupleAdderJob" should {
    JobTest(new TupleAdderJob(_))
      .source(TypedText.tsv[(String, String)]("input"), List(("a", "a"), ("b", "b")))
      .sink[(Int, String, String, Int, Int)](TypedText.tsv[(Int, String, String, Int, Int)]("output")) { outBuf =>
        "be able to use generated tuple adders" in {
          outBuf should have size 2
          outBuf.toSet shouldBe Set((1, "a", "a", 2, 3), (1, "b", "b", 2, 3))
        }
      }
      .run
      .finish()
  }
}

class TypedPipeJob(args: Args) extends Job(args) {
  //Word count using TypedPipe
  TextLine("inputFile")
    .flatMap { _.split("\\s+") }
    .map { w => (w, 1L) }
    .forceToDisk
    .group
    //.forceToReducers
    .sum
    .debug
    .write(TypedText.tsv[(String, Long)]("outputFile"))
}

class TypedPipeTest extends WordSpec with Matchers {
  "A TypedPipe" should {
    var idx = 0
    TUtil.printStack {
      JobTest(new TypedPipeJob(_))
        .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
        .sink[(String, Long)](TypedText.tsv[(String, Long)]("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.toMap
          (idx + ": count words correctly") in {
            outMap("hack") shouldBe 4
            outMap("and") shouldBe 1
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TypedSumByKeyJob(args: Args) extends Job(args) {
  //Word count using TypedPipe
  TextLine("inputFile")
    .flatMap { l => l.split("\\s+").map((_, 1L)) }
    .sumByKey
    .write(TypedText.tsv[(String, Long)]("outputFile"))
}

class TypedSumByKeyTest extends WordSpec with Matchers {
  "A TypedSumByKeyPipe" should {
    var idx = 0
    TUtil.printStack {
      JobTest(new TypedSumByKeyJob(_))
        .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
        .sink[(String, Long)](TypedText.tsv[(String, Long)]("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.toMap
          (idx + ": count words correctly") in {
            outMap("hack") shouldBe 4
            outMap("and") shouldBe 1
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TypedPipeSortByJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[(Int, Float, String)]("input"))
    .groupBy(_._1)
    .sortBy(_._2)
    .mapValues(_._3)
    .sum
    .write(TypedText.tsv[(Int, String)]("output"))
}

class TypedPipeSortByTest extends FunSuite {
  test("groups should not be disturbed by sortBy") {
    JobTest(new TypedPipeSortByJob(_))
      .source(TypedText.tsv[(Int, Float, String)]("input"),
        List((0, 0.6f, "6"),
          (0, 0.5f, "5"),
          (0, 0.1f, "1"),
          (1, 0.1f, "10"),
          (1, 0.5f, "50"),
          (1, 0.51f, "510")))
      .sink[(Int, String)](TypedText.tsv[(Int, String)]("output")){ outputBuffer =>
        val map = outputBuffer.toList.groupBy(_._1)
        assert(map.size == 2, "should be two keys")
        assert(map.forall { case (_, vs) => vs.size == 1 }, "only one key per value")
        assert(map.get(0) == Some(List((0, "156"))), "key(0) is correct")
        assert(map.get(1) == Some(List((1, "1050510"))), "key(1) is correct")
      }
      .run
      .runHadoop
      .finish()
  }
}

class TypedPipeJoinJob(args: Args) extends Job(args) {
  (Tsv("inputFile0").read.toTypedPipe[(Int, Int)](0, 1).group
    leftJoin TypedPipe.from[(Int, Int)](Tsv("inputFile1").read, (0, 1)).group)
    .toTypedPipe
    .write(TypedText.tsv[(Int, (Int, Option[Int]))]("outputFile"))
}

class TypedPipeJoinTest extends WordSpec with Matchers {
  "A TypedPipeJoin" should {
    JobTest(new com.twitter.scalding.TypedPipeJoinJob(_))
      .source(Tsv("inputFile0"), List((0, 0), (1, 1), (2, 2), (3, 3), (4, 5)))
      .source(Tsv("inputFile1"), List((0, 1), (1, 2), (2, 3), (3, 4)))
      .typedSink[(Int, (Int, Option[Int]))](TypedText.tsv[(Int, (Int, Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap should have size 5
          outMap(0) shouldBe (0, Some(1))
          outMap(1) shouldBe (1, Some(2))
          outMap(2) shouldBe (2, Some(3))
          outMap(3) shouldBe (3, Some(4))
          outMap(4) shouldBe (5, None)
        }
      }(implicitly[TypeDescriptor[(Int, (Int, Option[Int]))]].converter)
      .run
      .finish()
  }
}

// This is a non-serializable class
class OpaqueJoinBox(i: Int) { def get = i }

class TypedPipeJoinKryoJob(args: Args) extends Job(args) {
  val box = new OpaqueJoinBox(2)
  TypedPipe.from(TypedText.tsv[(Int, Int)]("inputFile0"))
    .join(TypedPipe.from(TypedText.tsv[(Int, Int)]("inputFile1")))
    .mapValues { case (x, y) => x * y * box.get }
    .write(TypedText.tsv[(Int, Int)]("outputFile"))
}

class TypedPipeJoinKryoTest extends WordSpec with Matchers {
  "OpaqueJoinBox" should {
    "not be serializable" in {
      serialization.Externalizer(new OpaqueJoinBox(1)).javaWorks shouldBe false
    }
    "closure not be serializable" in {
      val box = new OpaqueJoinBox(2)

      val fn = { v: Int => v * box.get }

      serialization.Externalizer(fn).javaWorks shouldBe false
    }
  }
  "A TypedPipeJoinKryo" should {
    JobTest(new com.twitter.scalding.TypedPipeJoinKryoJob(_))
      .source(TypedText.tsv[(Int, Int)]("inputFile0"), List((0, 0), (1, 1), (2, 2), (3, 3), (4, 5)))
      .source(TypedText.tsv[(Int, Int)]("inputFile1"), List((0, 1), (1, 2), (2, 3), (3, 4)))
      .typedSink[(Int, Int)](TypedText.tsv[(Int, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap should have size 4
          outMap(0) shouldBe 0
          outMap(1) shouldBe 4
          outMap(2) shouldBe 12
          outMap(3) shouldBe 24
        }
      }(implicitly[TypeDescriptor[(Int, Int)]].converter)
      .runHadoop // need hadoop to test serialization
      .finish()
  }
}

class TypedPipeDistinctJob(args: Args) extends Job(args) {
  Tsv("inputFile").read.toTypedPipe[(Int, Int)](0, 1)
    .distinct
    .write(TypedText.tsv[(Int, Int)]("outputFile"))
}

class TypedPipeDistinctTest extends WordSpec with Matchers {
  "A TypedPipeDistinctJob" should {
    JobTest(new TypedPipeDistinctJob(_))
      .source(Tsv("inputFile"), List((0, 0), (1, 1), (2, 2), (2, 2), (2, 5)))
      .sink[(Int, Int)](TypedText.tsv[(Int, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly count unique item sizes" in {
          outputBuffer.toSet should have size 4
        }
      }
      .run
      .finish()
  }
}

class TypedPipeDistinctWordsJob(args: Args) extends Job(args) {
  TextLine("inputFile")
    .flatMap(_.split("\\s+"))
    .distinct
    .write(TextLine("outputFile"))
}

class TypedPipeDistinctWordsTest extends WordSpec with Matchers {
  "A TypedPipeDistinctWordsJob" should {
    var idx = 0
    JobTest(new TypedPipeDistinctWordsJob(_))
      .source(TextLine("inputFile"), List(1 -> "a b b c", 2 -> "c d e"))
      .sink[String](TextLine("outputFile")){ outputBuffer =>
        s"$idx: correctly count unique item sizes" in {
          outputBuffer.toSet should have size 5
        }
        idx += 1
      }
      .run
      .runHadoop
      .finish()
  }
}

class TypedPipeDistinctByJob(args: Args) extends Job(args) {
  Tsv("inputFile").read.toTypedPipe[(Int, Int)](0, 1)
    .distinctBy(_._2)
    .write(TypedText.tsv[(Int, Int)]("outputFile"))
}

class TypedPipeDistinctByTest extends WordSpec with Matchers {
  "A TypedPipeDistinctByJob" should {
    JobTest(new TypedPipeDistinctByJob(_))
      .source(Tsv("inputFile"), List((0, 1), (1, 1), (2, 2), (2, 2), (2, 5)))
      .typedSink(TypedText.tsv[(Int, Int)]("outputFile")){ outputBuffer =>
        "correctly count unique item sizes" in {
          val outSet = outputBuffer.toSet
          outSet should have size 3
          List(outSet) should contain oneOf (Set((0, 1), (2, 2), (2, 5)), Set((1, 1), (2, 2), (2, 5)))
        }
      }
      .run
      .finish()
  }
}

class TypedPipeGroupedDistinctJob(args: Args) extends Job(args) {
  val groupedTP = Tsv("inputFile").read.toTypedPipe[(Int, Int)](0, 1)
    .group

  groupedTP
    .distinctValues
    .write(TypedText.tsv[(Int, Int)]("outputFile1"))
  groupedTP
    .distinctSize
    .write(TypedText.tsv[(Int, Long)]("outputFile2"))
}

class TypedPipeGroupedDistinctJobTest extends WordSpec with Matchers {
  "A TypedPipeGroupedDistinctJob" should {
    JobTest(new TypedPipeGroupedDistinctJob(_))
      .source(Tsv("inputFile"), List((0, 0), (0, 1), (0, 1), (1, 0), (1, 1)))
      .sink[(Int, Int)](TypedText.tsv[(Int, Int)]("outputFile1")){ outputBuffer =>
        val outSet = outputBuffer.toSet
        "correctly generate unique items" in {
          outSet should have size 4
        }
      }
      .sink[(Int, Int)](TypedText.tsv[(Int, Long)]("outputFile2")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly count unique item sizes" in {
          outMap(0) shouldBe 2
          outMap(1) shouldBe 2
        }
      }
      .run
      .finish()
  }
}

class TypedPipeHashJoinJob(args: Args) extends Job(args) {
  TypedText.tsv[(Int, Int)]("inputFile0")
    .group
    .hashLeftJoin(TypedText.tsv[(Int, Int)]("inputFile1").group)
    .write(TypedText.tsv[(Int, (Int, Option[Int]))]("outputFile"))
}

class TypedPipeHashJoinTest extends WordSpec with Matchers {
  "A TypedPipeHashJoinJob" should {
    JobTest(new TypedPipeHashJoinJob(_))
      .source(TypedText.tsv[(Int, Int)]("inputFile0"), List((0, 0), (1, 1), (2, 2), (3, 3), (4, 5)))
      .source(TypedText.tsv[(Int, Int)]("inputFile1"), List((0, 1), (1, 2), (2, 3), (3, 4)))
      .typedSink(TypedText.tsv[(Int, (Int, Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap should have size 5
          outMap(0) shouldBe (0, Some(1))
          outMap(1) shouldBe (1, Some(2))
          outMap(2) shouldBe (2, Some(3))
          outMap(3) shouldBe (3, Some(4))
          outMap(4) shouldBe (5, None)
        }
      }(implicitly[TypeDescriptor[(Int, (Int, Option[Int]))]].converter)
      .run
      .finish()
  }
}

class TypedImplicitJob(args: Args) extends Job(args) {
  def revTup[K, V](in: (K, V)): (V, K) = (in._2, in._1)
  TextLine("inputFile").read.typed(1 -> ('maxWord, 'maxCnt)) { tpipe: TypedPipe[String] =>
    tpipe.flatMap { _.split("\\s+") }
      .map { w => (w, 1L) }
      .group
      .sum
      .groupAll
      // Looks like swap, but on the values in the grouping:
      .mapValues { revTup _ }
      .forceToReducers
      .max
      // Throw out the Unit key and reverse the value tuple
      .values
      .swap
  }.write(TypedText.tsv[(String, Int)]("outputFile"))
}

class TypedPipeTypedTest extends WordSpec with Matchers {
  "A TypedImplicitJob" should {
    JobTest(new TypedImplicitJob(_))
      .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
      .typedSink(TypedText.tsv[(String, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "find max word" in {
          outMap should have size 1
          outMap("hack") shouldBe 4
        }
      }
      .run
      .finish()
  }
}

class TypedWithOnCompleteJob(args: Args) extends Job(args) {
  val onCompleteMapperStat = Stat("onCompleteMapper")
  val onCompleteReducerStat = Stat("onCompleteReducer")
  def onCompleteMapper() = onCompleteMapperStat.inc()
  def onCompleteReducer() = onCompleteReducerStat.inc()
  // find repeated words ignoring case
  TypedText.tsv[String]("input")
    .map(_.toUpperCase)
    .onComplete(onCompleteMapper)
    .groupBy(identity)
    .mapValueStream(words => Iterator(words.size))
    .filter { case (word, occurrences) => occurrences > 1 }
    .keys
    .onComplete(onCompleteReducer)
    .write(TypedText.tsv[String]("output"))
}

class TypedPipeWithOnCompleteTest extends WordSpec with Matchers {
  import Dsl._
  val inputText = "the quick brown fox jumps over the lazy LAZY dog"
  "A TypedWithOnCompleteJob" should {
    JobTest(new TypedWithOnCompleteJob(_))
      .source(TypedText.tsv[String]("input"), inputText.split("\\s+").map(Tuple1(_)))
      .counter("onCompleteMapper") { cnt => "have onComplete called on mapper" in { assert(cnt == 1) } }
      .counter("onCompleteReducer") { cnt => "have onComplete called on reducer" in { assert(cnt == 1) } }
      .sink[String](TypedText.tsv[String]("output")) { outbuf =>
        "have the correct output" in {
          val correct = inputText.split("\\s+").map(_.toUpperCase).groupBy(x => x).filter(_._2.size > 1).keys.toList.sorted
          val sortedL = outbuf.toList.sorted
          assert(sortedL == correct)
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedPipeWithOuterAndLeftJoin(args: Args) extends Job(args) {
  val userNames = TypedText.tsv[(Int, String)]("inputNames").group
  val userData = TypedText.tsv[(Int, Double)]("inputData").group
  val optionalData = TypedText.tsv[(Int, Boolean)]("inputOptionalData").group

  userNames
    .outerJoin(userData)
    .leftJoin(optionalData)
    .map { case (id, ((nameOpt, userDataOption), optionalDataOpt)) => id }
    .write(TypedText.tsv[Int]("output"))
}

class TypedPipeWithOuterAndLeftJoinTest extends WordSpec with Matchers {

  "A TypedPipeWithOuterAndLeftJoin" should {
    JobTest(new TypedPipeWithOuterAndLeftJoin(_))
      .source(TypedText.tsv[(Int, String)]("inputNames"), List((1, "Jimmy Foursquare")))
      .source(TypedText.tsv[(Int, Double)]("inputData"), List((1, 0.1), (5, 0.5)))
      .source(TypedText.tsv[(Int, Boolean)]("inputOptionalData"), List((1, true), (99, false)))
      .sink[Long](TypedText.tsv[Int]("output")) { outbuf =>
        "have output for user 1" in {
          assert(outbuf.toList.contains(1) == true)
        }
        "have output for user 5" in {
          assert(outbuf.toList.contains(5) == true)
        }
        "not have output for user 99" in {
          assert(outbuf.toList.contains(99) == false)
        }
      }
      .run
      .finish()
  }
}

class TJoinCountJob(args: Args) extends Job(args) {
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    .size
    .write(TypedText.tsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(TypedText.tsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    leftJoin TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, (Int, Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(TypedText.tsv[(Int, Int, Int)]("out3"))
}

/**
 * This test exercises the implicit from TypedPipe to HashJoinabl
 */
class TNiceJoinCountJob(args: Args) extends Job(args) {

  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    .size
    .write(TypedText.tsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(TypedText.tsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    leftJoin TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, (Int, Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(TypedText.tsv[(Int, Int, Int)]("out3"))
}

class TNiceJoinByCountJob(args: Args) extends Job(args) {
  import com.twitter.scalding.typed.Syntax._

  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    joinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    .size
    .write(TypedText.tsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    joinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1._2, kvw._2._2._2) }
    .write(TypedText.tsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    leftJoinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, ((Int, Int), Option[(Int, Int)])) =>
      (kvw._1, kvw._2._1._2, kvw._2._2.getOrElse((-1, -1))._2)
    }
    .write(TypedText.tsv[(Int, Int, Int)]("out3"))
}

class TypedPipeJoinCountTest extends WordSpec with Matchers {
  import Dsl._

  val joinTests = List("com.twitter.scalding.TJoinCountJob", "com.twitter.scalding.TNiceJoinCountJob", "com.twitter.scalding.TNiceJoinByCountJob")

  joinTests.foreach{ jobName =>
    "A " + jobName should {
      var idx = 0
      JobTest(jobName)
        .source(Tsv("in0", (0, 1)), List((0, 1), (0, 2), (1, 1), (1, 5), (2, 10)))
        .source(Tsv("in1", (0, 1)), List((0, 10), (1, 20), (1, 10), (1, 30)))
        .typedSink(TypedText.tsv[(Int, Long)]("out")) { outbuf =>
          val outMap = outbuf.toMap
          (idx + ": correctly reduce after cogroup") in {
            outMap should have size 2
            outMap(0) shouldBe 2
            outMap(1) shouldBe 6
          }
          idx += 1
        }
        .typedSink(TypedText.tsv[(Int, Int, Int)]("out2")) { outbuf2 =>
          val outMap = outbuf2.groupBy { _._1 }
          (idx + ": correctly do a simple join") in {
            outMap should have size 2
            outMap(0).toList.sorted shouldBe List((0, 1, 10), (0, 2, 10))
            outMap(1).toList.sorted shouldBe List((1, 1, 10), (1, 1, 20), (1, 1, 30), (1, 5, 10), (1, 5, 20), (1, 5, 30))
          }
          idx += 1
        }
        .typedSink(TypedText.tsv[(Int, Int, Int)]("out3")) { outbuf =>
          val outMap = outbuf.groupBy { _._1 }
          (idx + ": correctly do a simple leftJoin") in {
            outMap should have size 3
            outMap(0).toList.sorted shouldBe List((0, 1, 10), (0, 2, 10))
            outMap(1).toList.sorted shouldBe List((1, 1, 10), (1, 1, 20), (1, 1, 30), (1, 5, 10), (1, 5, 20), (1, 5, 30))
            outMap(2).toList.sorted shouldBe List((2, 10, -1))
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TCrossJob(args: Args) extends Job(args) {
  (TextLine("in0") cross TextLine("in1"))
    .write(TypedText.tsv[(String, String)]("crossed"))
}

class TypedPipeCrossTest extends WordSpec with Matchers {
  "A TCrossJob" should {
    var idx = 0
    TUtil.printStack {
      JobTest(new TCrossJob(_))
        .source(TextLine("in0"), List((0, "you"), (1, "all")))
        .source(TextLine("in1"), List((0, "every"), (1, "body")))
        .typedSink(TypedText.tsv[(String, String)]("crossed")) { outbuf =>
          val sortedL = outbuf.toList.sorted
          (idx + ": create a cross-product") in {
            sortedL shouldBe List(("all", "body"),
              ("all", "every"),
              ("you", "body"),
              ("you", "every"))
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TJoinTakeJob(args: Args) extends Job(args) {
  val items0 = TextLine("in0").flatMap { s => (1 to 10).map((_, s)) }.group
  val items1 = TextLine("in1").map { s => (s.toInt, ()) }.group

  items0.join(items1.take(1))
    .mapValues(_._1) // discard the ()
    .toTypedPipe
    .write(TypedText.tsv[(Int, String)]("joined"))
}

class TypedJoinTakeTest extends WordSpec with Matchers {
  "A TJoinTakeJob" should {
    var idx = 0
    TUtil.printStack {
      JobTest(new TJoinTakeJob(_))
        .source(TextLine("in0"), List((0, "you"), (1, "all")))
        .source(TextLine("in1"), List((0, "3"), (1, "2"), (0, "3")))
        .typedSink(TypedText.tsv[(Int, String)]("joined")) { outbuf =>
          val sortedL = outbuf.toList.sorted
          (idx + ": dedup keys by using take") in {
            sortedL shouldBe (List((3, "you"), (3, "all"), (2, "you"), (2, "all")).sorted)
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TGroupAllJob(args: Args) extends Job(args) {
  TextLine("in")
    .groupAll
    .sorted
    .values
    .write(TypedText.tsv[String]("out"))
}

class TypedGroupAllTest extends WordSpec with Matchers {
  "A TGroupAllJob" should {
    var idx = 0
    TUtil.printStack {
      val input = List((0, "you"), (1, "all"), (2, "everybody"))
      JobTest(new TGroupAllJob(_))
        .source(TextLine("in"), input)
        .typedSink(TypedText.tsv[String]("out")) { outbuf =>
          val sortedL = outbuf.toList
          val correct = input.map { _._2 }.sorted
          (idx + ": create sorted output") in {
            sortedL shouldBe correct
          }
          idx += 1
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class TSelfJoin(args: Args) extends Job(args) {
  val g = TypedText.tsv[(Int, Int)]("in").group
  g.join(g).values.write(TypedText.tsv[(Int, Int)]("out"))
}

class TSelfJoinTest extends WordSpec with Matchers {
  "A TSelfJoin" should {
    JobTest(new TSelfJoin(_))
      .source(TypedText.tsv[(Int, Int)]("in"), List((1, 2), (1, 3), (2, 1)))
      .typedSink(TypedText.tsv[(Int, Int)]("out")) { outbuf =>
        outbuf.toList.sorted shouldBe List((1, 1), (2, 2), (2, 3), (3, 2), (3, 3))
      }
      .run
      .runHadoop
      .finish()
  }
}

class TJoinWordCount(args: Args) extends Job(args) {

  def countWordsIn(pipe: TypedPipe[(String)]) = {
    pipe.flatMap { _.split("\\s+").map(_.toLowerCase) }
      .groupBy(identity)
      .mapValueStream(input => Iterator(input.size))
      .forceToReducers
  }

  val first = countWordsIn(TypedPipe.from(TextLine("in0")))

  val second = countWordsIn(TypedPipe.from(TextLine("in1")))

  first.outerJoin(second)
    .toTypedPipe
    .map {
      case (word, (firstCount, secondCount)) =>
        (word, firstCount.getOrElse(0), secondCount.getOrElse(0))
    }
    .write(TypedText.tsv[(String, Int, Int)]("out"))
}

class TypedJoinWCTest extends WordSpec with Matchers {
  "A TJoinWordCount" should {
    TUtil.printStack {
      val in0 = List((0, "you all everybody"), (1, "a b c d"), (2, "a b c"))
      val in1 = List((0, "you"), (1, "a b c d"), (2, "a a b b c c"))
      def count(in: List[(Int, String)]): Map[String, Int] = {
        in.flatMap { _._2.split("\\s+").map { _.toLowerCase } }.groupBy { identity }.mapValues { _.size }
      }
      def outerjoin[K, U, V](m1: Map[K, U], z1: U, m2: Map[K, V], z2: V): Map[K, (U, V)] = {
        (m1.keys ++ m2.keys).map { k => (k, (m1.getOrElse(k, z1), m2.getOrElse(k, z2))) }.toMap
      }
      val correct = outerjoin(count(in0), 0, count(in1), 0)
        .toList
        .map { tup => (tup._1, tup._2._1, tup._2._2) }
        .sorted

      JobTest(new TJoinWordCount(_))
        .source(TextLine("in0"), in0)
        .source(TextLine("in1"), in1)
        .typedSink(TypedText.tsv[(String, Int, Int)]("out")) { outbuf =>
          val sortedL = outbuf.toList
          "create sorted output" in {
            sortedL shouldBe correct
          }
        }
        .run
        .finish()
    }
  }
}

class TypedLimitJob(args: Args) extends Job(args) {
  val p = TypedText.tsv[String]("input").limit(10): TypedPipe[String]
  p.write(TypedText.tsv[String]("output"))
}

class TypedLimitTest extends WordSpec with Matchers {
  "A TypedLimitJob" should {
    JobTest(new TypedLimitJob(_))
      .source(TypedText.tsv[String]("input"), (0 to 100).map { i => Tuple1(i.toString) })
      .typedSink(TypedText.tsv[String]("output")) { outBuf =>
        "not have more than the limited outputs" in {
          outBuf.size should be <= 10
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedFlattenJob(args: Args) extends Job(args) {
  TypedText.tsv[String]("input").map { _.split(" ").toList }
    .flatten
    .write(TypedText.tsv[String]("output"))
}

class TypedFlattenTest extends WordSpec with Matchers {
  "A TypedLimitJob" should {
    JobTest(new TypedFlattenJob(_))
      .source(TypedText.tsv[String]("input"), List(Tuple1("you all"), Tuple1("every body")))
      .typedSink(TypedText.tsv[String]("output")) { outBuf =>
        "correctly flatten" in {
          outBuf.toSet shouldBe Set("you", "all", "every", "body")
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedMergeJob(args: Args) extends Job(args) {
  val tp = TypedPipe.from(TypedText.tsv[String]("input"))
  // This exercise a self merge
  (tp ++ tp)
    .write(TypedText.tsv[String]("output"))
  (tp ++ (tp.map(_.reverse)))
    .write(TypedText.tsv[String]("output2"))
}

class TypedMergeTest extends WordSpec with Matchers {
  "A TypedMergeJob" should {
    var idx = 0
    JobTest(new TypedMergeJob(_))
      .source(TypedText.tsv[String]("input"), List(Tuple1("you all"), Tuple1("every body")))
      .typedSink(TypedText.tsv[String]("output")) { outBuf =>
        (idx + ": correctly flatten") in {
          outBuf.toSet shouldBe Set("you all", "every body")
        }
        idx += 1
      }
      .typedSink(TypedText.tsv[String]("output2")) { outBuf =>
        (idx + ": correctly flatten") in {
          val correct = Set("you all", "every body")
          outBuf.toSet shouldBe (correct ++ correct.map(_.reverse))
        }
        idx += 1
      }
      .runHadoop
      .finish()
  }
}

class TypedShardJob(args: Args) extends Job(args) {
  (TypedPipe.from(TypedText.tsv[String]("input")) ++
    (TypedPipe.empty.map { _ => "hey" }) ++
    TypedPipe.from(List("item")))
    .shard(10)
    .write(TypedText.tsv[String]("output"))
}

class TypedShardTest extends WordSpec with Matchers {
  "A TypedShardJob" should {
    val genList = Gen.listOf(Gen.identifier)
    // Take one random sample
    lazy val mk: List[String] = genList.sample.getOrElse(mk)
    JobTest(new TypedShardJob(_))
      .source(TypedText.tsv[String]("input"), mk)
      .typedSink(TypedText.tsv[String]("output")) { outBuf =>
        "correctly flatten" in {
          outBuf should have size (mk.size + 1)
          outBuf.toSet shouldBe (mk.toSet + "item")
        }
      }
      .run
      .finish()
  }
}

class TypedLocalSumJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[String]("input"))
    .flatMap { s => s.split(" ").map((_, 1L)) }
    .sumByLocalKeys
    .write(TypedText.tsv[(String, Long)]("output"))
}

class TypedLocalSumTest extends WordSpec with Matchers {
  "A TypedLocalSumJob" should {
    var idx = 0
    val genList = Gen.listOf(Gen.identifier)
    // Take one random sample
    lazy val mk: List[String] = genList.sample.getOrElse(mk)
    JobTest(new TypedLocalSumJob(_))
      .source(TypedText.tsv[String]("input"), mk)
      .typedSink(TypedText.tsv[(String, Long)]("output")) { outBuf =>
        s"$idx: not expand and have correct total sum" in {
          import com.twitter.algebird.MapAlgebra.sumByKey
          val lres = outBuf.toList
          val fmapped = mk.flatMap { s => s.split(" ").map((_, 1L)) }
          lres.size should be <= (fmapped.size)
          sumByKey(lres) shouldBe (sumByKey(fmapped))
        }
        idx += 1
      }
      .run
      .runHadoop
      .finish()
  }
}

class TypedHeadJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[(Int, Int)]("input"))
    .group
    .head
    .write(TypedText.tsv[(Int, Int)]("output"))
}

class TypedHeadTest extends WordSpec with Matchers {
  "A TypedHeadJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedHeadJob(_))
      .source(TypedText.tsv[(Int, Int)]("input"), mk)
      .typedSink(TypedText.tsv[(Int, Int)]("output")) { outBuf =>
        "correctly take the first" in {
          val correct = mk.groupBy(_._1).mapValues(_.head._2)
          outBuf should have size (correct.size)
          outBuf.toMap shouldBe correct
        }
      }
      .run
      .finish()
  }
}

class TypedSortWithTakeJob(args: Args) extends Job(args) {
  val in = TypedPipe.from(TypedText.tsv[(Int, Int)]("input"))

  in
    .group
    .sortedReverseTake(5)
    .flattenValues
    .write(TypedText.tsv[(Int, Int)]("output"))

  in
    .group
    .sorted
    .reverse
    .bufferedTake(5)
    .write(TypedText.tsv[(Int, Int)]("output2"))
}

class TypedSortWithTakeTest extends WordSpec with Matchers {
  "A TypedSortWithTakeJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedSortWithTakeJob(_))
      .source(TypedText.tsv[(Int, Int)]("input"), mk)
      .sink[(Int, Int)](TypedText.tsv[(Int, Int)]("output")) { outBuf =>
        "correctly take the first" in {
          val correct = mk.groupBy(_._1).mapValues(_.map(i => i._2).sorted.reverse.take(5).toSet)
          outBuf.groupBy(_._1).mapValues(_.map { case (k, v) => v }.toSet) shouldBe correct
        }
      }
      .sink[(Int, Int)](TypedText.tsv[(Int, Int)]("output2")) { outBuf =>
        "correctly take the first using sorted.reverse.take" in {
          val correct = mk.groupBy(_._1).mapValues(_.map(i => i._2).sorted.reverse.take(5).toSet)
          outBuf.groupBy(_._1).mapValues(_.map { case (k, v) => v }.toSet) shouldBe correct
        }
      }
      .run
      .finish()
  }
}

class TypedLookupJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[Int]("input0"))
    .hashLookup(TypedPipe.from(TypedText.tsv[(Int, String)]("input1")).group)
    .mapValues { o: Option[String] => o.getOrElse("") }
    .write(TypedText.tsv[(Int, String)]("output"))
}

class TypedLookupJobTest extends WordSpec with Matchers {
  "A TypedLookupJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt.toString) }
    JobTest(new TypedLookupJob(_))
      .source(TypedText.tsv[Int]("input0"), (-1 to 100))
      .source(TypedText.tsv[(Int, String)]("input1"), mk)
      .typedSink(TypedText.tsv[(Int, String)]("output")) { outBuf =>
        "correctly TypedPipe.hashLookup" in {
          val data = mk.groupBy(_._1)
          val correct = (-1 to 100).flatMap { k =>
            data.get(k).getOrElse(List((k, "")))
          }.toList.sorted
          outBuf should have size (correct.size)
          outBuf.toList.sorted shouldBe correct
        }
      }(implicitly[TypeDescriptor[(Int, String)]].converter)
      .run
      .finish()
  }
}

class TypedLookupReduceJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[Int]("input0"))
    .hashLookup(TypedPipe.from(TypedText.tsv[(Int, String)]("input1")).group.max)
    .mapValues { o: Option[String] => o.getOrElse("") }
    .write(TypedText.tsv[(Int, String)]("output"))
}

class TypedLookupReduceJobTest extends WordSpec with Matchers {
  "A TypedLookupJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt.toString) }
    JobTest(new TypedLookupReduceJob(_))
      .source(TypedText.tsv[Int]("input0"), (-1 to 100))
      .source(TypedText.tsv[(Int, String)]("input1"), mk)
      .typedSink(TypedText.tsv[(Int, String)]("output")) { outBuf =>
        "correctly TypedPipe.hashLookup" in {
          val data = mk.groupBy(_._1)
            .mapValues { kvs =>
              val (k, v) = kvs.maxBy(_._2)
              (k, v)
            }
          val correct = (-1 to 100).map { k =>
            data.get(k).getOrElse((k, ""))
          }.toList.sorted
          outBuf should have size (correct.size)
          outBuf.toList.sorted shouldBe correct
        }
      }(implicitly[TypeDescriptor[(Int, String)]].converter)
      .run
      .finish()
  }
}

class TypedFilterJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[Int]("input"))
    .filter { _ > 50 }
    .filterNot { _ % 2 == 0 }
    .write(TypedText.tsv[Int]("output"))
}

class TypedFilterTest extends WordSpec with Matchers {
  "A TypedPipe" should {
    "filter and filterNot elements" in {
      val input = -1 to 100
      val isEven = (i: Int) => i % 2 == 0
      val expectedOutput = input filter { _ > 50 } filterNot isEven

      TUtil.printStack {
        JobTest(new com.twitter.scalding.TypedFilterJob(_))
          .source(TypedText.tsv[Int]("input"), input)
          .typedSink(TypedText.tsv[Int]("output")) { outBuf =>
            outBuf.toList shouldBe expectedOutput
          }
          .run
          .runHadoop
          .finish()
      }
    }
  }
}

class TypedPartitionJob(args: Args) extends Job(args) {
  val (p1, p2) = TypedPipe.from(TypedText.tsv[Int]("input")).partition { _ > 50 }
  p1.write(TypedText.tsv[Int]("output1"))
  p2.write(TypedText.tsv[Int]("output2"))
}

class TypedPartitionTest extends WordSpec with Matchers {
  "A TypedPipe" should {
    "partition elements" in {
      val input = -1 to 100
      val (expected1, expected2) = input partition { _ > 50 }

      TUtil.printStack {
        JobTest(new com.twitter.scalding.TypedPartitionJob(_))
          .source(TypedText.tsv[Int]("input"), input)
          .typedSink(TypedText.tsv[Int]("output1")) { outBuf =>
            outBuf.toList shouldBe expected1
          }
          .typedSink(TypedText.tsv[Int]("output2")) { outBuf =>
            outBuf.toList shouldBe expected2
          }
          .run
          .runHadoop
          .finish()
      }
    }
  }
}

class TypedMultiJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedText.tsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedText.tsv[(Int, Int)]("input1"))
  val two = TypedPipe.from(TypedText.tsv[(Int, Int)]("input2"))

  val cogroup = MultiJoin(zero, one.group.max, two.group.max)

  // make sure this is indeed a case with no self joins
  // distinct by mapped
  val distinct = cogroup.inputs.groupBy(identity).map(_._2.head).toList
  assert(distinct.size == cogroup.inputs.size)

  cogroup
    .map { case (k, (v0, v1, v2)) => (k, v0, v1, v2) }
    .write(TypedText.tsv[(Int, Int, Int, Int)]("output"))
}

class TypedMultiJoinJobTest extends WordSpec with Matchers {
  "A TypedMultiJoinJob" should {
    val rng = new java.util.Random
    val COUNT = 100 * 100
    val KEYS = 10
    def mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    val mk0 = mk
    val mk1 = mk
    val mk2 = mk
    JobTest(new TypedMultiJoinJob(_))
      .source(TypedText.tsv[(Int, Int)]("input0"), mk0)
      .source(TypedText.tsv[(Int, Int)]("input1"), mk1)
      .source(TypedText.tsv[(Int, Int)]("input2"), mk2)
      .typedSink(TypedText.tsv[(Int, Int, Int, Int)]("output")) { outBuf =>
        "correctly do a multi-join" in {
          def groupMax(it: Seq[(Int, Int)]): Map[Int, Int] =
            it.groupBy(_._1).map { case (_, kvs) => kvs.maxBy(_._2) }

          val d0 = mk0.groupBy(_._1).mapValues(_.map { case (_, v) => v })
          val d1 = groupMax(mk1)
          val d2 = groupMax(mk2)

          val correct = (d0.keySet ++ d1.keySet ++ d2.keySet).toList
            .flatMap { k =>
              (for {
                v0s <- d0.get(k)
                v1 <- d1.get(k)
                v2 <- d2.get(k)
              } yield (v0s, (k, v1, v2)))
            }
            .flatMap {
              case (v0s, (k, v1, v2)) =>
                v0s.map { (k, _, v1, v2) }
            }
            .sorted

          outBuf should have size (correct.size)
          outBuf.sorted shouldBe correct
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedMultiSelfJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedText.tsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedText.tsv[(Int, Int)]("input1"))
    // forceToReducers makes sure the first and the second part of
    .group.forceToReducers

  val cogroup = zero.group
    .join(one.max)
    .join(one.min)

  // make sure this is indeed a case with some self joins
  // distinct by mapped
  val distinct = cogroup.inputs.groupBy(identity).map(_._2.head).toList
  assert(distinct.size < cogroup.inputs.size)

  cogroup
    .map { case (k, ((v0, v1), v2)) => (k, v0, v1, v2) }
    .write(TypedText.tsv[(Int, Int, Int, Int)]("output"))
}

class TypedMultiSelfJoinJobTest extends WordSpec with Matchers {
  "A TypedMultiSelfJoinJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    def mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    val mk0 = mk
    val mk1 = mk
    JobTest(new TypedMultiSelfJoinJob(_))
      .source(TypedText.tsv[(Int, Int)]("input0"), mk0)
      .source(TypedText.tsv[(Int, Int)]("input1"), mk1)
      .typedSink(TypedText.tsv[(Int, Int, Int, Int)]("output")) { outBuf =>
        "correctly do a multi-self-join" in {
          def group(it: Seq[(Int, Int)])(red: (Int, Int) => Int): Map[Int, Int] =
            it.groupBy(_._1).map {
              case (k, kvs) =>
                (k, kvs.map(_._2).reduce(red))
            }

          val d0 = mk0.groupBy(_._1).mapValues(_.map { case (_, v) => v })
          val d1 = group(mk1)(_ max _)
          val d2 = group(mk1)(_ min _)

          val correct = (d0.keySet ++ d1.keySet ++ d2.keySet).toList
            .flatMap { k =>
              (for {
                v0s <- d0.get(k)
                v1 <- d1.get(k)
                v2 <- d2.get(k)
              } yield (v0s, (k, v1, v2)))
            }
            .flatMap {
              case (v0s, (k, v1, v2)) =>
                v0s.map { (k, _, v1, v2) }
            }
            .sorted

          outBuf should have size (correct.size)
          outBuf.sorted shouldBe correct
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedMapGroup(args: Args) extends Job(args) {
  TypedPipe.from(TypedText.tsv[(Int, Int)]("input"))
    .group
    .mapGroup { (k, iters) => iters.map(_ * k) }
    .max
    .write(TypedText.tsv[(Int, Int)]("output"))
}

class TypedMapGroupTest extends WordSpec with Matchers {
  "A TypedMapGroup" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedMapGroup(_))
      .source(TypedText.tsv[(Int, Int)]("input"), mk)
      .typedSink(TypedText.tsv[(Int, Int)]("output")) { outBuf =>
        "correctly do a mapGroup" in {
          def mapGroup(it: Seq[(Int, Int)]): Map[Int, Int] =
            it.groupBy(_._1).map {
              case (k, kvs) =>
                (k, kvs.map { case (k, v) => k * v }.max)
            }

          val correct = mapGroup(mk).toList.sorted
          outBuf should have size (correct.size)
          outBuf.sorted shouldBe correct
        }
      }
      .runHadoop
      .finish()
  }
}

class TypedSelfCrossJob(args: Args) extends Job(args) {
  val pipe = TypedPipe.from(TypedText.tsv[Int]("input"))

  pipe
    .cross(pipe.groupAll.sum.values)
    .write(TypedText.tsv[(Int, Int)]("output"))
}

class TypedSelfCrossTest extends WordSpec with Matchers {

  val input = (1 to 100).toList

  "A TypedSelfCrossJob" should {
    var idx = 0
    JobTest(new TypedSelfCrossJob(_))
      .source(TypedText.tsv[Int]("input"), input)
      .typedSink(TypedText.tsv[(Int, Int)]("output")) { outBuf =>
        (idx + ": not change the length of the input") in {
          outBuf should have size (input.size)
        }
        idx += 1
      }
      .run
      .runHadoop
      .finish()
  }
}

class TypedSelfLeftCrossJob(args: Args) extends Job(args) {
  val pipe = TypedPipe.from(TypedText.tsv[Int]("input"))

  pipe
    .leftCross(pipe.sum)
    .write(TypedText.tsv[(Int, Option[Int])]("output"))
}

class TypedSelfLeftCrossTest extends WordSpec with Matchers {

  val input = (1 to 100).toList

  "A TypedSelfLeftCrossJob" should {
    var idx = 0
    JobTest(new TypedSelfLeftCrossJob(_))
      .source(TypedText.tsv[Int]("input"), input)
      .typedSink(TypedText.tsv[(Int, Option[Int])]("output")) { outBuf =>
        s"$idx: attach the sum of all values correctly" in {
          outBuf should have size (input.size)
          val sum = input.reduceOption(_ + _)
          // toString to deal with our hadoop testing jank
          outBuf.toList.sortBy(_._1).toString shouldBe (input.sorted.map((_, sum)).toString)
        }
        idx += 1
      }(implicitly[TypeDescriptor[(Int, Option[Int])]].converter)
      .run
      .runHadoop
      .finish()
  }
}

class JoinMapGroupJob(args: Args) extends Job(args) {
  def r1 = TypedPipe.from(Seq((1, 10)))
  def r2 = TypedPipe.from(Seq((1, 1), (2, 2), (3, 3)))
  r1.groupBy(_._1).join(r2.groupBy(_._1))
    .mapGroup { case (a, b) => Iterator("a") }
    .write(TypedText.tsv("output"))
}

class JoinMapGroupJobTest extends WordSpec with Matchers {

  "A JoinMapGroupJob" should {
    JobTest(new JoinMapGroupJob(_))
      .typedSink(TypedText.tsv[(Int, String)]("output")) { outBuf =>
        "not duplicate keys" in {
          outBuf.toList shouldBe List((1, "a"))
        }
      }
      .run
      .finish()
  }
}

class MapValueStreamNonEmptyIteratorJob(args: Args) extends Job(args) {
  val input = TypedPipe.from[(Int, String)](Seq((1, "a"), (1, "b"), (3, "a")))
  val extraKeys = TypedPipe.from[(Int, String)](Seq((4, "a")))

  input
    .groupBy(_._1)
    .mapValueStream(values => List(values.size).toIterator)
    .leftJoin(extraKeys.group)
    .toTypedPipe
    .map { case (key, (iteratorSize, extraOpt)) => (key, iteratorSize) }
    .write(TypedText.tsv[(Int, Int)]("output"))
}

class MapValueStreamNonEmptyIteratorTest extends WordSpec with Matchers {

  "A MapValueStreamNonEmptyIteratorJob" should {
    JobTest(new MapValueStreamNonEmptyIteratorJob(_))
      .sink[(Int, Int)](TypedText.tsv[(Int, Int)]("output")) { outBuf =>
        "not have iterators of size 0" in {
          assert(outBuf.toList.filter(_._2 == 0) === Nil)
        }
      }
      .run
      .finish()
  }
}

class NullSinkJob(args: Args, m: scala.collection.mutable.Buffer[Int]) extends Job(args) {
  TypedPipe.from(0 to 100)
    .map { i => m += i; i } // side effect
    .write(source.NullSink)
}

class NullSinkJobTest extends WordSpec with Matchers {
  "A NullSinkJob" should {
    val buf = scala.collection.mutable.Buffer[Int]()
    JobTest(new NullSinkJob(_, buf))
      .typedSink[Any](source.NullSink) { _ =>
        "have a side effect" in {
          assert(buf.toSet === (0 to 100).toSet)
        }
      }
      .run
      .finish()
  }
}

class TypedSketchJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedText.tsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedText.tsv[(Int, Int)]("input1"))

  implicit def serialize(k: Int): Array[Byte] = k.toString.getBytes

  zero
    .sketch(args("reducers").toInt)
    .join(one)
    .map{ case (k, (v0, v1)) => (k, v0, v1) }
    .write(TypedText.tsv[(Int, Int, Int)]("output-sketch"))

  zero
    .group
    .join(one.group)
    .map{ case (k, (v0, v1)) => (k, v0, v1) }
    .write(TypedText.tsv[(Int, Int, Int)]("output-join"))
}

class TypedSketchLeftJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedText.tsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedText.tsv[(Int, Int)]("input1"))

  implicit def serialize(k: Int): Array[Byte] = k.toString.getBytes

  zero
    .sketch(args("reducers").toInt)
    .leftJoin(one)
    .map{ case (k, (v0, v1)) => (k, v0, v1.getOrElse(-1)) }
    .write(TypedText.tsv[(Int, Int, Int)]("output-sketch"))

  zero
    .group
    .leftJoin(one.group)
    .map{ case (k, (v0, v1)) => (k, v0, v1.getOrElse(-1)) }
    .write(TypedText.tsv[(Int, Int, Int)]("output-join"))
}

object TypedSketchJoinTestHelper {

  val rng = new java.util.Random
  def generateInput(size: Int, max: Int, dist: (Int) => Int): List[(Int, Int)] = {
    def next: Int = rng.nextInt(max)

    (0 to size).flatMap { i =>
      val k = next
      (1 to dist(k)).map { j => (k, next) }
    }.toList
  }

  def runJobWithArguments(fn: (Args) => Job, reducers: Int, dist: (Int) => Int): (List[(Int, Int, Int)], List[(Int, Int, Int)]) = {

    val sketchResult = Buffer[(Int, Int, Int)]()
    val innerResult = Buffer[(Int, Int, Int)]()
    JobTest(fn)
      .arg("reducers", reducers.toString)
      .source(TypedText.tsv[(Int, Int)]("input0"), generateInput(1000, 100, dist))
      .source(TypedText.tsv[(Int, Int)]("input1"), generateInput(100, 100, x => 1))
      .typedSink(TypedText.tsv[(Int, Int, Int)]("output-sketch")) { outBuf => sketchResult ++= outBuf }
      .typedSink(TypedText.tsv[(Int, Int, Int)]("output-join")) { outBuf => innerResult ++= outBuf }
      .run
      .runHadoop
      .finish()

    (sketchResult.toList.sorted, innerResult.toList.sorted)
  }
}

class TypedSketchJoinJobTest extends WordSpec with Matchers {
  import TypedSketchJoinTestHelper._

  "A TypedSketchJoinJob" should {
    "get the same result as an inner join" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => 1)
      sk shouldBe inner
    }

    "get the same result when half the left keys are missing" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x < 50) 0 else 1)
      sk shouldBe inner
    }

    "get the same result with a massive skew to one key" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x == 50) 1000 else 1)
      sk shouldBe inner
    }

    "still work with only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => 1)
      sk shouldBe inner
    }

    "still work with massive skew and only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => if (x == 50) 1000 else 1)
      sk shouldBe inner
    }
  }
}

class TypedSketchLeftJoinJobTest extends WordSpec with Matchers {
  import TypedSketchJoinTestHelper._

  "A TypedSketchLeftJoinJob" should {
    "get the same result as a left join" in {
      val (sk, left) = runJobWithArguments(new TypedSketchLeftJoinJob(_), 10, x => 1)
      sk shouldBe left
    }

    "get the same result when half the left keys are missing" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x < 50) 0 else 1)
      sk shouldBe inner
    }

    "get the same result with a massive skew to one key" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x == 50) 1000 else 1)
      sk shouldBe inner
    }

    "still work with only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => 1)
      sk shouldBe inner
    }

    "still work with massive skew and only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => if (x == 50) 1000 else 1)
      sk shouldBe inner
    }
  }
}

class TypedPipeRequireTest extends FunSuite {
  test("requireSingleValuePerKey should not cause a job to fail") {

    def ex(req: Boolean) = {
      val ex =
        TypedPipe.from((1 to 1000))
          .map { k => (k.toString, k) }
          .join(TypedPipe.from((1 to 1000 by 5)).map(_.toString).asKeys)
      val g =
        if (req) ex.group.requireSingleValuePerKey.toTypedPipe
        else ex.group.toTypedPipe

      g.toIterableExecution
    }

    assert(ex(false).waitFor(Config.empty, Local(true)).get.toList.sorted ==
      ex(true).waitFor(Config.empty, Local(true)).get.toList.sorted)
  }
}

object TypedPipeConverterTest {
  class TypedTsvWithCustomConverter[T: TypeDescriptor](nonSerializableOjb: Any, path: String*) extends FixedTypedText[T](TypedText.TAB, path: _*) {
    override def converter[U >: T]: TupleConverter[U] =
      super.converter.andThen { t: T => nonSerializableOjb; t }
  }

  class NonSerializableObj

  val source = new TypedTsvWithCustomConverter[Int](new NonSerializableObj(), "input")

  class JobWithCustomConverter(args: Args) extends Job(args) {
    TypedPipe.from(source)
      .map(i => i + 1)
      .write(TypedText.tsv[Int]("output"))
  }
}

class TypedPipeConverterTest extends FunSuite {
  import TypedPipeConverterTest._

  test("any converter should be serializable") {
    val expected = mutable.Buffer[Int](0 to 10: _*)
    val result = mutable.Buffer[Int]()

    JobTest(new JobWithCustomConverter(_))
      .source(source, expected.map(_ - 1))
      .typedSink(TypedText.tsv[Int]("output")) { outBuf => result ++= outBuf }
      .runHadoop
      .finish()

    assert(result == expected)
  }
}
