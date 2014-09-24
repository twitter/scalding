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

import org.specs._

// Use the scalacheck generators
import org.scalacheck.Gen
import scala.collection.mutable.Buffer

import TDsl._

object TUtil {
  def printStack(fn: => Unit) {
    try { fn } catch { case e: Throwable => e.printStackTrace; throw e }
  }
}

class TupleAdderJob(args: Args) extends Job(args) {

  TypedTsv[(String, String)]("input", ('a, 'b))
    .map{ f =>
      (1 +: f) ++ (2, 3)
    }
    .write(TypedTsv[(Int, String, String, Int, Int)]("output"))
}

class TupleAdderTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TupleAdderJob" should {
    JobTest(new TupleAdderJob(_))
      .source(TypedTsv[(String, String)]("input", ('a, 'b)), List(("a", "a"), ("b", "b")))
      .sink[(Int, String, String, Int, Int)](TypedTsv[(Int, String, String, Int, Int)]("output")) { outBuf =>
        "be able to use generated tuple adders" in {
          outBuf.size must_== 2
          outBuf.toSet must_== Set((1, "a", "a", 2, 3), (1, "b", "b", 2, 3))
        }
      }
      .run
      .finish
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
    .write(TypedTsv[(String, Long)]("outputFile"))
}

class TypedPipeTest extends Specification {
  import Dsl._
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TypedPipe" should {
    TUtil.printStack {
      JobTest(new com.twitter.scalding.TypedPipeJob(_)).
        source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
        sink[(String, Long)](TypedTsv[(String, Long)]("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.toMap
          "count words correctly" in {
            outMap("hack") must be_==(4)
            outMap("and") must be_==(1)
          }
        }.
        run.
        runHadoop.
        finish
    }
  }
}

class TypedSumByKeyJob(args: Args) extends Job(args) {
  //Word count using TypedPipe
  TextLine("inputFile")
    .flatMap { l => l.split("\\s+").map((_, 1L)) }
    .sumByKey
    .write(TypedTsv[(String, Long)]("outputFile"))
}

class TypedSumByKeyTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TypedSumByKeyPipe" should {
    TUtil.printStack {
      JobTest(new com.twitter.scalding.TypedSumByKeyJob(_)).
        source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
        sink[(String, Long)](TypedTsv[(String, Long)]("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.toMap
          "count words correctly" in {
            outMap("hack") must be_==(4)
            outMap("and") must be_==(1)
          }
        }.
        run.
        runHadoop.
        finish
    }
  }
}

class TypedPipeJoinJob(args: Args) extends Job(args) {
  (Tsv("inputFile0").read.toTypedPipe[(Int, Int)](0, 1).group
    leftJoin TypedPipe.from[(Int, Int)](Tsv("inputFile1").read, (0, 1)).group)
    .toTypedPipe
    .write(TypedTsv[(Int, (Int, Option[Int]))]("outputFile"))
}

class TypedPipeJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeJoin" should {
    JobTest(new com.twitter.scalding.TypedPipeJoinJob(_))
      .source(Tsv("inputFile0"), List((0, 0), (1, 1), (2, 2), (3, 3), (4, 5)))
      .source(Tsv("inputFile1"), List((0, 1), (1, 2), (2, 3), (3, 4)))
      .sink[(Int, (Int, Option[Int]))](TypedTsv[(Int, (Int, Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0, Some(1)))
          outMap(1) must be_==((1, Some(2)))
          outMap(2) must be_==((2, Some(3)))
          outMap(3) must be_==((3, Some(4)))
          outMap(4) must be_==((5, None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}

class TypedPipeDistinctJob(args: Args) extends Job(args) {
  Tsv("inputFile").read.toTypedPipe[(Int, Int)](0, 1)
    .distinct
    .write(TypedTsv[(Int, Int)]("outputFile"))
}

class TypedPipeDistinctTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeDistinctJob" should {
    JobTest(new com.twitter.scalding.TypedPipeDistinctJob(_))
      .source(Tsv("inputFile"), List((0, 0), (1, 1), (2, 2), (2, 2), (2, 5)))
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly count unique item sizes" in {
          val outSet = outputBuffer.toSet
          outSet.size must_== 4
        }
      }.
      run.
      finish
  }
}

class TypedPipeDistinctByJob(args: Args) extends Job(args) {
  Tsv("inputFile").read.toTypedPipe[(Int, Int)](0, 1)
    .distinctBy(_._2)
    .write(TypedTsv[(Int, Int)]("outputFile"))
}

class TypedPipeDistinctByTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeDistinctByJob" should {
    JobTest(new com.twitter.scalding.TypedPipeDistinctByJob(_))
      .source(Tsv("inputFile"), List((0, 1), (1, 1), (2, 2), (2, 2), (2, 5)))
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly count unique item sizes" in {
          val outSet = outputBuffer.toSet
          outSet.size must_== 3
          outSet must beOneOf (Set((0, 1), (2, 2), (2, 5)), Set((1, 1), (2, 2), (2, 5)))
        }
      }.
      run.
      finish
  }
}

class TypedPipeHashJoinJob(args: Args) extends Job(args) {
  TypedTsv[(Int, Int)]("inputFile0")
    .group
    .hashLeftJoin(TypedTsv[(Int, Int)]("inputFile1").group)
    .write(TypedTsv[(Int, (Int, Option[Int]))]("outputFile"))
}

class TypedPipeHashJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeHashJoinJob" should {
    JobTest(new com.twitter.scalding.TypedPipeHashJoinJob(_))
      .source(TypedTsv[(Int, Int)]("inputFile0"), List((0, 0), (1, 1), (2, 2), (3, 3), (4, 5)))
      .source(TypedTsv[(Int, Int)]("inputFile1"), List((0, 1), (1, 2), (2, 3), (3, 4)))
      .sink[(Int, (Int, Option[Int]))](TypedTsv[(Int, (Int, Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0, Some(1)))
          outMap(1) must be_==((1, Some(2)))
          outMap(2) must be_==((2, Some(3)))
          outMap(3) must be_==((3, Some(4)))
          outMap(4) must be_==((5, None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
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
  }.write(TypedTsv[(String, Int)]("outputFile"))
}

class TypedPipeTypedTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedImplicitJob" should {
    JobTest(new com.twitter.scalding.TypedImplicitJob(_))
      .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
      .sink[(String, Int)](TypedTsv[(String, Int)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "find max word" in {
          outMap("hack") must be_==(4)
          outMap.size must be_==(1)
        }
      }
      .run
      .finish
  }
}

class TypedWithOnCompleteJob(args: Args) extends Job(args) {
  val onCompleteMapperStat = Stat("onCompleteMapper")
  val onCompleteReducerStat = Stat("onCompleteReducer")
  def onCompleteMapper() = onCompleteMapperStat.inc
  def onCompleteReducer() = onCompleteReducerStat.inc
  // find repeated words ignoring case
  TypedTsv[String]("input")
    .map(_.toUpperCase)
    .onComplete(onCompleteMapper)
    .groupBy(identity)
    .mapValueStream(words => Iterator(words.size))
    .filter { case (word, occurrences) => occurrences > 1 }
    .keys
    .onComplete(onCompleteReducer)
    .write(TypedTsv[String]("output"))
}

class TypedPipeWithOnCompleteTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  val inputText = "the quick brown fox jumps over the lazy LAZY dog"
  "A TypedWithOnCompleteJob" should {
    JobTest(new TypedWithOnCompleteJob(_))
      .source(TypedTsv[String]("input"), inputText.split("\\s+").map(Tuple1(_)))
      .counter("onCompleteMapper") { cnt => "have onComplete called on mapper" in { cnt must_== 1 } }
      .counter("onCompleteReducer") { cnt => "have onComplete called on reducer" in { cnt must_== 1 } }
      .sink[String](TypedTsv[String]("output")) { outbuf =>
        "have the correct output" in {
          val correct = inputText.split("\\s+").map(_.toUpperCase).groupBy(x => x).filter(_._2.size > 1).keys.toList.sorted
          val sortedL = outbuf.toList.sorted
          sortedL must_== (correct)
        }
      }
      .runHadoop
      .finish
  }
}

class TypedPipeWithOuterAndLeftJoin(args: Args) extends Job(args) {
  val userNames = TypedTsv[(Int, String)]("inputNames").group
  val userData = TypedTsv[(Int, Double)]("inputData").group
  val optionalData = TypedTsv[(Int, Boolean)]("inputOptionalData").group

  userNames
    .outerJoin(userData)
    .leftJoin(optionalData)
    .map { case (id, ((nameOpt, userDataOption), optionalDataOpt)) => id }
    .write(TypedTsv[Int]("output"))
}

class TypedPipeWithOuterAndLeftJoinTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedPipeWithOuterAndLeftJoin" should {
    JobTest(new TypedPipeWithOuterAndLeftJoin(_))
      .source(TypedTsv[(Int, String)]("inputNames"), List((1, "Jimmy Foursquare")))
      .source(TypedTsv[(Int, Double)]("inputData"), List((1, 0.1), (5, 0.5)))
      .source(TypedTsv[(Int, Boolean)]("inputOptionalData"), List((1, true), (99, false)))
      .sink[Long](TypedTsv[Int]("output")) { outbuf =>
        "have output for user 1" in {
          outbuf.toList.contains(1) must_== true
        }
        "have output for user 5" in {
          outbuf.toList.contains(5) must_== true
        }
        "not have output for user 99" in {
          outbuf.toList.contains(99) must_== false
        }
      }
      .run
      .finish
  }
}

class TJoinCountJob(args: Args) extends Job(args) {
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    .size
    .write(TypedTsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(TypedTsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1)).group
    leftJoin TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)).group)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, (Int, Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(TypedTsv[(Int, Int, Int)]("out3"))
}

/**
 * This test exercises the implicit from TypedPipe to HashJoinabl
 */
class TNiceJoinCountJob(args: Args) extends Job(args) {

  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    .size
    .write(TypedTsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    join TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(TypedTsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    leftJoin TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, (Int, Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(TypedTsv[(Int, Int, Int)]("out3"))
}

class TNiceJoinByCountJob(args: Args) extends Job(args) {
  import com.twitter.scalding.typed.Syntax._

  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    joinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    .size
    .write(TypedTsv[(Int, Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    joinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1._2, kvw._2._2._2) }
    .write(TypedTsv[(Int, Int, Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int, Int)](Tsv("in0", (0, 1)), (0, 1))
    leftJoinBy TypedPipe.from[(Int, Int)](Tsv("in1", (0, 1)), (0, 1)))(_._1, _._1)
    //Flatten out to three values:
    .toTypedPipe
    .map { kvw: (Int, ((Int, Int), Option[(Int, Int)])) =>
      (kvw._1, kvw._2._1._2, kvw._2._2.getOrElse((-1, -1))._2)
    }
    .write(TypedTsv[(Int, Int, Int)]("out3"))
}

class TypedPipeJoinCountTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._

  val joinTests = List("com.twitter.scalding.TJoinCountJob", "com.twitter.scalding.TNiceJoinCountJob", "com.twitter.scalding.TNiceJoinByCountJob")

  joinTests.foreach{ jobName =>
    "A " + jobName should {
      JobTest(jobName)
        .source(Tsv("in0", (0, 1)), List((0, 1), (0, 2), (1, 1), (1, 5), (2, 10)))
        .source(Tsv("in1", (0, 1)), List((0, 10), (1, 20), (1, 10), (1, 30)))
        .sink[(Int, Long)](TypedTsv[(Int, Long)]("out")) { outbuf =>
          val outMap = outbuf.toMap
          "correctly reduce after cogroup" in {
            outMap(0) must be_==(2)
            outMap(1) must be_==(6)
            outMap.size must be_==(2)
          }
        }
        .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("out2")) { outbuf2 =>
          val outMap = outbuf2.groupBy { _._1 }
          "correctly do a simple join" in {
            outMap.size must be_==(2)
            outMap(0).toList.sorted must be_==(List((0, 1, 10), (0, 2, 10)))
            outMap(1).toList.sorted must be_==(List((1, 1, 10), (1, 1, 20), (1, 1, 30), (1, 5, 10), (1, 5, 20), (1, 5, 30)))
          }
        }
        .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("out3")) { outbuf =>
          val outMap = outbuf.groupBy { _._1 }
          "correctly do a simple leftJoin" in {
            outMap.size must be_==(3)
            outMap(0).toList.sorted must be_==(List((0, 1, 10), (0, 2, 10)))
            outMap(1).toList.sorted must be_==(List((1, 1, 10), (1, 1, 20), (1, 1, 30), (1, 5, 10), (1, 5, 20), (1, 5, 30)))
            outMap(2).toList.sorted must be_==(List((2, 10, -1)))
          }
        }
        .run
        .runHadoop
        .finish
    }
  }
}

class TCrossJob(args: Args) extends Job(args) {
  (TextLine("in0") cross TextLine("in1"))
    .write(TypedTsv[(String, String)]("crossed"))
}

class TypedPipeCrossTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TCrossJob" should {
    TUtil.printStack {
      JobTest(new com.twitter.scalding.TCrossJob(_))
        .source(TextLine("in0"), List((0, "you"), (1, "all")))
        .source(TextLine("in1"), List((0, "every"), (1, "body")))
        .sink[(String, String)](TypedTsv[(String, String)]("crossed")) { outbuf =>
          val sortedL = outbuf.toList.sorted
          "create a cross-product" in {
            sortedL must be_==(List(("all", "body"),
              ("all", "every"),
              ("you", "body"),
              ("you", "every")))
          }
        }
        .run
        .runHadoop
        .finish
    }
  }
}

class TJoinTakeJob(args: Args) extends Job(args) {
  val items0 = TextLine("in0").flatMap { s => (1 to 10).map((_, s)) }.group
  val items1 = TextLine("in1").map { s => (s.toInt, ()) }.group

  items0.join(items1.take(1))
    .mapValues(_._1) // discard the ()
    .toTypedPipe
    .write(TypedTsv[(Int, String)]("joined"))
}

class TypedJoinTakeTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TJoinTakeJob" should {
    TUtil.printStack {
      JobTest(new TJoinTakeJob(_))
        .source(TextLine("in0"), List((0, "you"), (1, "all")))
        .source(TextLine("in1"), List((0, "3"), (1, "2"), (0, "3")))
        .sink[(Int, String)](TypedTsv[(Int, String)]("joined")) { outbuf =>
          val sortedL = outbuf.toList.sorted
          "dedup keys by using take" in {
            sortedL must be_==(
              List((3, "you"), (3, "all"), (2, "you"), (2, "all")).sorted)
          }
        }
        .run
        .runHadoop
        .finish
    }
  }
}

class TGroupAllJob(args: Args) extends Job(args) {
  TextLine("in")
    .groupAll
    .sorted
    .values
    .write(TypedTsv[String]("out"))
}

class TypedGroupAllTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TGroupAllJob" should {
    TUtil.printStack {
      val input = List((0, "you"), (1, "all"), (2, "everybody"))
      JobTest(new TGroupAllJob(_))
        .source(TextLine("in"), input)
        .sink[String](TypedTsv[String]("out")) { outbuf =>
          val sortedL = outbuf.toList
          val correct = input.map { _._2 }.sorted
          "create sorted output" in {
            sortedL must_== (correct)
          }
        }
        .run
        .runHadoop
        .finish
    }
  }
}

class TSelfJoin(args: Args) extends Job(args) {
  val g = TypedTsv[(Int, Int)]("in").group
  g.join(g).values.write(TypedTsv[(Int, Int)]("out"))
}

class TSelfJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TSelfJoin" should {
    JobTest(new TSelfJoin(_))
      .source(TypedTsv[(Int, Int)]("in"), List((1, 2), (1, 3), (2, 1)))
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("out")) { outbuf =>
        outbuf.toList.sorted must be_==(List((1, 1), (2, 2), (2, 3), (3, 2), (3, 3)))
      }
      .run
      .runHadoop
      .finish
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
    .write(TypedTsv[(String, Int, Int)]("out"))
}

class TypedJoinWCTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
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
        .sink[(String, Int, Int)](TypedTsv[(String, Int, Int)]("out")) { outbuf =>
          val sortedL = outbuf.toList
          "create sorted output" in {
            sortedL must_== (correct)
          }
        }
        .run
        .finish
    }
  }
}

class TypedLimitJob(args: Args) extends Job(args) {
  val p = TypedTsv[String]("input").limit(10): TypedPipe[String]
  p.write(TypedTsv[String]("output"))
}

class TypedLimitTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedLimitJob" should {
    JobTest(new TypedLimitJob(_))
      .source(TypedTsv[String]("input"), (0 to 100).map { i => Tuple1(i.toString) })
      .sink[String](TypedTsv[String]("output")) { outBuf =>
        "not have more than the limited outputs" in {
          outBuf.size must be_<=(10)
        }
      }
      .runHadoop
      .finish
  }
}

class TypedFlattenJob(args: Args) extends Job(args) {
  TypedTsv[String]("input").map { _.split(" ").toList }
    .flatten
    .write(TypedTsv[String]("output"))
}

class TypedFlattenTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedLimitJob" should {
    JobTest(new TypedFlattenJob(_))
      .source(TypedTsv[String]("input"), List(Tuple1("you all"), Tuple1("every body")))
      .sink[String](TypedTsv[String]("output")) { outBuf =>
        "correctly flatten" in {
          outBuf.toSet must be_==(Set("you", "all", "every", "body"))
        }
      }
      .runHadoop
      .finish
  }
}

class TypedMergeJob(args: Args) extends Job(args) {
  val tp = TypedPipe.from(TypedTsv[String]("input"))
  // This exercise a self merge
  (tp ++ tp)
    .write(TypedTsv[String]("output"))
  (tp ++ (tp.map(_.reverse)))
    .write(TypedTsv[String]("output2"))
}

class TypedMergeTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedMergeJob" should {
    JobTest(new TypedMergeJob(_))
      .source(TypedTsv[String]("input"), List(Tuple1("you all"), Tuple1("every body")))
      .sink[String](TypedTsv[String]("output")) { outBuf =>
        "correctly flatten" in {
          outBuf.toSet must be_==(Set("you all", "every body"))
        }
      }
      .sink[String](TypedTsv[String]("output2")) { outBuf =>
        "correctly flatten" in {
          val correct = Set("you all", "every body")
          outBuf.toSet must be_==(correct ++ correct.map(_.reverse))
        }
      }
      .runHadoop
      .finish
  }
}

class TypedShardJob(args: Args) extends Job(args) {
  (TypedPipe.from(TypedTsv[String]("input")) ++
    (TypedPipe.empty.map { _ => "hey" }) ++
    TypedPipe.from(List("item")))
    .shard(10)
    .write(TypedTsv[String]("output"))
}

class TypedShardTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedShardJob" should {
    val genList = Gen.listOf(Gen.identifier)
    // Take one random sample
    lazy val mk: List[String] = genList.sample.getOrElse(mk)
    JobTest(new TypedShardJob(_))
      .source(TypedTsv[String]("input"), mk)
      .sink[String](TypedTsv[String]("output")) { outBuf =>
        "correctly flatten" in {
          outBuf.size must be_==(mk.size + 1)
          outBuf.toSet must be_==(mk.toSet + "item")
        }
      }
      .run
      .finish
  }
}

class TypedLocalSumJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[String]("input"))
    .flatMap { s => s.split(" ").map((_, 1L)) }
    .sumByLocalKeys
    .write(TypedTsv[(String, Long)]("output"))
}

class TypedLocalSumTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedLocalSumJob" should {
    val genList = Gen.listOf(Gen.identifier)
    // Take one random sample
    lazy val mk: List[String] = genList.sample.getOrElse(mk)
    JobTest(new TypedLocalSumJob(_))
      .source(TypedTsv[String]("input"), mk)
      .sink[(String, Long)](TypedTsv[(String, Long)]("output")) { outBuf =>
        "not expand and have correct total sum" in {
          import com.twitter.algebird.MapAlgebra.sumByKey
          val lres = outBuf.toList
          val fmapped = mk.flatMap { s => s.split(" ").map((_, 1L)) }
          lres.size must be_<=(fmapped.size)
          sumByKey(lres) must be_==(sumByKey(fmapped))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TypedHeadJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[(Int, Int)]("input"))
    .group
    .head
    .write(TypedTsv[(Int, Int)]("output"))
}

class TypedHeadTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedHeadJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedHeadJob(_))
      .source(TypedTsv[(Int, Int)]("input"), mk)
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("output")) { outBuf =>
        "correctly take the first" in {
          val correct = mk.groupBy(_._1).mapValues(_.head._2)
          outBuf.size must be_==(correct.size)
          outBuf.toMap must be_==(correct)
        }
      }
      .run
      .finish
  }
}

class TypedSortWithTakeJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[(Int, Int)]("input"))
    .group
    .sortedReverseTake(5)
    .mapValues { (s: Seq[Int]) => s.toString }
    .write(TypedTsv[(Int, String)]("output"))
}

class TypedSortWithTakeTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedSortWithTakeJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedSortWithTakeJob(_))
      .source(TypedTsv[(Int, Int)]("input"), mk)
      .sink[(Int, String)](TypedTsv[(Int, String)]("output")) { outBuf =>
        "correctly take the first" in {
          val correct = mk.groupBy(_._1).mapValues(_.map(i => i._2).sorted.reverse.take(5).toList.toString)
          outBuf.size must be_==(correct.size)
          outBuf.toMap must be_==(correct)
        }
      }
      .run
      .finish
  }
}

class TypedLookupJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[Int]("input0"))
    .hashLookup(TypedPipe.from(TypedTsv[(Int, String)]("input1")).group)
    .write(TypedTsv[(Int, Option[String])]("output"))
}

class TypedLookupJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedLookupJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt.toString) }
    JobTest(new TypedLookupJob(_))
      .source(TypedTsv[Int]("input0"), (-1 to 100))
      .source(TypedTsv[(Int, String)]("input1"), mk)
      .sink[(Int, Option[String])](TypedTsv[(Int, Option[String])]("output")) { outBuf =>
        "correctly TypedPipe.hashLookup" in {
          val data = mk.groupBy(_._1)
            .mapValues(kvs => kvs.map { case (k, v) => (k, Some(v)) })
          val correct = (-1 to 100).flatMap { k =>
            data.get(k).getOrElse(List((k, None)))
          }.toList.sorted
          outBuf.size must be_==(correct.size)
          outBuf.toList.sorted must be_==(correct)
        }
      }
      .run
      .finish
  }
}

class TypedLookupReduceJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[Int]("input0"))
    .hashLookup(TypedPipe.from(TypedTsv[(Int, String)]("input1")).group.max)
    .write(TypedTsv[(Int, Option[String])]("output"))
}

class TypedLookupReduceJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedLookupJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt.toString) }
    JobTest(new TypedLookupReduceJob(_))
      .source(TypedTsv[Int]("input0"), (-1 to 100))
      .source(TypedTsv[(Int, String)]("input1"), mk)
      .sink[(Int, Option[String])](TypedTsv[(Int, Option[String])]("output")) { outBuf =>
        "correctly TypedPipe.hashLookup" in {
          val data = mk.groupBy(_._1)
            .mapValues { kvs =>
              val (k, v) = kvs.maxBy(_._2)
              (k, Some(v))
            }
          val correct = (-1 to 100).map { k =>
            data.get(k).getOrElse((k, None))
          }.toList.sorted
          outBuf.size must be_==(correct.size)
          outBuf.toList.sorted must be_==(correct)
        }
      }
      .run
      .finish
  }
}

class TypedFilterJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[Int]("input"))
    .filter { _ > 50 }
    .filterNot { _ % 2 == 0 }
    .write(TypedTsv[Int]("output"))
}

class TypedFilterTest extends Specification {
  import Dsl._
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TypedPipe" should {
    "filter and filterNot elements" in {
      val input = -1 to 100
      val isEven = (i: Int) => i % 2 == 0
      val expectedOutput = input filter { _ > 50 } filterNot isEven

      TUtil.printStack {
        JobTest(new com.twitter.scalding.TypedFilterJob(_)).
          source(TypedTsv[Int]("input"), input).
          sink[Int](TypedTsv[Int]("output")) { outBuf =>
            outBuf.toList must be_==(expectedOutput)
          }.
          run.
          runHadoop.
          finish
      }
    }
  }
}

class TypedPartitionJob(args: Args) extends Job(args) {
  val (p1, p2) = TypedPipe.from(TypedTsv[Int]("input")).partition { _ > 50 }
  p1.write(TypedTsv[Int]("output1"))
  p2.write(TypedTsv[Int]("output2"))
}

class TypedPartitionTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedPipe" should {
    "partition elements" in {
      val input = -1 to 100
      val (expected1, expected2) = input partition { _ > 50 }

      TUtil.printStack {
        JobTest(new com.twitter.scalding.TypedPartitionJob(_))
          .source(TypedTsv[Int]("input"), input)
          .sink[Int](TypedTsv[Int]("output1")) { outBuf =>
            outBuf.toList must be_==(expected1)
          }
          .sink[Int](TypedTsv[Int]("output2")) { outBuf =>
            outBuf.toList must be_==(expected2)
          }
          .run
          .runHadoop
          .finish
      }
    }
  }
}

class TypedMultiJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedTsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedTsv[(Int, Int)]("input1"))
  val two = TypedPipe.from(TypedTsv[(Int, Int)]("input2"))

  val cogroup = zero.group
    .join(one.group.max)
    .join(two.group.max)

  // make sure this is indeed a case with no self joins
  // distinct by mapped
  val distinct = cogroup.inputs.groupBy(identity).map(_._2.head).toList
  assert(distinct.size == cogroup.inputs.size)

  cogroup
    .map { case (k, ((v0, v1), v2)) => (k, v0, v1, v2) }
    .write(TypedTsv[(Int, Int, Int, Int)]("output"))
}

class TypedMultiJoinJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedMultiJoinJob" should {
    val rng = new java.util.Random
    val COUNT = 100 * 100
    val KEYS = 10
    def mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    val mk0 = mk
    val mk1 = mk
    val mk2 = mk
    JobTest(new TypedMultiJoinJob(_))
      .source(TypedTsv[(Int, Int)]("input0"), mk0)
      .source(TypedTsv[(Int, Int)]("input1"), mk1)
      .source(TypedTsv[(Int, Int)]("input2"), mk2)
      .sink[(Int, Int, Int, Int)](TypedTsv[(Int, Int, Int, Int)]("output")) { outBuf =>
        "correctly do a multi-join" in {
          def groupMax(it: Seq[(Int, Int)]): Map[Int, Int] =
            it.groupBy(_._1).mapValues { kvs =>
              val (k, v) = kvs.maxBy(_._2)
              v
            }.toMap

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
            .toList.sorted

          outBuf.size must be_==(correct.size)
          outBuf.toList.sorted must be_==(correct)
        }
      }
      .runHadoop
      .finish
  }
}

class TypedMultiSelfJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedTsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedTsv[(Int, Int)]("input1"))
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
    .write(TypedTsv[(Int, Int, Int, Int)]("output"))
}

class TypedMultiSelfJoinJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedMultiSelfJoinJob" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    def mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    val mk0 = mk
    val mk1 = mk
    JobTest(new TypedMultiSelfJoinJob(_))
      .source(TypedTsv[(Int, Int)]("input0"), mk0)
      .source(TypedTsv[(Int, Int)]("input1"), mk1)
      .sink[(Int, Int, Int, Int)](TypedTsv[(Int, Int, Int, Int)]("output")) { outBuf =>
        "correctly do a multi-self-join" in {
          def group(it: Seq[(Int, Int)])(red: (Int, Int) => Int): Map[Int, Int] =
            it.groupBy(_._1).mapValues { kvs =>
              kvs.map(_._2).reduce(red)
            }.toMap

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
            .toList.sorted

          outBuf.size must be_==(correct.size)
          outBuf.toList.sorted must be_==(correct)
        }
      }
      .runHadoop
      .finish
  }
}

class TypedMapGroup(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[(Int, Int)]("input"))
    .group
    .mapGroup { (k, iters) => iters.map(_ * k) }
    .max
    .write(TypedTsv[(Int, Int)]("output"))
}

class TypedMapGroupTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TypedMapGroup" should {
    val rng = new java.util.Random
    val COUNT = 10000
    val KEYS = 100
    val mk = (1 to COUNT).map { _ => (rng.nextInt % KEYS, rng.nextInt) }
    JobTest(new TypedMapGroup(_))
      .source(TypedTsv[(Int, Int)]("input"), mk)
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("output")) { outBuf =>
        "correctly do a mapGroup" in {
          def mapGroup(it: Seq[(Int, Int)]): Map[Int, Int] =
            it.groupBy(_._1).mapValues { kvs =>
              kvs.map { case (k, v) => k * v }.max
            }.toMap
          val correct = mapGroup(mk).toList.sorted
          outBuf.size must be_==(correct.size)
          outBuf.toList.sorted must be_==(correct)
        }
      }
      .runHadoop
      .finish
  }
}

class TypedSelfCrossJob(args: Args) extends Job(args) {
  val pipe = TypedPipe.from(TypedTsv[Int]("input"))

  pipe
    .cross(pipe.groupAll.sum.values)
    .write(TypedTsv[(Int, Int)]("output"))
}

class TypedSelfCrossTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  val input = (1 to 100).toList

  "A TypedSelfCrossJob" should {
    JobTest(new TypedSelfCrossJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](TypedTsv[(Int, Int)]("output")) { outBuf =>
        "not change the length of the input" in {
          outBuf.size must_== input.size
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TypedSelfLeftCrossJob(args: Args) extends Job(args) {
  val pipe = TypedPipe.from(TypedTsv[Int]("input"))

  pipe
    .leftCross(pipe.sum)
    .write(TypedTsv[(Int, Option[Int])]("output"))
}

class TypedSelfLeftCrossTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  val input = (1 to 100).toList

  "A TypedSelfLeftCrossJob" should {
    JobTest(new TypedSelfLeftCrossJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Option[Int])](TypedTsv[(Int, Option[Int])]("output")) { outBuf =>
        "attach the sum of all values correctly" in {
          outBuf.size must_== input.size
          val sum = input.reduceOption(_ + _)
          // toString to deal with our hadoop testing jank
          outBuf.toList.sortBy(_._1).toString must be_== (input.sorted.map((_, sum)).toString)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class JoinMapGroupJob(args: Args) extends Job(args) {
  def r1 = TypedPipe.from(Seq((1, 10)))
  def r2 = TypedPipe.from(Seq((1, 1), (2, 2), (3, 3)))
  r1.groupBy(_._1).join(r2.groupBy(_._1))
    .mapGroup { case (a, b) => Iterator("a") }
    .write(TypedTsv("output"))
}
class JoinMapGroupJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  "A JoinMapGroupJob" should {
    JobTest(new JoinMapGroupJob(_))
      .sink[(Int, String)](TypedTsv[(Int, String)]("output")) { outBuf =>
        "not duplicate keys" in {
          outBuf.toList must be_==(List((1, "a")))
        }
      }
      .run
      .finish
  }
}

class TypedSketchJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedTsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedTsv[(Int, Int)]("input1"))

  implicit def serialize(k: Int) = k.toString.getBytes

  zero
    .sketch(args("reducers").toInt)
    .join(one)
    .map{ case (k, (v0, v1)) => (k, v0, v1) }
    .write(TypedTsv[(Int, Int, Int)]("output-sketch"))

  zero
    .group
    .join(one.group)
    .map{ case (k, (v0, v1)) => (k, v0, v1) }
    .write(TypedTsv[(Int, Int, Int)]("output-join"))
}

class TypedSketchLeftJoinJob(args: Args) extends Job(args) {
  val zero = TypedPipe.from(TypedTsv[(Int, Int)]("input0"))
  val one = TypedPipe.from(TypedTsv[(Int, Int)]("input1"))

  implicit def serialize(k: Int) = k.toString.getBytes

  zero
    .sketch(args("reducers").toInt)
    .leftJoin(one)
    .map{ case (k, (v0, v1)) => (k, v0, v1.getOrElse(-1)) }
    .write(TypedTsv[(Int, Int, Int)]("output-sketch"))

  zero
    .group
    .leftJoin(one.group)
    .map{ case (k, (v0, v1)) => (k, v0, v1.getOrElse(-1)) }
    .write(TypedTsv[(Int, Int, Int)]("output-join"))
}

object TypedSketchJoinTestHelper {
  import Dsl._

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
      .source(TypedTsv[(Int, Int)]("input0"), generateInput(1000, 100, dist))
      .source(TypedTsv[(Int, Int)]("input1"), generateInput(100, 100, x => 1))
      .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("output-sketch")) { outBuf => sketchResult ++= outBuf }
      .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("output-join")) { outBuf => innerResult ++= outBuf }
      .run
      .runHadoop
      .finish

    (sketchResult.toList.sorted, innerResult.toList.sorted)
  }
}

class TypedSketchJoinJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  import TypedSketchJoinTestHelper._

  "A TypedSketchJoinJob" should {
    "get the same result as an inner join" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => 1)
      sk must_== inner
    }

    "get the same result when half the left keys are missing" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x < 50) 0 else 1)
      sk must_== inner
    }

    "get the same result with a massive skew to one key" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x == 50) 1000 else 1)
      sk must_== inner
    }

    "still work with only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => 1)
      sk must_== inner
    }

    "still work with massive skew and only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => if (x == 50) 1000 else 1)
      sk must_== inner
    }
  }
}

class TypedSketchLeftJoinJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  import TypedSketchJoinTestHelper._

  "A TypedSketchLeftJoinJob" should {
    "get the same result as a left join" in {
      val (sk, left) = runJobWithArguments(new TypedSketchLeftJoinJob(_), 10, x => 1)
      sk must_== left
    }

    "get the same result when half the left keys are missing" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x < 50) 0 else 1)
      sk must_== inner
    }

    "get the same result with a massive skew to one key" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 10, x => if (x == 50) 1000 else 1)
      sk must_== inner
    }

    "still work with only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => 1)
      sk must_== inner
    }

    "still work with massive skew and only one reducer" in {
      val (sk, inner) = runJobWithArguments(new TypedSketchJoinJob(_), 1, x => if (x == 50) 1000 else 1)
      sk must_== inner
    }
  }
}

