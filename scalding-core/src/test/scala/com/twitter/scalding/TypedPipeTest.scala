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

import TDsl._

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class TupleAdderJob(args: Args) extends Job(args) {

  TypedTsv[(String, String)]("input", ('a, 'b))
    .map{ f =>
      (1 +: f) ++ (2, 3)
    }
    .write(TypedTsv[(Int,String,String,Int,Int)]("output"))
}

class TupleAdderTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A TupleAdderJob" should {
    JobTest(new TupleAdderJob(_))
      .source(TypedTsv[(String, String)]("input", ('a, 'b)), List(("a", "a"), ("b", "b")))
      .sink[(Int, String, String, Int, Int)](TypedTsv[(Int,String,String,Int,Int)]("output")) { outBuf =>
        "be able to use generated tuple adders" in {
          outBuf.size must_== 2
          outBuf.toSet must_== Set((1, "a", "a", 2, 3), (1, "b", "b", 2, 3))
        }
      }
      .run
      .finish
  }
}

class TypedPipeJob(args : Args) extends Job(args) {
  //Word count using TypedPipe
  TextLine("inputFile")
    .flatMap { _.split("\\s+") }
    .map { w => (w, 1L) }
    .forceToDisk
    .group
    //.forceToReducers
    .sum
    .debug
    .write(TypedTsv[(String,Long)]("outputFile"))
}

class TypedPipeTest extends Specification {
  import Dsl._
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TypedPipe" should {
    TUtil.printStack {
    JobTest(new com.twitter.scalding.TypedPipeJob(_)).
      source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
      sink[(String,Long)](TypedTsv[(String,Long)]("outputFile")){ outputBuffer =>
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

class TypedPipeJoinJob(args : Args) extends Job(args) {
  (Tsv("inputFile0").read.toTypedPipe[(Int,Int)](0, 1).group
    leftJoin TypedPipe.from[(Int,Int)](Tsv("inputFile1").read, (0, 1)).group)
    .toTypedPipe
    .write(TypedTsv[(Int,(Int,Option[Int]))]("outputFile"))
}

class TypedPipeJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeJoin" should {
    JobTest(new com.twitter.scalding.TypedPipeJoinJob(_))
      .source(Tsv("inputFile0"), List((0,0), (1,1), (2,2), (3,3), (4,5)))
      .source(Tsv("inputFile1"), List((0,1), (1,2), (2,3), (3,4)))
      .sink[(Int,(Int,Option[Int]))](TypedTsv[(Int,(Int,Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0,Some(1)))
          outMap(1) must be_==((1,Some(2)))
          outMap(2) must be_==((2,Some(3)))
          outMap(3) must be_==((3,Some(4)))
          outMap(4) must be_==((5,None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}


class TypedPipeDistinctJob(args : Args) extends Job(args) {
  Tsv("inputFile").read.toTypedPipe[(Int,Int)](0, 1)
    .distinct
    .write(TypedTsv[(Int, Int)]("outputFile"))
}


class TypedPipeDistinctTest extends Specification {
noDetailedDiffs() //Fixes an issue with scala 2.9
import Dsl._
"A TypedPipeDistinctJob" should {
  JobTest(new com.twitter.scalding.TypedPipeDistinctJob(_))
    .source(Tsv("inputFile"), List((0,0), (1,1), (2,2), (2,2), (2,5)))
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


class TypedPipeHashJoinJob(args : Args) extends Job(args) {
  TypedTsv[(Int,Int)]("inputFile0")
    .group
    .hashLeftJoin(TypedTsv[(Int,Int)]("inputFile1").group)
    .write(TypedTsv[(Int,(Int,Option[Int]))]("outputFile"))
}

class TypedPipeHashJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedPipeHashJoinJob" should {
    JobTest(new com.twitter.scalding.TypedPipeHashJoinJob(_))
      .source(TypedTsv[(Int,Int)]("inputFile0"), List((0,0), (1,1), (2,2), (3,3), (4,5)))
      .source(TypedTsv[(Int,Int)]("inputFile1"), List((0,1), (1,2), (2,3), (3,4)))
      .sink[(Int,(Int,Option[Int]))](TypedTsv[(Int,(Int,Option[Int]))]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0,Some(1)))
          outMap(1) must be_==((1,Some(2)))
          outMap(2) must be_==((2,Some(3)))
          outMap(3) must be_==((3,Some(4)))
          outMap(4) must be_==((5,None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}

class TypedImplicitJob(args : Args) extends Job(args) {
  def revTup[K,V](in : (K,V)) : (V,K) = (in._2, in._1)
  TextLine("inputFile").read.typed(1 -> ('maxWord, 'maxCnt)) { tpipe : TypedPipe[String] =>
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
  }.write(TypedTsv[(String,Int)]("outputFile"))
}

class TypedPipeTypedTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TypedImplicitJob" should {
    JobTest(new com.twitter.scalding.TypedImplicitJob(_))
      .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
      .sink[(String,Int)](TypedTsv[(String,Int)]("outputFile")){ outputBuffer =>
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

class TJoinCountJob(args : Args) extends Job(args) {
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1)).group
    join TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)).group)
    .size
    .write(TypedTsv[(Int,Long)]("out"))

  //Also check simple joins:
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1)).group
    join TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)).group)
   //Flatten out to three values:
    .toTypedPipe
    .map { kvw => (kvw._1, kvw._2._1, kvw._2._2) }
    .write(TypedTsv[(Int,Int,Int)]("out2"))

  //Also check simple leftJoins:
  (TypedPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1)).group
    leftJoin TypedPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)).group)
   //Flatten out to three values:
    .toTypedPipe
    .map { kvw : (Int,(Int,Option[Int])) =>
      (kvw._1, kvw._2._1, kvw._2._2.getOrElse(-1))
    }
    .write(TypedTsv[(Int,Int,Int)]("out3"))
}

class TypedPipeJoinCountTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TJoinCountJob" should {
    JobTest(new com.twitter.scalding.TJoinCountJob(_))
      .source(Tsv("in0",(0,1)), List((0,1),(0,2),(1,1),(1,5),(2,10)))
      .source(Tsv("in1",(0,1)), List((0,10),(1,20),(1,10),(1,30)))
      .sink[(Int,Long)](TypedTsv[(Int,Long)]("out")) { outbuf =>
        val outMap = outbuf.toMap
        "correctly reduce after cogroup" in {
          outMap(0) must be_==(2)
          outMap(1) must be_==(6)
          outMap.size must be_==(2)
        }
      }
      .sink[(Int,Int,Int)](TypedTsv[(Int,Int,Int)]("out2")) { outbuf2 =>
        val outMap = outbuf2.groupBy { _._1 }
        "correctly do a simple join" in {
          outMap.size must be_==(2)
          outMap(0).toList.sorted must be_==(List((0,1,10),(0,2,10)))
          outMap(1).toList.sorted must be_==(List((1,1,10),(1,1,20),(1,1,30),(1,5,10),(1,5,20),(1,5,30)))
        }
      }
      .sink[(Int,Int,Int)](TypedTsv[(Int,Int,Int)]("out3")) { outbuf =>
        val outMap = outbuf.groupBy { _._1 }
        "correctly do a simple leftJoin" in {
          outMap.size must be_==(3)
          outMap(0).toList.sorted must be_==(List((0,1,10),(0,2,10)))
          outMap(1).toList.sorted must be_==(List((1,1,10),(1,1,20),(1,1,30),(1,5,10),(1,5,20),(1,5,30)))
          outMap(2).toList.sorted must be_==(List((2,10,-1)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TCrossJob(args : Args) extends Job(args) {
  (TextLine("in0") cross TextLine("in1"))
    .write(TypedTsv[(String,String)]("crossed"))
}

class TypedPipeCrossTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TCrossJob" should {
    TUtil.printStack {
    JobTest(new com.twitter.scalding.TCrossJob(_))
      .source(TextLine("in0"), List((0,"you"),(1,"all")))
      .source(TextLine("in1"), List((0,"every"),(1,"body")))
      .sink[(String,String)](TypedTsv[(String,String)]("crossed")) { outbuf =>
        val sortedL = outbuf.toList.sorted
        "create a cross-product" in {
          sortedL must be_==(List(("all","body"),
            ("all","every"),
            ("you","body"),
            ("you","every")))
        }
      }
      .run
      .runHadoop
      .finish
    }
  }
}

class TGroupAllJob(args : Args) extends Job(args) {
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
    val input = List((0,"you"),(1,"all"), (2,"everybody"))
    JobTest(new TGroupAllJob(_))
      .source(TextLine("in"), input)
      .sink[String](TypedTsv[String]("out")) { outbuf =>
        val sortedL = outbuf.toList
        val correct = input.map { _._2 }.sorted
        "create sorted output" in {
          sortedL must_==(correct)
        }
      }
      .run
      .runHadoop
      .finish
    }
  }
}

class TSelfJoin(args: Args) extends Job(args) {
  val g = TypedTsv[(Int,Int)]("in").group
  g.join(g).values.write(TypedTsv[(Int,Int)]("out"))
}

class TSelfJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TSelfJoin" should {
    JobTest(new TSelfJoin(_))
      .source(TypedTsv[(Int,Int)]("in"), List((1,2), (1,3), (2,1)))
      .sink[(Int,Int)](TypedTsv[(Int,Int)]("out")) { outbuf =>
        outbuf.toList.sorted must be_==(List((1,1),(2,2),(2,3),(3,2),(3,3)))
      }
      .run
      .runHadoop
      .finish
  }
}

class TJoinWordCount(args : Args) extends Job(args) {

  def countWordsIn(pipe: TypedPipe[(String)]) = {
    pipe.flatMap { _.split("\\s+"). map(_.toLowerCase) }
      .groupBy(identity)
      .mapValueStream(input => Iterator(input.size))
      .forceToReducers
  }

  val first = countWordsIn(TypedPipe.from(TextLine("in0")))

  val second = countWordsIn(TypedPipe.from(TextLine("in1")))

  first.outerJoin(second)
    .toTypedPipe
    .map { case (word, (firstCount, secondCount)) =>
        (word, firstCount.getOrElse(0), secondCount.getOrElse(0))
    }
    .write(TypedTsv[(String,Int,Int)]("out"))
}

class TypedJoinWCTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A TJoinWordCount" should {
    TUtil.printStack {
    val in0 = List((0,"you all everybody"),(1,"a b c d"), (2,"a b c"))
    val in1 = List((0,"you"),(1,"a b c d"), (2,"a a b b c c"))
    def count(in : List[(Int,String)]) : Map[String, Int] = {
      in.flatMap { _._2.split("\\s+").map { _.toLowerCase } }.groupBy { identity }.mapValues { _.size }
    }
    def outerjoin[K,U,V](m1 : Map[K,U], z1 : U, m2 : Map[K,V], z2 : V) : Map[K,(U,V)] = {
      (m1.keys ++ m2.keys).map { k => (k, (m1.getOrElse(k, z1), m2.getOrElse(k, z2))) }.toMap
    }
    val correct = outerjoin(count(in0), 0, count(in1), 0)
      .toList
      .map { tup => (tup._1, tup._2._1, tup._2._2) }
      .sorted

    JobTest(new TJoinWordCount(_))
      .source(TextLine("in0"), in0)
      .source(TextLine("in1"), in1)
      .sink[(String,Int,Int)](TypedTsv[(String,Int,Int)]("out")) { outbuf =>
        val sortedL = outbuf.toList
        "create sorted output" in {
          sortedL must_==(correct)
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
