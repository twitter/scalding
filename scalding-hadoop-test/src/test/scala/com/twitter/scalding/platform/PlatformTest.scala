/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.platform

import com.twitter.scalding._

import org.scalatest.{ Matchers, WordSpec }

class InAndOutJob(args: Args) extends Job(args) {
  Tsv("input").read.write(Tsv("output"))
}

object TinyJoinAndMergeJob {
  val peopleInput = TypedTsv[Int]("input1")
  val peopleData = List(1, 2, 3, 4)

  val messageInput = TypedTsv[Int]("input2")
  val messageData = List(1, 2, 3)

  val output = TypedTsv[(Int, Int)]("output")
  val outputData = List((1, 2), (2, 2), (3, 2), (4, 1))
}

class TinyJoinAndMergeJob(args: Args) extends Job(args) {
  import TinyJoinAndMergeJob._

  val people = peopleInput.read.mapTo(0 -> 'id) { v: Int => v }

  val messages = messageInput.read
    .mapTo(0 -> 'id) { v: Int => v }
    .joinWithTiny('id -> 'id, people)

  (messages ++ people).groupBy('id) { _.size('count) }.write(output)
}

object TsvNoCacheJob {
  val dataInput = TypedTsv[String]("fakeInput")
  val data = List("-0.2f -0.3f -0.5f", "-0.1f", "-0.5f")

  val throwAwayOutput = Tsv("output1")
  val typedThrowAwayOutput = TypedTsv[Float]("output1")
  val realOuput = Tsv("output2")
  val typedRealOutput = TypedTsv[Float]("output2")
  val outputData = List(-0.5f, -0.2f, -0.3f, -0.1f).sorted
}
class TsvNoCacheJob(args: Args) extends Job(args) {
  import TsvNoCacheJob._
  dataInput.read
    .flatMap(new cascading.tuple.Fields(Integer.valueOf(0)) -> 'word){ line: String => line.split("\\s") }
    .groupBy('word){ group => group.size }
    .mapTo('word -> 'num) { (w: String) => w.toFloat }
    .write(throwAwayOutput)
    .groupAll { _.sortBy('num) }
    .write(realOuput)
}

// Keeping all of the specifications in the same tests puts the result output all together at the end.
// This is useful given that the Hadoop MiniMRCluster and MiniDFSCluster spew a ton of logging.
class PlatformTests extends WordSpec with Matchers with HadoopSharedPlatformTest {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.ERROR)

  "An InAndOutTest" should {
    val inAndOut = Seq("a", "b", "c")

    "reading then writing shouldn't change the data" in {
      HadoopPlatformJobTest(new InAndOutJob(_), cluster)
        .source("input", inAndOut)
        .sink[String]("output") { _.toSet shouldBe (inAndOut.toSet) }
        .run
    }
  }

  "A TinyJoinAndMergeJob" should {
    import TinyJoinAndMergeJob._

    "merge and joinWithTiny shouldn't duplicate data" in {
      HadoopPlatformJobTest(new TinyJoinAndMergeJob(_), cluster)
        .source(peopleInput, peopleData)
        .source(messageInput, messageData)
        .sink(output) { _.toSet shouldBe (outputData.toSet) }
        .run
    }
  }

  "A TsvNoCacheJob" should {
    import TsvNoCacheJob._

    "Writing to a tsv in a flow shouldn't effect the output" in {
      HadoopPlatformJobTest(new TsvNoCacheJob(_), cluster)
        .source(dataInput, data)
        .sink(typedThrowAwayOutput) { _.toSet should have size 4 }
        .sink(typedRealOutput) { _.map{ f: Float => (f * 10).toInt }.toList shouldBe (outputData.map{ f: Float => (f * 10).toInt }.toList) }
        .run
    }
  }
}

object IterableSourceDistinctJob {
  val data = List("a", "b", "c")
}

class IterableSourceDistinctJob(args: Args) extends Job(args) {
  import IterableSourceDistinctJob._

  TypedPipe.from(data ++ data ++ data).distinct.write(TypedTsv("output"))
}

class IterableSourceDistinctIdentityJob(args: Args) extends Job(args) {
  import IterableSourceDistinctJob._

  TypedPipe.from(data ++ data ++ data).distinctBy(identity).write(TypedTsv("output"))
}

class NormalDistinctJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[String]("input")).distinct.write(TypedTsv("output"))
}

class IterableSourceDistinctTest extends WordSpec with Matchers with HadoopPlatformTest {
  "A IterableSource" should {
    import IterableSourceDistinctJob._

    "distinct properly from normal data" in {
      HadoopPlatformJobTest(new NormalDistinctJob(_), cluster)
        .source[String]("input", data ++ data ++ data)
        .sink[String]("output") { _.toList shouldBe data }
        .run
    }

    "distinctBy(identity) properly from a list in memory" in {
      HadoopPlatformJobTest(new IterableSourceDistinctIdentityJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run
    }

    "distinct properly from a list" in {
      HadoopPlatformJobTest(new IterableSourceDistinctJob(_), cluster)
        .sink[String]("output") { _.toList shouldBe data }
        .run
    }
  }
}

object MultipleGroupByJobData {
  val data: List[String] = {
    val rnd = new scala.util.Random(22)
    (0 until 20).map { _ => rnd.nextLong.toString }.toList
  }.distinct
}

class MultipleGroupByJob(args: Args) extends Job(args) {
  import com.twitter.scalding.serialization._
  import MultipleGroupByJobData._
  implicit val stringOrdSer = new StringOrderedSerialization()
  implicit val stringTup2OrdSer = new OrderedSerialization2(stringOrdSer, stringOrdSer)
  val otherStream = TypedPipe.from(data).map{ k => (k, k) }.group

  TypedPipe.from(data)
    .map{ k => (k, 1L) }
    .group[String, Long](implicitly, stringOrdSer)
    .sum
    .map {
      case (k, _) =>
        ((k, k), 1L)
    }
    .sumByKey[(String, String), Long](implicitly, stringTup2OrdSer, implicitly)
    .map(_._1._1)
    .map { t =>
      (t.toString, t)
    }
    .group
    .leftJoin(otherStream)
    .map(_._1)
    .write(TypedTsv("output"))

}

class MultipleGroupByJobTest extends WordSpec with Matchers with HadoopPlatformTest {
  "A grouped job" should {
    import MultipleGroupByJobData._

    "do some ops and not stamp on each other ordered serializations" in {
      HadoopPlatformJobTest(new MultipleGroupByJob(_), cluster)
        .source[String]("input", data)
        .sink[String]("output") { _.toSet shouldBe data.map(_.toString).toSet }
        .run
    }

  }
}
