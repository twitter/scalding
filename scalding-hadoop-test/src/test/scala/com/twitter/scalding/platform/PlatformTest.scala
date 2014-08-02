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

import org.specs._

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
class PlatformTests extends Specification {

  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.ERROR)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.ERROR)

  noDetailedDiffs() //Fixes an issue with scala 2.9

  "An InAndOutTest" should {
    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    val inAndOut = Seq("a", "b", "c")

    "reading then writing shouldn't change the data" in {
      HadoopPlatformJobTest(new InAndOutJob(_), cluster)
        .source("input", inAndOut)
        .sink[String]("output") { _.toSet must_== inAndOut.toSet }
        .run
    }

    doLast { cluster.shutdown() }
  }

  "A TinyJoinAndMergeJob" should {
    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    import TinyJoinAndMergeJob._

    "merge and joinWithTiny shouldn't duplicate data" in {
      HadoopPlatformJobTest(new TinyJoinAndMergeJob(_), cluster)
        .source(peopleInput, peopleData)
        .source(messageInput, messageData)
        .sink(output) { _.toSet must_== outputData.toSet }
        .run
    }

    doLast { cluster.shutdown() }
  }

  "A TsvNoCacheJob" should {
    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    import TsvNoCacheJob._

    "Writing to a tsv in a flow shouldn't effect the output" in {
      HadoopPlatformJobTest(new TsvNoCacheJob(_), cluster)
        .source(dataInput, data)
        .sink(typedThrowAwayOutput) { _.toSet.size must_== 4 }
        .sink(typedRealOutput) { _.map{ f: Float => (f * 10).toInt }.toList must_== outputData.map{ f: Float => (f * 10).toInt }.toList }
        .run
    }

    doLast { cluster.shutdown() }
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

class IterableSourceDistinctTest extends Specification {
  noDetailedDiffs()

  "A IterableSource" should {
    import IterableSourceDistinctJob._

    val cluster = LocalCluster()
    doFirst { cluster.initialize() }

    setSequential()

    "distinct properly from normal data" in {
      HadoopPlatformJobTest(new NormalDistinctJob(_), cluster)
        .source[String]("input", data ++ data ++ data)
        .sink[String]("output") { _.toList must_== data }
        .run
    }

    "distinctBy(identity) properly from a list in memory" in {
      HadoopPlatformJobTest(new IterableSourceDistinctIdentityJob(_), cluster)
        .sink[String]("output") { _.toList must_== data }
        .run
    }

    "distinct properly from a list" in {
      HadoopPlatformJobTest(new IterableSourceDistinctJob(_), cluster)
        .sink[String]("output") { _.toList must_== data }
        .run
    }

    doLast { cluster.shutdown() }
  }
}
