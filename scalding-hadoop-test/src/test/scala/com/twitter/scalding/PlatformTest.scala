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

import cascading.tuple.Fields
import cascading.tuple.TupleEntry
import java.util.concurrent.TimeUnit

import org.specs._
import java.lang.{Integer => JInt}

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

  val people = peopleInput.read.mapTo(0-> 'id) { v: Int => v}

  //TODO we need this logic to be spread over multiple mappers. Can we do that in local mode?
  val messages = messageInput.read
      .mapTo(0 -> 'id) { v: Int => v }
      .joinWithTiny('id -> 'id, people)

  (messages ++ people).groupBy('id) { _.size('count) }.write(output)
}


class SharedLocalClusterTest extends Specification {
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
}
