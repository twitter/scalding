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

class InAndOutTest extends Specification {
  val inAndOut = Seq("a", "b", "c")
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "An InAndOutTest" should {
    HadoopPlatformJobTest(new InAndOutJob(_))
      .createData("input", inAndOut)
      .expect("output") { _ must_== inAndOut }
      .run
  }
}

object TinyJoinAndMergeJob {
  val peopleInput = Tsv("input1")
  val peopleData = List(1, 2, 3, 4).map { _.toString }

  val messageInput = Tsv("input2")
  val messageData = List(1, 2, 3).map { _.toString }

  val output = Tsv("output")
}

//TODO make sure that this doesn't affect the typed api as well
class TinyJoinAndMergeJob(args: Args) extends Job(args) {
  import TinyJoinAndMergeJob._

  val people = peopleInput.read.mapTo(0-> 'id) { v: Int => v}

  //TODO we need this logic to be spread over multiple mappers. Can we do that in local mode?
  val messages = messageInput.read
      .mapTo(0 -> 'id) { v: Int => v }
      .joinWithTiny('id -> 'id, people)

  (messages ++ people).groupBy('id) { _.size('count) }.write(output)
}
//TODO in runHadoop or run can we synthetically replicate the many mappers?
class TinyJoinAndMergeTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TinyJoinAndMergeJob" should {
    import TinyJoinAndMergeJob._

    HadoopPlatformJobTest(new TinyJoinAndMergeJob(_))
      .createData("input1", peopleData)
      .createData("input2", messageData)
      .expect("output") { lines =>
        lines must_== messageData.map { _.toString + ", 1" }
      }
      .run
  }
}
