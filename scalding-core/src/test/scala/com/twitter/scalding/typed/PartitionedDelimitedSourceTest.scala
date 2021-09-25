//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.twitter.scalding.typed

import java.io.File

import scala.io.{ Source => ScalaSource }

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._
import TDsl._

object PartitionedDelimitedTestSources {
  val singlePartition = PartitionedCsv[String, (String, String)]("out", "%s")
}

class PartitionedDelimitedWriteJob(args: Args) extends Job(args) {
  import PartitionedDelimitedTestSources._
  TypedCsv[(String, String, String)]("in")
    .map { case (v1, v2, v3) => (v1, (v2, v3)) }
    .write(singlePartition)
}

class PartitionedDelimitedTest extends WordSpec with Matchers {
  import PartitionedDelimitedTestSources._

  "PartitionedDelimited" should {
    "write out CSVs" in {
      val input = Seq(("A", "X", "1"), ("A", "Y", "2"), ("B", "Z", "3"))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new PartitionedDelimitedWriteJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(TypedCsv[(String, String, String)]("in"), input)
        .runHadoop
        .finish()

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(singlePartition))

      directory.listFiles().map({ _.getName() }).toSet shouldBe Set("A", "B")

      val aSource = ScalaSource.fromFile(new File(directory, "A/part-00000-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "B/part-00000-00001"))

      aSource.getLines.toList shouldBe Seq("X,1", "Y,2")
      bSource.getLines.toList shouldBe Seq("Z,3")
    }
  }
}
