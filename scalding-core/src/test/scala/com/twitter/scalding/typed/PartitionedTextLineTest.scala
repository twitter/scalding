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

object PartitionedTextLineTestSources {
  val singlePartition = PartitionedTextLine[String]("out", "%s")
  val multiplePartition = PartitionedTextLine[(String, String)]("out", "%s/%s")
}

class PartitionedTextLineSingleWriteJob(args: Args) extends Job(args) {
  import PartitionedTextLineTestSources._
  TypedCsv[(String, String)]("in").write(singlePartition)
}

class PartitionedTextLineMultipleWriteJob(args: Args) extends Job(args) {
  import PartitionedTextLineTestSources._
  TypedCsv[(String, String, String)]("in")
    .map { case (v1, v2, v3) => ((v1, v2), v3) }
    .write(multiplePartition)
}

class PartitionedTextLineTest extends WordSpec with Matchers {
  import PartitionedTextLineTestSources._

  "PartitionedTextLine" should {
    "be able to split output by a single partition" in {
      val input = Seq(("A", "1"), ("A", "2"), ("B", "3"))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new PartitionedTextLineSingleWriteJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(TypedCsv[(String, String)]("in"), input)
        .runHadoop
        .finish()

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(singlePartition))
      println(directory)

      directory.listFiles().map({ _.getName() }).toSet shouldBe Set("A", "B")

      val aSource = ScalaSource.fromFile(new File(directory, "A/part-00000-00000"))
      val bSource = ScalaSource.fromFile(new File(directory, "B/part-00000-00001"))

      aSource.getLines.toList shouldBe Seq("1", "2")
      bSource.getLines.toList shouldBe Seq("3")
    }
    "be able to split output by multiple  partitions" in {
      val input = Seq(("A", "X", "1"), ("A", "Y", "2"), ("B", "Z", "3"))

      // Need to save the job to allow, find the temporary directory data was written to
      var job: Job = null;
      def buildJob(args: Args): Job = {
        job = new PartitionedTextLineMultipleWriteJob(args)
        job
      }

      JobTest(buildJob(_))
        .source(TypedCsv[(String, String, String)]("in"), input)
        .runHadoop
        .finish()

      val testMode = job.mode.asInstanceOf[HadoopTest]

      val directory = new File(testMode.getWritePathFor(multiplePartition))
      println(directory)

      directory.listFiles.flatMap(d => d.listFiles.map(d.getName + "/" + _.getName)).toSet shouldBe Set("A/X", "A/Y", "B/Z")

      val axSource = ScalaSource.fromFile(new File(directory, "A/X/part-00000-00000"))
      val aySource = ScalaSource.fromFile(new File(directory, "A/Y/part-00000-00001"))
      val bzSource = ScalaSource.fromFile(new File(directory, "B/Z/part-00000-00002"))

      axSource.getLines.toList shouldBe Seq("1")
      aySource.getLines.toList shouldBe Seq("2")
      bzSource.getLines.toList shouldBe Seq("3")
    }
  }
}
