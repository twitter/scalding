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

import com.twitter.scalding._
import com.twitter.scalding.source.TypedText
import org.scalatest.{ Matchers, WordSpec }

import scala.io.{ Source => ScalaSource }
import TDsl._

/* Moved from scalding-core due to fabric-specific code */

object PartitionedDelimitedTestSources {
  val singlePartition = PartitionedCsv[String, (String, String)]("out", "%s")
}

class PartitionedDelimitedWriteJob(args: Args) extends Job(args) {
  import PartitionedDelimitedTestSources._

  TypedText.csv[(String, String, String)]("in")
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
        .source(TypedText.csv[(String, String, String)]("in"), input)
        .runWithMinicluster
        .finish()

      job.mode match {
        case testMode: HadoopFamilyTestMode =>
          import TestFileUtil._
          val directory = RichDirectory(testMode.getWritePathFor(singlePartition))

          // println(s"looking at ${directory}")

          directory.list shouldBe Set("A", "B") // this proves the partition strategy WAS applied.

          val aDir = RichDirectory(directory, "A")
          val aFiles = aDir.fileNameSetExSuccess
          aFiles.size shouldBe 1
          aDir.partFiles.size shouldBe 1

          val bDir = RichDirectory(directory, "B")
          val bFiles = bDir.fileNameSetExSuccess
          bFiles.size shouldBe 1
          bDir.partFiles.size shouldBe 1

          val aSource = ScalaSource.fromFile(new File(aDir.dir, aDir.partFiles.head))
          val bSource = ScalaSource.fromFile(new File(bDir.dir, bDir.partFiles.head))

          aSource.getLines.toList shouldBe Seq("X,1", "Y,2")
          bSource.getLines.toList shouldBe Seq("Z,3")
        case _ => ???
      }

    }
  }
}
