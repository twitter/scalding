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

package com.twitter.scalding.commons.extensions

import com.twitter.scalding._
import org.scalatest.WordSpec
import scala.collection.mutable.Buffer

/**
 * @author Mike Jahr
 */

class CheckpointJob(args: Args) extends Job(args) {
  implicit val implicitArgs: Args = args

  def in0 = Checkpoint[(Int, Int, Int)]("c0", ('x0, 'y0, 's0)) {
    Tsv("input0").read.mapTo((0, 1, 2) -> ('x0, 'y0, 's0)) { x: (Int, Int, Int) => x }
  }
  def in1 = Checkpoint[(Int, Int, Int)]("c1", ('x1, 'y1, 's1)) {
    Tsv("input1").read.mapTo((0, 1, 2) -> ('x1, 'y1, 's1)) { x: (Int, Int, Int) => x }
  }
  def out = Checkpoint[(Int, Int, Int)]("c2", ('x0, 'x1, 'score)) {
    in0
      .joinWithSmaller('y0 -> 'y1, in1)
      .map(('s0, 's1) -> 'score) { v: (Int, Int) => v._1 * v._2 }
      .groupBy('x0, 'x1) { _.sum[Double]('score) }
  }

  out.write(Tsv("output"))
}

class TypedCheckpointJob(args: Args) extends Job(args) {
  import TDsl._
  implicit val implicitArgs: Args = args

  def in0 = Checkpoint[(Int, Int, Int)]("c0") {
    TypedTsv[(Int, Int, Int)]("input0").map(x => x)
  }
  def in1 = Checkpoint[(Int, Int, Int)]("c1"){
    TypedTsv[(Int, Int, Int)]("input1").map(x => x)
  }
  def out = Checkpoint[(Int, Int, Double)]("c2") {
    in0.groupBy(_._2)
      .join(in1.groupBy(_._2))
      .mapValues{ case (l, r) => ((l._1, r._1), (l._3 * r._3).toDouble) }
      .values
      .group
      .sum
      .map{ tup => (tup._1._1, tup._1._2, tup._2) } // super ugly, don't do this in a real job
  }

  out.write(TypedTsv[(Int, Int, Double)]("output"))
}

class CheckpointSpec extends WordSpec {
  "A CheckpointJob" should {
    val in0 = Set((0, 0, 1), (0, 1, 1), (1, 0, 2), (2, 0, 4))
    val in1 = Set((0, 1, 1), (1, 0, 2), (2, 4, 5))
    val out = Set((0, 1, 2.0), (0, 0, 1.0), (1, 1, 4.0), (2, 1, 8.0))

    // Verifies output when passed as a callback to JobTest.sink().
    def verifyOutput[A](expectedOutput: Set[A], actualOutput: Buffer[A]): Unit =
      assert(actualOutput.toSet === expectedOutput)

    // Runs a test in both local test and hadoop test mode, verifies the final
    // output, and clears the local file set.
    def runTest(test: JobTest) =
      // runHadoop seems to have trouble with sequencefile format; use TSV.
      test
        .arg("checkpoint.format", "tsv")
        .sink[(Int, Int, Double)](Tsv("output"))(verifyOutput(out, _))
        .run
        .runHadoop
        .finish()

    "run without checkpoints" in runTest {
      JobTest(new CheckpointJob(_))
        .source(Tsv("input0"), in0)
        .source(Tsv("input1"), in1)
    }

    "read c0, write c1 and c2" in runTest {
      // Adding filenames to Checkpoint.testFileSet makes Checkpoint think that
      // they exist.
      JobTest(new CheckpointJob(_))
        .arg("checkpoint.file", "test")
        .registerFile("test_c0")
        .source(Tsv("test_c0"), in0)
        .source(Tsv("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c1"))(verifyOutput(in1, _))
        .sink[(Int, Int, Double)](Tsv("test_c2"))(verifyOutput(out, _))
    }

    "read c2, skipping c0 and c1" in runTest {
      JobTest(new CheckpointJob(_))
        .arg("checkpoint.file", "test")
        .registerFile("test_c2")
        .source(Tsv("test_c2"), out)
    }

    "clobber c0" in runTest {
      JobTest(new CheckpointJob(_))
        .arg("checkpoint.file.c0", "test_c0")
        .arg("checkpoint.clobber", "")
        .registerFile("test_c0")
        .source(Tsv("input0"), in0)
        .source(Tsv("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c0"))(verifyOutput(in0, _))
    }

    "read c0 and clobber c1" in runTest {
      JobTest(new CheckpointJob(_))
        .arg("checkpoint.file", "test")
        .arg("checkpoint.clobber.c1", "")
        .registerFile("test_c0")
        .registerFile("test_c1")
        .source(Tsv("test_c0"), in0)
        .source(Tsv("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c1"))(verifyOutput(in1, _))
        .sink[(Int, Int, Double)](Tsv("test_c2"))(verifyOutput(out, _))
    }
  }
}

class TypedCheckpointSpec extends WordSpec {
  "A TypedCheckpointJob" should {
    val in0 = Set((0, 0, 1), (0, 1, 1), (1, 0, 2), (2, 0, 4))
    val in1 = Set((0, 1, 1), (1, 0, 2), (2, 4, 5))
    val out = Set((0, 1, 2.0), (0, 0, 1.0), (1, 1, 4.0), (2, 1, 8.0))

    // Verifies output when passed as a callback to JobTest.sink().
    def verifyOutput[A](expectedOutput: Set[A], actualOutput: Buffer[A]): Unit =
      assert(actualOutput.toSet === expectedOutput)

    // Runs a test in both local test and hadoop test mode, verifies the final
    // output, and clears the local file set.
    def runTest(test: JobTest) =
      // runHadoop seems to have trouble with sequencefile format; use TSV.
      test
        .arg("checkpoint.format", "tsv")
        .sink[(Int, Int, Double)](TypedTsv[(Int, Int, Double)]("output"))(verifyOutput(out, _))
        .run
        .runHadoop
        .finish()

    "run without checkpoints" in runTest {
      JobTest(new TypedCheckpointJob(_))
        .source(TypedTsv[(Int, Int, Int)]("input0"), in0)
        .source(TypedTsv[(Int, Int, Int)]("input1"), in1)
    }

    "read c0, write c1 and c2" in runTest {
      // Adding filenames to Checkpoint.testFileSet makes Checkpoint think that
      // they exist.
      JobTest(new TypedCheckpointJob(_))
        .arg("checkpoint.file", "test")
        .registerFile("test_c0")
        .source(Tsv("test_c0"), in0)
        .source(TypedTsv[(Int, Int, Int)]("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c1"))(verifyOutput(in1, _))
        .sink[(Int, Int, Double)](Tsv("test_c2"))(verifyOutput(out, _))
    }

    "read c2, skipping c0 and c1" in runTest {
      JobTest(new TypedCheckpointJob(_))
        .arg("checkpoint.file", "test")
        .registerFile("test_c2")
        .source(Tsv("test_c2"), out)
    }

    "clobber c0" in runTest {
      JobTest(new TypedCheckpointJob(_))
        .arg("checkpoint.file.c0", "test_c0")
        .arg("checkpoint.clobber", "")
        .registerFile("test_c0")
        .source(TypedTsv[(Int, Int, Int)]("input0"), in0)
        .source(TypedTsv[(Int, Int, Int)]("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c0"))(verifyOutput(in0, _))
    }

    "read c0 and clobber c1" in runTest {
      JobTest(new TypedCheckpointJob(_))
        .arg("checkpoint.file", "test")
        .arg("checkpoint.clobber.c1", "")
        .registerFile("test_c0")
        .registerFile("test_c1")
        .source(Tsv("test_c0"), in0)
        .source(TypedTsv[(Int, Int, Int)]("input1"), in1)
        .sink[(Int, Int, Int)](Tsv("test_c1"))(verifyOutput(in1, _))
        .sink[(Int, Int, Double)](Tsv("test_c2"))(verifyOutput(out, _))
    }
  }
}
