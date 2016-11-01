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

import org.scalatest.{ Matchers, WordSpec }

import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source.fromFile
import java.io.File
import cascading.cascade.Cascade
import cascading.flow.FlowSkipIfSinkNotStale

class Job1(args: Args) extends Job(args) {
  Tsv(args("input0"), ('line)).pipe.map[String, String]('line -> 'line)((x: String) => "job1:" + x).write(Tsv(args("output0"), fields = 'line))
}

class Job2(args: Args) extends Job(args) {
  Tsv(args("output0"), ('line)).pipe.map[String, String]('line -> 'line)((x: String) => "job2" + x).write(Tsv(args("output1")))
}

class CascadeTestJob(args: Args) extends CascadeJob(args) {

  val jobs = List(new Job1(args), new Job2(args))

  override def preProcessCascade(cascade: Cascade) = {
    cascade.setFlowSkipStrategy(new FlowSkipIfSinkNotStale())
  }

  override def postProcessCascade(cascade: Cascade) = {
    println(cascade.getCascadeStats())
  }

}

class TwoPhaseCascadeTest extends WordSpec with Matchers with FieldConversions {
  "A Cascade job" should {
    CascadeTest("com.twitter.scalding.CascadeTestJob")
      .arg("input0", "input0")
      .arg("output0", "output0")
      .arg("output1", "output1")
      .source(Tsv("input0", ('line)), List(Tuple1("line1"), Tuple1("line2"), Tuple1("line3"), Tuple1("line4")))
      .sink[String](Tsv("output1")) { ob =>
        "verify output got changed by both flows" in {
          ob.toList shouldBe List("job2job1:line1", "job2job1:line2", "job2job1:line3", "job2job1:line4")
        }
      }
      .runHadoop
      .finish()
  }

  "A Cascade job run though Tool.main" should {
    val input0 = File.createTempFile("cascading-job-input0-", "")
    input0.mkdirs()

    val writer = new BufferedWriter(new FileWriter(input0));
    writer.write("a\nb\nc\nd\ne");
    writer.close();

    val output0 = File.createTempFile("cascading-job-output0-", "")
    output0.mkdir()

    val output1 = File.createTempFile("cascading-job-output1-", "")
    output1.mkdir()

    val args = Array[String]("com.twitter.scalding.CascadeTestJob", "--local",
      "--input0", input0.getAbsolutePath,
      "--output0", output0.getAbsolutePath,
      "--output1", output1.getAbsolutePath)

    Tool.main(args)

    val lines = fromFile(output1.getAbsolutePath).getLines.toList

    "verify output got changed by both flows" in {
      lines shouldBe List("job2job1:a", "job2job1:b", "job2job1:c", "job2job1:d", "job2job1:e")
    }

    input0.delete()
    output0.delete()
    output1.delete()

    ()
  }
}
