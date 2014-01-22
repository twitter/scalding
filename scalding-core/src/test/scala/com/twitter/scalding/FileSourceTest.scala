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

class MultiTsvInputJob(args: Args) extends Job(args) {
  try {
    MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)).read.write(Tsv("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }

}

class SequenceFileInputJob(args: Args) extends Job(args) {
  try {
    SequenceFile("input0").read.write(SequenceFile("output0"))
    WritableSequenceFile("input1", ('query, 'queryStats)).read.write(WritableSequenceFile("output1", ('query, 'queryStats)))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class FileSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A MultipleTsvFile Source" should {
    JobTest(new MultiTsvInputJob(_)).
      source(MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)),
              List(("foobar", 1), ("helloworld", 2))).
      sink[(String, Int)](Tsv("output0")) {
        outBuf =>
          "take multiple Tsv files as input sources" in {
            outBuf.length must be_==(2)
            outBuf.toList must be_==(List(("foobar", 1), ("helloworld", 2)))
          }
      }
      .run
      .finish
  }

  "A WritableSequenceFile Source" should {
    JobTest(new SequenceFileInputJob(_)).
      source(SequenceFile("input0"),
              List(("foobar0", 1), ("helloworld0", 2))).
      source(WritableSequenceFile("input1", ('query, 'queryStats)),
              List(("foobar1", 1), ("helloworld1", 2))).
      sink[(String, Int)](SequenceFile("output0")) {
        outBuf =>
          "sequence file input" in {
            outBuf.length must be_==(2)
            outBuf.toList must be_==(List(("foobar0", 1), ("helloworld0", 2)))
          }
      }
      .sink[(String, Int)](WritableSequenceFile("output1", ('query, 'queryStats))) {
        outBuf =>
          "writable sequence file input" in {
            outBuf.length must be_==(2)
            outBuf.toList must be_==(List(("foobar1", 1), ("helloworld1", 2)))
          }
      }
      .run
      .finish
  }
}
