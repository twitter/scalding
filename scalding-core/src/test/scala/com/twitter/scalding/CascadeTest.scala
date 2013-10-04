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
import cascading.cascade.Cascade
import cascading.flow.FlowSkipIfSinkNotStale
import cascading.tuple.Fields

class Job1(args : Args) extends Job(args) {
    Tsv("input0", ('line)).pipe.map[String, String]('line -> 'line)( (x: String) => "job1:"+x).write(Tsv("output0", fields='line ) )
}

class Job2(args : Args) extends Job(args) {
    Tsv("output0", ('line)).pipe.map[String, String]('line -> 'line)( (x: String) => "job2"+x).write(Tsv("output1"))
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

class TwoPhaseCascadeTest extends Specification with FieldConversions {
  "A Cascade job" should {
    CascadeTest("com.twitter.scalding.CascadeTestJob").
      source(Tsv("input0", ('line)), List(Tuple1("line1"), Tuple1("line2"), Tuple1("line3"), Tuple1("line4")))
      .sink[String](Tsv("output1")) { ob =>
        "verify output got changed by both flows" in {
          ob.toList must_== List("job2job1:line1", "job2job1:line2", "job2job1:line3", "job2job1:line4")
        }
      }
      .runHadoop
      .finish
  }
}

