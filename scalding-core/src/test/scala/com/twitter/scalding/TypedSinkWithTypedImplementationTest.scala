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

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import org.apache.hadoop.conf.Configuration
import org.scalatest.{Matchers, WordSpec}

class TypedSinkWithTypedImplementation(path: String) extends TypedSink[String] {
  private val fields = new Fields(0)

  override def setter[U <: String]: TupleSetter[U] = TupleSetter.singleSetter[U]

  override def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    TypedPipe.from[String](pipe, fields).write(TypedTsv[String](path))
    pipe
  }
}

class TypedSinkWithTypedImplementationRecursive(path: String) extends TypedSink[String] {
  private val fields = new Fields(0)

  override def setter[U <: String]: TupleSetter[U] = TupleSetter.singleSetter[U]

  override def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    TypedPipe.from[String](pipe, fields).write(new TypedSinkWithTypedImplementationRecursive(path))
    pipe
  }
}

class TypedSinkWithTypedImplementationJob(args: Args) extends Job(args) {
  TypedPipe.from(List("test"))
    .write(new TypedSinkWithTypedImplementation("output"))
}

class TypedSinkWithTypedImplementationTest extends WordSpec with Matchers {
  "A TypedSinkWithTypedImplementationJob" should {
    "should produce correct results" in {
      JobTest(new TypedSinkWithTypedImplementationJob(_))
        .sink[String](TypedTsv[String]("output"))(_.toList == List("test"))
        .runHadoop
        .finish()
    }
  }

  "A TypedSinkWithTypedImplementation" should {
    "should work with .writeExecution" in {
      val elements = List("test")
      val elementsFromExecution = TypedPipe.from(elements)
        .writeExecution(new TypedSinkWithTypedImplementation("output"))
        .flatMap(_ => TypedPipe.from(TypedTsv[String]("output")).toIterableExecution)
        .waitFor(Config.default, HadoopTest(new Configuration(), _ => None))
        .get
        .toList

      assert(elements == elementsFromExecution)
    }
  }

  "A TypedSinkWithTypedImplementation" should {
    "should work with Execution.fromFn" in {
      val elements = List("test")
      val elementsFromExecution = Execution.fromFn { case (confArg, modeArg) =>
        implicit val flowDef = new FlowDef
        implicit val mode = modeArg
        TypedPipe.from(elements).write(new TypedSinkWithTypedImplementation("output"))
        flowDef
      }
        .flatMap(_ => TypedPipe.from(TypedTsv[String]("output")).toIterableExecution)
        .waitFor(Config.default, HadoopTest(new Configuration(), _ => None))
        .get
        .toList

      assert(elements == elementsFromExecution)
    }
  }

  "A TypedSinkWithTypedImplementationRecursive" should {
    "should fail" in {
      assert(TypedPipe.from(List("test"))
        .writeExecution(new TypedSinkWithTypedImplementationRecursive("output"))
        .waitFor(Config.default, HadoopTest(new Configuration(), _ => None))
        .isFailure)
    }
  }
}
