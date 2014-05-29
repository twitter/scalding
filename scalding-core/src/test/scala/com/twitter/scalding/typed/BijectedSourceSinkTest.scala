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
package com.twitter.scalding.typed

import org.specs._

import com.twitter.scalding._

private[typed] object LongIntPacker {
   def lr(l: Int, r: Int) : Long = (l.toLong << 32) | r
   def l(rowCol: Long) = (rowCol >>> 32).toInt
   def r(rowCol: Long) = (rowCol & 0xFFFFFFFF).toInt
}

class MutatedSourceJob(args : Args) extends Job(args) {
  import com.twitter.bijection._
  val bij = new AbstractBijection[Long, (Int, Int)] {
    override def apply(x: Long) = (LongIntPacker.l(x), LongIntPacker.r(x))
    override def invert(y: (Int, Int)) = LongIntPacker.lr(y._1, y._2)
  }

  def bijectedSourceSinkBuilder(path: String): TypedSource[(Int, Int)] with TypedSink[(Int, Int)] =
    BijectedSourceSink(TypedTsv[Long](path), bij)

  val in0 = TypedPipe.from(bijectedSourceSinkBuilder("input0"))

  in0.map { tup: (Int, Int) =>
    (tup._1*2, tup._2*2)
  }
  .write(bijectedSourceSinkBuilder("output"))
}

class MutatedSourceTest extends Specification {
  import Dsl._
  "A MutatedSourceJob" should {
    "Not throw when using a converted source" in {
      JobTest(new MutatedSourceJob(_))
        .source(TypedTsv[Long]("input0"), List(5L, 8L, 12L))
        .sink[Long](TypedTsv[Long]("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered(16L) must be_==(true)
          // unordered((1,2,1L,3L)) must be_==(true)
          // unordered((2,4,2L,9L)) must be_==(true)
        }
        .run
        .runHadoop
        .finish
    }
  }
}
