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

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding._

private[typed] object LongIntPacker {
  def lr(l: Int, r: Int): Long = (l.toLong << 32) | r
  def l(rowCol: Long) = (rowCol >>> 32).toInt
  def r(rowCol: Long) = (rowCol & 0xFFFFFFFF).toInt
}

class MutatedSourceJob(args: Args) extends Job(args) {
  import com.twitter.bijection._
  implicit val bij: AbstractBijection[Long, (Int, Int)] = new AbstractBijection[Long, (Int, Int)] {
    override def apply(x: Long) = (LongIntPacker.l(x), LongIntPacker.r(x))
    override def invert(y: (Int, Int)) = LongIntPacker.lr(y._1, y._2)
  }

  val in0: TypedPipe[(Int, Int)] = TypedPipe.from(BijectedSourceSink(TypedTsv[Long]("input0")))

  in0.map { tup: (Int, Int) =>
    (tup._1 * 2, tup._2 * 2)
  }
    .write(BijectedSourceSink(TypedTsv[Long]("output")))
}

class MutatedSourceTest extends WordSpec with Matchers {
  import Dsl._
  "A MutatedSourceJob" should {
    "Not throw when using a converted source" in {
      JobTest(new MutatedSourceJob(_))
        .source(TypedTsv[Long]("input0"), List(8L, 4123423431L, 12L))
        .typedSink(TypedTsv[Long]("output")) { outBuf =>
          val unordered = outBuf.toSet
          // Size should be unchanged
          unordered should have size 3

          // Simple case, 2*8L won't run into the packer logic
          unordered should contain (16L)
          // Big one that should be in both the high and low 4 bytes of the Long
          val big = 4123423431L
          val newBig = LongIntPacker.lr(LongIntPacker.l(big) * 2, LongIntPacker.r(big) * 2)
          unordered should contain (newBig)
        }
        .run
        .runHadoop
        .finish()
    }
  }
}

class ContraMappedAndThenSourceJob(args: Args) extends Job(args) {
  TypedPipe.from(TypedTsv[Long]("input0").andThen { x => (LongIntPacker.l(x), LongIntPacker.r(x)) })
    .map { case (l, r) => (l * 2, r * 2) }
    .write(TypedTsv[Long]("output").contraMap { case (l, r) => LongIntPacker.lr(l, r) })
}

class ContraMappedAndThenSourceTest extends WordSpec with Matchers {
  import Dsl._
  "A ContraMappedAndThenSourceJob" should {
    "Not throw when using a converted source" in {
      JobTest(new ContraMappedAndThenSourceJob(_))
        .source(TypedTsv[Long]("input0"), List(8L, 4123423431L, 12L))
        .typedSink(TypedTsv[Long]("output")) { outBuf =>
          val unordered = outBuf.toSet
          // Size should be unchanged
          unordered should have size 3

          // Simple case, 2*8L won't run into the packer logic
          unordered should contain (16L)
          // Big one that should be in both the high and low 4 bytes of the Long
          val big = 4123423431L
          val newBig = LongIntPacker.lr(LongIntPacker.l(big) * 2, LongIntPacker.r(big) * 2)
          unordered should contain (newBig)
        }
        .run
        .runHadoop
        .finish()
    }
  }
}
