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

object RangedArgs {
  implicit def rangedFromArgs(args: Args): RangedArgs = new RangedArgs(args)
}

case class Range[T](lower: T, upper: T)(implicit ord: Ordering[T]) {
  assert(ord.lteq(lower, upper), "Bad range: " + lower + " > " + upper)

  def assertLowerBound(min: T): Unit = {
    assert(ord.lteq(min, lower), "Range out of bounds: " + lower + " < " + min)
  }

  def assertUpperBound(max: T): Unit = {
    assert(ord.gteq(max, upper), "Range out of bounds: " + upper + " > " + max)
  }

  def assertBounds(min: T, max: T): Unit = {
    assertLowerBound(min)
    assertUpperBound(max)
  }

  def mkString(sep: String) = {
    if (ord.equiv(lower, upper)) {
      lower.toString
    } else {
      lower.toString + sep + upper.toString
    }
  }
}

class RangedArgs(args: Args) {
  def range[T](argName: String)(cnv: String => T)(implicit ord: Ordering[T]): Range[T] = args.list(argName) match {
    case List(v) =>
      Range(cnv(v), cnv(v))
    case List(v1, v2) =>
      Range(cnv(v1), cnv(v2))
    case _ =>
      throw new IllegalArgumentException(argName + " must have either 1 or 2 values specified")
  }
}
