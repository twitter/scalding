/*
Copyright 2013 Twitter, Inc.

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

import java.io.Serializable

import com.twitter.scalding.TupleConverter
import cascading.tuple.TupleEntry

/** Closures are difficult for serialization. This class avoids that. */
sealed trait FlatMapFn[R] extends Function1[TupleEntry, TraversableOnce[R]]
  with java.io.Serializable {

  def filter(fn2: R => Boolean): FlatMapFn[R] =
    FilteredFn(this, fn2)
  def flatMap[R1](fn2: R => TraversableOnce[R1]): FlatMapFn[R1] =
    FlatMappedFn(this, fn2)
  def map[R1](fn2: R => R1): FlatMapFn[R1] =
    MapFn(this, fn2)
}

/* This is the initial way we get a FlatMapFn */
case class Converter[R](conv: TupleConverter[R]) extends FlatMapFn[R] {
  def apply(te: TupleEntry) = Iterable(conv(te))
}
// This can't take a TraversableOnce, because apply may be called many times
case class Const[R](constant: Iterable[R]) extends FlatMapFn[R] {
  def apply(te: TupleEntry) = constant
}
/* This is the mzero of this Monad */
case class Empty[R]() extends FlatMapFn[R] {
  def apply(te: TupleEntry) = Iterable.empty[R]

  override def filter(fn2: R => Boolean): FlatMapFn[R] = this
  override def flatMap[R1](fn2: R => TraversableOnce[R1]): FlatMapFn[R1] = Empty()
  override def map[R1](fn2: R => R1): FlatMapFn[R1] = Empty()
}
case class MapFn[T,R](fmap: FlatMapFn[T], fn: T => R) extends FlatMapFn[R] {
  def apply(te: TupleEntry) = fmap(te).map(fn)
}
case class FlatMappedFn[T,R](fmap: FlatMapFn[T], fn: T => TraversableOnce[R]) extends FlatMapFn[R] {
  def apply(te: TupleEntry) = fmap(te).flatMap(fn)
}
case class FilteredFn[R](fmap: FlatMapFn[R], fn: R => Boolean) extends FlatMapFn[R] {
  def apply(te: TupleEntry) = fmap(te).filter(fn)
}

