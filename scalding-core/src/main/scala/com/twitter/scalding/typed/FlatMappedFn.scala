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

/** Closures are difficult for serialization. This class avoids that. */
sealed trait FlatMapFn[-T, +U] extends Function1[T, TraversableOnce[U]]
  with java.io.Serializable {

  def filter(fn2: U => Boolean): FlatMapFn[T, U] =
    FilteredFn(this, fn2)

  def flatMap[R1](fn2: U => TraversableOnce[R1]): FlatMapFn[T, R1] =
    FlatMappedFn(this, fn2)

  def map[R1](fn2: U => R1): FlatMapFn[T, R1] =
    MapFn(this, fn2)
}

case class NoOpFlatMapFn[T]() extends FlatMapFn[T, T] {
  def apply(e: T): TraversableOnce[T] = Seq(e)
}
/* This is the mzero of this Monad */
case object Empty extends FlatMapFn[Any, Nothing] {
  def apply(te: Any) = Iterator.empty

  override def filter(fn2: Nothing => Boolean): FlatMapFn[Any, Nothing] = this
  override def flatMap[R1](fn2: Nothing => TraversableOnce[R1]): FlatMapFn[Any, Nothing] = this
  override def map[R1](fn2: Nothing => R1): FlatMapFn[Any, Nothing] = this
}
case class MapFn[T, R, U](fmap: FlatMapFn[T, R], fn: R => U) extends FlatMapFn[T, U] {
  def apply(te: T) = fmap(te).map(fn)
}
case class FlatMappedFn[T, R, U](fmap: FlatMapFn[T, R], fn: R => TraversableOnce[U]) extends FlatMapFn[T, U] {
  def apply(te: T) = fmap(te).flatMap(fn)
}
case class FilteredFn[T, U](fmap: FlatMapFn[T, U], fn: U => Boolean) extends FlatMapFn[T, U] {
  def apply(te: T) = fmap(te).filter(fn)
}
