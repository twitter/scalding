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
package com.twitter

import org.apache.hadoop.fs.{ Path, PathFilter }

package object scalding {
  /**
   * The objects for the Typed-API live in the scalding.typed package
   * but are aliased here.
   */
  val TDsl = com.twitter.scalding.typed.TDsl
  val TypedPipe = com.twitter.scalding.typed.TypedPipe
  type TypedPipe[+T] = com.twitter.scalding.typed.TypedPipe[T]
  type TypedSink[-T] = com.twitter.scalding.typed.TypedSink[T]
  type TypedSource[+T] = com.twitter.scalding.typed.TypedSource[T]
  type KeyedList[K, +V] = com.twitter.scalding.typed.KeyedList[K, V]
  type ValuePipe[+T] = com.twitter.scalding.typed.ValuePipe[T]
  type Grouped[K, +V] = com.twitter.scalding.typed.Grouped[K, V]

  /**
   * Make sure this is in sync with version.sbt
   */
  val scaldingVersion: String = "0.17.0"

  object RichPathFilter {
    implicit def toRichPathFilter(f: PathFilter) = new RichPathFilter(f)
  }

  class RichPathFilter(f: PathFilter) {

    def and(filters: PathFilter*): PathFilter = {
      new AndPathFilter(Seq(f) ++ filters)
    }

    def or(filters: PathFilter*): PathFilter = {
      new OrPathFilter(Seq(f) ++ filters)
    }

    def not: PathFilter = {
      new NotPathFilter(f)
    }

  }

  private[this] class AndPathFilter(filters: Seq[PathFilter]) extends PathFilter {
    override def accept(p: Path): Boolean = {
      filters.forall(_.accept(p))
    }
  }

  private[this] class OrPathFilter(filters: Seq[PathFilter]) extends PathFilter {
    override def accept(p: Path): Boolean = {
      filters.exists(_.accept(p))
    }
  }

  private[this] class NotPathFilter(filter: PathFilter) extends PathFilter {
    override def accept(p: Path): Boolean = {
      !filter.accept(p)
    }
  }
}
