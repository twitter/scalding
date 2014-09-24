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

import com.twitter.scalding._

object Joiner extends java.io.Serializable {
  def toCogroupJoiner2[K, V, U, R](hashJoiner: (K, V, Iterable[U]) => Iterator[R]): (K, Iterator[V], Iterable[U]) => Iterator[R] = {
    (k: K, itv: Iterator[V], itu: Iterable[U]) =>
      itv.flatMap { hashJoiner(k, _, itu) }
  }

  def hashInner2[K, V, U] = { (key: K, v: V, itu: Iterable[U]) => itu.iterator.map { (v, _) } }
  def hashLeft2[K, V, U] = { (key: K, v: V, itu: Iterable[U]) => asOuter(itu.iterator).map { (v, _) } }

  def inner2[K, V, U] = { (key: K, itv: Iterator[V], itu: Iterable[U]) =>
    itv.flatMap { v => itu.map { u => (v, u) } }
  }
  def asOuter[U](it: Iterator[U]): Iterator[Option[U]] = {
    if (it.isEmpty) {
      Iterator(None)
    } else {
      it.map { Some(_) }
    }
  }
  def outer2[K, V, U] = { (key: K, itv: Iterator[V], itu: Iterable[U]) =>
    if (itv.isEmpty && itu.isEmpty) {
      Iterator.empty
    } else {
      asOuter(itv).flatMap { v => asOuter(itu.iterator).map { u => (v, u) } }
    }
  }
  def left2[K, V, U] = { (key: K, itv: Iterator[V], itu: Iterable[U]) =>
    itv.flatMap { v => asOuter(itu.iterator).map { u => (v, u) } }
  }
  def right2[K, V, U] = { (key: K, itv: Iterator[V], itu: Iterable[U]) =>
    asOuter(itv).flatMap { v => itu.map { u => (v, u) } }
  }
}

