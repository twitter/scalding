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

import cascading.tuple.{Tuple => CTuple}

import com.twitter.scalding._

import scala.collection.JavaConverters._

object Joiner extends java.io.Serializable {
  // Returns the key from the FIRST tuple. Suitable for a single JoinerClosure
  def getKeyValue[K](tupit: java.util.Iterator[CTuple]): (Option[K], Iterator[CTuple]) = {
    val stupit = tupit.asScala
    if (stupit.isEmpty) {
      (None, stupit)
    }
    else {
      val first = stupit.next
      val key = Some(first.getObject(0).asInstanceOf[K])
      val value = Iterator(Dsl.tupleAt(1)(first))
      (key, value ++ stupit.map { Dsl.tupleAt(1) })
    }
  }

  def inner2[K,V,U] = { (key: K, itv: Iterator[V], itu: () => Iterator[U]) =>
    itv.flatMap { v => itu().map { u => (v,u) } }
  }
  def asOuter[U](it : Iterator[U]) : Iterator[Option[U]] = {
    if(it.isEmpty) {
      Iterator(None)
    }
    else {
      it.map { Some(_) }
    }
  }
  def outer2[K,V,U] = { (key: K, itv: Iterator[V], itu: () => Iterator[U]) =>
    asOuter(itv).flatMap { v => asOuter(itu()).map { u => (v,u) } }
  }
  def left2[K,V,U] = { (key: K, itv: Iterator[V], itu: () => Iterator[U]) =>
    itv.flatMap { v => asOuter(itu()).map { u => (v,u) } }
  }
  def right2[K,V,U] = { (key: K, itv: Iterator[V], itu: () => Iterator[U]) =>
    asOuter(itv).flatMap { v => itu().map { u => (v,u) } }
  }
  // TODO: implement CoGroup3, and inner3, outer3 (probably best to leave the other modes as custom)
}
