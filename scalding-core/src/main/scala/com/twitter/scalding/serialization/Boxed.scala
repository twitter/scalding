/*
Copyright 2015 Twitter, Inc.

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
package com.twitter.scalding.serialization

import java.util.concurrent.atomic.AtomicReference
import java.io.{ InputStream, OutputStream }

/**
 * This interface is a way of wrapping a value in a marker class
 * whose class identity is used to control which serialization we
 * use. This is an internal implementation detail about how we
 * interact with cascading and hadoop. Users should never care.
 */
trait Boxed[+K] {
  def get: K
}

// TODO: Make more of these with a script
class Boxed0[K](override val get: K) extends Boxed[K]
class Boxed1[K](override val get: K) extends Boxed[K]
class Boxed2[K](override val get: K) extends Boxed[K]
class Boxed3[K](override val get: K) extends Boxed[K]

// TODO this could be any general bijection
case class BoxedOrderedSerialization[K](box: K => Boxed[K],
  ord: OrderedSerialization[K]) extends OrderedSerialization[Boxed[K]] {

  override def compare(a: Boxed[K], b: Boxed[K]) = ord.compare(a.get, b.get)
  override def hash(k: Boxed[K]) = ord.hash(k.get)
  override def compareBinary(a: InputStream, b: InputStream) = ord.compareBinary(a, b)
  override def read(from: InputStream) = ord.read(from).map(box)
  override def write(into: OutputStream, bk: Boxed[K]) = ord.write(into, bk.get)
}

object Boxed {
  private[this] val allBoxes = List(
    ({ t: Any => new Boxed0(t) }, classOf[Boxed0[Any]]),
    ({ t: Any => new Boxed1(t) }, classOf[Boxed1[Any]]),
    ({ t: Any => new Boxed2(t) }, classOf[Boxed2[Any]]),
    ({ t: Any => new Boxed3(t) }, classOf[Boxed3[Any]]))

  private[this] val boxes: AtomicReference[List[(Any => Boxed[Any], Class[_ <: Boxed[Any]])]] =
    new AtomicReference(allBoxes)

  def allClasses: Seq[Class[_ <: Boxed[_]]] = allBoxes.map(_._2)

  def next[K]: (K => Boxed[K], Class[Boxed[K]]) = boxes.get match {
    case list @ (h :: tail) if boxes.compareAndSet(list, tail) =>
      h.asInstanceOf[(K => Boxed[K], Class[Boxed[K]])]
    case (h :: tail) => next[K] // Try again
    case Nil => sys.error("Exhausted the boxed classes")
  }
}
