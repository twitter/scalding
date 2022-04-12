/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

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

package com.twitter.scalding.dagon

import com.twitter.scalding.dagon.ScalaVersionCompat.{lazyListFromIterator, LazyList}

import java.io.Serializable

/**
 * This is a weak heterogenous map. It uses equals on the keys, so it is your responsibilty that if k: K[_] ==
 * k2: K[_] then the types are actually equal (either be careful or store a type identifier).
 */
final class HMap[K[_], V[_]](protected val map: Map[K[_], V[_]]) extends Serializable {

  type Pair[t] = (K[t], V[t])

  override def toString: String =
    "H%s".format(map)

  override def equals(that: Any): Boolean =
    that match {
      case null          => false
      case h: HMap[_, _] => map.equals(h.map)
      case _             => false
    }

  override def hashCode: Int =
    map.hashCode

  def updated[T](k: K[T], v: V[T]): HMap[K, V] =
    HMap.from[K, V](map.updated(k, v))

  def +[T](kv: (K[T], V[T])): HMap[K, V] =
    HMap.from[K, V](map + kv)

  def ++(other: HMap[K, V]): HMap[K, V] =
    HMap.from[K, V](map ++ other.map)

  def -(k: K[_]): HMap[K, V] =
    HMap.from[K, V](map - k)

  def apply[T](id: K[T]): V[T] =
    map(id).asInstanceOf[V[T]]

  def get[T](id: K[T]): Option[V[T]] =
    map.get(id).asInstanceOf[Option[V[T]]]

  def contains[T](id: K[T]): Boolean =
    map.contains(id)

  def isEmpty: Boolean = map.isEmpty

  def size: Int = map.size

  def forallKeys(p: K[_] => Boolean): Boolean =
    map.forall { case (k, _) => p(k) }

  def filterKeys(p: K[_] => Boolean): HMap[K, V] =
    HMap.from[K, V](map.filter { case (k, _) => p(k) })

  def keySet: Set[K[_]] =
    map.keySet

  def keysOf[T](v: V[T]): Set[K[T]] =
    map.iterator.collect {
      case (k, w) if v == w => k.asInstanceOf[K[T]]
    }.toSet

  def optionMap[R[_]](f: FunctionK[Pair, Lambda[x => Option[R[x]]]]): LazyList[R[_]] = {
    val fnAny = f.toFunction[Any].andThen(_.iterator)
    lazyListFromIterator(map.iterator.asInstanceOf[Iterator[(K[Any], V[Any])]].flatMap(fnAny))
  }

  def mapValues[V1[_]](f: FunctionK[V, V1]): HMap[K, V1] =
    HMap.from[K, V1](map.map { case (k, v) => k -> f(v) }.toMap)
}

object HMap {

  def empty[K[_], V[_]]: HMap[K, V] =
    from[K, V](Map.empty[K[_], V[_]])

  private[dagon] def from[K[_], V[_]](m: Map[K[_], V[_]]): HMap[K, V] =
    new HMap[K, V](m)
}
