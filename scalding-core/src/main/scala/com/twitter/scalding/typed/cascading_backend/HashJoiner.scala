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
package com.twitter.scalding.typed.cascading_backend

import cascading.pipe.joiner.{ Joiner => CJoiner, JoinerClosure }
import cascading.tuple.{ Tuple => CTuple }

import com.twitter.scalding.serialization.Externalizer
import com.twitter.scalding.typed.MultiJoinFunction

import scala.collection.JavaConverters._

/**
 * Only intended to be use to implement the hashCogroup on TypedPipe/Grouped
 */
class HashJoiner[K, V, W, R](
  rightHasSingleValue: Boolean,
  rightGetter: MultiJoinFunction[K, W],
  joiner: (K, V, Iterable[W]) => Iterator[R]) extends CJoiner {

  private[this] val joinEx = Externalizer(joiner)

  override def getIterator(jc: JoinerClosure) = {
    // The left one cannot be iterated multiple times on Hadoop:
    val leftIt = jc.getIterator(0).asScala // should only be 0 or 1 here
    if (leftIt.isEmpty) {
      (Iterator.empty: Iterator[CTuple]).asJava // java is not covariant so we need this
    } else {
      // In this branch there must be at least one item on the left in a hash-join
      val left = leftIt.buffered
      val key = left.head.getObject(0).asInstanceOf[K]

      // It is safe to iterate over the right side again and again

      val rightIterable =
        if (rightHasSingleValue) {
          // Materialize this once for all left values
          rightGetter(key, jc.getIterator(1).asScala.map(_.getObject(1): Any), Nil).toList
        } else {
          // TODO: it might still be good to count how many there are and materialize
          // in memory without reducing again
          new Iterable[W] {
            def iterator = rightGetter(key, jc.getIterator(1).asScala.map(_.getObject(1): Any), Nil)
          }
        }

      left.flatMap { kv =>
        val leftV = kv.getObject(1).asInstanceOf[V] // get just the Vs

        joinEx.get(key, leftV, rightIterable)
          .map { rval =>
            // There always has to be four resulting fields
            // or otherwise the flow planner will throw
            val res = CTuple.size(4)
            res.set(0, key)
            res.set(1, rval)
            res
          }
      }.asJava
    }
  }

  override val numJoins = 1
}
