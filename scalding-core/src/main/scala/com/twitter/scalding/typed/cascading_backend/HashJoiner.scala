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

import cascading.pipe.joiner.{ Joiner => CJoiner, JoinerClosure }
import cascading.tuple.{ Tuple => CTuple, Fields, TupleEntry }

import com.twitter.scalding._

import scala.collection.JavaConverters._

/**
 * Only intended to be use to implement the hashCogroup on TypedPipe/Grouped
 */
class HashJoiner[K, V, W, R](rightGetter: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[W],
  joiner: (K, V, Iterable[W]) => Iterator[R]) extends CJoiner {

  override def getIterator(jc: JoinerClosure) = {
    // The left one cannot be iterated multiple times on Hadoop:
    val leftIt = jc.getIterator(0).asScala // should only be 0 or 1 here
    if (leftIt.isEmpty) {
      (Iterator.empty: Iterator[CTuple]).asJava // java is not covariant so we need this
    } else {
      val left = leftIt.buffered
      // There must be at least one item on the left in a hash-join
      val key = left.head.getObject(0).asInstanceOf[K]

      // It is safe to iterate over the right side again and again
      val rightIterable = new Iterable[W] {
        def iterator = rightGetter(key, jc.getIterator(1).asScala, Nil)
      }

      left.flatMap { kv =>
        val leftV = kv.getObject(1).asInstanceOf[V] // get just the Vs

        joiner(key, leftV, rightIterable)
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
