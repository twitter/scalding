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

import cascading.pipe.HashJoin
import cascading.pipe.joiner.{Joiner => CJoiner, JoinerClosure}
import cascading.tuple.{Tuple => CTuple, Fields, TupleEntry}

import com.twitter.scalding._

import scala.collection.JavaConverters._

/** This fully replicates the entire right hand side to the left.
 * This means that we never see the case where the key is absent on the left. This
 * means implementing a right-join is impossible.
 * Note, there is no reduce-phase in this operation.
 * The next issue is that obviously, unlike a cogroup, for a fixed key, each joiner will
 * NOT See all the tuples with those keys. This is because the keys on the left are
 * distributed across many machines
 * See hashjoin:
 * http://docs.cascading.org/cascading/2.0/javadoc/cascading/pipe/HashJoin.html
 */
class HashCoGrouped2[K,V,W,R](left: TypedPipe[(K,V)],
  right: Grouped[K,W],
  hashjoiner: (K, V, Iterable[W]) => Iterator[R])
  extends java.io.Serializable {

  lazy val toTypedPipe : TypedPipe[(K,R)] = {
    // Actually make a new coGrouping:
    assert(right.reduceStep.valueOrdering == None, "secondary sorting unsupported in HashCoGrouped2")

    import Dsl._

    val leftGroupKey = RichFields(StringField("key")(right.reduceStep.keyOrdering, None))
    val rightGroupKey = RichFields(StringField("key1")(right.reduceStep.keyOrdering, None))
    val newPipe = new HashJoin(RichPipe.assignName(left.toPipe(('key, 'value))), leftGroupKey,
      right.reduceStep.mapped.toPipe[(K,Any)](('key1, 'value1)),
      rightGroupKey,
      new HashJoiner(right.reduceStep.streamMapping, hashjoiner))

    //Construct the new TypedPipe
    TypedPipe.from[(K,R)](newPipe.project('key,'value), ('key, 'value))
  }
}

class HashJoiner[K,V,W,R](rightGetter: (K, Iterator[CTuple]) => Iterator[W],
  joiner: (K, V, Iterable[W]) => Iterator[R]) extends CJoiner {

  override def getIterator(jc: JoinerClosure) = {
    // The left one cannot be iterated multiple times on Hadoop:
    val leftIt = jc.getIterator(0).asScala // should only be 0 or 1 here
    if(leftIt.isEmpty) {
      (Iterator.empty: Iterator[CTuple]).asJava // java is not covariant so we need this
    }
    else {
      val left = leftIt.next
      val (key, leftV) = {
        val k = left.getObject(0).asInstanceOf[K]
        val v = left.getObject(1).asInstanceOf[V]
        (k, v)
      }

      // It is safe to iterate over the right side again and again
      val rightIterable = new Iterable[W] {
        def iterator = rightGetter(key, jc.getIterator(1).asScala)
      }

      joiner(key, leftV, rightIterable).map { rval =>
        // There always has to be four resulting fields
        // or otherwise the flow planner will throw
        val res = CTuple.size(4)
        res.set(0, key)
        res.set(1, rval)
        res
      }.asJava
    }
  }
  override val numJoins = 1
}
