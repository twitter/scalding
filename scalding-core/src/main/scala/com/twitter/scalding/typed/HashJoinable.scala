/*
Copyright 2014 Twitter, Inc.

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

import cascading.flow.FlowDef
import cascading.pipe._
import com.twitter.scalding._

import com.twitter.scalding.TupleConverter.tuple2Converter
import com.twitter.scalding.TupleSetter.tup2Setter

// For the Fields conversions
import Dsl._

/**
 * If we can HashJoin, then we can CoGroup, but not vice-versa
 * i.e., HashJoinable is a strict subset of CoGroupable (CoGrouped, for instance
 * is CoGroupable, but not HashJoinable).
 */
trait HashJoinable[K, +V] extends CoGroupable[K, V] with KeyedPipe[K] {
  /** A HashJoinable has a single input into to the cogroup */
  override def inputs = List(mapped)
  /**
   * This fully replicates this entire Grouped to the argument: mapside.
   * This means that we never see the case where the key is absent in the pipe. This
   * means implementing a right-join (from the pipe) is impossible.
   * Note, there is no reduce-phase in this operation.
   * The next issue is that obviously, unlike a cogroup, for a fixed key, each joiner will
   * NOT See all the tuples with those keys. This is because the keys on the left are
   * distributed across many machines
   * See hashjoin:
   * http://docs.cascading.org/cascading/2.0/javadoc/cascading/pipe/HashJoin.html
   */
  def hashCogroupOn[V1, R](mapside: TypedPipe[(K, V1)])(joiner: (K, V1, Iterable[V]) => Iterator[R]): TypedPipe[(K, R)] =
    // Note, the Ordering must have that compare(x,y)== 0 being consistent with hashCode and .equals to
    // otherwise, there may be funky issues with cascading
    TypedPipeFactory({ (fd, mode) =>
      val newPipe = new HashJoin(
        RichPipe.assignName(mapside.toPipe(('key, 'value))(fd, mode, tup2Setter)),
        Field.singleOrdered("key")(keyOrdering),
        getMappedPipe(fd, mode),
        Field.singleOrdered("key1")(keyOrdering),
        WrappedJoiner(new HashJoiner(joinFunction, joiner)))

      //Construct the new TypedPipe
      TypedPipe.from[(K, R)](newPipe.project('key, 'value), ('key, 'value))(fd, mode, tuple2Converter)
    })

  /**
   * Returns a Pipe for the mapped (rhs) pipe with checkpointing applied if needed.
   * Currently we skip checkpointing if:
   * 1) If the pipe is a ForceToDiskTypedPipe (and thus the Cascading pipe is a Checkpoint)
   * 2) If the Cascading pipe is a GroupBy / CoGroup
   * 3) If while walking the pipe's parents we hit an Every before an Each
   */
  private def getMappedPipe(fd: FlowDef, mode: Mode): Pipe = {
    val mappedPipe = mapped.toPipe(('key1, 'value1))(fd, mode, tup2Setter)

    mappedPipe match {
      case _: Checkpoint => mappedPipe
      case _: GroupBy => mappedPipe
      case _: CoGroup => mappedPipe
      case _ =>
        val checkParents = checkEveryBeforeEach(mappedPipe)
        checkParents match {
          case Some(true) => mappedPipe
          case _ => mappedPipe.forceToDisk
        }
    }
  }

  /**
   * Check if we encounter an Every before an Each while traversing parents of the pipe
   */
  private def checkEveryBeforeEach(pipe: Pipe): Option[Boolean] = {
    Iterator
      .iterate(Seq(pipe))(pipes => for (p <- pipes; prev <- p.getPrevious) yield prev)
      .takeWhile(_.length > 0)
      .flatten
      .find {
        case _: Each => true
        case _: Every => true
        case _ => false
      }
      .map {
        case _: Each => false
        case _: Every => true
      }
  }
}
