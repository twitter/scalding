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
import cascading.operation.Operation
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
        getForceToDiskPipeIfNecessary(fd, mode),
        Field.singleOrdered("key1")(keyOrdering),
        WrappedJoiner(new HashJoiner(joinFunction, joiner)))

      //Construct the new TypedPipe
      TypedPipe.from[(K, R)](newPipe.project('key, 'value), ('key, 'value))(fd, mode, tuple2Converter)
    })

  /**
   * Returns a Pipe for the mapped (rhs) pipe with checkpointing (forceToDisk) applied if needed.
   * Currently we skip checkpointing if we're confident that the underlying rhs Pipe is persisted
   * (e.g. a source / Checkpoint / GroupBy / CoGroup / Every) and we have 0 or more Each operator Fns
   * that are not doing any real work (e.g. Converter, CleanupIdentityFunction)
   */
  private def getForceToDiskPipeIfNecessary(fd: FlowDef, mode: Mode): Pipe = {
    val mappedPipe = mapped.toPipe(('key1, 'value1))(fd, mode, tup2Setter)

    // if the user has turned off auto force right, we fall back to the old behavior and
    //just return the mapped pipe
    if (!getHashJoinAutoForceRight(mode) || isSafeToSkipForceToDisk(mappedPipe)) mappedPipe
    else mappedPipe.forceToDisk
  }

  /**
   * Computes if it is safe to skip a force to disk (only if the user hasn't turned this off using
   * Config.HashJoinAutoForceRight).
   * If we know the pipe is persisted,we can safely skip. If the Pipe is an Each operator, we check
   * if the function it holds can be skipped and we recurse to check its parent pipe.
   * Recursion handles situations where we have a couple of Each ops in a row.
   * For example: pipe.forceToDisk.onComplete results in: Each -> Each -> Checkpoint
   */
  private def isSafeToSkipForceToDisk(pipe: Pipe): Boolean =
    pipe match {
      case eachPipe: Each =>
        if (canSkipEachOperation(eachPipe.getOperation)) {
          //need to recurse down to see if parent pipe is ok
          getPreviousPipe(eachPipe).exists(prevPipe => isSafeToSkipForceToDisk(prevPipe))
        } else false
      case _: Checkpoint => true
      case _: GroupBy => true
      case _: CoGroup => true
      case _: Every => true
      case p if isSourcePipe(p) => true
      case _ => false
    }

  /**
   * Checks the transform to deduce if it is safe to skip the force to disk.
   * If the FlatMapFunction is a converter / EmptyFn / IdentityFn then we can skip
   * For FilteredFn we could potentially save substantially so we want to forceToDisk
   * For map and flatMap we can't definitively infer if it is OK to skip the forceToDisk.
   * Thus we just go ahead and forceToDisk in those two cases - users can opt out if needed.
   */
  private def canSkipEachOperation(eachOperation: Operation[_]): Boolean =
    eachOperation match {
      case f: FlatMapFunction[_, _] =>
        f.getFunction match {
          case _: Converter[_] => true
          case _: FilteredFn[_] => false //we'd like to forceToDisk after a filter
          case _: MapFn[_, _] => false
          case _: FlatMappedFn[_, _] => false
          case _ => false // treat empty fn as a Filter all so forceToDisk
        }
      case _: CleanupIdentityFunction => true
      case _ => false
    }

  private def getHashJoinAutoForceRight(mode: Mode): Boolean = {
    mode match {
      case h: HadoopMode =>
        val config = Config.fromHadoop(h.jobConf)
        config.getHashJoinAutoForceRight
      case _ => false //default to false
    }
  }

  private def getPreviousPipe(p: Pipe): Option[Pipe] = {
    if (p.getPrevious != null && p.getPrevious.length == 1) p.getPrevious.headOption
    else None
  }

  /**
   * Return true if a pipe is a source Pipe (has no parents / previous) and isn't a
   * Splice.
   */
  private def isSourcePipe(pipe: Pipe): Boolean = {
    pipe.getParent == null &&
      (pipe.getPrevious == null || pipe.getPrevious.isEmpty) &&
      (!pipe.isInstanceOf[Splice])
  }
}
