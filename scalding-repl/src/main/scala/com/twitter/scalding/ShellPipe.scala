/*  Copyright 2013 Twitter, inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twitter.scalding

import java.util.UUID
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.typed.{ Converter, TypedPipeInst }
import collection.JavaConverters._

/**
 * Enrichment on TypedPipes allowing them to be run locally, independent of the overall flow.
 * @param pipe to wrap
 */
class ShellTypedPipe[T](pipe: TypedPipe[T]) {
  import Dsl.flowDefToRichFlowDef

  /**
   * Shorthand for .write(dest).run
   */
  def save(dest: TypedSink[T] with Mappable[T]): TypedPipe[T] = {

    val p = pipe.toPipe(dest.sinkFields)(dest.setter)

    val localFlow = flowDef.onlyUpstreamFrom(p)
    dest.writeFrom(p)(localFlow, mode)
    run(localFlow)

    TypedPipe.from(dest)
  }

  /**
   * Save snapshot of a typed pipe to a temporary sequence file.
   * @return A TypedPipe to a new Source, reading from the sequence file.
   */
  def snapshot: TypedPipe[T] = {

    // come up with unique temporary filename
    // TODO: refactor into TemporarySequenceFile class
    val tmpSeq = "/tmp/scalding-repl/snapshot-" + UUID.randomUUID() + ".seq"
    val dest = SequenceFile(tmpSeq, 'record)
    val p = pipe.toPipe('record)

    val localFlow = flowDef.onlyUpstreamFrom(p)
    dest.writeFrom(p)(localFlow, mode)
    run(localFlow)

    TypedPipe.fromSingleField[T](SequenceFile(tmpSeq))
  }

  // TODO: add back `toList` based on `snapshot` this time

  // TODO: add `dump` to view contents without reading into memory
  def toIterator: Iterator[T] = pipe match {
    case tp: TypedPipeInst[_] =>
      val p = tp.inpipe
      val srcs = flowDef.getSources
      if (p.getPrevious.length == 0) { // is a head
        if (srcs.containsKey(p.getName)) {
          val tap = srcs.get(p.getName)
          // val conv = Converter(TupleConverter.singleConverter[T])
          // mode.openForRead(tap).asScala.flatMap(v => conv(v))
          mode.openForRead(tap).asScala.flatMap(v => tp.flatMapFn(v))
        } else {
          throw new RuntimeException("Unable to open for reading.")
        }
      } else {
        // not a head pipe, so we should generate a snapshot and use that
        println("@> need to generate snapshot")
        pipe.snapshot.toIterator
      }
    case _ =>
      println("@> need to generate snapshot")
      pipe.snapshot.toIterator
  }

  def toList: List[T] = toIterator.toList

  def dump: Unit = toIterator.foreach(println(_))

}
