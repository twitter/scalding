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
import cascading.flow.FlowDef
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.typed.{ IterablePipe, MemorySink, Converter, TypedPipeInst }
import collection.JavaConverters._
import cascading.tuple.{ TupleEntry, Fields }
import cascading.pipe.Each

/**
 * SequenceFile with explicit types. Useful for debugging flows using the Typed API.
 * Not to be used for permanent storage: uses Kryo serialization which may not be
 * consistent across JVM instances. Use Thrift sources instead.
 */
class TypedSequenceFile[T](path: String) extends SequenceFile(path, 0) with Mappable[T] with TypedSink[T] {
  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])
  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
}

object TypedSequenceFile {
  def apply[T](path: String): TypedSequenceFile[T] =
    new TypedSequenceFile[T](path)
}

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
    val p = pipe.toPipe(0)
    val localFlow = flowDef.onlyUpstreamFrom(p)
    mode match {
      case Local(_) =>
        val dest = new MemorySink[T]
        dest.writeFrom(p)(localFlow, mode)
        run(localFlow)
        TypedPipe.from(dest.readResults)
      case Hdfs(_, _) =>
        // come up with unique temporary filename
        // TODO: refactor into TemporarySequenceFile class
        val tmpSeq = "/tmp/scalding-repl/snapshot-" + UUID.randomUUID() + ".seq"
        val dest = TypedSequenceFile[T](tmpSeq)
        dest.writeFrom(p)(localFlow, mode)
        run(localFlow)
        TypedPipe.from(dest)
    }
  }

  // TODO: add back `toList` based on `snapshot` this time

  // TODO: add `dump` to view contents without reading into memory
  def toIterator: Iterator[T] = pipe match {
    // if this is just a Converter on a head pipe
    // (true for the first pipe on a source, e.g. a snapshot pipe)
    case TypedPipeInst(p, fields, conv: Converter[_]) if p.getPrevious.isEmpty =>
      val srcs = flowDef.getSources
      if (srcs.containsKey(p.getName)) {
        val tap = srcs.get(p.getName)
        mode.openForRead(tap).asScala.flatMap(tup => conv(tup.selectEntry(fields)))
      } else {
        throw new RuntimeException("Unable to open for reading.")
      }
    // if it's already just a wrapped iterable (MemorySink), just return it
    case IterablePipe(iter, _, _) => iter.toIterator
    // otherwise, snapshot the pipe and get an iterator on that
    case _ =>
      pipe.snapshot.toIterator
  }

  def toList: List[T] = toIterator.toList

  def dump: Unit = toIterator.foreach(println(_))

}
