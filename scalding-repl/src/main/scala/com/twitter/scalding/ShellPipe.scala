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
import cascading.tuple.Fields
import com.twitter.scalding.typed._
import scala.collection.JavaConverters._
import com.twitter.scalding.source.TypedSequenceFile

/**
 * Enrichment on TypedPipes allowing them to be run locally, independent of the overall flow.
 * @param pipe to wrap
 */
class ShellTypedPipe[T](pipe: TypedPipe[T]) {
  import Dsl.flowDefToRichFlowDef
  import ReplImplicits._

  /**
   * Shorthand for .write(dest).run
   */
  def save(dest: TypedSink[T] with Mappable[T])(implicit fd: FlowDef, md: Mode): TypedPipe[T] = {

    val p = pipe.toPipe(dest.sinkFields)(dest.setter)

    val localFlow = fd.onlyUpstreamFrom(p)
    dest.writeFrom(p)(localFlow, md)
    run(localFlow, md)

    TypedPipe.from(dest)(fd, md)
  }

  /**
   * Save snapshot of a typed pipe to a temporary sequence file.
   * @return A TypedPipe to a new Source, reading from the sequence file.
   */
  def snapshot(implicit fd: FlowDef, md: Mode): TypedPipe[T] = {
    val p = pipe.toPipe(0)
    val localFlow = fd.onlyUpstreamFrom(p)
    md match {
      case _: CascadingLocal => // Local or Test mode
        val dest = new MemorySink[T]
        dest.writeFrom(p)(localFlow, md)
        run(localFlow, md)
        TypedPipe.from(dest.readResults)(fd, md)
      case _: HadoopMode =>
        // come up with unique temporary filename
        // TODO: refactor into TemporarySequenceFile class
        val tmpSeq = "/tmp/scalding-repl/snapshot-" + UUID.randomUUID + ".seq"
        val dest = TypedSequenceFile[T](tmpSeq)
        dest.writeFrom(p)(localFlow, md)
        run(localFlow, md)
        TypedPipe.from(dest)(fd, md)
    }
  }

  /**
   * Create a (local) iterator over the pipe. For non-trivial pipes (anything except
   * a head-pipe reading from a source), a snapshot is automatically created and
   * iterated over.
   * @return local iterator
   */
  def toIterator(implicit fd: FlowDef, md: Mode): Iterator[T] = pipe match {
    // if this is just a Converter on a head pipe
    // (true for the first pipe on a source, e.g. a snapshot pipe)
    case TypedPipeInst(p, fields, Converter(conv)) if p.getPrevious.isEmpty =>
      val srcs = fd.getSources
      if (srcs.containsKey(p.getName)) {
        val tap = srcs.get(p.getName)
        md.openForRead(tap).asScala.map(tup => conv(tup.selectEntry(fields)))
      } else {
        sys.error("Invalid head: pipe has no previous, but there is no registered source.")
      }
    // if it's already just a wrapped iterable (MemorySink), just return it
    case IterablePipe(iter, _, _) => iter.toIterator
    // otherwise, snapshot the pipe and get an iterator on that
    case _ =>
      pipe.snapshot.toIterator
  }

  /**
   * Create a list from the pipe in memory. Uses `ShellTypedPipe.toIterator`.
   * Warning: user must ensure that the results will actually fit in memory.
   */
  def toList(implicit fd: FlowDef, md: Mode): List[T] = toIterator.toList

  /**
   * Print the contents of a pipe to stdout. Uses `ShellTypedPipe.toIterator`.
   */
  def dump(implicit fd: FlowDef, md: Mode): Unit = toIterator.foreach(println(_))

}

class ShellValuePipe[T](vp: ValuePipe[T]) {
  import ReplImplicits.typedPipeToShellTypedPipe
  def toOption(implicit fd: FlowDef, md: Mode): Option[T] = vp match {
    case _: EmptyValue => None
    case LiteralValue(v) => Some(v)
    case ComputedValue(tp) => tp.snapshot.toList match {
      case Nil => None
      case v :: Nil => Some(v)
    }
  }
}
