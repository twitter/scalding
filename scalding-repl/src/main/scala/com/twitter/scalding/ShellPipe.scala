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
  def save(dest: TypedSink[T] with Mappable[T])(implicit md: Mode): TypedPipe[T] = {
    val localFlow = new FlowDef
    pipe.write(dest)(localFlow, md)
    run(localFlow, md)

    TypedPipe.from(dest)
  }

  /**
   * Save snapshot of a typed pipe to a temporary sequence file.
   * @return A TypedPipe to a new Source, reading from the sequence file.
   */
  def snapshot(implicit md: Mode): TypedPipe[T] = {
    val localFlow = new FlowDef
    md match {
      case _: CascadingLocal => // Local or Test mode
        val dest = new MemorySink[T]
        pipe.write(dest)(localFlow, md)
        run(localFlow, md)
        TypedPipe.from(dest.readResults)
      case _: HadoopMode =>
        // come up with unique temporary filename
        // TODO: refactor into TemporarySequenceFile class
        val tmpSeq = "/tmp/scalding-repl/snapshot-" + UUID.randomUUID + ".seq"
        val dest = TypedSequenceFile[T](tmpSeq)
        pipe.write(dest)(localFlow, md)
        run(localFlow, md)
        TypedPipe.from(dest)
    }
  }

  /**
   * Create a (local) iterator over the pipe. For non-trivial pipes (anything except
   * a head-pipe reading from a source), a snapshot is automatically created and
   * iterated over.
   * @return local iterator
   */
  def toIterator(implicit md: Mode): Iterator[T] = pipe match {
    // if this is just a Converter on a head pipe
    // (true for the first pipe on a source, e.g. a snapshot pipe)
    case tp: TypedPipeInst[_] =>
      // TODO, I think 2.10, will not warn if we put T in the above
      // as you can clearly prove this can't fail
      val tpT = tp.asInstanceOf[TypedPipeInst[T]]
      tpT.openIfHead match {
        // TODO: it might be good to apply flatMaps locally,
        // since we obviously need to iterate all,
        // but filters we might want the cluster to apply
        // for us. So unwind until you hit the first filter, snapshot,
        // then apply the unwound functions
        case Some((tap, fields, Converter(conv))) =>
          md.openForRead(tap).asScala.map(tup => conv(tup.selectEntry(fields)))
        case _ => snapshot.toIterator
      }
    case TypedPipeFactory(next) =>
      val nextPipe = next(new FlowDef, md)
      (new ShellTypedPipe(nextPipe)).toIterator
    // if it's already just a wrapped iterable (MemorySink), just return it
    case IterablePipe(iter) => iter.toIterator
    // handle empty pipe
    case EmptyTypedPipe => Iterator.empty
    // otherwise, snapshot the pipe and get an iterator on that
    case _ => snapshot.toIterator
  }

  /**
   * Create a list from the pipe in memory. Uses `ShellTypedPipe.toIterator`.
   * Warning: user must ensure that the results will actually fit in memory.
   */
  def toList(implicit md: Mode): List[T] = toIterator.toList

  /**
   * Print the contents of a pipe to stdout. Uses `ShellTypedPipe.toIterator`.
   */
  def dump(implicit md: Mode): Unit = toIterator.foreach(println(_))

}

class ShellValuePipe[T](vp: ValuePipe[T]) {
  import ReplImplicits.typedPipeToShellTypedPipe
  def toOption(implicit fd: FlowDef, md: Mode): Option[T] = vp match {
    case EmptyValue => None
    case LiteralValue(v) => Some(v)
    // (only take 2 from iterator to avoid blowing out memory in case there's some bug)
    case ComputedValue(tp) => tp.snapshot.toIterator.take(2).toList match {
      case Nil => None
      case v :: Nil => Some(v)
      case _ => sys.error("More than one value in ValuePipe.")
    }
  }
}
