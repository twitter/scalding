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
  import ReplImplicits.execute

  /**
   * Shorthand for .write(dest).run
   */
  def save(dest: TypedSink[T] with TypedSource[T]): TypedPipe[T] =
    execute(pipe.writeThrough(dest))

  /**
   * Save snapshot of a typed pipe to a temporary sequence file.
   * @return A TypedPipe to a new Source, reading from the sequence file.
   */
  def snapshot: TypedPipe[T] =
    execute(pipe.forceToDiskExecution)

  /**
   * Create a (local) iterator over the pipe. For non-trivial pipes (anything except
   * a head-pipe reading from a source), a snapshot is automatically created and
   * iterated over.
   * @return local iterator
   */
  def toIterator: Iterator[T] =
    execute(pipe.toIteratorExecution)

  /**
   * Create a list from the pipe in memory. Uses `ShellTypedPipe.toIterator`.
   * Warning: user must ensure that the results will actually fit in memory.
   */
  def toList: List[T] = toIterator.toList

  /**
   * Print the contents of a pipe to stdout. Uses `ShellTypedPipe.toIterator`.
   */
  def dump: Unit = toIterator.foreach(println(_))
}

class ShellValuePipe[T](vp: ValuePipe[T]) {
  import ReplImplicits.execute
  def toOption: Option[T] = vp match {
    case EmptyValue => None
    case LiteralValue(v) => Some(v)
    // (only take 2 from iterator to avoid blowing out memory in case there's some bug)
    case _ => execute(vp.toOptionExecution)
  }
}
