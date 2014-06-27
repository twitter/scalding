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

import cascading.flow.Flow
import cascading.flow.FlowDef
import cascading.pipe.Pipe
import java.util.UUID
import com.twitter.scalding.ReplImplicits._

/**
 * Adds ability to run a pipe in the REPL.
 *
 * @param pipe to wrap.
 */
class ShellObj[T](obj: T) {

  def toList[R](implicit ev: T <:< TypedPipe[R], manifest: Manifest[R]): List[R] = {
    import ReplImplicits._
    ev(obj).toPipe("el").write(Tsv("item"))
    run
    TypedTsv[R]("item").toIterator.toList
  }

}

/**
 * Enrichment on TypedPipes allowing them to be run locally, independent of the overall flow.
 * @param pipe to wrap
 */
class ShellTypedPipe[T](pipe: TypedPipe[T]) extends ShellObj[TypedPipe[T]](pipe) {

  /**
   * Shorthand for .write(dest).run
   */
  def save(dest: TypedSink[T] with Mappable[T]): TypedPipe[T] = {

    val p = pipe.toPipe(dest.sinkFields)(dest.setter)

    val localFlow = p.localizedFlow
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

    val localFlow = p.localizedFlow
    dest.writeFrom(p)(localFlow, mode)
    run(localFlow)

    TypedPipe.fromSingleField[T](SequenceFile(tmpSeq))
  }

}
