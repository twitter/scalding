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

import com.twitter.scalding._

import scala.collection.mutable.Buffer

import java.util.UUID

import cascading.pipe.Pipe
import cascading.flow.FlowDef
import cascading.scheme.NullScheme
import cascading.tuple.Tuple

/*
 * This is useful for in-memory testing with Execution
 * It only works for CascadingLocal mode.
 */
class MemorySink[T] extends TypedSink[T] {
  private[this] val buf = Buffer[Tuple]()
  private[this] val name: String = UUID.randomUUID.toString

  // takes a copy as of NOW. Don't call this before the job has run
  def readResults: Iterable[T] =
    buf.iterator.map(_.getObject(0).asInstanceOf[T]).toList

  def setter[U <: T] = TupleSetter.asSubSetter(TupleSetter.singleSetter[T])
  def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe =
    mode match {
      case cl: CascadingLocal =>
        val tap = new MemoryTap(new NullScheme(sinkFields, sinkFields), buf)
        flowDef.addSink(name, tap)
        flowDef.addTail(new Pipe(name, pipe))
        pipe
      case _ => sys.error("MemorySink only usable with cascading local")
    }
}
