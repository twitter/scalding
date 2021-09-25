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
package com.twitter.scalding

import com.twitter.maple.tap.MemorySourceTap

import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.Fields
import cascading.scheme.NullScheme

import java.io.{ InputStream, OutputStream }

import scala.collection.mutable.Buffer
import scala.collection.JavaConverters._

/**
 * Allows working with an iterable object defined in the job (on the submitter)
 * to be used within a Job as you would a Pipe/RichPipe
 *
 * These lists should probably be very tiny by Hadoop standards.  If they are
 * getting large, you should probably dump them to HDFS and use the normal
 * mechanisms to address the data (a FileSource).
 */
case class IterableSource[+T](@transient iter: Iterable[T], inFields: Fields = Fields.NONE)(implicit set: TupleSetter[T], conv: TupleConverter[T]) extends Source with Mappable[T] {

  def fields = {
    if (inFields.isNone && set.arity > 0) {
      Dsl.intFields(0 until set.arity)
    } else inFields
  }

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)

  @transient
  private val asBuffer: Buffer[Tuple] = iter.map { set(_) }.toBuffer

  private lazy val hdfsTap: Tap[_, _, _] = new MemorySourceTap(asBuffer.asJava, fields)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    if (readOrWrite == Write) {
      sys.error("IterableSource is a Read-only Source")
    }
    mode match {
      case Local(_) => new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), asBuffer)
      case Test(_) => new MemoryTap[InputStream, OutputStream](new NullScheme(fields, fields), asBuffer)
      case Hdfs(_, _) => hdfsTap
      case HadoopTest(_, _) => hdfsTap
      case _ => throw ModeException("Unsupported mode for IterableSource: " + mode.toString)
    }
  }

  /**
   * Don't use the whole string of the iterable, which can be huge.
   * We take the first 10 items + the identityHashCode of the iter.
   */
  override val sourceId: String =
    "IterableSource(%s)-%d".format(iter.take(10).toString, System.identityHashCode(iter))
}
