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

package com.twitter.scalding.source

import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.hadoop.WritableSequenceFile
import cascading.tuple.Fields
import com.twitter.bijection.{Bijection, Injection}
import com.twitter.chill.MeatLocker
import com.twitter.scalding._

import java.util.Arrays
import org.apache.hadoop.io.BytesWritable

/**
 * Source used to write some type T into a WritableSequenceFile using a codec on T
 * for serialization.
 */

object BytesWritableCodec {
  def get =
    Bijection.build[Array[Byte], BytesWritable] { arr =>
      new BytesWritable(arr)
    } { w =>
      Arrays.copyOfRange(w.getBytes, 0, w.getLength)
    }
}

object CodecSource {
  def apply[T](paths: String*)(implicit codec: Injection[T, Array[Byte]]) = new CodecSource[T](paths)
}

class CodecSource[T] private (val hdfsPaths: Seq[String], val maxFailures: Int = 0)(implicit @transient injection: Injection[T, Array[Byte]])
extends FileSource
with Mappable[T] {
  import Dsl._

  val fieldSym = 'encodedBytes
  lazy val field = new Fields(fieldSym.name)
  val injectionBox = MeatLocker(injection andThen BytesWritableCodec.get)

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])
  override def localPath = sys.error("Local mode not yet supported.")
  override def hdfsScheme =
    HadoopSchemeInstance(new WritableSequenceFile(field, classOf[BytesWritable]).asInstanceOf[Scheme[_, _, _, _, _]])

  protected lazy val checkedInversion = new MaxFailuresCheck[T, BytesWritable](maxFailures)(injectionBox.get)
  override def transformForRead(pipe: Pipe) =
    pipe.flatMap((fieldSym) -> (fieldSym)) { (bw: BytesWritable) => checkedInversion(bw) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (fieldSym)) { injectionBox.get.apply(_: T) }
}
