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

package com.twitter.scalding.commons.source

import cascading.pipe.Pipe
import cascading.scheme.Scheme

import org.apache.thrift.TBase
import com.google.protobuf.Message
import com.twitter.bijection.Injection
import com.twitter.elephantbird.cascading3.scheme._
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import com.twitter.scalding.source.{ CheckedInversion, MaxFailuresCheck }
import com.twitter.scalding.typed.TypedSink
import scala.collection.JavaConverters._

trait LzoCodec[T] extends FileSource with SingleMappable[T] with TypedSink[T] with LocalTapSource {
  def injection: Injection[T, Array[Byte]]
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def hdfsScheme = HadoopSchemeInstance((new LzoByteArrayScheme).asInstanceOf[Scheme[_, _, _, _, _]])
  override def transformForRead(pipe: Pipe) =
    pipe.flatMap(0 -> 0) { fromBytes(_: Array[Byte]) }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> 0) { injection.apply(_: T) }

  protected def fromBytes(b: Array[Byte]): Option[T] = Some(injection.invert(b).get)

  override def toIterator(implicit config: Config, mode: Mode): Iterator[T] = {
    val tap = createTap(Read)(mode)
    mode.openForRead(config, tap)
      .asScala
      .flatMap { te =>
        fromBytes(te.selectTuple(sourceFields).getObject(0).asInstanceOf[Array[Byte]])
      }
  }
}

trait ErrorHandlingLzoCodec[T] extends LzoCodec[T] {
  def checkedInversion: CheckedInversion[T, Array[Byte]]

  override def fromBytes(b: Array[Byte]) = checkedInversion(b)
}

// Common case of setting a maximum number of errors
trait ErrorThresholdLzoCodec[T] extends ErrorHandlingLzoCodec[T] {
  def maxErrors: Int
  lazy val checkedInversion: CheckedInversion[T, Array[Byte]] = new MaxFailuresCheck(maxErrors)(injection)
}

trait LzoProtobuf[T <: Message] extends LocalTapSource with SingleMappable[T] with TypedSink[T] {
  def column: Class[_]
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def hdfsScheme = HadoopSchemeInstance((new LzoProtobufScheme[T](column)).asInstanceOf[Scheme[_, _, _, _, _]])
}

trait LzoThrift[T <: TBase[_, _]] extends LocalTapSource with SingleMappable[T] with TypedSink[T] {
  def column: Class[_]
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def hdfsScheme = HadoopSchemeInstance((new LzoThriftScheme[T](column)).asInstanceOf[Scheme[_, _, _, _, _]])
}

trait LzoText extends LocalTapSource with SingleMappable[String] with TypedSink[String] {
  override def setter[U <: String] = TupleSetter.asSubSetter[String, U](TupleSetter.singleSetter[String])
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine())
  override def sourceFields = Dsl.intFields(Seq(1))
}

trait LzoTsv extends DelimitedScheme with LocalTapSource {
  override def hdfsScheme = HadoopSchemeInstance((new LzoTextDelimited(fields, skipHeader, writeHeader, separator, strict, quote, types, safe)).asInstanceOf[Scheme[_, _, _, _, _]])
}

trait LzoTypedTsv[T] extends DelimitedScheme with Mappable[T] with TypedSink[T] with LocalTapSource {
  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def hdfsScheme = HadoopSchemeInstance((new LzoTextDelimited(fields, skipHeader, writeHeader, separator, strict, quote, types, safe)).asInstanceOf[Scheme[_, _, _, _, _]])

  def mf: Manifest[T]

  override val types: Array[Class[_]] = {
    if (classOf[scala.Product].isAssignableFrom(mf.runtimeClass)) {
      //Assume this is a Tuple:
      mf.typeArguments.map { _.runtimeClass }.toArray
    } else {
      //Assume there is only a single item
      Array(mf.runtimeClass)
    }
  }
}
