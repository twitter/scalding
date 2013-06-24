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

import collection.mutable.ListBuffer

import cascading.pipe.Pipe
import cascading.scheme.local.{ TextDelimited => CLTextDelimited, TextLine => CLTextLine }
import cascading.scheme.Scheme

import org.apache.thrift.TBase
import com.google.protobuf.Message
import com.twitter.bijection.Injection
import com.twitter.elephantbird.cascading2.scheme._
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import com.twitter.scalding.source.{ CheckedInversion, MaxFailuresCheck }

trait LzoCodec[T] extends FileSource with Mappable[T] {
  def injection: Injection[T,Array[Byte]]
  override def localPath = sys.error("Local mode not yet supported.")
  override def hdfsScheme = HadoopSchemeInstance((new LzoByteArrayScheme).asInstanceOf[Scheme[_, _, _, _, _]])
  override def transformForRead(pipe: Pipe) =
    pipe.map(0 -> 0) { injection.invert(_: Array[Byte]).get }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> 0) { injection.apply(_: T) }
  override def converter[U >:T] = TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])
}

trait ErrorHandlingLzoCodec[T] extends LzoCodec[T] {
  def checkedInversion: CheckedInversion[T, Array[Byte]]

  override def transformForRead(pipe: Pipe) =
    pipe.flatMap(0 -> 0) { (b: Array[Byte]) => checkedInversion(b) }
}

// Common case of setting a maximum number of errors
trait ErrorThresholdLzoCodec[T] extends ErrorHandlingLzoCodec[T] {
  def maxErrors: Int
  lazy val checkedInversion: CheckedInversion[T, Array[Byte]] = new MaxFailuresCheck(maxErrors)(injection)
}

trait LzoProtobuf[T <: Message] extends Mappable[T] {
  def column: Class[_]
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(sourceFields) }
  override def hdfsScheme = HadoopSchemeInstance((new LzoProtobufScheme[T](column)).asInstanceOf[Scheme[_,_,_,_,_]])
}

trait LzoThrift[T <: TBase[_, _]] extends Mappable[T] {
  def column: Class[_]
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(sourceFields) }
  override def hdfsScheme = HadoopSchemeInstance((new LzoThriftScheme[T](column)).asInstanceOf[Scheme[_,_,_,_,_]])
}

trait LzoText extends Mappable[String] {
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine())
}

trait LzoTsv extends DelimitedScheme {
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(fields, separator, types) }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextDelimited(fields, separator, types))
}

trait LzoTypedTsv[T] extends DelimitedScheme with Mappable[T] {
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(fields, separator, types) }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextDelimited(fields, separator, types))

  val mf: Manifest[T]

  override val types: Array[Class[_]] = {
    if (classOf[scala.Product].isAssignableFrom(mf.erasure)) {
      //Assume this is a Tuple:
      mf.typeArguments.map { _.erasure }.toArray
    } else {
      //Assume there is only a single item
      Array(mf.erasure)
    }
  }

  protected def getTypeHack(implicit m: Manifest[T], c: TupleConverter[T]) = (m, c)
}
