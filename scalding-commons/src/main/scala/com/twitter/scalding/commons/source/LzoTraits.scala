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

import org.apache.thrift.TBase
import com.google.protobuf.Message
import com.twitter.bijection.Injection
import com.twitter.elephantbird.cascading2.scheme._
import com.twitter.scalding._
import com.twitter.scalding.Dsl._

import java.util.concurrent.atomic.AtomicInteger

/** Handles the error checking for Injection inversion
 * if check fails, it will throw an unrecoverable exception stopping the job
 * TODO: probably belongs in Bijection
 */
trait CheckedInversion[T,U] extends java.io.Serializable {
  def injection: Injection[T,U]
  def apply(input: U): Option[T]
}

trait LzoCodec[T] extends FileSource with Mappable[T] {
  def injection: Injection[T,Array[Byte]]
  override def localPath = sys.error("Local mode not yet supported.")
  override def hdfsScheme = HadoopSchemeInstance(new LzoByteArrayScheme)
  override val converter = Dsl.singleConverter[T]
  override def transformForRead(pipe: Pipe) =
    pipe.map(0 -> 0) { injection.invert(_: Array[Byte]).get }

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo(0 -> 0) { injection.apply(_: T) }
}

// TODO: this should actually increment an read a Hadoop counter
class MaxFailuresCheck[T,U](val maxFailures: Int)(implicit override val injection: Injection[T,U])
  extends CheckedInversion[T,U] {

  private val failures = new AtomicInteger(0)
  def apply(input: U): Option[T] = {
    try {
      Some(injection.invert(input).get)
    }
    catch {
      case e =>
        // TODO: use proper logging
        e.printStackTrace()
        assert(failures.incrementAndGet <= maxFailures, "maximum decoding errors exceeded")
        None
    }
  }
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
  override def hdfsScheme = HadoopSchemeInstance(new LzoProtobufScheme[T](column))
  override val converter = Dsl.singleConverter[T]
}

trait LzoThrift[T <: TBase[_, _]] extends Mappable[T] {
  def column: Class[_]
  override def localScheme = { println("This does not work yet"); new CLTextDelimited(sourceFields) }
  override def hdfsScheme = HadoopSchemeInstance(new LzoThriftScheme[T](column))
  override val converter = Dsl.singleConverter[T]
}

trait LzoText extends Mappable[String] {
  override def localScheme = { println("This does not work yet"); new CLTextLine }
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine())
  override val converter = Dsl.singleConverter[String]
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
