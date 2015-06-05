/*
Copyright 2015 Twitter, Inc.

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

import com.twitter.elephantbird.mapreduce.io.BinaryConverter
import com.twitter.scrooge.{ BinaryThriftStructSerializer, ThriftStructCodec, ThriftStruct }
import scala.reflect.ClassTag
import scala.util.Try

/*
 * Common BinaryConverters to be used with GenericSource / GenericScheme.
 */

case object IdentityBinaryConverter extends BinaryConverter[Array[Byte]] {
  override def fromBytes(messageBuffer: Array[Byte]) = messageBuffer
  override def toBytes(message: Array[Byte]) = message
}

object ScroogeBinaryConverter {

  // codec code borrowed from chill's ScroogeThriftStructSerializer class
  private[this] def codecForNormal[T <: ThriftStruct](thriftStructClass: Class[T]): Try[ThriftStructCodec[T]] =
    Try(Class.forName(thriftStructClass.getName + "$").getField("MODULE$").get(null))
      .map(_.asInstanceOf[ThriftStructCodec[T]])

  private[this] def codecForUnion[T <: ThriftStruct](maybeUnion: Class[T]): Try[ThriftStructCodec[T]] =
    Try(Class.forName(maybeUnion.getName.reverse.dropWhile(_ != '$').reverse).getField("MODULE$").get(null))
      .map(_.asInstanceOf[ThriftStructCodec[T]])

  def apply[T <: ThriftStruct: ClassTag]: BinaryConverter[T] = {
    val ct = implicitly[ClassTag[T]]
    new BinaryConverter[T] {
      val serializer = BinaryThriftStructSerializer[T] {
        val clazz = ct.runtimeClass.asInstanceOf[Class[T]]
        codecForNormal[T](clazz).orElse(codecForUnion[T](clazz)).get
      }
      override def toBytes(struct: T) = serializer.toBytes(struct)
      override def fromBytes(bytes: Array[Byte]): T = serializer.fromBytes(bytes)
    }
  }
}

