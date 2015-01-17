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
package com.twitter.scalding.serialization

import org.apache.hadoop.io.serializer.{ Serialization => HSerialization, Deserializer, Serializer }
import org.apache.hadoop.conf.{ Configurable, Configuration }

import java.io.{ DataInputStream, DataOutputStream, InputStream, OutputStream }
import com.twitter.bijection.{ Injection, JavaSerializationInjection, Base64String }

/**
 * WrappedSerialization wraps a value in a wrapper class that
 * has an associated Binary that is used to deserialize
 * items wrapped in the wrapper
 */
class WrappedSerialization[T] extends HSerialization[T] with Configurable {

  import WrappedSerialization.ClassSerialization

  private var conf: Option[Configuration] = None
  private var serializations: Option[Iterable[ClassSerialization[_]]] = None

  override def getConf: Configuration = conf.get
  override def setConf(config: Configuration) {
    conf = Some(config)
    serializations = WrappedSerialization.getBinary(config)
  }

  def accept(c: Class[_]): Boolean =
    serializations.map(_.exists { case (cls, _) => cls == c }).getOrElse(false)

  def getSerialization(c: Class[T]): Option[Serialization[T]] =
    serializations.flatMap(_.collectFirst { case (cls, b) if cls == c => b })
      // This cast should never fail since we matched the class
      .asInstanceOf[Option[Serialization[T]]]

  def getSerializer(c: Class[T]): Serializer[T] =
    new BinarySerializer(getSerialization(c).getOrElse(sys.error(s"Class: ${c} not found")))

  def getDeserializer(c: Class[T]): Deserializer[T] =
    new BinaryDeserializer(getSerialization(c).getOrElse(sys.error(s"Class: ${c} not found")))

}

class BinarySerializer[T](buf: Serialization[T]) extends Serializer[T] {
  private var out: OutputStream = _
  def open(os: OutputStream): Unit = {
    out = os
  }
  def close(): Unit = { out = null }
  def serialize(t: T): Unit = {
    require(out != null, "OutputStream is null")
    buf.write(out, t).get
  }
}

class BinaryDeserializer[T](buf: Serialization[T]) extends Deserializer[T] {
  private var is: InputStream = _
  def open(i: InputStream): Unit = { is = i }
  def close(): Unit = { is = null }
  def deserialize(t: T): T = {
    require(is != null, "InputStream is null")
    buf.read(is).get
  }
}

object WrappedSerialization {
  type ClassSerialization[T] = (Class[T], Serialization[T])

  private def getSerializer[U]: Injection[Externalizer[U], String] = {
    implicit val initialInj = JavaSerializationInjection[Externalizer[U]]
    Injection.connect[Externalizer[U], Array[Byte], Base64String, String]
  }

  private def serialize[T](b: T): String =
    getSerializer[T](Externalizer(b))

  private def deserialize[T](str: String): T =
    getSerializer[T].invert(str).get.get

  private val confKey = "com.twitter.scalding.serialization.WrappedSerialization"

  def rawSetBinary(bufs: Iterable[ClassSerialization[_]], fn: (String, String) => Unit) = {
    fn(confKey, bufs.map { case (cls, buf) => s"${cls.getName}:${serialize(buf)}" }.mkString(","))
  }
  def setBinary(conf: Configuration, bufs: Iterable[ClassSerialization[_]]): Unit =
    rawSetBinary(bufs, { case (k, v) => conf.set(k, v) })

  def getBinary(conf: Configuration): Option[Iterable[ClassSerialization[_]]] =
    Option(conf.getStrings(confKey)).map { strings =>
      strings.toIterable.map { clsbuf =>
        clsbuf.split(":") match {
          case Array(className, bufferable) =>
            // Jump through a hoop to get scalac happy
            def deser[T](cls: Class[T]): ClassSerialization[T] = (cls, deserialize[Serialization[T]](bufferable))
            deser(conf.getClassByName(className))
          case _ => sys.error(s"ill formed bufferables: ${strings}")
        }
      }
    }
}
