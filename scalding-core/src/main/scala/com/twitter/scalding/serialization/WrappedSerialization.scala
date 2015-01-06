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

import org.apache.hadoop.io.serializer.{ Serialization, Deserializer, Serializer }
import org.apache.hadoop.conf.{ Configurable, Configuration }

import java.io.{ DataInputStream, DataOutputStream, InputStream, OutputStream }
import java.nio.ByteBuffer
import java.nio.channels.{ Channels, WritableByteChannel }
/*
import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer


import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{ Serializer => KSerializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.serializers.FieldSerializer

import cascading.tuple.hadoop.TupleSerialization
import cascading.tuple.hadoop.io.BufferedInputStream

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.immutable.HashMap

import com.twitter.scalding.DateRange
import com.twitter.scalding.RichDate
import com.twitter.scalding.Args

import com.twitter.chill._
import com.twitter.chill.config.Config
*/

import com.twitter.bijection.Bufferable

/**
 * WrappedSerialization wraps a value in a wrapper class that
 * has an associated Bufferable that is used to deserialize
 * items wrapped in the wrapper
 */
class WrappedSerialization[T] extends Serialization[T] with Configurable {

  import WrappedSerialization.ClassBufferable

  private var conf: Option[Configuration] = None
  private var bufferables: Option[Iterable[ClassBufferable[_]]] = None

  override def getConf: Configuration = conf.get
  override def setConf(config: Configuration) {
    conf = Some(config)
    bufferables = WrappedSerialization.getBufferables(config)
  }

  def accept(c: Class[_]): Boolean =
    bufferables.map(_.exists { case (cls, _) => cls == c }).getOrElse(false)

  def getBufferable(c: Class[T]): Option[Bufferable[T]] =
    bufferables.flatMap(_.collectFirst { case (cls, b) if cls == c => b})
      // This cast should never fail since we matched the class
      .asInstanceOf[Option[Bufferable[T]]]

  def getSerializer(c: Class[T]): Serializer[T] =
    new BufferableSerializer(getBufferable(c).getOrElse(sys.error(s"Class: ${c} not found")))

  def getDeserializer(c: Class[T]): Deserializer[T] =
    new BufferableDeserializer(getBufferable(c).getOrElse(sys.error(s"Class: ${c} not found")))
}

class BufferableSerializer[T](buf: Bufferable[T]) extends Serializer[T] {
  private var chan: WritableByteChannel = _
  def open(os: OutputStream): Unit = {
    chan = Channels.newChannel(os)
  }
  def close(): Unit = { chan = null }
  def serialize(t: T): Unit = {
    // allocate a new ByteBuffer, save space for size at the header with the putInt(0)
    val bb1 = Bufferable.reallocatingPut(ByteBuffer.allocate(128).putInt(0)) { buf.put(_, t) }
    val len = bb1.position - 4 // 4 for the int for size
    bb1.position(0)
    bb1.putInt(len)
    chan.write(bb1)
  }
}

class BufferableDeserializer[T](buf: Bufferable[T]) extends Deserializer[T] {
  private var dis: DataInputStream = _
  def open(is: InputStream): Unit = {
    dis = is match {
      case d: DataInputStream => d
      case nond => new DataInputStream(nond)
    }
  }
  def close(): Unit = try { if(dis != null) dis.close } finally { dis = null }
  def deserialize(t: T): T = {
    // TODO, Bufferable should not require a copy in this case
    val bytes = new Array[Byte](dis.readInt)
    dis.readFully(bytes)
    val bb = ByteBuffer.wrap(bytes)
    buf.unsafeGet(bb)._2
  }
}

object WrappedSerialization {
  type ClassBufferable[T] = (Class[T], Bufferable[T])

  private def serialize[T](b: Bufferable[T]): String = sys.error("TODO")
  private def deserialize[T](str: String): Bufferable[T] = sys.error("TODO")

  private val confKey = "com.twitter.scalding.serialization.WrappedSerialization"

  def setBufferables(conf: Configuration, bufs: Iterable[ClassBufferable[_]]): Unit = {
    conf.set(confKey,
      bufs.map { case (cls, buf) => s"${cls.getName}:${serialize(buf)}" }.mkString(","))
  }
  def getBufferables(conf: Configuration): Option[Iterable[ClassBufferable[_]]] =
    Option(conf.getStrings(confKey)).map { strings =>
      strings.toIterable.map { clsbuf =>
        clsbuf.split(":") match {
          case Array(className, bufferable) =>
            // Jump through a hoop to get scalac happy
            def deser[T](cls: Class[T]): ClassBufferable[T] = (cls, deserialize[T](bufferable))
            deser(conf.getClassByName(className))
          case _ => sys.error(s"ill formed bufferables: ${strings}")
        }
      }
    }
}
