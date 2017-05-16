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
package com.twitter.scalding.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{ Serializer => KSerializer }
import com.esotericsoftware.kryo.io.{ Input, Output }

import com.twitter.scalding._

/**
 * This is a runtime check for types we should never be serializing
 */
class ThrowingSerializer[T] extends KSerializer[T] {
  override def write(kryo: Kryo, output: Output, t: T): Unit = {
    sys.error(s"Kryo should never be used to serialize an instance: $t")
  }
  override def read(kryo: Kryo, input: Input, t: Class[T]): T =
    sys.error("Kryo should never be used to serialize an instance, class: $t")
}

/**
 * *
 * Below are some serializers for objects in the scalding project.
 */
class RichDateSerializer extends KSerializer[RichDate] {
  // RichDates are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, date: RichDate): Unit = {
    out.writeLong(date.timestamp, true)
  }

  def read(kser: Kryo, in: Input, cls: Class[RichDate]): RichDate =
    RichDate(in.readLong(true))
}

class DateRangeSerializer extends KSerializer[DateRange] {
  // DateRanges are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, range: DateRange): Unit = {
    out.writeLong(range.start.timestamp, true)
    out.writeLong(range.end.timestamp, true)
  }

  def read(kser: Kryo, in: Input, cls: Class[DateRange]): DateRange = {
    DateRange(RichDate(in.readLong(true)), RichDate(in.readLong(true)))
  }
}

class ArgsSerializer extends KSerializer[Args] {
  // Args are immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, a: Args): Unit = {
    out.writeString(a.toString)
  }
  def read(kser: Kryo, in: Input, cls: Class[Args]): Args =
    Args(in.readString)
}

class IntFieldSerializer extends KSerializer[IntField[_]] {
  //immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, a: IntField[_]): Unit = {
    out.writeInt(a.id)
    kser.writeClassAndObject(out, a.ord)
    kser.writeClassAndObject(out, a.mf)
  }
  def read(kser: Kryo, in: Input, cls: Class[IntField[_]]): IntField[_] = {
    val id = in.readInt
    val ord = kser.readClassAndObject(in).asInstanceOf[Ordering[Any]]
    val mf = kser.readClassAndObject(in).asInstanceOf[Option[Manifest[Any]]]
    IntField[Any](id)(ord, mf)
  }
}

class StringFieldSerializer extends KSerializer[StringField[_]] {
  //immutable, no need to copy them
  setImmutable(true)
  def write(kser: Kryo, out: Output, a: StringField[_]): Unit = {
    out.writeString(a.id)
    kser.writeClassAndObject(out, a.ord)
    kser.writeClassAndObject(out, a.mf)
  }
  def read(kser: Kryo, in: Input, cls: Class[StringField[_]]): StringField[_] = {
    val id = in.readString
    val ord = kser.readClassAndObject(in).asInstanceOf[Ordering[Any]]
    val mf = kser.readClassAndObject(in).asInstanceOf[Option[Manifest[Any]]]
    StringField[Any](id)(ord, mf)
  }
}

