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

import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.hadoop.io.serializer.{Serialization, Deserializer, Serializer, WritableSerialization}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import cascading.kryo.KryoSerialization;
import cascading.tuple.hadoop.TupleSerialization
import cascading.tuple.hadoop.io.BufferedInputStream

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable.{Map => MMap}

import com.twitter.scalding.DateRange
import com.twitter.scalding.RichDate
import com.twitter.scalding.Args

// Singletons are easy, you just return the singleton and don't read:
// It's important you actually do this, or Kryo will generate Nil != Nil, or None != None
class SingletonSerializer[T](obj: T) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {}
  def read(kser: Kryo, in: Input, cls: Class[T]): T = obj
}

// Long Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer[V, T <: List[V]](emptyList : List[V]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    //Write the size:
    out.writeInt(obj.size, true)
    /*
     * An excellent question arises at this point:
     * How do we deal with List[List[T]]?
     * Since by the time this method is called, the ListSerializer has
     * already been registered, this iterative method will be used on
     * each element, and we should be safe.
     * The only risk is List[List[List[List[.....
     * But anyone who tries that gets what they deserve
     */
    obj.foreach { t =>
      val tRef = t.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, tRef)
      // After each itermediate object, flush
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
    //Produce the reversed list:
    val size = in.readInt(true)
    if (size == 0) {
      emptyList.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(emptyList) { (l, i) =>
        val iT = kser.readClassAndObject(in).asInstanceOf[V]
        iT :: l
      }.reverse.asInstanceOf[T]
    }
  }
}

class VectorSerializer[T] extends KSerializer[Vector[T]] {
  def write(kser: Kryo, out: Output, obj: Vector[T]) {
    //Write the size:
    out.writeInt(obj.size, true)
    obj.foreach { t  =>
      val tRef = t.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, tRef)
      // After each intermediate object, flush
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[Vector[T]]) : Vector[T] = {
    //Produce the reversed list:
    val size = in.readInt(true)
    if (size == 0) {
      /*
       * this is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      Vector.empty[T]
    }
    else {
      (0 until size).foldLeft(Vector.empty[T]) { (vec, i) =>
        val iT = kser.readClassAndObject(in).asInstanceOf[T]
        vec :+ iT
      }
    }
  }
}


class MapSerializer[K,V,T <: Map[K,V]](emptyMap : Map[K,V]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    out.writeInt(obj.size, true)
    obj.foreach { pair : (K,V) =>
      val kRef = pair._1.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, kRef)
      out.flush

      val vRef = pair._2.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, vRef)
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
    val size = in.readInt(true)
    if (size == 0) {
      emptyMap.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(emptyMap) { (map, i) =>
        val key = kser.readClassAndObject(in).asInstanceOf[K]
        val value = kser.readClassAndObject(in).asInstanceOf[V]

        map + (key -> value)
      }.asInstanceOf[T]
    }
  }
}

class SetSerializer[V,T<:Set[V]](empty : Set[V]) extends KSerializer[T] {
  def write(kser : Kryo, out : Output, obj : T) {
    out.writeInt(obj.size, true)
    obj.foreach { v =>
      val vRef = v.asInstanceOf[AnyRef]
      kser.writeClassAndObject(out, vRef)
      out.flush
    }
  }
  def read(kser : Kryo, in : Input, cls : Class[T]) : T = {
    val size = in.readInt(true)
    (0 until size).foldLeft(empty) { (set, i) =>
      set + (kser.readClassAndObject(in).asInstanceOf[V])
    }.asInstanceOf[T]
  }
}

/** Uses facts about how scala compiles object singletons to Java + reflection
 */
class ObjectSerializer[T] extends KSerializer[T] {
  val cachedObj = MMap[Class[_],Option[T]]()

  // Does nothing
  override def write(kser: Kryo, out: Output, obj: T) { }

  protected def createSingleton(cls : Class[_]) : Option[T] = {
    try {
      if(isObj(cls)) {
        Some(cls.getDeclaredField("MODULE$")
            .get(null)
            .asInstanceOf[T])
      }
      else {
        None
      }
    }
    catch {
      case e: NoSuchFieldException => None
    }
  }

  protected def cachedRead(cls : Class[_]) : Option[T] = {
    cachedObj.synchronized { cachedObj.getOrElseUpdate(cls, createSingleton(cls)) }
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]) : T = cachedRead(cls).get

  def accepts(cls : Class[_]) : Boolean = cachedRead(cls).isDefined

  protected def isObj(klass : Class[_]) : Boolean =
    klass.getName.last == '$' &&
      classOf[scala.ScalaObject].isAssignableFrom(klass)
}

/***
 * Below are some serializers for objects in the scalding project.
 */
class RichDateSerializer() extends KSerializer[RichDate] {
  def write(kser: Kryo, out: Output, date: RichDate) {
    out.writeLong(date.value.getTime, true);
  }

  def read(kser: Kryo, in: Input, cls: Class[RichDate]): RichDate = {
    RichDate(in.readLong(true))
  }
}

class DateRangeSerializer() extends KSerializer[DateRange] {
  def write(kser: Kryo, out: Output, range: DateRange) {
    out.writeLong(range.start.value.getTime, true);
    out.writeLong(range.end.value.getTime, true);
  }

  def read(kser: Kryo, in: Input, cls: Class[DateRange]): DateRange = {
    DateRange(RichDate(in.readLong(true)), RichDate(in.readLong(true)));
  }
}

class ArgsSerializer extends KSerializer[Args] {
  def write(kser: Kryo, out : Output, a : Args) {
    out.writeString(a.toString)
  }
  def read(kser : Kryo, in : Input, cls : Class[Args]) : Args =
    Args(in.readString)
}
