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

import com.twitter.scalding.DateRange
import com.twitter.scalding.RichDate

object ClassMap {
  implicit def apply(objects : Iterable[_]) = {
    new ClassMap(objects
      .map { _.asInstanceOf[AnyRef].getClass }
      .toSet //Make sure they are all unique
      .toIndexedSeq)
  }
  def read(in : Input) : ClassMap = {
    val size = in.readInt(true);
    val classSeq = (0 until size).foldLeft(Vector[Class[_]]()) { (cls, idx) =>
      cls :+ Class.forName(in.readString)
    }
    new ClassMap(classSeq)
  }
}

/**
 * We've been seeing some issues with letting Kryo track the classes.
 * This is arguably a hack, and Kryo should handle it, but we're working
 * around for now.
 */
class ClassMap(val idxToClass : IndexedSeq[Class[_]]) {
  // Get the index of a class
  lazy val idxOf = idxToClass.zipWithIndex.toMap
  def apply(cls : Class[_]) : Int = idxOf(cls)
  def apply(idx : Int) = idxToClass(idx)
  def :+(klass : Class[_]) : ClassMap = {
    if (idxOf.contains(klass)) {
      this
    }
    else {
      val newIdxToClass = idxToClass :+ klass
      new ClassMap(newIdxToClass)
    }
  }
  def ++(other : ClassMap) : ClassMap = {
    if(idxToClass.size < other.idxToClass.size) {
      //Reverse:
      other ++ this
    }
    else {
      other.idxToClass.foldLeft(this) { (cm, cls) => cm :+ cls }
    }
  }

  def write(out : Output) {
    out.writeInt(idxToClass.size, true)
    idxToClass.foreach { cls => out.writeString(cls.getName) }
  }
  def writeClass(out : Output, klass : Class[_]) {
    out.writeInt(apply(klass), true)
  }
  def readClass(in : Input) : Class[_] = {
    apply(in.readInt(true))
  }
  override def toString = {
    "ClassMap(" + idxToClass.mkString(", ") + ")"
  }
}

// Singletons are easy, you just return the singleton and don't read:
// It's important you actually do this, or Kryo will generate Nil != Nil, or None != None
class SingletonSerializer[T](obj: T) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {}
  def read(kser: Kryo, in: Input, cls: Class[T]): T = obj
}

// Long Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer[V, T <: List[V]](emptyList : List[V]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    // Kryo is not efficient with deeper references, so we handle it manually:
    val cm = ClassMap(obj)
    cm.write(out)
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
      cm.writeClass(out, tRef.getClass)
      kser.writeObject(out, tRef)
      // After each itermediate object, flush
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
    //Read the classmap:
    val cm = ClassMap.read(in)
    val size = in.readInt(true);

    //Produce the reversed list:
    if (size == 0) {
      /*
       * this is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      emptyList.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(emptyList) { (l, i) =>
        val klass = cm.readClass(in).asInstanceOf[Class[V]]
        val iT = kser.readObject(in, klass)
        iT :: l
      }.reverse.asInstanceOf[T]
    }
  }
}

class VectorSerializer[T] extends KSerializer[Vector[T]] {
  def write(kser: Kryo, out: Output, obj: Vector[T]) {
    //Write the classMap
    val cm = ClassMap(obj)
    cm.write(out)
    //Write the size:
    out.writeInt(obj.size, true)
    obj.foreach { t  =>
      val tRef = t.asInstanceOf[AnyRef]
      cm.writeClass(out, tRef.getClass)
      kser.writeObject(out, tRef)
      // After each intermediate object, flush
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[Vector[T]]) : Vector[T] = {
    //Read the classmap:
    val cm = ClassMap.read(in)
    val size = in.readInt(true);

    //Produce the reversed list:
    if (size == 0) {
      /*
       * this is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      Vector.empty[T]
    }
    else {
      (0 until size).foldLeft(Vector.empty[T]) { (vec, i) =>
        val klass = cm.readClass(in).asInstanceOf[Class[T]]
        val iT = kser.readObject(in, klass)
        vec :+ iT
      }
    }
  }
}


class MapSerializer[K,V,T <: Map[K,V]](emptyMap : Map[K,V]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    //Write the size:
    val cm = ClassMap(obj.keys ++ obj.values)
    cm.write(out)

    out.writeInt(obj.size, true)
    obj.foreach { pair : (K,V) =>
      val kRef = pair._1.asInstanceOf[AnyRef]
      val vRef = pair._2.asInstanceOf[AnyRef]
      cm.writeClass(out, kRef.getClass)
      kser.writeObject(out, kRef)
      out.flush
      cm.writeClass(out, vRef.getClass)
      kser.writeObject(out, vRef)
      out.flush
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
    val cm = ClassMap.read(in)
    val size = in.readInt(true);

    if (size == 0) {
      /*
       * this cast is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      emptyMap.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(emptyMap) { (map, i) =>
        val kklass = cm.readClass(in).asInstanceOf[Class[K]]
        val key = kser.readObject(in, kklass)

        val vklass = cm.readClass(in).asInstanceOf[Class[V]]
        val value = kser.readObject(in, vklass)

        map + (key -> value)
      }.asInstanceOf[T]
    }
  }
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
