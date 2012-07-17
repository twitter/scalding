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
      .map { _.asInstanceOf[AnyRef] }
      .map { _.getClass }
      .toSeq)
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
class ClassMap(val classes : Seq[Class[_]]) {
  // Get the index of a class
  val idxOf = classes.zipWithIndex.toMap
  def apply(cls : Class[_]) : Int = idxOf(cls)
  def apply(idx : Int) = classes(idx)
  def +(other : ClassMap) : ClassMap = {
    new ClassMap((classes.toSet ++ other.classes.toSet).toSeq)
  }

  def write(out : Output) {
    out.writeInt(classes.size, true)
    classes.foreach { cls => out.writeString(cls.getName) }
  }
  def writeClassAndObject(kser : Kryo, out : Output, obj : AnyRef) {
    out.writeInt(apply(obj.getClass), true)
    kser.writeObject(out, obj)
  }
  def readClassAndObject(kser : Kryo, in : Input) : AnyRef = {
    val clsIdx = in.readInt(true)
    kser.readObject(in, apply(clsIdx).asInstanceOf[Class[AnyRef]])
  }
}

// Singletons are easy, you just return the singleton and don't read:
// It's important you actually do this, or Kryo will generate Nil != Nil, or None != None
class SingletonSerializer[T](obj: T) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {}
  def read(kser: Kryo, in: Input, cls: Class[T]): T = obj
}

// Long Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer[T <: List[_]](emptyList : List[_]) extends KSerializer[T] {
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
    obj.foreach { (t : Any) =>
      cm.writeClassAndObject(kser, out, t.asInstanceOf[AnyRef])
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
      (0 until size).foldLeft(emptyList.asInstanceOf[List[AnyRef]]) { (l, i) =>
        val iT = cm.readClassAndObject(kser, in)
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
    obj.foreach { (t : Any) => cm.writeClassAndObject(kser, out, t.asInstanceOf[AnyRef]) }
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
        val iT = cm.readClassAndObject(kser, in).asInstanceOf[T]
        vec :+ iT
      }
    }
  }
}


class MapSerializer[T <: Map[_,_]](emptyMap : Map[_,_]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    //Write the size:
    val cm = ClassMap(obj.keys ++ obj.values)
    cm.write(out)

    out.writeInt(obj.size, true)
    obj.asInstanceOf[Map[Any, Any]].foreach { pair : (Any,Any) =>
      cm.writeClassAndObject(kser, out, pair._1.asInstanceOf[AnyRef])
      cm.writeClassAndObject(kser, out, pair._2.asInstanceOf[AnyRef])
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
      (0 until size).foldLeft(emptyMap.asInstanceOf[Map[AnyRef,AnyRef]]) { (map, i) =>
        val key = cm.readClassAndObject(kser, in)
        val value = cm.readClassAndObject(kser, in)

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
