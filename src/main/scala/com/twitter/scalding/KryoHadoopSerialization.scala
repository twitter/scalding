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
package com.twitter.scalding

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

class KryoHadoopSerialization extends KryoSerialization {

  /** TODO!!!
   * Deal with this issue.  The problem is grouping by Kryo serialized
   * objects silently breaks the results.  If Kryo gets in front of TupleSerialization
   * (and possibly Writable, unclear at this time), grouping is broken.
   * There are two issues here:
   * 1) Kryo objects not being compared properly.
   * 2) Kryo being used instead of cascading.
   *
   * We must identify each and fix these bugs.
   */
  val highPrioritySerializations = List(new WritableSerialization, new TupleSerialization)

  override def accept(klass : Class[_]) = {
    highPrioritySerializations.forall { !_.accept(klass) }
  }

  override def decorateKryo(newK : Kryo) : Kryo = {

    newK.addDefaultSerializer(classOf[List[Any]], new ListSerializer(List[AnyRef]()))
    newK.addDefaultSerializer(classOf[Vector[Any]], new VectorSerializer[Any])
    newK.register(classOf[RichDate], new RichDateSerializer())
    newK.register(classOf[DateRange], new DateRangeSerializer())
    // Add some maps
    newK.addDefaultSerializer(classOf[ListMap[Any,Any]],
      new MapSerializer[ListMap[Any,Any]](ListMap[Any,Any]()))
    newK.addDefaultSerializer(classOf[Map[Any,Any]],
      new MapSerializer[Map[Any,Any]](Map[Any,Any]()))

    //Add commonly used types with Fields serializer:
    registeredTypes.foreach { cls => newK.register(cls) }
    // Register some commonly used Scala singleton objects. Because these
    // are singletons, we must return the exact same local object when we
    // deserialize rather than returning a clone as FieldSerializer would.
    // TODO there may be a smarter way to detect scala object (singletons)
    // and do this automatically
    //This includes Nil:
    singletons.foreach { inst => newK.register( inst.getClass, new SingletonSerializer(inst) ) }
    newK
  }

  //Put any singleton objects that should be serialized here
  def singletons : Iterable[AnyRef] = {
    List(None, Nil, ().asInstanceOf[AnyRef])
  }

  // Types to pre-register.
  // TODO: this was cargo-culted from spark. We should actually measure to see the best
  // choices for the common use cases. Since hadoop tells us the class we are deserializing
  // the benefit of this is much less than spark
  def registeredTypes : List[Class[_]] = {
    List(
      // Arrays
      Array(1), Array(1.0), Array(1.0f), Array(1L), Array(""), Array(("", "")),
      Array(new java.lang.Object), Array(1.toByte), Array(true), Array('c'),
      // Specialized Tuple2s: (Int,Long,Double) are defined for 1,2
      Tuple1(""), Tuple1(1), Tuple1(1L), Tuple1(1.0),
      ("", ""), (1, 1), (1.0, 1.0), (1L, 1L),
      (1, 1.0), (1.0, 1), (1L, 1.0), (1.0, 1L), (1, 1L), (1L, 1),
      // Options and Either
      Some(1), Left(1), Right(1),
      // Higher-dimensional tuples, not specialized
      (1, 1, 1), (1, 1, 1, 1), (1, 1, 1, 1, 1)
    ).map { _.getClass }
  }
}

// Singletons are easy, you just return the singleton and don't read:
// It's important you actually do this, or Kryo will generate Nil != Nil, or None != None
class SingletonSerializer[T](obj: T) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {}
  def read(kser: Kryo, in: Input, cls: Class[T]): T = obj
}

// Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer[T <: List[_]](emptyList : List[_]) extends KSerializer[T] {
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
     // TODO: build a map class => int and just write ints to the classes
     // probably each list only contains a few different classes
    obj.foreach { (t : Any) => kser.writeClassAndObject(out, t) }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
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
        val iT = kser.readClassAndObject(in)
        iT :: l
      }.reverse.asInstanceOf[T]
    }
  }
}

class VectorSerializer[T] extends KSerializer[Vector[T]] {
  def write(kser: Kryo, out: Output, obj: Vector[T]) {
    //Write the size:
    out.writeInt(obj.size, true)
    obj.foreach { (t : Any) => kser.writeClassAndObject(out, t) }
  }

  def read(kser: Kryo, in: Input, cls: Class[Vector[T]]) : Vector[T] = {
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
        val iT = kser.readClassAndObject(in).asInstanceOf[T]
        vec :+ iT
      }
    }
  }
}


class MapSerializer[T <: Map[_,_]](emptyMap : Map[_,_]) extends KSerializer[T] {
  def write(kser: Kryo, out: Output, obj: T) {
    //Write the size:
    out.writeInt(obj.size, true)
    obj.asInstanceOf[Map[Any, Any]].foreach { pair : (Any,Any) =>
      // TODO: build a map class => int and just write ints to the classes
      // probably each list only contains a few different classes
      kser.writeClassAndObject(out, pair._1)
      kser.writeClassAndObject(out, pair._2)
    }
  }

  def read(kser: Kryo, in: Input, cls: Class[T]) : T = {
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
        val key = kser.readClassAndObject(in)
        val value = kser.readClassAndObject(in)
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
