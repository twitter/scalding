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

import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.serialize.DateSerializer

import cascading.kryo.KryoSerialization;
import cascading.tuple.hadoop.BufferedInputStream
import cascading.tuple.hadoop.TupleSerialization
import cascading.kryo.Kryo

import scala.annotation.tailrec

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

    /* Here is some jank for your reading pleasure:
     * Kryo looks up the serializer in a hashtable of java.lang.Class objects.
     * What if X is a subclass of Y and my serialization is co-variant?
     * TOO BAD!  Scala uses class scala.collection.immutable.List if you get classOf[List[_]]
     * but class scala.collection.immutable.$colon$colon for most concrete instances
     * This deals with the case of containers holding lists, even if klass is not directly one.
     */
    val listSer = new ListSerializer(newK)
    newK.register(List(1).getClass, listSer)
    //Make sure to register the Nil singleton, different from List(1).getClass
    newK.register(Nil.getClass, listSer)
    newK.register(classOf[java.util.Date], new DateSerializer)
    newK.register(classOf[RichDate], new RichDateSerializer(newK))
    newK.register(classOf[DateRange], new DateRangeSerializer(newK))

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
    //Note: Nil is a singleton, but handled by the ListSerializer correctly
    List(None)
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
class SingletonSerializer(obj: AnyRef) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {}
  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = obj.asInstanceOf[T]
}

// Lists cause stack overflows for Kryo because they are cons cells.
class ListSerializer(kser : Kryo) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
    //Write the size:
    val list = obj.asInstanceOf[List[AnyRef]]
    kser.writeObjectData(buf, new java.lang.Integer(list.size))
    /*
     * An excellent question arrises at this point:
     * How do we deal with List[List[T]]?
     * Since by the time this method is called, the ListSerializer has
     * already been registered, this iterative method will be used on
     * each element, and we should be safe.
     * The only risk is List[List[List[List[.....
     * But anyone who tries that gets what they deserve
     */
    list.foreach { t => kser.writeClassAndObject(buf, t) }
  }

  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]) : T = {
    val size = kser.readObjectData(buf, classOf[java.lang.Integer]).intValue
    //Produce the reversed list:
    if (size == 0) {
      /*
       * this is only here at compile time.  The type T is erased, but the
       * compiler verifies that we are intending to return a type T here.
       */
      Nil.asInstanceOf[T]
    }
    else {
      (0 until size).foldLeft(List[AnyRef]()) { (l, i) =>
        val iT = kser.readClassAndObject(buf)
        iT :: l
      }.reverse.asInstanceOf[T]
    }
  }
}

/***
 * Below are some serializers for objects in the scalding project.
 */
class RichDateSerializer(kser : Kryo) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
    kser.writeObjectData(buf,
      new java.lang.Long(obj.asInstanceOf[RichDate].value.getTime))
  }
  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = {
    RichDate(kser.readObjectData[java.lang.Long](buf,classOf[java.lang.Long]).longValue)
      .asInstanceOf[T]
  }
}

class DateRangeSerializer(kser : Kryo) extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
    kser.writeObjectData(buf,
      new java.lang.Long(obj.asInstanceOf[DateRange].start.value.getTime))
    kser.writeObjectData(buf,
      new java.lang.Long(obj.asInstanceOf[DateRange].end.value.getTime))
  }
  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = {
    val start = kser.readObjectData[java.lang.Long](buf,classOf[java.lang.Long])
    val end = kser.readObjectData[java.lang.Long](buf,classOf[java.lang.Long])
    DateRange(RichDate(start.longValue), RichDate(end.longValue))
      .asInstanceOf[T]
  }
}
