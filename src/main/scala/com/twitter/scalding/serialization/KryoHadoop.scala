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

class KryoHadoop extends KryoSerialization {

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
