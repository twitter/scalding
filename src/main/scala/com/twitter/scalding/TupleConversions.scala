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

import cascading.tuple.TupleEntry
import cascading.tuple.TupleEntryIterator
import cascading.tuple.{Tuple => CTuple}
import cascading.tuple.Tuples

import scala.collection.JavaConverters._

@deprecated("No need to extend this trait", "0.8.2")
trait TupleConversions {

}

object TupleConversions extends java.io.Serializable {

  // Convert a TupleEntry to a List of CTuple, of length 2, with key, value
 // from the TupleEntry (useful for RichPipe.unpivot)
  def toKeyValueList(tupe : TupleEntry) : List[CTuple] = {
    val keys = tupe.getFields
    (0 until keys.size).map { idx =>
      new CTuple(keys.get(idx), tupe.getObject(idx))
    }.toList
  }

  // Convert a Cascading TupleEntryIterator into a Stream of a given type
  def toStream[T](it : TupleEntryIterator)(implicit conv : TupleConverter[T]) : Stream[T] = {
    if(null != it && it.hasNext) {
      val next = conv(it.next)
      // Note that Stream is lazy in the second parameter, so this doesn't blow up the stack
      Stream.cons(next, toStream(it)(conv))
    }
    else {
      Stream.Empty
    }
  }

  def tupleAt(idx: Int)(tup: CTuple): CTuple = {
    val obj = tup.getObject(idx)
    val res = CTuple.size(1)
    res.set(0, obj)
    res
  }

  def productToTuple(in : Product) : CTuple = {
    val t = new CTuple
    in.productIterator.foreach(t.add(_))
    t
  }
}
