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

import cascading.tuple.{Tuple => CTuple}

import java.io.Serializable

abstract class TupleGetter[@specialized(Int,Long,Float,Double)T] extends Serializable {
  def get(tup : CTuple, i : Int) : T
}

trait LowPriorityTupleGetter extends Serializable {
  implicit def castingGetter[T] = new TupleGetter[T] {
    def get(tup : CTuple, i : Int) = tup.getObject(i).asInstanceOf[T]
  }
}

/**
 * Holds all the implicit resolution for TupleGetter following the typeclass pattern
 */
object TupleGetter extends LowPriorityTupleGetter with Serializable {
  def get[T](tup: CTuple, i: Int)(implicit tg: TupleGetter[T]): T = tg.get(tup, i)

  implicit object UnitGetter extends TupleGetter[Unit] {
    override def get(tup : CTuple, i : Int) = ()
  }

  implicit object BooleanGetter extends TupleGetter[Boolean] {
    override def get(tup : CTuple, i : Int) = tup.getBoolean(i)
  }

  implicit object ShortGetter extends TupleGetter[Short] {
    override def get(tup : CTuple, i : Int) = tup.getShort(i)
  }

  implicit object IntGetter extends TupleGetter[Int] {
    override def get(tup : CTuple, i : Int) = tup.getInteger(i)
  }

  implicit object LongGetter extends TupleGetter[Long] {
    override def get(tup : CTuple, i : Int) = tup.getLong(i)
  }

  implicit object FloatGetter extends TupleGetter[Float] {
    override def get(tup : CTuple, i : Int) = tup.getFloat(i)
  }

  implicit object DoubleGetter extends TupleGetter[Double] {
    override def get(tup : CTuple, i : Int) = tup.getDouble(i)
  }

  implicit object StringGetter extends TupleGetter[String] {
    override def get(tup : CTuple, i : Int) = tup.getString(i)
  }
}
