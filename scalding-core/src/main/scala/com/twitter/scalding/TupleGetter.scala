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

trait TupleGetter[@specialized(Int, Long, Float, Double) T] extends java.io.Serializable {
  def get(tup: Seq[String], i: Int): T
}

trait LowPriorityTupleGetter extends java.io.Serializable {
  implicit def castingGetter[T]: TupleGetter[T] = new TupleGetter[T] {
    def get(tup: Seq[String], i: Int) = tup(i).asInstanceOf[T]
  }
}

object TupleGetter extends LowPriorityTupleGetter {

  def get[T](tup: Seq[String], i: Int)(implicit tg: TupleGetter[T]): T = tg.get(tup, i)
  def of[T](implicit tg: TupleGetter[T]): TupleGetter[T] = tg

  implicit object UnitGetter extends TupleGetter[Unit] {
    override def get(tup: Seq[String], i: Int) = ()
  }

  implicit object BooleanGetter extends TupleGetter[Boolean] {
    override def get(tup: Seq[String], i: Int) = tup(i).toBoolean
  }

  implicit object ShortGetter extends TupleGetter[Short] {
    override def get(tup: Seq[String], i: Int) = tup(i).toShort
  }

  implicit object IntGetter extends TupleGetter[Int] {
    override def get(tup: Seq[String], i: Int): Int = tup(i).toInt
  }

  implicit object LongGetter extends TupleGetter[Long] {
    override def get(tup: Seq[String], i: Int) = tup(i).toLong
  }

  implicit object FloatGetter extends TupleGetter[Float] {
    override def get(tup: Seq[String], i: Int) = tup(i).toFloat
  }

  implicit object DoubleGetter extends TupleGetter[Double] {
    override def get(tup: Seq[String], i: Int) = tup(i).toDouble
  }

  implicit object StringGetter extends TupleGetter[String] {
    override def get(tup: Seq[String], i: Int) = tup(i).toString
  }
}
