//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package com.twitter.scalding
package typed

import cascading.tuple.{ Fields, Tuple, TupleEntry }

/** Utility functions to assist with creating partitioned sourced. */
object PartitionUtil {
  // DO NOT USE intFields, scalding / cascading Fields.merge is broken and gets called in bowels of
  // TemplateTap. See scalding/#803.
  def toFields(start: Int, end: Int): Fields =
    Dsl.strFields((start until end).map(_.toString))

  /** A tuple converter that splits a cascading tuple into a pair of types.*/
  def converter[P, T, U >: (P, T)](valueConverter: TupleConverter[T], partitionConverter: TupleConverter[P]) = {
    TupleConverter.asSuperConverter[(P, T), U](new TupleConverter[(P, T)] {
      val arity = valueConverter.arity + partitionConverter.arity

      def apply(te: TupleEntry): (P, T) = {
        val value = Tuple.size(valueConverter.arity)
        val partition = Tuple.size(partitionConverter.arity)

        (0 until valueConverter.arity).foreach(idx => value.set(idx, te.getObject(idx)))
        (0 until partitionConverter.arity)
          .foreach(idx => partition.set(idx, te.getObject(idx + valueConverter.arity)))

        val valueTE = new TupleEntry(toFields(0, valueConverter.arity), value)
        val partitionTE = new TupleEntry(toFields(0, partitionConverter.arity), partition)

        (partitionConverter(partitionTE), valueConverter(valueTE))
      }
    })
  }

  /** A tuple setter for a pair of types which are flattened into a cascading tuple.*/
  def setter[P, T, U <: (P, T)](valueSetter: TupleSetter[T], partitionSetter: TupleSetter[P]): TupleSetter[U] =
    TupleSetter.asSubSetter[(P, T), U](new TupleSetter[(P, T)] {
      val arity = valueSetter.arity + partitionSetter.arity

      def apply(in: (P, T)) = {
        val partition = partitionSetter(in._1)
        val value = valueSetter(in._2)
        val output = Tuple.size(partition.size + value.size)

        (0 until value.size).foreach(idx => output.set(idx, value.getObject(idx)))
        (0 until partition.size).foreach(idx =>
          output.set(idx + value.size, partition.getObject(idx)))

        output
      }
    })
}
