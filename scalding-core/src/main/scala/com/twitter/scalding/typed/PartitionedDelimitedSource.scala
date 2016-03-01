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

import java.util.Properties
import java.io.{ InputStream, OutputStream, Serializable }

import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextDelimited
import cascading.scheme.local.{ TextDelimited => LocalTextDelimited }
import cascading.tap.{ Tap, SinkMode }
import cascading.tap.hadoop.{ Hfs, PartitionTap }
import cascading.tap.local.{ FileTap, PartitionTap => LocalPartitionTap }
import cascading.tap.partition.Partition
import cascading.tuple.{ Fields, Tuple, TupleEntry }

/**
 * Scalding source to read or write partitioned delimited text.
 *
 * For writing it expects a pair of `(P, T)`, where `P` is the data used for partitioning and
 * `T` is the output to write out. Below is an example.
 * {{{
 * val data = List(
 *   (("a", "x"), ("i", 1)),
 *   (("a", "y"), ("j", 2)),
 *   (("b", "z"), ("k", 3))
 * )
 * IterablePipe(data, flowDef, mode)
 *   .write(PartitionedDelimited[(String, String), (String, Int)](args("out"), "col1=%s/col2=%s"))
 * }}}
 *
 * For reading it produces a pair `(P, T)` where `P` is the partition data and `T` is data in the
 * files. Below is an example.
 * {{{
 * val in: TypedPipe[((String, String), (String, Int))] = PartitionedDelimited[(String, String), (String, Int)](args("in"), "col1=%s/col2=%s")
 * }}}
 */
case class PartitionedDelimitedSource[P, T](
  path: String, template: String, separator: String, fields: Fields, skipHeader: Boolean = false,
  writeHeader: Boolean = false, quote: String = "\"", strict: Boolean = true, safe: Boolean = true)(implicit mt: Manifest[T], val valueSetter: TupleSetter[T], val valueConverter: TupleConverter[T],
    val partitionSetter: TupleSetter[P], val partitionConverter: TupleConverter[P]) extends PartitionSchemed[P, T] with Serializable {
  assert(
    fields.size == valueSetter.arity,
    "The number of fields needs to be the same as the arity of the value setter")

  val types: Array[Class[_]] = {
    if (classOf[scala.Product].isAssignableFrom(mt.runtimeClass)) {
      //Assume this is a Tuple:
      mt.typeArguments.map { _.runtimeClass }.toArray
    } else {
      //Assume there is only a single item
      Array(mt.runtimeClass)
    }
  }

  // Create the underlying scheme and explicitly set the sink fields to be only the specified fields
  // see sinkFields in PartitionSchemed for other half of this work around.
  override def hdfsScheme = {
    val scheme =
      HadoopSchemeInstance(new TextDelimited(fields, null, skipHeader, writeHeader, separator, strict, quote, types, safe)
        .asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(fields)
    scheme
  }

  // Create the underlying scheme and explicitly set the sink fields to be only the specified fields
  // see sinkFields in PartitionSchemed for other half of this work around.
  override def localScheme = {
    val scheme =
      new LocalTextDelimited(fields, skipHeader, writeHeader, separator, strict, quote, types, safe)
        .asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
    scheme.setSinkFields(fields)
    scheme
  }
}

/**
 * Trait to assist with creating objects such as [[PartitionedTsv]] to read from separated files.
 * Override separator, skipHeader, writeHeader as needed.
 */
trait PartitionedDelimited extends Serializable {
  def separator: String

  def apply[P: Manifest: TupleConverter: TupleSetter, T: Manifest: TupleConverter: TupleSetter](path: String, template: String): PartitionedDelimitedSource[P, T] =
    PartitionedDelimitedSource(path, template, separator, PartitionUtil.toFields(0, implicitly[TupleSetter[T]].arity))

  def apply[P: Manifest: TupleConverter: TupleSetter, T: Manifest: TupleConverter: TupleSetter](path: String, template: String, fields: Fields): PartitionedDelimitedSource[P, T] =
    PartitionedDelimitedSource(path, template, separator, fields)
}

/** Partitioned typed tab separated source.*/
object PartitionedTsv extends PartitionedDelimited {
  val separator = "\t"
}

/** Partitioned typed commma separated source.*/
object PartitionedCsv extends PartitionedDelimited {
  val separator = ","
}

/** Partitioned typed pipe separated source.*/
object PartitionedPsv extends PartitionedDelimited {
  val separator = "|"
}

/** Partitioned typed `\1` separated source (commonly used by Pig).*/
object PartitionedOsv extends PartitionedDelimited {
  val separator = "\u0001"
}
