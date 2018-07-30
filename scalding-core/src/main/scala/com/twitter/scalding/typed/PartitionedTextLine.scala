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
import java.io.{ InputStream, OutputStream }

import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextLine
import cascading.scheme.local.{ TextLine => LocalTextLine }
import cascading.tap.{ Tap, SinkMode }
import cascading.tap.hadoop.PartitionTap
import cascading.tap.local.{ FileTap, PartitionTap => LocalPartitionTap }
import cascading.tuple.Fields

/**
 * Scalding source to read or write partitioned text.
 *
 * For writing it expects a pair of `(P, String)`, where `P` is the data used for partitioning and
 * `String` is the output to write out. Below is an example.
 * {{{
 * val data = List(
 *   (("a", "x"), "line1"),
 *   (("a", "y"), "line2"),
 *   (("b", "z"), "line3")
 * )
 * IterablePipe(data, flowDef, mode)
 *   .write(PartitionTextLine[(String, String)](args("out"), "col1=%s/col2=%s"))
 * }}}
 *
 * For reading it produces a pair `(P, (Long, String))` where `P` is the partition data, `Long`
 * is the offset into the file and `String` is a line from the file. Below is an example.
 * {{{
 * val in: TypedPipe[((String, String), (Long, String))] = PartitionTextLine[(String, String)](args("in"), "col1=%s/col2=%s")
 * }}}
 *
 * @param path Base path of the partitioned directory
 * @param template Template for the partitioned path
 * @param encoding Text encoding of the file content
 */
case class PartitionedTextLine[P](
  path: String, template: String, encoding: String = TextLine.DEFAULT_CHARSET)(implicit val valueSetter: TupleSetter[String], val valueConverter: TupleConverter[(Long, String)],
    val partitionSetter: TupleSetter[P], val partitionConverter: TupleConverter[P]) extends SchemedSource with TypedSink[(P, String)] with Mappable[(P, (Long, String))] with HfsTapProvider
  with java.io.Serializable {

  // The partition fields, offset by the value arity.
  val partitionFields =
    PartitionUtil.toFields(valueSetter.arity, valueSetter.arity + partitionSetter.arity)

  // Create the underlying scheme and explicitly set the sink fields to be only the specified fields
  // see sinkFields in PartitionSchemed for other half of this work around.
  override def hdfsScheme = {
    val scheme =
      HadoopSchemeInstance(new TextLine(TextLine.DEFAULT_SOURCE_FIELDS, encoding)
        .asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(PartitionUtil.toFields(0, valueSetter.arity))
    scheme
  }

  // Create the underlying scheme and explicitly set the sink fields to be only the specified fields
  // see sinkFields in PartitionSchemed for other half of this work around.
  override def localScheme = {
    val scheme =
      new LocalTextLine(TextLine.DEFAULT_SOURCE_FIELDS, encoding)
        .asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
    scheme.setSinkFields(PartitionUtil.toFields(0, valueSetter.arity))
    scheme
  }

  /*
   Advertise all the sinkFields, both the value and partition ones, this needs to be like this even
   though it is the incorrect sink fields, otherwise scalding validation falls over, see hdfsScheme
   for other part of tweak to narrow fields back to value again to work around this.
   */
  override def sinkFields: Fields =
    PartitionUtil.toFields(0, valueSetter.arity + partitionSetter.arity)

  /** Creates the taps for local and hdfs mode.*/
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    mode match {
      case Local(_) => {
        val fileTap = new FileTap(localScheme, path, SinkMode.REPLACE)
        new LocalPartitionTap(fileTap, new TemplatePartition(partitionFields, template), SinkMode.UPDATE)
          .asInstanceOf[Tap[_, _, _]]
      }
      case Hdfs(_, _) => {
        val hfs = createHfsTap(hdfsScheme, path, SinkMode.REPLACE)
        new PartitionTap(hfs, new TemplatePartition(partitionFields, template), SinkMode.UPDATE)
          .asInstanceOf[Tap[_, _, _]]
      }
      case hdfsTest @ HadoopTest(_, _) => {
        val hfs = createHfsTap(hdfsScheme, hdfsTest.getWritePathFor(this), SinkMode.REPLACE)
        new PartitionTap(hfs, new TemplatePartition(partitionFields, template), SinkMode.UPDATE)
          .asInstanceOf[Tap[_, _, _]]
      }
      case _ => TestTapFactory(this, hdfsScheme).createTap(readOrWrite)
    }

  /**
   * Combine both the partition and value converter to extract the data from a flat cascading tuple
   * into a pair of `P` and `(offset, line)`.
   */
  override def converter[U >: (P, (Long, String))] =
    PartitionUtil.converter[P, (Long, String), U](valueConverter, partitionConverter)

  /** Flatten a pair of `P` and `line` into a cascading tuple.*/
  override def setter[U <: (P, String)] =
    PartitionUtil.setter[P, String, U](valueSetter, partitionSetter)
}
