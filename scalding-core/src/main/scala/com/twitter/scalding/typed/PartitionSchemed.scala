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

import cascading.tap.hadoop.{ PartitionTap => HdfsPartitionTap }
import cascading.tap.local.{ FileTap, PartitionTap => LocalPartitionTap }
import cascading.tap.{ SinkMode, Tap }
import cascading.tuple.Fields
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

trait PartitionSchemedOps extends SchemedSource with HfsTapProvider with LocalTapProvider {
  def partitionFields: Fields
  def template: String

  protected def makePartition = new TemplatePartition(partitionFields, template)

  override def createHdfsReadTap(strictSources: Boolean, conf: Configuration, mode: Mode, sinkMode: SinkMode): Tap[_, _, _] =
    {
      val hfsTap = createHfsTap(hdfsScheme, hdfsWritePath, SinkMode.REPLACE)
      new HdfsPartitionTap(hfsTap, makePartition, SinkMode.UPDATE)
    }

  override def createLocalReadTap(sinkMode: SinkMode): Tap[_, _, _] =
    {
      val localTap = super.createLocalFileTap(localWritePath, SinkMode.REPLACE)
      new LocalPartitionTap(localTap, makePartition, SinkMode.UPDATE)
    }

  override def createHdfsWriteTap(sinkMode: SinkMode): Tap[_, _, _] = {
    val hfsTap = createHfsTap(hdfsScheme, hdfsWritePath, SinkMode.REPLACE)
    new HdfsPartitionTap(hfsTap, makePartition, SinkMode.UPDATE)
  }

  override def createLocalWriteTap(sinkMode: SinkMode): Tap[_, _, _] = {
    val localTap = super.createLocalFileTap(localWritePath, SinkMode.REPLACE)
    new LocalPartitionTap(localTap, makePartition, SinkMode.UPDATE)
  }
}
/**
 * Trait to assist with creating partitioned sources.
 *
 * Apart from the abstract members below, `hdfsScheme` and `localScheme` also need to be set.
 * Note that for both of them the sink fields need to be set to only include the actual fields
 * that should be written to file and not the partition fields.
 */
trait PartitionSchemed[P, T] extends PartitionSchemedOps with TypedSink[(P, T)] with Mappable[(P, T)] {
  def path: String
  def template: String
  def valueSetter: TupleSetter[T]
  def valueConverter: TupleConverter[T]
  def partitionSetter: TupleSetter[P]
  def partitionConverter: TupleConverter[P]
  def fields: Fields

  // The partition fields, offset by the value arity.
  def partitionFields =
    PartitionUtil.toFields(valueSetter.arity, valueSetter.arity + partitionSetter.arity)

  /*
   Advertise all the sinkFields, both the value and partition ones, this needs to be like this even
   though it is the incorrect sink fields, otherwise scalding validation falls over. The sink fields
   of the scheme itself then to be over written to only include the actual sink fields.
   */
  override def sinkFields: Fields = fields.append(partitionFields)

  /**
   * Combine both the partition and value converter to extract the data from a flat cascading tuple
   * into a pair of `P` and `T`.
   */
  override def converter[U >: (P, T)] =
    PartitionUtil.converter[P, T, U](valueConverter, partitionConverter)

  /** Flatten a pair of `P` and `T` into a cascading tuple.*/
  override def setter[U <: (P, T)] =
    PartitionUtil.setter[P, T, U](valueSetter, partitionSetter)
}
