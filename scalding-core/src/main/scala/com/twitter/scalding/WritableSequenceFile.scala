/*
Copyright 2013 Twitter, Inc.

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

import cascading.scheme.hadoop.{ WritableSequenceFile => CHWritableSequenceFile }
import cascading.tap.SinkMode
import cascading.tuple.Fields

import org.apache.hadoop.io.Writable

trait WritableSequenceFileScheme extends SchemedSource {
  /**
   * There are three allowed cases:
   * fields.size == 1 and keyType == null
   * fields.size == 1 and valueType == null
   * fields.size == 2 and keyType != null and valueType != null
   */
  def fields: Fields
  def keyType: Class[_ <: Writable]
  def valueType: Class[_ <: Writable]

  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme =
    HadoopSchemeInstance(new CHWritableSequenceFile(fields, keyType, valueType).asInstanceOf[cascading.scheme.Scheme[_, _, _, _, _]])
}

object WritableSequenceFile {
  /** by default uses the first two fields in the tuple */
  def apply[K <: Writable: Manifest, V <: Writable: Manifest](path: String): WritableSequenceFile[K, V] =
    WritableSequenceFile(path, Dsl.intFields(0 to 1))
}

case class WritableSequenceFile[K <: Writable: Manifest, V <: Writable: Manifest](
  p: String,
  f: Fields,
  override val sinkMode: SinkMode = SinkMode.REPLACE)
  extends FixedPathSource(p)
  with WritableSequenceFileScheme
  with LocalTapSource
  with TypedSink[(K, V)]
  with Mappable[(K, V)] {

  override val fields = f
  override val keyType = manifest[K].runtimeClass.asInstanceOf[Class[_ <: Writable]]
  override val valueType = manifest[V].runtimeClass.asInstanceOf[Class[_ <: Writable]]

  def setter[U <: (K, V)]: TupleSetter[U] =
    TupleSetter.asSubSetter[(K, V), U](TupleSetter.tup2Setter[(K, V)])
  override def sinkFields = f

  def converter[U >: (K, V)]: TupleConverter[U] =
    TupleConverter.asSuperConverter(TupleConverter.tuple2Converter[K, V])
  override def sourceFields = f
}

object MultipleWritableSequenceFiles {
  /** by default uses the first two fields in the tuple */
  def apply[K <: Writable: Manifest, V <: Writable: Manifest](paths: Seq[String]): MultipleWritableSequenceFiles[K, V] =
    MultipleWritableSequenceFiles(paths, Dsl.intFields(0 to 1))
}

/**
 * This is only a TypedSource (which is a superclass of Mappable) as sinking into multiple directories
 * is not well defined
 */
case class MultipleWritableSequenceFiles[K <: Writable: Manifest, V <: Writable: Manifest](
  p: Seq[String], f: Fields)
  extends FixedPathSource(p: _*)
  with WritableSequenceFileScheme
  with LocalTapSource
  with Mappable[(K, V)] {

  override val fields = f
  override val keyType = manifest[K].runtimeClass.asInstanceOf[Class[_ <: Writable]]
  override val valueType = manifest[V].runtimeClass.asInstanceOf[Class[_ <: Writable]]

  def converter[U >: (K, V)]: TupleConverter[U] =
    TupleConverter.asSuperConverter(TupleConverter.tuple2Converter[K, V])
  override def sourceFields = f
}
