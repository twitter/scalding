/*  Copyright 2013 eBay, inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twitter.scalding.avro

import cascading.avro.AvroScheme
import cascading.avro.PackedAvroScheme
import cascading.avro.local.{ AvroScheme => LAvroScheme, PackedAvroScheme => LPackedAvroScheme }
import com.twitter.scalding._
import org.apache.avro.Schema
import cascading.scheme.Scheme
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import cascading.tuple.Fields
import org.apache.hadoop.conf.Configuration
import collection.JavaConverters._
import org.apache.hadoop.mapred.{ OutputCollector, RecordReader, JobConf }

trait UnpackedAvroFileScheme extends FileSource {
  def schema: Option[Schema]

  // HadoopSchemeInstance gives compile errors in 2.10 for some reason
  override def hdfsScheme = (new AvroScheme(schema.getOrElse(null))).asInstanceOf[Scheme[Configuration, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def localScheme = (new LAvroScheme(schema.getOrElse(null))).asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]

}

trait PackedAvroFileScheme[T] extends FileSource {
  def schema: Schema

  // HadoopSchemeInstance gives compile errors for this in 2.10 for some reason
  override def hdfsScheme = (new PackedAvroScheme[T](schema)).asInstanceOf[Scheme[Configuration, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def localScheme = (new LPackedAvroScheme[T](schema)).asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]
}

object UnpackedAvroSource {

  def apply[T](path: String, schema: Schema) =
    new UnpackedAvroSource[T](Seq(path), Some(schema))

  def apply[T](path: String) =
    new UnpackedAvroSource[T](Seq(path), None)

  def apply[T](paths: Seq[String], schema: Schema) =
    new UnpackedAvroSource[T](paths, Some(schema))

  def apply[T](paths: Seq[String]) =
    new UnpackedAvroSource[T](paths, None)

  def apply[T](path: String, schema: Option[Schema]) =
    new UnpackedAvroSource[T](Seq(path), schema)
}

case class UnpackedAvroSource[T](paths: Seq[String], schema: Option[Schema])(implicit val conv: TupleConverter[T], tset: TupleSetter[T])

  extends FixedPathSource(paths: _*)
  with UnpackedAvroFileScheme with Mappable[T] with TypedSink[T] {

  override def sinkFields: Fields = {
    val outFields = schema.map {
      schema =>
        val schemaFields = schema.getFields
        schemaFields.asScala.foldLeft(new Fields())((cFields, sField) => cFields.append(new Fields(sField.name())))
    }
    outFields.getOrElse(Dsl.intFields(0 until setter.arity))
  }

  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](tset)

}

object PackedAvroSource {
  def apply[T: AvroSchemaType: Manifest: TupleConverter](path: String) = new PackedAvroSource[T](Seq(path))
}

case class PackedAvroSource[T](paths: Seq[String])(implicit val mf: Manifest[T], conv: TupleConverter[T], tset: TupleSetter[T], avroType: AvroSchemaType[T])
  extends FixedPathSource(paths: _*) with PackedAvroFileScheme[T] with Mappable[T] with TypedSink[T] {
  override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](tset)

  override def schema = avroType.schema
}
