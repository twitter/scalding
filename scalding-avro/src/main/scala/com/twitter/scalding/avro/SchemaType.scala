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

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import java.nio.ByteBuffer

trait AvroSchemaType[T] extends Serializable {
  def schema: Schema
}

object AvroSchemaType {

  // primitive types

  implicit def BooleanSchema: AvroSchemaType[Boolean] = new AvroSchemaType[Boolean] {
    def schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit def ByteBufferSchema: AvroSchemaType[ByteBuffer] = new AvroSchemaType[ByteBuffer] {
    def schema = Schema.create(Schema.Type.BYTES)
  }

  implicit def DoubleSchema: AvroSchemaType[Double] = new AvroSchemaType[Double] {
    def schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def FloatSchema: AvroSchemaType[Float] = new AvroSchemaType[Float] {
    def schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit def IntSchema: AvroSchemaType[Int] = new AvroSchemaType[Int] {
    def schema = Schema.create(Schema.Type.INT)
  }

  implicit def LongSchema: AvroSchemaType[Long] = new AvroSchemaType[Long] {
    def schema = Schema.create(Schema.Type.LONG)
  }

  implicit def StringSchema: AvroSchemaType[String] = new AvroSchemaType[String] {
    def schema = Schema.create(Schema.Type.STRING)
  }

  // collections
  implicit def CollectionSchema[CC[x] <: Iterable[x], T](implicit sch: AvroSchemaType[T]): AvroSchemaType[CC[T]] = new AvroSchemaType[CC[T]] {
    def schema = Schema.createArray(sch.schema)
  }

  implicit def ArraySchema[CC[x] <: Array[x], T](implicit sch: AvroSchemaType[T]): AvroSchemaType[CC[T]] { val schema: Schema } = new AvroSchemaType[CC[T]] {
    val schema = Schema.createArray(sch.schema)
  }

  //maps
  implicit def MapSchema[CC[String, x] <: Map[String, x], T](implicit sch: AvroSchemaType[T]): AvroSchemaType[CC[String, T]] = new AvroSchemaType[CC[String, T]] {
    def schema = Schema.createMap(sch.schema)
  }

  // Avro SpecificRecord
  implicit def SpecificRecordSchema[T <: SpecificRecord](implicit mf: Manifest[T]): AvroSchemaType[T] = new AvroSchemaType[T] {
    def schema = mf.runtimeClass.newInstance.asInstanceOf[SpecificRecord].getSchema
  }

}