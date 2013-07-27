package com.twitter.scalding.avro

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import java.nio.ByteBuffer


trait AvroSchemaType[T] extends Serializable {
  def schema: Schema
}

object AvroSchemaType {

  // primitive types

  implicit def BooleanSchema = new AvroSchemaType[Boolean] {
    def schema = Schema.create(Schema.Type.BOOLEAN)
  }

  implicit def ByteBufferSchema = new AvroSchemaType[ByteBuffer] {
    def schema = Schema.create(Schema.Type.BYTES)
  }

  implicit def DoubleSchema = new AvroSchemaType[Double] {
    def schema = Schema.create(Schema.Type.DOUBLE)
  }

  implicit def FloatSchema = new AvroSchemaType[Float] {
    def schema = Schema.create(Schema.Type.FLOAT)
  }

  implicit def IntSchema = new AvroSchemaType[Int] {
    def schema = Schema.create(Schema.Type.INT)
  }

  implicit def LongSchema = new AvroSchemaType[Long] {
    def schema = Schema.create(Schema.Type.LONG)
  }

  implicit def StringSchema = new AvroSchemaType[String] {
    def schema = Schema.create(Schema.Type.STRING)
  }

  // collections
  implicit def CollectionSchema[CC[x] <: Iterable[x], T](implicit sch: AvroSchemaType[T]) = new AvroSchemaType[CC[T]] {
    def schema = Schema.createArray(sch.schema)
  }

  implicit def ArraySchema[CC[x] <: Array[x], T](implicit sch: AvroSchemaType[T]) = new AvroSchemaType[CC[T]] {
    val schema = Schema.createArray(sch.schema)
  }

  //maps
  implicit def MapSchema[CC[String, x] <: Map[String, x], T](implicit sch: AvroSchemaType[T]) = new AvroSchemaType[CC[String, T]] {
    def schema = Schema.createMap(sch.schema)
  }

  // Avro SpecificRecord
  implicit def SpecificRecordSchema[T <: SpecificRecord](implicit mf: Manifest[T]) = new AvroSchemaType[T] {
    def schema = mf.erasure.newInstance.asInstanceOf[SpecificRecord].getSchema
  }

}