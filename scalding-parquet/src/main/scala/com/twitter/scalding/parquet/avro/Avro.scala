package com.twitter.scalding.parquet.avro

import scala.reflect.ClassTag
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

object Avro {
  def getSchema[A <: IndexedRecord : ClassTag] : Schema = {
    val mf = implicitly[ClassTag[A]]
    mf.runtimeClass.getMethod("getClassSchema").invoke(mf.runtimeClass).asInstanceOf[Schema]
  }
}
