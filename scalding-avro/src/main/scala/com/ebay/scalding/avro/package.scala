package com.twitter.scalding

import com.twitter.scalding._
import cascading.flow.FlowDef
import org.apache.avro.Schema

package object avro {
  def writePackedAvro[T](pipe: TypedPipe[T], path: String)
                        (implicit mf: Manifest[T], st: AvroSchemaType[T], conv: TupleConverter[T], set: TupleSetter[T], flow: FlowDef, mode: Mode) {
    val sink = PackedAvroSource[T](path)
    pipe.write(sink)
  }

    def writeUnpackedAvro[T <: Product](pipe: TypedPipe[T], path: String, schema: Schema)
      (implicit mf: Manifest[T], conv: TupleConverter[T], set: TupleSetter[T], flow: FlowDef, mode : Mode) {
      val sink = UnpackedAvroSource[T](path, Some(schema))
      pipe.write(sink)
    }
}