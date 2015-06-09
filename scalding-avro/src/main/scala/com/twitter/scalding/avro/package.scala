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

package com.twitter.scalding

import cascading.flow.FlowDef
import org.apache.avro.Schema
import collection.JavaConverters._
import cascading.tuple.Fields

package object avro {
  def writePackedAvro[T](pipe: TypedPipe[T], path: String)(implicit
    mf: Manifest[T],
    st: AvroSchemaType[T],
    conv: TupleConverter[T],
    set: TupleSetter[T],
    flow: FlowDef,
    mode: Mode): Unit = {
    val sink = PackedAvroSource[T](path)
    pipe.write(sink)
  }

  def writeUnpackedAvro[T <: Product](pipe: TypedPipe[T], path: String, schema: Schema)(implicit
    mf: Manifest[T],
    conv: TupleConverter[T],
    set: TupleSetter[T],
    flow: FlowDef,
    mode: Mode): Unit = {
    import Dsl._
    val sink = UnpackedAvroSource[T](path, Some(schema))
    val outFields = {
      val schemaFields = schema.getFields
      schemaFields.asScala.foldLeft(new Fields())((cFields, sField) => cFields.append(new Fields(sField.name())))
    }
    pipe.toPipe(outFields).write(sink)
  }
}