/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding.typed

import java.io.Serializable

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields

import com.twitter.scalding._

/**
 * implicits for the type-safe DSL
 * import TDsl._ to get the implicit conversions from Grouping/CoGrouping to Pipe,
 *   to get the .toTypedPipe method on standard cascading Pipes.
 *   to get automatic conversion of Mappable[T] to TypedPipe[T]
 */
object TDsl extends Serializable with GeneratedTupleAdders {
  implicit def pipeTExtensions(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): PipeTExtensions =
    new PipeTExtensions(pipe, flowDef, mode)

  implicit def mappableToTypedPipe[T](src: Mappable[T]): TypedPipe[T] =
    TypedPipe.from(src)

  implicit def sourceToTypedPipe[T](src: TypedSource[T]): TypedPipe[T] =
    TypedPipe.from(src)

  implicit def mappableToTypedPipeKeyed[K, V](src: Mappable[(K, V)]): TypedPipe.Keyed[K, V] =
    new TypedPipe.Keyed(TypedPipe.from(src))

  implicit def sourceToTypedPipeKeyed[K, V](src: TypedSource[(K, V)]): TypedPipe.Keyed[K, V] =
    new TypedPipe.Keyed(TypedPipe.from(src))
}

/*
 * This is an Enrichment pattern of adding methods to Pipe relevant to TypedPipe
 */
class PipeTExtensions(pipe: Pipe, flowDef: FlowDef, mode: Mode) extends Serializable {
  /* Give you a syntax (you must put the full type on the TypedPipe, else type inference fails
   *   pipe.typed(('in0, 'in1) -> 'out) { tpipe : TypedPipe[(Int,Int)] =>
   *    // let's group all:
   *     tpipe.groupBy { x => 1 }
   *       .mapValues { tup => tup._1 + tup._2 }
   *       .sum
   *       .map { _._2 } //discard the key value, which is 1.
   *   }
   *  The above sums all the tuples and returns a TypedPipe[Int] which has the total sum.
   */
  def typed[T, U](fielddef: (Fields, Fields))(fn: TypedPipe[T] => TypedPipe[U])(implicit conv: TupleConverter[T], setter: TupleSetter[U]): Pipe =
    fn(TypedPipe.from(pipe, fielddef._1)(flowDef, mode, conv)).toPipe(fielddef._2)(flowDef, mode, setter)

  def toTypedPipe[T](fields: Fields)(implicit conv: TupleConverter[T]): TypedPipe[T] =
    TypedPipe.from[T](pipe, fields)(flowDef, mode, conv)

  def packToTypedPipe[T](fields: Fields)(implicit tp: TuplePacker[T]): TypedPipe[T] = {
    val conv = tp.newConverter(fields)
    toTypedPipe(fields)(conv)
  }
}
