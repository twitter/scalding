/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.scalding.commons.source

import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.typed.{ DelimitingOptions, TypedTextSink, TypedTextSource }
import com.twitter.scalding.macros.Macros
import com.twitter.bijection.macros.IsCaseClass

/**
 * This is a convenience object for making sources and sinks
 */
object TypedText extends java.io.Serializable {
  def tsvSource[T: IsCaseClass](rp: ReadPathProvider): Mappable[T] =
    new TypedTextSource(DelimitingOptions.tsv(Macros.toFields[T]),
      rp,
      Macros.caseClassTupleConverter[T])

  def tsvSink[T: IsCaseClass](wp: WritePathProvider): TypedSink[T] =
    new TypedTextSink(DelimitingOptions.tsv(Macros.toFields[T]),
      wp,
      Macros.caseClassTupleSetter[T])
}
