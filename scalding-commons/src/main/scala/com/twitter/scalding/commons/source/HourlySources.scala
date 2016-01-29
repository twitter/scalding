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

package com.twitter.scalding.commons.source

import cascading.tuple.Fields
import com.google.protobuf.Message
import com.twitter.bijection.Injection
import com.twitter.chill.Externalizer
import com.twitter.scalding._
import com.twitter.scalding.source._
import org.apache.thrift.TBase

import scala.annotation.meta.param

abstract class HourlySuffixLzoCodec[T](prefix: String, dateRange: DateRange)(implicit @(transient @param) suppliedInjection: Injection[T, Array[Byte]])
  extends HourlySuffixSource(prefix, dateRange) with LzoCodec[T] {
  val boxed = Externalizer(suppliedInjection)
  override lazy val injection = boxed.get
}

case class HourlySuffixLzoTsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with LzoTsv {
  override val fields = fs
}

abstract class HourlySuffixLzoThrift[T <: TBase[_, _]: Manifest](prefix: String, dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with LzoThrift[T] {
  override def column = manifest[T].runtimeClass
}

abstract class HourlySuffixLzoProtobuf[T <: Message: Manifest](prefix: String, dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with LzoProtobuf[T] {
  override def column = manifest[T].runtimeClass
}

abstract class HourlySuffixLzoText(prefix: String, dateRange: DateRange)
  extends HourlySuffixSource(prefix, dateRange) with LzoText
