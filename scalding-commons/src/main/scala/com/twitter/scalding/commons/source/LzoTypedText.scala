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

import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tap.Tap
import com.etsy.cascading.tap.local.LocalTap
import com.twitter.scalding.typed._
import com.twitter.scalding._
import com.twitter.elephantbird.cascading2.scheme.LzoTextDelimited
import java.io.{ InputStream, OutputStream }
import java.util.Properties
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import scala.util.{ Failure, Success, Try }

class LzoTypedTextSource[T](
  formatting: DelimitingOptions,
  readPath: ReadPathProvider,
  tconverter: TupleConverter[T]) extends TypedTextSource(formatting, readPath, tconverter) {

  protected override def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] = {
    import formatting._
    new LzoTextDelimited(sourceFields,
      hasHeader, // if we have a header, we should skip it and write it, so it is repeated
      hasHeader,
      separator,
      strict,
      quote.orNull,
      formatting.columns.getTypesClasses,
      safe)
  }
  protected override def localScheme = sys.error("Unused for Lzo")
  /*
   * Lzo is used by wrapping with the etsy LocalTap.
   */
  protected override def makeLocal(m: Local): (String) => Tap[Properties, InputStream, _] = { (path: String) =>
    (new LocalTap(path, hdfsScheme, SinkMode.KEEP)).asInstanceOf[Tap[Properties, InputStream, _]]
  }
}

class LzoTypedTextSink[T](
  formatting: DelimitingOptions,
  writePath: WritePathProvider,
  tsetter: TupleSetter[T]) extends TypedTextSink(formatting, writePath, tsetter) {

  protected override def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] = {
    import formatting._
    new LzoTextDelimited(sinkFields,
      hasHeader, // if we have a header, we should skip it and write it, so it is repeated
      hasHeader,
      separator,
      strict,
      quote.orNull,
      formatting.columns.getTypesClasses,
      safe)
  }

  protected override def localScheme = sys.error("Unused for Lzo")

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    (readOrWrite, mode) match {
      case (Write, Local(_)) =>
        val path = writePath.writePath(mode).getOrElse("dummy:readPath-error")
        new LocalTap(path, hdfsScheme, SinkMode.REPLACE)
      case _ => super.createTap(readOrWrite)
    }
}
