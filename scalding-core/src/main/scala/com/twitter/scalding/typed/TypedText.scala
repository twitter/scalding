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
package com.twitter.scalding.typed

import com.twitter.scalding._
import cascading.tap.local.FileTap
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tap.hadoop.Hfs
import cascading.scheme.local.{ TextDelimited => CLTextDelimited }
import cascading.scheme.hadoop.{ TextDelimited => CHTextDelimited }
import cascading.scheme.Scheme
import java.io.{ InputStream, OutputStream }
import java.util.Properties
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import scala.util.{ Failure, Success, Try }

/**
 * These are the settings used to format text delimited sources.
 * we recommend using named arguments to construct these:
 * eg. TextFormatting(skipHeader = true, ...
 */
case class DelimitingOptions(
  columns: Fields, // You should set the types if at all possible
  separator: String, // usually "\t" or "\1"
  hasHeader: Boolean = false, // implies write and skip header
  strict: Boolean = true, // true means must have correct number of columns, false means null pad.
  quote: Option[String] = None,
  safe: Boolean = false) // true means that if type coercion fails put null. false means throw.

class TypedTextSource[T](
  formatting: DelimitingOptions,
  readPath: ReadPathProvider,
  tconverter: TupleConverter[T]) extends Source with Mappable[T] {

  override def sourceFields: Fields = formatting.columns

  private def getTypes: Array[Class[_]] =
    formatting.columns.getTypesClasses

  protected def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] = {
    import formatting._
    new CHTextDelimited(sourceFields,
      null /* compression */ ,
      hasHeader, // if we have a header, we should skip it and write it, so it is repeated
      hasHeader,
      separator,
      strict,
      quote.orNull,
      getTypes,
      safe)
  }

  protected def localScheme: Scheme[Properties, InputStream, OutputStream, _, _] = {
    import formatting._
    new CLTextDelimited(sourceFields,
      hasHeader, // if we have a header, we should skip it and write it, so it is repeated
      hasHeader,
      separator,
      strict,
      quote.orNull,
      getTypes,
      safe)
  }

  protected def makeLocal(m: Local): (String) => Tap[Properties, InputStream, _] = { (path: String) =>
    new FileTap(localScheme, path, SinkMode.KEEP)
  }
  protected def makeHdfs(m: Hdfs): (String) => Tap[JobConf, RecordReader[_, _], _] = { (path: String) =>
    new Hfs(hdfsScheme, path, SinkMode.KEEP)
  }

  protected def makeMulti[C, I](m: Mode, paths: Seq[String], fn: String => Tap[C, I, _]): Tap[C, I, _] =
    paths match {
      case Seq() => fn("dummy:empty-paths")
      case Seq(one) => fn(one)
      case many => new ScaldingMultiSourceTap(many.map(fn))
    }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
    readOrWrite match {
      case Write => sys.error("Write not supported")
      case Read =>
        val paths = readPath.readPath(mode).getOrElse(List("dummy:readPath-error")).toSeq
        mode match {
          // TODO support strict in Local
          case loc @ Local(_) => makeMulti(loc, paths, makeLocal(loc))
          case hdfsMode @ Hdfs(_, _) => makeMulti(hdfsMode, paths, makeHdfs(hdfsMode))
          case _: TestMode =>
            TestTapFactory(this, sourceFields, SinkMode.KEEP)
              .createTap(readOrWrite)
              .asInstanceOf[Tap[Any, Any, Any]]
          case _ => sys.error(s"Unsupported mode: ${mode}")
        }
    }

  final override def converter[U >: T] = TupleConverter.asSuperConverter(tconverter)

  final override def validateTaps(mode: Mode): Unit = readPath.readPath(mode) match {
    case Failure(e) => new InvalidSourceException(e.getMessage)
    case Success(ps) if ps.isEmpty => new InvalidSourceException("no valid input paths found")
    case _ => ()
  }
}
