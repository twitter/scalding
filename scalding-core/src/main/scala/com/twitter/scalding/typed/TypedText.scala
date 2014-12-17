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

trait SchemeProvider { self =>
  def getScheme(m: Mode): Try[m.CScheme]

  def orElse(that: SchemeProvider): SchemeProvider = new SchemeProvider {
    // somehow the type is needed
    def getScheme(m: Mode): Try[m.CScheme] = self.getScheme(m).orElse(that.getScheme(m))
  }
}

trait TapProvider { self =>
  def getTap(m: Mode): Try[m.CTap]
  def orElse(that: TapProvider): TapProvider = new TapProvider {
    // somehow the type is needed
    def getTap(m: Mode): Try[m.CTap] =
      self.getTap(m).orElse(that.getTap(m).asInstanceOf[Try[m.CTap]])
  }
}

// Easily build a source by subclassing this and providing the TapProvider
class TapProviderSource(self: TapProvider) extends Source {
  override def createTap(rorw: AccessMode)(implicit m: Mode): Tap[_, _, _] =
    // this cast should not be needed, but don't want to push this change all the way to Source
    // yet
    self.getTap(m).get.asInstanceOf[Tap[_, _, _]]

  override def validateTaps(m: Mode) = self.getTap(m).get
}

class TapProviderTypedSource[T](self: TapProvider, tc: TupleConverter[T]) extends Mappable[T] {
  override def converter[U >: T] = TupleConverter.asSuperConverter(tc)
  override def createTap(rorw: AccessMode)(implicit m: Mode) =
    self.getTap(m).get.asInstanceOf[Tap[_, _, _]]
  override def validateTaps(m: Mode) = self.getTap(m).get
}

object TapProvider {

  /**
   * If you are reading from file, you just need the scheme and the read paths
   */
  def fileSource(s: SchemeProvider, p: ReadPathProvider): TapProvider = new TapProvider {

    def getTap(m: Mode): Try[m.CTap] = {
      // This shouldn't be needed. It would be cool to get scalac happy without it
      def cast[A, B, C](t: Tap[A, B, C]): m.CTap = t.asInstanceOf[m.CTap]
      def makeMulti[C, I, O](paths: Seq[String], fn: String => Tap[C, I, O]): Try[Tap[C, I, _]] =
        paths match {
          case Seq() => Failure(new InvalidSourceException("Cannot read an empty set of paths"))
          case Seq(one) => Success(fn(one))
          case many => Success(new ScaldingMultiSourceTap(many.map(fn)))
        }
      m match {
        // sucks to repeat myself here, but the compiler needs the types to be explicit
        case h@Hdfs(_, _) => for {
          scheme <- s.getScheme(h)
          paths <- p.readPath(h)
          result <- makeMulti[JobConf, RecordReader[_, _], OutputCollector[_, _]](paths.toSeq,
            p => new Hfs(scheme, p))
        } yield cast(result) // should be able to see that this is already m.CTap due to match
        case l@Local(_) => for {
          scheme <- s.getScheme(l)
          paths <- p.readPath(l)
          result <- makeMulti(paths.toSeq, p => new FileTap(scheme, p))
        } yield cast(result) // should be able to see that this is already m.CTap due to match
        case _ => Failure(ModeException(s"${m} not supported"))
      }
    }
  }
  // unfortunately scalding mocks sources. this is obviously not useful yet
  // it's a challenge to fix this without breaking old code.
  def testSource(s: Source, f: Fields): TapProvider = new TapProvider {
    def getTap(m: Mode): Try[m.CTap] = m match {
      case _: TestMode =>
        Try(TestTapFactory(s, f, SinkMode.KEEP)
          // these java types are invariant, so we cast here
          .createTap(Read)(m)
          .asInstanceOf[m.CTap])
      case _ => Failure(ModeException(s"{$m} not supported by testSource"))
    }
  }

  def testSink(s: Source, f: Fields): TapProvider = new TapProvider {
    def getTap(m: Mode): Try[m.CTap] = m match {
      case _: TestMode =>
        Try(TestTapFactory(s, f, SinkMode.REPLACE)
          // these java types are invariant, so we cast here
          .createTap(Write)(m)
          .asInstanceOf[m.CTap])
      case _ => Failure(ModeException(s"{$m} not supported by testSink"))
    }
  }
}

class DelimitedSchemeProvider(formatting: DelimitingOptions) extends SchemeProvider {

  def sourceFields: Fields = formatting.columns
  def getTypes: Array[Class[_]] =
    formatting.columns.getTypesClasses

  def getScheme(m: Mode): Try[m.CScheme] = {
    // this should not be needed
    // due to the case match, we can see that the types match
    def compilerIsDumb(t: Scheme[_, _, _, _, _]): m.CScheme = t.asInstanceOf[m.CScheme]
    m match {
      case Hdfs(_, _) => Try(compilerIsDumb(hdfsScheme))
      case Local(_) => Try(compilerIsDumb(localScheme))
    }
  }

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
}
