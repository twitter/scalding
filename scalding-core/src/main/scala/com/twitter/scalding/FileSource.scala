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
package com.twitter.scalding

import java.io.{ InputStream, OutputStream }
import java.util.{ UUID, Properties }

import cascading.scheme.Scheme
import cascading.scheme.local.{ TextLine => CLTextLine, TextDelimited => CLTextDelimited }
import cascading.scheme.hadoop.{
  TextLine => CHTextLine,
  TextDelimited => CHTextDelimited,
  SequenceFile => CHSequenceFile
}
import cascading.tap.hadoop.Hfs
import cascading.tap.MultiSourceTap
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.local.FileTap
import cascading.tuple.Fields

import com.etsy.cascading.tap.local.LocalTap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, PathFilter, Path }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import scala.util.control.Exception.allCatch

/**
 * A base class for sources that take a scheme trait.
 */
abstract class SchemedSource extends Source {

  /** The scheme to use if the source is local. */
  def localScheme: Scheme[Properties, InputStream, OutputStream, _, _] =
    throw ModeException("Cascading local mode not supported for: " + toString)

  /** The scheme to use if the source is on hdfs. */
  def hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] =
    throw ModeException("Cascading Hadoop mode not supported for: " + toString)

  // The mode to use for output taps determining how conflicts with existing output are handled.
  val sinkMode: SinkMode = SinkMode.REPLACE
}

/**
 * A trait which provides a method to create a local tap.
 */
trait LocalSourceOverride extends SchemedSource {
  /** A path to use for the local tap. */
  def localPath: String

  /**
   * Creates a local tap.
   *
   * @param sinkMode The mode for handling output conflicts.
   * @returns A tap.
   */
  def createLocalTap(sinkMode: SinkMode): Tap[_, _, _] = new FileTap(localScheme, localPath, sinkMode)
}

object HiddenFileFilter extends PathFilter {
  def accept(p: Path) = {
    val name = p.getName
    !name.startsWith("_") && !name.startsWith(".")
  }
}

object SuccessFileFilter extends PathFilter {
  def accept(p: Path) = { p.getName == "_SUCCESS" }
}

object AcceptAllPathFilter extends PathFilter {
  def accept(p: Path) = true
}

object FileSource {

  def glob(glob: String, conf: Configuration, filter: PathFilter = AcceptAllPathFilter): Iterable[FileStatus] = {
    val path = new Path(glob)
    Option(path.getFileSystem(conf).globStatus(path, filter)).map {
      _.toIterable // convert java Array to scala Iterable
    } getOrElse {
      Iterable.empty
    }
  }

  /**
   * @return whether globPath contains non hidden files
   */
  def globHasNonHiddenPaths(globPath: String, conf: Configuration): Boolean = {
    !glob(globPath, conf, HiddenFileFilter).isEmpty
  }

  /**
   * @return whether globPath contains a _SUCCESS file
   */
  def globHasSuccessFile(globPath: String, conf: Configuration): Boolean = {
    !glob(globPath, conf, SuccessFileFilter).isEmpty
  }

}

/**
 * This is a base class for File-based sources
 */
abstract class FileSource extends SchemedSource with LocalSourceOverride {

  /**
   * Determines if a path is 'valid' for this source. In strict mode all paths must be valid.
   * In non-strict mode, all invalid paths will be filtered out.
   *
   * Subclasses can override this to validate paths.
   *
   * The default implementation is a quick sanity check to look for missing or empty directories.
   * It is necessary but not sufficient -- there are cases where this will return true but there is
   * in fact missing data.
   *
   * TODO: consider writing a more in-depth version of this method in [[TimePathedSource]] that looks for
   * TODO: missing days / hours etc.
   */
  protected def pathIsGood(p: String, conf: Configuration) = FileSource.globHasNonHiddenPaths(p, conf)

  def hdfsPaths: Iterable[String]
  // By default, we write to the LAST path returned by hdfsPaths
  def hdfsWritePath = hdfsPaths.last

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      // TODO support strict in Local
      case Local(_) => {
        createLocalTap(sinkMode)
      }
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => createHdfsReadTap(hdfsMode)
        case Write => CastHfsTap(new Hfs(hdfsScheme, hdfsWritePath, sinkMode))
      }
      case _ => {
        allCatch.opt(
          TestTapFactory(this, hdfsScheme, sinkMode)).map {
            _.createTap(readOrWrite) // these java types are invariant, so we cast here
              .asInstanceOf[Tap[Any, Any, Any]]
          }
          .orElse {
            allCatch.opt(
              TestTapFactory(this, localScheme.getSourceFields, sinkMode)).map {
                _.createTap(readOrWrite)
                  .asInstanceOf[Tap[Any, Any, Any]]
              }
          }.getOrElse(sys.error("Failed to create a tap for: " + toString))
      }
    }
  }

  // This is only called when Mode.sourceStrictness is true
  protected def hdfsReadPathsAreGood(conf: Configuration) = {
    hdfsPaths.forall { pathIsGood(_, conf) }
  }

  /*
   * This throws InvalidSourceException if:
   * 1) we are in sourceStrictness mode and all sources are not present.
   * 2) we are not in the above, but some source has no input whatsoever
   * TODO this only does something for HDFS now. Maybe we should do the same for LocalMode
   */
  override def validateTaps(mode: Mode): Unit = {
    mode match {
      case Hdfs(strict, conf) => {
        if (strict && (!hdfsReadPathsAreGood(conf))) {
          throw new InvalidSourceException(
            "[" + this.toString + "] Data is missing from one or more paths in: " +
              hdfsPaths.toString)
        } else if (!hdfsPaths.exists { pathIsGood(_, conf) }) {
          //Check that there is at least one good path:
          throw new InvalidSourceException(
            "[" + this.toString + "] No good paths in: " + hdfsPaths.toString)
        }
      }
      case _ => ()
    }
  }

  /*
   * Get all the set of valid paths based on source strictness.
   */
  protected def goodHdfsPaths(hdfsMode: Hdfs) = {
    hdfsMode match {
      //we check later that all the paths are good
      case Hdfs(true, _) => hdfsPaths
      // If there are no matching paths, this is still an error, we need at least something:
      case Hdfs(false, conf) => hdfsPaths.filter{ pathIsGood(_, conf) }
    }
  }

  protected def createHdfsReadTap(hdfsMode: Hdfs): Tap[JobConf, _, _] = {
    val taps: List[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]] =
      goodHdfsPaths(hdfsMode)
        .toList.map { path => CastHfsTap(new Hfs(hdfsScheme, path, sinkMode)) }
    taps.size match {
      case 0 => {
        // This case is going to result in an error, but we don't want to throw until
        // validateTaps, so we just put a dummy path to return something so the
        // Job constructor does not fail.
        CastHfsTap(new Hfs(hdfsScheme, hdfsPaths.head, sinkMode))
      }
      case 1 => taps.head
      case _ => new ScaldingMultiSourceTap(taps)
    }
  }
}

class ScaldingMultiSourceTap(taps: Seq[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]])
  extends MultiSourceTap[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]], JobConf, RecordReader[_, _]](taps: _*) {
  private final val randomId = UUID.randomUUID.toString
  override def getIdentifier() = randomId
}

/**
 * The fields here are ('offset, 'line)
 */

trait TextSourceScheme extends SchemedSource {
  // The text-encoding to use when writing out the lines (default is UTF-8).
  val textEncoding: String = CHTextLine.DEFAULT_CHARSET

  override def localScheme = new CLTextLine(new Fields("offset", "line"), Fields.ALL, textEncoding)
  override def hdfsScheme = HadoopSchemeInstance(new CHTextLine(CHTextLine.DEFAULT_SOURCE_FIELDS, textEncoding))
}

trait TextLineScheme extends TextSourceScheme with SingleMappable[String] {
  //In textline, 0 is the byte position, the actual text string is in column 1
  override def sourceFields = Dsl.intFields(Seq(1))
}

/**
 * Mix this in for delimited schemes such as TSV or one-separated values
 * By default, TSV is given
 */
trait DelimitedScheme extends SchemedSource {
  //override these as needed:
  val fields = Fields.ALL
  //This is passed directly to cascading where null is interpretted as string
  val types: Array[Class[_]] = null
  val separator = "\t"
  val skipHeader = false
  val writeHeader = false
  val quote: String = null

  // Whether to throw an exception or not if the number of fields does not match an expected number.
  // If set to false, missing fields will be set to null.
  val strict = true

  // Whether to throw an exception if a field cannot be coerced to the right type.
  // If set to false, then fields that cannot be coerced will be set to null.
  val safe = true

  //These should not be changed:
  override def localScheme = new CLTextDelimited(fields, skipHeader, writeHeader, separator, strict, quote, types, safe)

  override def hdfsScheme =
    HadoopSchemeInstance(new CHTextDelimited(fields, null, skipHeader, writeHeader, separator, strict, quote, types, safe))
}

trait SequenceFileScheme extends SchemedSource {
  //override these as needed:
  val fields = Fields.ALL
  // TODO Cascading doesn't support local mode yet
  override def hdfsScheme = {
    HadoopSchemeInstance(new CHSequenceFile(fields))
  }
}

/**
 * Ensures that a _SUCCESS file is present in the Source path, which must be a glob,
 * as well as the requirements of [[FileSource.pathIsGood]]
 */
trait SuccessFileSource extends FileSource {
  override protected def pathIsGood(p: String, conf: Configuration) = {
    FileSource.globHasNonHiddenPaths(p, conf) && FileSource.globHasSuccessFile(p, conf)
  }
}

/**
 * Use this class to add support for Cascading local mode via the Hadoop tap.
 * Put another way, this runs a Hadoop tap outside of Hadoop in the Cascading local mode
 */
trait LocalTapSource extends LocalSourceOverride {
  override def createLocalTap(sinkMode: SinkMode) = new LocalTap(localPath, hdfsScheme, sinkMode).asInstanceOf[Tap[_, _, _]]
}

abstract class FixedPathSource(path: String*) extends FileSource {
  def localPath = { assert(path.size == 1, "Cannot use multiple input files on local mode"); path(0) }
  def hdfsPaths = path.toList
  override def toString = getClass.getName + path
  override def hashCode = toString.hashCode
  override def equals(that: Any): Boolean = (that != null) && (that.toString == toString)
}

/**
 * Tab separated value source
 */

case class Tsv(p: String, override val fields: Fields = Fields.ALL,
  override val skipHeader: Boolean = false, override val writeHeader: Boolean = false,
  override val sinkMode: SinkMode = SinkMode.REPLACE) extends FixedPathSource(p) with DelimitedScheme

/**
 * Allows the use of multiple Tsv input paths. The Tsv files will
 * be process through your flow as if they are a single pipe. Tsv
 * files must have the same schema.
 * For more details on how multiple files are handled check the
 * cascading docs.
 */
case class MultipleTsvFiles(p: Seq[String], override val fields: Fields = Fields.ALL,
  override val skipHeader: Boolean = false, override val writeHeader: Boolean = false) extends FixedPathSource(p: _*)
  with DelimitedScheme

/**
 * Csv value source
 * separated by commas and quotes wrapping all fields
 */
case class Csv(p: String,
  override val separator: String = ",",
  override val fields: Fields = Fields.ALL,
  override val skipHeader: Boolean = false,
  override val writeHeader: Boolean = false,
  override val quote: String = "\"",
  override val sinkMode: SinkMode = SinkMode.REPLACE) extends FixedPathSource(p) with DelimitedScheme

/**
 * One separated value (commonly used by Pig)
 */
case class Osv(p: String, f: Fields = Fields.ALL,
  override val sinkMode: SinkMode = SinkMode.REPLACE) extends FixedPathSource(p)
  with DelimitedScheme {
  override val fields = f
  override val separator = "\1"
}

object TextLine {
  // Default encoding is UTF-8
  val defaultTextEncoding: String = CHTextLine.DEFAULT_CHARSET
  val defaultSinkMode: SinkMode = SinkMode.REPLACE

  def apply(p: String, sm: SinkMode = defaultSinkMode, textEncoding: String = defaultTextEncoding): TextLine =
    new TextLine(p, sm, textEncoding)
}

class TextLine(p: String, override val sinkMode: SinkMode, override val textEncoding: String) extends FixedPathSource(p) with TextLineScheme {
  // For some Java interop
  def this(p: String) = this(p, TextLine.defaultSinkMode, TextLine.defaultTextEncoding)
}

/**
 * Alternate typed TextLine source that keeps both 'offset and 'line fields.
 */
class OffsetTextLine(filepath: String,
  override val sinkMode: SinkMode,
  override val textEncoding: String)
  extends FixedPathSource(filepath) with Mappable[(Long, String)] with TextSourceScheme {

  override def converter[U >: (Long, String)] =
    TupleConverter.asSuperConverter[(Long, String), U](TupleConverter.of[(Long, String)])
}

/**
 * Alternate typed TextLine source that keeps both 'offset and 'line fields.
 */
object OffsetTextLine {
  // Default encoding is UTF-8
  val defaultTextEncoding: String = CHTextLine.DEFAULT_CHARSET
  val defaultSinkMode: SinkMode = SinkMode.REPLACE

  def apply(p: String, sm: SinkMode = defaultSinkMode, textEncoding: String = defaultTextEncoding): OffsetTextLine =
    new OffsetTextLine(p, sm, textEncoding)
}

case class SequenceFile(p: String, f: Fields = Fields.ALL, override val sinkMode: SinkMode = SinkMode.REPLACE)
  extends FixedPathSource(p) with SequenceFileScheme with LocalTapSource {
  override val fields = f
}

case class MultipleSequenceFiles(p: String*) extends FixedPathSource(p: _*) with SequenceFileScheme with LocalTapSource

case class MultipleTextLineFiles(p: String*) extends FixedPathSource(p: _*) with TextLineScheme

/**
 * Delimited files source
 * allowing to override separator and quotation characters and header configuration
 */
case class MultipleDelimitedFiles(f: Fields,
  override val separator: String,
  override val quote: String,
  override val skipHeader: Boolean,
  override val writeHeader: Boolean,
  p: String*) extends FixedPathSource(p: _*) with DelimitedScheme {
  override val fields = f
}
