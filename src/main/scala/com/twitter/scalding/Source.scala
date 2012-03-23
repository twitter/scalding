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

import com.twitter.meatlocker.tap.MemorySourceTap

import java.io.File
import java.util.TimeZone
import java.util.Calendar
import java.util.{Map => JMap}

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.{FlowProcess, FlowDef}
import cascading.flow.local.LocalFlowProcess
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.local.{LocalScheme => CLScheme, TextLine => CLTextLine, TextDelimited => CLTextDelimited}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}
import cascading.tap.hadoop.Hfs
import cascading.tap.MultiSourceTap
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.local.FileTap
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import collection.mutable.{Buffer, MutableList}
import scala.collection.JavaConverters._

/**
 * thrown when validateTaps fails
 */
class InvalidSourceException(message : String) extends RuntimeException(message)

/**
* Every source must have a correct toString method.  If you use
* case classes for instances of sources, you will get this for free.
* This is one of the several reasons we recommend using cases classes
*
* java.io.Serializable is needed if the Source is going to have any
* methods attached that run on mappers or reducers, which will happen
* if you implement transformForRead or transformForWrite.
*/
abstract class Source extends java.io.Serializable {
  type RawScheme = Scheme[_ <: FlowProcess[_],_,_,_,_,_]
  type LocalScheme = CLScheme[_, _, _, _]
  type RawFlowProcess = cascading.flow.FlowProcess[_]
  type RawTap = Tap[_ <: FlowProcess[_], _, _, _]

  class AccessMode
  case class Read() extends AccessMode
  case class Write() extends AccessMode

  def read(implicit flowDef : FlowDef, mode : Mode) = {
    //insane workaround for scala compiler bug
    val sources = flowDef.getSources().asInstanceOf[JMap[String,Any]]
    val srcName = this.toString
    if (!sources.containsKey(srcName)) {
      sources.put(srcName, createTap(Read(), mode))
    }
    mode.getReadPipe(this, transformForRead(new Pipe(srcName)))
  }

  /**
  * write the pipe and return the input so it can be chained into
  * the next operation
  */
  def write(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    //insane workaround for scala compiler bug
    val sinks = flowDef.getSinks().asInstanceOf[JMap[String,Any]]
    val sinkName = this.toString
    if (!sinks.containsKey(sinkName)) {
      sinks.put(sinkName, createTap(Write(),mode))
    }
    flowDef.addTail(new Pipe(sinkName, transformForWrite(pipe)))
    pipe
  }

  protected def transformForWrite(pipe : Pipe) = pipe
  protected def transformForRead(pipe : Pipe) = pipe

  /*
  * There MUST have already been a registered sink or source in the Test mode.
  * to access this.  You must explicitly name each of your test sources in your
  * JobTest.
  */
  protected def testBuffer(buffers : Map[Source,Buffer[Tuple]], am : AccessMode) = {
    am match {
      case Read() => buffers(this)
      case Write() => {
        //Make sure we wipe it out first:
        val buf = buffers(this)
        buf.clear()
        buf
      }
    }
  }

  protected def pathIsGood(p : String, conf : Configuration) = {
    val path = new Path(p)
    Option(path.getFileSystem(conf).globStatus(path)).
        map(_.length > 0).
        getOrElse(false)
  }

  def hdfsPaths : Iterable[String]
  // By default, we write to the LAST path returned by hdfsPaths
  def hdfsWritePath = hdfsPaths.last
  def localPath : String
  def localScheme : LocalScheme
  def hdfsScheme : Scheme[_ <: FlowProcess[_],_,_,_,_,_]

  protected def createTap(readOrWrite : AccessMode, mode : Mode) : RawTap = {
    mode match {
      // TODO support strict in Local
      case Local(_) => {
        val sinkmode = readOrWrite match {
          case Read() => SinkMode.KEEP
          case Write() => SinkMode.REPLACE
        }
        new FileTap(localScheme, localPath, sinkmode)
      }
      case Test(buffers) => new MemoryTap(localScheme, testBuffer(buffers, readOrWrite))
      case HadoopTest(conf, buffers) => readOrWrite match {
        case Read() => createHadoopTestReadTap(buffers(this))
        case Write() => createHadoopTestWriteTap
      }
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read() => createHdfsReadTap(hdfsMode)
        case Write() => createHdfsWriteTap(hdfsMode)
      }
    }
  }

  protected def createHadoopTestReadTap(buffer : Iterable[Tuple]) :
    Tap[HadoopFlowProcess, JobConf, RecordReader[_,_], _] = {
    new MemorySourceTap(buffer.toList.asJava, hdfsScheme.getSourceFields())
  }

  protected def hadoopTestPath = "/tmp/scalding/" + hdfsWritePath
  protected def createHadoopTestWriteTap :
    Tap[HadoopFlowProcess, JobConf, RecordReader[_,_], OutputCollector[_,_]] = {
    new Hfs(hdfsScheme, hadoopTestPath, SinkMode.REPLACE)
  }

  def finalizeHadoopTestOutput(mode : Mode) {
    mode match {
      case HadoopTest(conf, buffers) => {
        val fp = new HadoopFlowProcess(new JobConf(conf))
        // We read the write tap in order to add its contents in the test buffers
        val it = createHadoopTestWriteTap.openForRead(fp)
        val buf = buffers(this)
        buf.clear()
        while(it != null && it.hasNext) {
          buf += new Tuple(it.next.getTuple)
        }
        new File(hadoopTestPath).delete()
      }
      case _ => throw new RuntimeException("Cannot read test data in a non-test mode")
    }
  }

  // This is only called when Mode.sourceStrictness is true
  protected def hdfsReadPathsAreGood(conf : Configuration) = {
    hdfsPaths.forall { pathIsGood(_, conf) }
  }

  /*
   * This throws InvalidSourceException if:
   * 1) we are in sourceStrictness mode and all sources are not present.
   * 2) we are not in the above, but some source has no input whatsoever
   * TODO this only does something for HDFS now. Maybe we should do the same for LocalMode
   */
  def validateTaps(mode : Mode) : Unit = {
    mode match {
      case Hdfs(strict, conf) => {
        if (strict && (!hdfsReadPathsAreGood(conf))) {
          throw new InvalidSourceException("[" + this.toString + "] No good paths in: " + hdfsPaths.toString)
        }
        else if (!hdfsPaths.exists { pathIsGood(_, conf) }) {
          //Check that there is at least one good path:
          throw new InvalidSourceException("[" + this.toString + "] No good paths in: " + hdfsPaths.toString)
        }
      }
      case _ => ()
    }
  }

  /*
   * Get all the set of valid paths based on source strictness.
   */
  protected def goodHdfsPaths(hdfsMode : Hdfs) = {
    if (hdfsMode.sourceStrictness) {
      //we check later that all the paths are good
      hdfsPaths
    }
    else {
      // If there are no matching paths, this is still an error, we need at least something:
      hdfsPaths.filter{ pathIsGood(_, hdfsMode.config) }
    }
  }

  protected def createHdfsReadTap(hdfsMode : Hdfs) :
    Tap[HadoopFlowProcess, JobConf, RecordReader[_,_], _] = {
    val taps = goodHdfsPaths(hdfsMode).map { new Hfs(hdfsScheme, _, SinkMode.KEEP) }
    taps.size match {
      case 0 => {
        // This case is going to result in an error, but we don't want to throw until
        // validateTaps, so we just put a dummy path to return something so the
        // Job constructor does not fail.
        new Hfs(hdfsScheme, hdfsPaths.head, SinkMode.KEEP)
      }
      case 1 => taps.head
      case _ => new MultiSourceTap[Hfs, HadoopFlowProcess, JobConf, RecordReader[_,_]]( taps.toSeq : _*)
    }
  }
  protected def createHdfsWriteTap(hdfsMode : Hdfs) :
    Tap[HadoopFlowProcess, JobConf, _, OutputCollector[_,_]] = {
    new Hfs(hdfsScheme, hdfsWritePath, SinkMode.REPLACE)
  }

  /**
  * Allows you to read a Tap on the submit node NOT FOR USE IN THE MAPPERS OR REDUCERS.
  * Typical use might be to read in Job.next to determine if another job is needed
  */
  def readAtSubmitter[T](implicit mode : Mode, conv : TupleConverter[T]) : Stream[T] = {
    val tupleEntryIterator = mode match {
      case Local(_) => {
        // TODO support strict here
        val fp = new LocalFlowProcess()
        val tap = new FileTap(localScheme, localPath, SinkMode.KEEP)
        tap.openForRead(fp)
      }
      case Test(buffers) => {
        val fp = new LocalFlowProcess()
        val tap = new MemoryTap(localScheme, testBuffer(buffers, Read()))
        tap.openForRead(fp)
      }
      case HadoopTest(conf, buffers) => {
        val fp = new HadoopFlowProcess(new JobConf(conf))
        val tap = createHadoopTestReadTap(buffers(this))
        tap.openForRead(fp)
      }
      case hdfsMode @ Hdfs(_, conf) => {
        val fp = new HadoopFlowProcess(new JobConf(conf))
        val tap = createHdfsReadTap(hdfsMode)
        tap.openForRead(fp)
      }
    }
    def convertToStream(it : TupleEntryIterator) : Stream[T] = {
      if(null != it && it.hasNext) {
        val next = conv(it.next)
        Stream.cons(next, convertToStream(it))
      }
      else {
        Stream.Empty
      }
    }
    convertToStream(tupleEntryIterator)
  }
}

/**
* Usually as soon as we open a source, we read and do some mapping
* operation on a single column or set of columns.
* T is the type of the single column.  If doing multiple columns
* T will be a TupleN representing the types, e.g. (Int,Long,String)
*/
trait Mappable[T] extends Source {
  // These are the default column number YOU MAY NEED TO OVERRIDE!
  val columnNums = Seq(0)
  private def in = RichPipe.intFields(columnNums)
  private def out(fs : Seq[Symbol]) = RichPipe.fields(fs)

  def mapTo[U](f : Symbol*)(mf : (T) => U)
    (implicit flowDef : FlowDef, mode : Mode,
     conv : TupleConverter[T], setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).mapTo[T,U](in -> out(f))(mf)(conv, setter)
  }
  /**
  * If you want to filter, you should use this and output a 0 or 1 length Iterable.
  * Filter does not change column names, and we generally expect to change columns here
  */
  def flatMapTo[U](f : Symbol*)(mf : (T) => Iterable[U])
    (implicit flowDef : FlowDef, mode : Mode,
     conv : TupleConverter[T], setter : TupleSetter[U]) = {
    RichPipe(read(flowDef, mode)).flatMapTo[T,U](in -> out(f))(mf)(conv, setter)
  }
}

/**
* The fields here are ('offset, 'line)
*/
trait TextLineScheme extends Mappable[String] {
  def localScheme = new CLTextLine()
  def hdfsScheme = new CHTextLine().asInstanceOf[Scheme[_ <: FlowProcess[_],_,_,_,_,_]]
  //In textline, 0 is the byte position, the actual text string is in column 1
  override val columnNums = Seq(1)
}

/**
* Mix this in for delimited schemes such as TSV or one-separated values
* By default, TSV is given
*/
trait DelimitedScheme {
  //override these as needed:
  val fields = Fields.ALL
  //This is passed directly to cascading where null is interpretted as string
  val types : Array[Class[_]] = null
  val separator = "\t"
  //These should not be changed:
  def localScheme = new CLTextDelimited(fields, separator, types)
  def hdfsScheme = {
    new CHTextDelimited(fields, separator, types).asInstanceOf[Scheme[_ <: FlowProcess[_],_,_,_,_,_]]
  }
}

trait SequenceFileScheme {
  //override these as needed:
  val fields = Fields.ALL
  //doesn't support local mode yet
  def localScheme = null
  def hdfsScheme = {
    new CHSequenceFile(fields).asInstanceOf[Scheme[_ <: FlowProcess[_],_,_,_,_,_]]
  }
}

abstract class FixedPathSource(path : String*) extends Source {
  def localPath = { assert(path.size == 1); path(0) }
  def hdfsPaths = path.toList
}

/**
* Tab separated value source
*/
case class Tsv(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p)
  with DelimitedScheme {
    override val fields = f
}

/**
* One separated value (commonly used by Pig)
*/
case class Osv(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p)
  with DelimitedScheme {
    override val fields = f
    override val separator = "\1"
}

object TimePathedSource {
  val YEAR_MONTH_DAY = "/%1$tY/%1$tm/%1$td"
  val YEAR_MONTH_DAY_HOUR = "/%1$tY/%1$tm/%1$td/%1$tH"
}

/**
 * This will automatically produce a globbed version of the given path.
 * THIS MEANS YOU MUST END WITH A / followed by * to match a file
 * For writing, we write to the directory specified by the END time.
 */
abstract class TimePathedSource(pattern : String, dateRange : DateRange, tz : TimeZone) extends Source {
  val glober = Globifier(pattern)(tz)
  override def hdfsPaths = glober.globify(dateRange)
  //Write to the path defined by the end time:
  override def hdfsWritePath = {
    val lastSlashPos = pattern.lastIndexOf('/')
    assert(lastSlashPos >= 0, "/ not found in: " + pattern)
    val stripped = pattern.slice(0,lastSlashPos)
    String.format(stripped, dateRange.end.toCalendar(tz))
  }
  override def localPath = pattern

  /*
   * Get path statuses based on daterange.
   */
  protected def getPathStatuses(conf : Configuration) : Iterable[(String, Boolean)] = {
    List("%1$tH" -> Hours(1), "%1$td" -> Days(1)(tz),
      "%1$tm" -> Months(1)(tz), "%1$tY" -> Years(1)(tz))
      .find { unitDur : (String,Duration) => pattern.contains(unitDur._1) }
      .map { unitDur =>
        // This method is exhaustive, but too expensive for Cascading's JobConf writing.
        dateRange.each(unitDur._2)
          .map { dr : DateRange =>
            val path = String.format(pattern, dr.start.toCalendar(tz))
            val good = pathIsGood(path, conf)
            (path, good)
          }
      }
      .getOrElse(Nil : Iterable[(String, Boolean)])
  }

  // Override because we want to check UNGLOBIFIED paths that each are present.
  override def hdfsReadPathsAreGood(conf : Configuration) : Boolean = {
    getPathStatuses(conf).forall{ x =>
      if (!x._2) {
        System.err.println("[ERROR] Path: " + x._1 + " is missing in: " + toString)
      }
      x._2
    }
  }
}

/*
 * A source that contains the most recent existing path in this date range.
 */
abstract class MostRecentGoodSource(p : String, dr : DateRange, t : TimeZone)
    extends TimePathedSource(p, dr, t) {

  override protected def goodHdfsPaths(hdfsMode : Hdfs) = getPathStatuses(hdfsMode.config)
    .toList
    .reverse
    .find{ _._2 }
    .map{ x => x._1 }

  override def hdfsReadPathsAreGood(conf : Configuration) = getPathStatuses(conf)
    .exists{ _._2 }
}

case class TextLine(p : String) extends FixedPathSource(p) with TextLineScheme

case class SequenceFile(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p) with SequenceFileScheme
