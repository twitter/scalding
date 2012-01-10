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


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.fs.Path
import cascading.tuple.{Tuple, TupleEntryIterator, Fields}
import collection.mutable.{Buffer, MutableList}

/**
* Every source must have a correct toString method.  If you use
* case classes for instances of sources, you will get this for free.
* This is one of the several reasons we recommend using cases classes
*/
abstract class Source {
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

  private def pathIsGood(p : String, conf : Configuration) = {
    val path = new Path(p)
    Option(path.getFileSystem(conf).globStatus(path)).
        map(_.length > 0).
        getOrElse(false)
  }

  protected def readWriteMode(rwmode : AccessMode) : SinkMode = {
    rwmode match {
      case Read() => SinkMode.KEEP
      case Write() => SinkMode.REPLACE
    }
  }

  def hdfsPaths : Iterable[String]
  def localPath : String
  def localScheme : LocalScheme
  def hdfsScheme : Scheme[_ <: FlowProcess[_],_,_,_,_,_]

  protected def createTap(m : AccessMode, mode : Mode) : RawTap = {
    mode match {
      case Local() => new FileTap(localScheme, localPath, readWriteMode(m))
      case Test(buffers) => new MemoryTap(localScheme, testBuffer(buffers, m))
      case Hdfs(conf) => createHdfsTap(m, conf)
    }
  }

  protected def createHdfsTap(m : AccessMode, conf : Configuration) : RawTap = {
    val taps = hdfsPaths.
                filter{ (m == Write()) || pathIsGood(_, conf) }.
                map(new Hfs(hdfsScheme, _, readWriteMode(m)).asInstanceOf[RawTap])

    m match {
      case Read() => taps.size match {
          case 0 => error("No existing paths found in " + hdfsPaths)
          case 1 => taps.head
          case _ => new MultiSourceTap(taps.toSeq : _*)
        }
      case Write() => taps.head
    }
  }

  /**
  * Allows you to read a Tap on the submit node NOT FOR USE IN THE MAPPERS OR REDUCERS.
  * Typical use might be to read in Job.next to determine if another job is needed
  */
  def readAtSubmitter[T](implicit mode : Mode, conv : TupleConverter[T]) : Stream[T] = {
    val tupleEntryIterator = mode match {
      case Local() => {
        val fp = new LocalFlowProcess()
        val tap = new FileTap(localScheme, localPath, SinkMode.KEEP)
        tap.openForRead(fp)
      }
      case Test(buffers) => {
        val fp = new LocalFlowProcess()
        val tap = new MemoryTap(localScheme, testBuffer(buffers, Read()))
        tap.openForRead(fp)
      }
      case Hdfs(conf) => {
        val fp = new HadoopFlowProcess(new JobConf(conf))
        val taps = hdfsPaths.
                filter{ pathIsGood(_, conf) }.
                map(new Hfs(hdfsScheme, _, SinkMode.KEEP).asInstanceOf[RawTap])
        val tap = new MultiSourceTap[HadoopFlowProcess,
          JobConf, RecordReader[_,_], OutputCollector[_,_]](taps.toSeq : _*)
        tap.openForRead(fp)
      }
    }
    def convertToStream(it : TupleEntryIterator) : Stream[T] = {
      if(null != it && it.hasNext) {
        val next = conv.get(it.next)
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
  val YEAR_MONTH_DAY_HOUR = YEAR_MONTH_DAY + "/%1$tH"
}
abstract class TimePathedSource(pattern : String, dateRange : DateRange, duration : Duration, tz : TimeZone) extends Source {
  def hdfsPaths = {
    dateRange.each(duration)(tz)
      .map { (dr : DateRange) => String.format(pattern, dr.start.toCalendar(tz)) }
  }

  def localPath = pattern
}

abstract class CalendarPathedSource(pattern : String, dateRange : DateRange, duration : CalendarDuration, tz : TimeZone) extends Source {
  def hdfsPaths = {
    dateRange.each(duration)(tz)
      .map { (dr : DateRange) => String.format(pattern, dr.start.toCalendar(tz)) }
  }

  def localPath = pattern
}


case class TextLine(p : String) extends FixedPathSource(p) with TextLineScheme

case class SequenceFile(p : String, f : Fields = Fields.ALL) extends FixedPathSource(p) with SequenceFileScheme
