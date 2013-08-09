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

import java.io.{File, InputStream, OutputStream}
import java.util.{TimeZone, Calendar, Map => JMap, Properties}

import cascading.flow.FlowDef
import cascading.flow.FlowProcess
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.local.LocalFlowProcess
import cascading.scheme.{NullScheme, Scheme}
import cascading.scheme.local.{TextLine => CLTextLine, TextDelimited => CLTextDelimited}
import cascading.scheme.hadoop.{TextLine => CHTextLine, TextDelimited => CHTextDelimited, SequenceFile => CHSequenceFile}
import cascading.tap.hadoop.Hfs
import cascading.tap.{MultiSourceTap, SinkMode}
import cascading.tap.{Tap, SinkTap}
import cascading.tap.local.FileTap
import cascading.tuple.{Fields, Tuple => CTuple, TupleEntry, TupleEntryCollector}

import cascading.pipe.Pipe

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

/*
 * Denotes the access mode for a Source
 */
sealed abstract class AccessMode
case object Read extends AccessMode
case object Write extends AccessMode

// Scala is pickier than Java about type parameters, and Cascading's Scheme
// declaration leaves some type parameters underspecified.  Fill in the type
// parameters with wildcards so the Scala compiler doesn't complain.

object HadoopSchemeInstance {
  def apply(scheme: Scheme[_, _, _, _, _]) =
    scheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}

object CastHfsTap {
  // The scala compiler has problems with the generics in Cascading
  def apply(tap : Hfs) : Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]] =
    tap.asInstanceOf[Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]]]
}

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
  def read(implicit flowDef : FlowDef, mode : Mode): Pipe = {
    checkFlowDefNotNull

    //workaround for a type erasure problem, this is a map of String -> Tap[_,_,_]
    val sources = flowDef.getSources().asInstanceOf[JMap[String,Any]]
    val srcName = this.toString
    if (!sources.containsKey(srcName)) {
      sources.put(srcName, createTap(Read)(mode))
    }
    FlowStateMap.mutate(flowDef) {
      _.getReadPipe(this, transformForRead(new Pipe(srcName)))
    }
  }

  /**
  * write the pipe and return the input so it can be chained into
  * the next operation
  */
  def writeFrom(pipe : Pipe)(implicit flowDef : FlowDef, mode : Mode) = {
    checkFlowDefNotNull

    //insane workaround for scala compiler bug
    val sinks = flowDef.getSinks().asInstanceOf[JMap[String,Any]]
    val sinkName = this.toString
    if (!sinks.containsKey(sinkName)) {
      sinks.put(sinkName, createTap(Write)(mode))
    }
    flowDef.addTail(new Pipe(sinkName, transformForWrite(pipe)))
    pipe
  }

  protected def checkFlowDefNotNull(implicit flowDef : FlowDef, mode : Mode) {
    assert(flowDef != null, "Trying to access null FlowDef while in mode: %s".format(mode))
  }

  protected def transformForWrite(pipe : Pipe) = pipe
  protected def transformForRead(pipe : Pipe) = pipe

  /**
  * Subclasses of Source MUST override this method. They may call out to TestTapFactory for
  * making Taps suitable for testing.
  */
  def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_]
  /*
   * This throws InvalidSourceException if this source is invalid.
   */
  def validateTaps(mode : Mode) : Unit = { }
  /**
  * Allows you to read a Tap on the submit node NOT FOR USE IN THE MAPPERS OR REDUCERS.
  * Typical use might be to read in Job.next to determine if another job is needed
  */
  def readAtSubmitter[T](implicit mode : Mode, conv : TupleConverter[T]) : Stream[T] = {
    val tap = createTap(Read)(mode)
    mode.openForRead(tap).asScala.map { conv(_) }.toStream
  }
}

/**
* Usually as soon as we open a source, we read and do some mapping
* operation on a single column or set of columns.
* T is the type of the single column.  If doing multiple columns
* T will be a TupleN representing the types, e.g. (Int,Long,String)
*
* Prefer to use TypedSource unless you are working with the fields API
*
* NOTE: If we don't make this extend Source, established implicits are ambiguous
* when TDsl is in scope.
*/
trait Mappable[+T] extends Source with TypedSource[T] {

  final def mapTo[U](out : Fields)(mf : (T) => U)
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]): Pipe = {
    RichPipe(read(flowDef, mode)).mapTo[T,U](sourceFields -> out)(mf)(converter, setter)
  }
  /**
  * If you want to filter, you should use this and output a 0 or 1 length Iterable.
  * Filter does not change column names, and we generally expect to change columns here
  */
  final def flatMapTo[U](out : Fields)(mf : (T) => TraversableOnce[U])
    (implicit flowDef : FlowDef, mode : Mode, setter : TupleSetter[U]): Pipe = {
    RichPipe(read(flowDef, mode)).flatMapTo[T,U](sourceFields -> out)(mf)(converter, setter)
  }
}

/**
 * A tap that output nothing. It is used to drive execution of a task for side effect only. This
 * can be used to drive a pipe without actually writing to HDFS.
 */
class NullTap[Config, Input, Output, SourceContext, SinkContext]
  extends SinkTap[Config, Output] (
    new NullScheme[Config, Input, Output, SourceContext, SinkContext](Fields.NONE, Fields.ALL),
      SinkMode.UPDATE) {

  def getIdentifier = "nullTap"
  def openForWrite(flowProcess: FlowProcess[Config], output: Output) =
    new TupleEntryCollector {
      override def add(te: TupleEntry) {}
      override def add(t: CTuple) {}
      protected def collect(te: TupleEntry) {}
    }

  def createResource(conf: Config) = true
  def deleteResource(conf: Config) = false
  def resourceExists(conf: Config) = true
  def getModifiedTime(conf: Config) = 0
}

/**
 * A source outputs nothing. It is used to drive execution of a task for side effect only.
 */
object NullSource extends Source {
  override def createTap(readOrWrite : AccessMode)(implicit mode : Mode) : Tap[_,_,_] = {
    readOrWrite match {
      case Read => throw new Exception("not supported, reading from null")
      case Write => mode match {
        case Hdfs(_, _) => new NullTap[JobConf, RecordReader[_,_], OutputCollector[_,_], Any, Any]
        case Local(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
        case Test(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
      }
    }
  }
}
