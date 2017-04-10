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
import java.util.{ Map => JMap, Properties, UUID }

import cascading.flow.FlowDef
import cascading.flow.FlowProcess
import cascading.scheme.{ NullScheme, Scheme }
import cascading.tap.hadoop.Hfs
import cascading.tap.SinkMode
import cascading.tap.{ Tap, SourceTap, SinkTap }
import cascading.tuple.{ Fields, Tuple => CTuple, TupleEntry, TupleEntryCollector, TupleEntryIterator }

import cascading.pipe.Pipe

import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import scala.collection.JavaConverters._

/**
 * thrown when validateTaps fails
 */
class InvalidSourceException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * InvalidSourceTap used in createTap method when we want to defer
 * the failures to validateTaps method.
 *
 * This is used because for Job classes, createTap method on sources is called
 * when the class is initialized. In most cases though, we want any exceptions to be
 * thrown by validateTaps method, which is called subsequently during flow planning.
 *
 * hdfsPaths represents user-supplied list that was detected as not containing any valid paths.
 */
class InvalidSourceTap(val e: Throwable) extends SourceTap[JobConf, RecordReader[_, _]] {

  def this(hdfsPaths: Iterable[String]) =
    this(new InvalidSourceException(s"No good paths in $hdfsPaths"))

  private final val randomId = UUID.randomUUID.toString

  override def getIdentifier: String = s"InvalidSourceTap-$randomId"

  override def hashCode: Int = randomId.hashCode

  override def getModifiedTime(conf: JobConf): Long = 0L

  override def openForRead(flow: FlowProcess[JobConf], input: RecordReader[_, _]): TupleEntryIterator = throw new InvalidSourceException("Encountered InvalidSourceTap!", e)

  override def resourceExists(conf: JobConf): Boolean = false

  override def getScheme = new NullScheme()

  // We set a dummy input format here so that mapred.input.format.class key is present,
  // which is a requirement for casading's MultiInputFormat at flow plan time.
  // So the order of operations here will be:
  // 1. source.createTap
  // 2. tap.sourceConfInit
  // 3. scheme.sourceConfInit
  // 4. source.validateTaps (throws InvalidSourceException)
  // In the worst case if the flow plan is misconfigured,
  // openForRead on mappers should fail when using this tap.
  override def sourceConfInit(flow: FlowProcess[JobConf], conf: JobConf): Unit = {
    conf.setInputFormat(classOf[InvalidInputFormat])
    super.sourceConfInit(flow, conf)
  }
}

/**
 * Better error messaging for the occassion where an InvalidSourceTap does not
 * fail in validation.
 */
private[scalding] class InvalidInputFormat extends InputFormat[Nothing, Nothing] {
  override def getSplits(conf: JobConf, numSplits: Int): Nothing =
    throw new InvalidSourceException("getSplits called on InvalidInputFormat")
  override def getRecordReader(split: InputSplit, conf: JobConf, reporter: org.apache.hadoop.mapred.Reporter): Nothing =
    throw new InvalidSourceException("getRecordReader called on InvalidInputFormat")
}

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
  def apply(tap: Hfs): Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]] =
    tap.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]]
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

  /**
   * The mock passed in to scalding.JobTest may be considered
   * as a mock of the Tap or the Source. By default, as of 0.9.0,
   * it is considered as a Mock of the Source. If you set this
   * to true, the mock in TestMode will be considered to be a
   * mock of the Tap (which must be transformed) and not the Source.
   */
  def transformInTest: Boolean = false

  /**
   * This is a name the refers to this exact instance of the source
   * (put another way, if s1.sourceId == s2.sourceId, the job should
   * work the same if one is replaced with the other
   */
  def sourceId: String = toString

  def read(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    checkFlowDefNotNull()

    //workaround for a type erasure problem, this is a map of String -> Tap[_,_,_]
    val sources = flowDef.getSources().asInstanceOf[JMap[String, Any]]
    /*
     * Starting in scalding 0.12, we assign a unique name for each head
     * pipe so that we can always merge two FlowDefs
     */
    val uuid = java.util.UUID.randomUUID
    val srcName = sourceId + uuid.toString
    assert(!sources.containsKey(srcName), "Source %s had collision in uuid: %s".format(this, uuid))
    sources.put(srcName, createTap(Read)(mode))
    FlowStateMap.mutate(flowDef) { st =>
      (st.addSource(srcName, this), ())
    }
    (mode, transformInTest) match {
      case (test: TestMode, false) => new Pipe(srcName)
      case _ => transformForRead(new Pipe(srcName))
    }
  }

  /**
   * write the pipe but return the input so it can be chained into
   * the next operation
   */
  def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    checkFlowDefNotNull()

    //insane workaround for scala compiler bug
    val sinks = flowDef.getSinks.asInstanceOf[JMap[String, Any]]
    val sinkName = sourceId
    if (!sinks.containsKey(sinkName)) {
      sinks.put(sinkName, createTap(Write)(mode))
    }
    val newPipe = (mode, transformInTest) match {
      case (test: TestMode, false) => pipe
      case _ => transformForWrite(pipe)
    }
    val outPipe = new Pipe(sinkName, newPipe)
    flowDef.addTail(outPipe)
    pipe
  }

  protected def checkFlowDefNotNull()(implicit flowDef: FlowDef, mode: Mode): Unit = {
    assert(flowDef != null, "Trying to access null FlowDef while in mode: %s".format(mode))
  }

  protected def transformForWrite(pipe: Pipe) = pipe
  protected def transformForRead(pipe: Pipe) = pipe

  /**
   * Subclasses of Source MUST override this method. They may call out to TestTapFactory for
   * making Taps suitable for testing.
   */
  def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _]
  /*
   * This throws InvalidSourceException if this source is invalid.
   */
  def validateTaps(mode: Mode): Unit = {} // linter:ignore

  @deprecated("replace with Mappable.toIterator", "0.9.0")
  def readAtSubmitter[T](implicit mode: Mode, conv: TupleConverter[T]): Stream[T] = {
    validateTaps(mode)
    val tap = createTap(Read)(mode)
    mode.openForRead(Config.defaultFrom(mode), tap).asScala.map { conv(_) }.toStream
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

  final def mapTo[U](out: Fields)(mf: (T) => U)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {
    RichPipe(read(flowDef, mode)).mapTo[T, U](sourceFields -> out)(mf)(converter, setter)
  }
  /**
   * If you want to filter, you should use this and output a 0 or 1 length Iterable.
   * Filter does not change column names, and we generally expect to change columns here
   */
  final def flatMapTo[U](out: Fields)(mf: (T) => TraversableOnce[U])(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {
    RichPipe(read(flowDef, mode)).flatMapTo[T, U](sourceFields -> out)(mf)(converter, setter)
  }

  /**
   * Allows you to read a Tap on the submit node NOT FOR USE IN THE MAPPERS OR REDUCERS.
   * Typical use might be to read in Job.next to determine if another job is needed
   */
  def toIterator(implicit config: Config, mode: Mode): Iterator[T] = {
    validateTaps(mode)
    val tap = createTap(Read)(mode)
    val conv = converter
    mode.openForRead(config, tap).asScala.map { te => conv(te.selectEntry(sourceFields)) }
  }

  /**
   * Transform this Mappable into another by mapping after.
   * We don't call this map because of conflicts with Mappable, unfortunately
   */
  override def andThen[U](fn: T => U): Mappable[U] = {
    val self = this // compiler generated self can cause problems with serialization
    new Mappable[U] {
      override def sourceFields = self.sourceFields
      def converter[V >: U]: TupleConverter[V] = self.converter.andThen(fn)
      override def read(implicit fd: FlowDef, mode: Mode): Pipe = self.read
      override def andThen[U1](fn2: U => U1) = self.andThen(fn.andThen(fn2))
      def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] =
        self.createTap(readOrWrite)(mode)
      override def validateTaps(mode: Mode): Unit = self.validateTaps(mode)
    }
  }

}

/**
 * Mappable extension that defines the proper converter
 * implementation for a Mappable with a single item.
 */
trait SingleMappable[T] extends Mappable[T] {
  override def converter[U >: T] = TupleConverter.asSuperConverter(TupleConverter.singleConverter[T])
}

/**
 * A tap that output nothing. It is used to drive execution of a task for side effect only. This
 * can be used to drive a pipe without actually writing to HDFS.
 */
class NullTap[Config, Input, Output, SourceContext, SinkContext]
  extends SinkTap[Config, Output](
    new NullScheme[Config, Input, Output, SourceContext, SinkContext](Fields.NONE, Fields.ALL),
    SinkMode.UPDATE) {

  def getIdentifier = "nullTap"
  def openForWrite(flowProcess: FlowProcess[Config], output: Output) =
    new TupleEntryCollector {
      override def add(te: TupleEntry): Unit = ()
      override def add(t: CTuple): Unit = ()
      protected def collect(te: TupleEntry): Unit = ()
    }

  def createResource(conf: Config) = true
  def deleteResource(conf: Config) = false
  def resourceExists(conf: Config) = true
  def getModifiedTime(conf: Config) = 0
}

trait BaseNullSource extends Source {
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    readOrWrite match {
      case Read => throw new Exception("not supported, reading from null")
      case Write => mode match {
        case Hdfs(_, _) => new NullTap[JobConf, RecordReader[_, _], OutputCollector[_, _], Any, Any]
        case Local(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
        case Test(_) => new NullTap[Properties, InputStream, OutputStream, Any, Any]
      }
    }
  }
}
/**
 * A source outputs nothing. It is used to drive execution of a task for side effect only.
 */
object NullSource extends BaseNullSource
