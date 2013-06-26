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

import cascading.flow.{Flow, FlowDef, FlowProps, FlowListener}
import cascading.pipe.Pipe
import cascading.tuple.collect.SpillableProps

import org.apache.hadoop.io.serializer.{Serialization => HSerialization}

//For java -> scala implicits on collections
import scala.collection.JavaConversions._

import java.util.Calendar
import java.util.concurrent.{Executors, TimeUnit, ThreadFactory, Callable, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import java.security.MessageDigest

object Job {
  // Uses reflection to create a job by name
  def apply(jobName : String, args : Args) : Job =
    Class.forName(jobName).
      getConstructor(classOf[Args]).
      newInstance(args).
      asInstanceOf[Job]
}

class Job(val args : Args) extends FieldConversions with java.io.Serializable {
  // Set specific Mode
  implicit def mode : Mode = Mode.getMode(args).getOrElse(sys.error("No Mode defined"))

  /**
  * you should never call these directly, there are here to make
  * the DSL work.  Just know, you can treat a Pipe as a RichPipe and
  * vice-versa within a Job
  */
  implicit def p2rp(pipe : Pipe) = new RichPipe(pipe)
  implicit def rp2p(rp : RichPipe) = rp.pipe
  implicit def source2rp(src : Source) : RichPipe = RichPipe(src.read)

  // This converts an interable into a Source with index (int-based) fields
  implicit def iterToSource[T](iter : Iterable[T])(implicit set: TupleSetter[T], conv : TupleConverter[T]) : Source = {
    IterableSource[T](iter)(set, conv)
  }
  //
  implicit def iterToPipe[T](iter : Iterable[T])(implicit set: TupleSetter[T], conv : TupleConverter[T]) : Pipe = {
    iterToSource(iter)(set, conv).read
  }
  implicit def iterToRichPipe[T](iter : Iterable[T])
    (implicit set: TupleSetter[T], conv : TupleConverter[T]) : RichPipe = {
    RichPipe(iterToPipe(iter)(set, conv))
  }

  // Override this if you want change how the mapred.job.name is written in Hadoop
  def name : String = getClass.getName

  //This is the FlowDef used by all Sources this job creates
  @transient
  implicit val flowDef = {
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  /** Copy this job
   * By default, this uses reflection and the single argument Args constructor
   */
  def clone(nextargs : Args) : Job = {
    this.getClass
    .getConstructor(classOf[Args])
    .newInstance(nextargs)
    .asInstanceOf[Job]
  }

  /**
  * Implement this method if you want some other jobs to run after the current
  * job. These will not execute until the current job has run successfully.
  */
  def next : Option[Job] = None

  /** Keep 100k tuples in memory by default before spilling
   * Turn this up as high as you can without getting OOM.
   *
   * This is ignored if there is a value set in the incoming mode.config
   */
  def defaultSpillThreshold: Int = 100 * 1000

  def fromInputStream(s: java.io.InputStream): Array[Byte] =
    Stream.continually(s.read).takeWhile(-1 !=).map(_.toByte).toArray

  def toHexString(bytes: Array[Byte]): String =
    bytes.map("%02X".format(_)).mkString

  def md5Hex(bytes: Array[Byte]): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(bytes)
    toHexString(md.digest)
  }

  // Generated the MD5 hex of the the bytes in the job classfile
  lazy val classIdentifier : String = {
    val classAsPath = getClass.getName.replace(".", "/") + ".class"
    val is = getClass.getClassLoader.getResourceAsStream(classAsPath)
    val bytes = fromInputStream(is)
    is.close()
    md5Hex(bytes)
  }

  /** This is the exact config that is passed to the Cascading FlowConnector.
   * By default:
   *   if there are no spill thresholds in mode.config, we replace with defaultSpillThreshold
   *   we overwrite io.serializations with ioSerializations
   *   we overwrite cascading.tuple.element.comparator.default to defaultComparator
   *   we add some scalding keys for debugging/logging
   *
   * Tip: override this method, call super, and ++ your additional
   * map to add or overwrite more options
   */
  def config: Map[AnyRef,AnyRef] = {
    // These are ignored if set in mode.config
    val lowPriorityDefaults =
      Map(SpillableProps.LIST_THRESHOLD -> defaultSpillThreshold.toString,
          SpillableProps.MAP_THRESHOLD -> defaultSpillThreshold.toString)

    lowPriorityDefaults ++
      mode.config ++
      // Optionally set a default Comparator
      (defaultComparator match {
        case Some(defcomp) => Map(FlowProps.DEFAULT_ELEMENT_COMPARATOR -> defcomp.getName)
        case None => Map.empty[AnyRef, AnyRef]
      }) ++
      Map(
        "io.serializations" -> ioSerializations.map { _.getName }.mkString(","),
        "scalding.version" -> "0.9.0-SNAPSHOT",
        "cascading.app.name" -> name,
        "cascading.app.id" -> name,
        "scalding.flow.class.name" -> getClass.getName,
        "scalding.flow.class.signature" -> classIdentifier,
        "scalding.job.args" -> args.toString,
        "scalding.flow.submitted.timestamp" ->
          Calendar.getInstance().getTimeInMillis().toString
      )
  }

  /**
   * combine the config, flowDef and the Mode to produce a flow
   */
  final def buildFlow: Flow[_] =
    mode.newFlowConnector(config).connect(flowDef)

  //Override this if you need to do some extra processing other than complete the flow
  def run : Boolean = {
    val flow = buildFlow
    listeners.foreach{l => flow.addListener(l)}
    flow.complete
    flow.getFlowStats.isSuccessful
  }

  //override this to add any listeners you need
  def listeners : List[FlowListener] = Nil

  /** The exact list of Hadoop serializations passed into the config
   * These replace the config serializations
   * Cascading tuple serialization should be in this list, and probably
   * before any custom code
   */
  def ioSerializations: List[Class[_ <: HSerialization[_]]] = List(
    classOf[org.apache.hadoop.io.serializer.WritableSerialization],
    classOf[cascading.tuple.hadoop.TupleSerialization],
    classOf[serialization.KryoHadoop]
  )
  /** Override this if you want to customize comparisons/hashing for your job
    * the config method overwrites using this before sending to cascading
    */
  def defaultComparator: Option[Class[_ <: java.util.Comparator[_]]] =
    Some(classOf[IntegralComparator])

  //Largely for the benefit of Java jobs
  implicit def read(src : Source) : Pipe = src.read
  def write(pipe : Pipe, src : Source) {src.writeFrom(pipe)}

  /*
   * Need to be lazy to be used within pipes.
   */
  private lazy val timeoutExecutor =
    Executors.newSingleThreadExecutor(new NamedPoolThreadFactory("job-timer", true))

  /*
   * Safely execute some operation within a deadline.
   *
   * TODO: once we have a mechanism to access FlowProcess from user functions, we can use this
   *       function to allow long running jobs by notifying Cascading of progress.
   */
  def timeout[T](timeout: AbsoluteDuration)(t: =>T): Option[T] = {
    val f = timeoutExecutor.submit(new Callable[Option[T]] {
      def call(): Option[T] = Some(t)
    });
    try {
      f.get(timeout.toMillisecs, TimeUnit.MILLISECONDS)
    } catch {
      case _: TimeoutException =>
        f.cancel(true)
        None
    }
  }
}

/*
 * NamedPoolThreadFactory is copied from util.core to avoid dependency.
 */
class NamedPoolThreadFactory(name: String, makeDaemons: Boolean) extends ThreadFactory {
  def this(name: String) = this(name, false)

  val group = new ThreadGroup(Thread.currentThread().getThreadGroup(), name)
  val threadNumber = new AtomicInteger(1)

  def newThread(r: Runnable) = {
    val thread = new Thread(group, r, name + "-" + threadNumber.getAndIncrement())
    thread.setDaemon(makeDaemons)
    if (thread.getPriority != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY)
    }
    thread
  }
}


/**
* Sets up an implicit dateRange to use in your sources and an implicit
* timezone.
* Example args: --date 2011-10-02 2011-10-04 --tz UTC
* If no timezone is given, Pacific is assumed.
*/
trait DefaultDateRangeJob extends Job {
  //Get date implicits and PACIFIC and UTC vals.
  import DateOps._

  // Optionally take --tz argument, or use Pacific time.  Derived classes may
  // override defaultTimeZone to change the default.
  def defaultTimeZone = PACIFIC
  implicit lazy val tz = args.optional("tz") match {
                      case Some(tzn) => java.util.TimeZone.getTimeZone(tzn)
                      case None => defaultTimeZone
                    }

  // Optionally take a --period, which determines how many days each job runs over (rather
  // than over the whole date range)
  // --daily and --weekly are aliases for --period 1 and --period 7 respectively
  val period =
    if (args.boolean("daily"))
      1
    else if (args.boolean("weekly"))
      7
    else
      args.getOrElse("period", "0").toInt

  lazy val (startDate, endDate) = {
    val (start, end) = args.list("date") match {
      case List(s, e) => (RichDate(s), RichDate.upperBound(e))
      case List(o) => (RichDate(o), RichDate.upperBound(o))
      case x => sys.error("--date must have exactly one or two date[time]s. Got: " + x.toString)
    }
    //Make sure the end is not before the beginning:
    assert(start <= end, "end of date range must occur after the start")
    (start, end)
  }

  implicit lazy val dateRange = DateRange(startDate, if (period > 0) startDate + Days(period) - Millisecs(1) else endDate)

  override def next : Option[Job] =
    if (period > 0) {
      val nextStartDate = startDate + Days(period)
      if (nextStartDate + Days(period - 1) > endDate)
        None  // we're done
      else  // return a new job with the new startDate
        Some(clone(args + ("date" -> List(nextStartDate.toString("yyyy-MM-dd"), endDate.toString("yyyy-MM-dd")))))
    }
    else
      None
}

// DefaultDateRangeJob with default time zone as UTC instead of Pacific.
trait UtcDateRangeJob extends DefaultDateRangeJob {
  override def defaultTimeZone = DateOps.UTC
}

/*
 * Run a list of shell commands through bash in the given order. Return success
 * when all commands succeed. Excution stops after the first failure. The
 * failing command is printed to stdout.
 */
class ScriptJob(cmds: Iterable[String]) extends Job(Args("")) {
  override def run : Boolean = {
    try {
      cmds.dropWhile {
        cmd: String => {
          new java.lang.ProcessBuilder("bash", "-c", cmd).start().waitFor() match {
            case x if x != 0 =>
              println(cmd + " failed, exitStatus: " + x)
              false
            case 0 => true
          }
        }
      }.isEmpty
    } catch {
      case e : Exception => {
        e.printStackTrace
        false
      }
    }
  }
}
