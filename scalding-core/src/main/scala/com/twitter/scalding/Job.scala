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


//For java -> scala implicits on collections
import scala.collection.JavaConversions._

import java.util.Calendar
import java.util.{Map => JMap}
import java.util.concurrent.{Executors, TimeUnit, ThreadFactory, Callable, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger

object Job {
  // Uses reflection to create a job by name
  def apply(jobName : String, args : Args) : Job =
    Class.forName(jobName).
      getConstructor(classOf[Args]).
      newInstance(args).
      asInstanceOf[Job]
}

class Job(val args : Args) extends TupleConversions
  with FieldConversions with java.io.Serializable {

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

  // Use reflection to copy this job:
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

  // Only very different styles of Jobs should override this.
  def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    mode.newFlowConnector(config).connect(flowDef)
  }

  /**
   * By default we only set two keys:
   * io.serializations
   * cascading.tuple.element.comparator.default
   * Override this class, call base and ++ your additional
   * map to set more options
   */
  def config(implicit mode : Mode) : Map[AnyRef,AnyRef] = {
    val ioserVals = (ioSerializations ++
      List("com.twitter.scalding.serialization.KryoHadoop")).mkString(",")

    mode.config ++
    Map("io.serializations" -> ioserVals) ++
      (defaultComparator match {
        case Some(defcomp) => Map(FlowProps.DEFAULT_ELEMENT_COMPARATOR -> defcomp)
        case None => Map[String,String]()
      }) ++
    Map("cascading.spill.threshold" -> "100000", //Tune these for better performance
        "cascading.spillmap.threshold" -> "100000") ++
    Map("scalding.version" -> "0.9.0-SNAPSHOT",
        "cascading.app.name" -> name,
        "scalding.flow.class.name" -> getClass.getName,
        "scalding.job.args" -> args.toString,
        "scalding.flow.submitted.timestamp" ->
          Calendar.getInstance().getTimeInMillis().toString
       )
  }

  //Override this if you need to do some extra processing other than complete the flow
  def run(implicit mode : Mode) = {
    val flow = buildFlow(mode)
    listeners.foreach{l => flow.addListener(l)}
    flow.complete
    flow.getFlowStats.isSuccessful
  }

  //override this to add any listeners you need
  def listeners(implicit mode : Mode) : List[FlowListener] = Nil

  // Add any serializations you need to deal with here (after these)
  def ioSerializations = List[String](
    "org.apache.hadoop.io.serializer.WritableSerialization",
    "cascading.tuple.hadoop.TupleSerialization"
  )
  // Override this if you want to customize comparisons/hashing for your job
  def defaultComparator : Option[String] = {
    Some("com.twitter.scalding.IntegralComparator")
  }

  //Largely for the benefit of Java jobs
  implicit def read(src : Source) : Pipe = src.read
  def write(pipe : Pipe, src : Source) {src.writeFrom(pipe)}

  def validateSources(mode : Mode) {
    flowDef.getSources()
      .asInstanceOf[JMap[String,AnyRef]]
      // this is a map of (name, Tap)
      .foreach { nameTap =>
        // Each named source must be present:
        mode.getSourceNamed(nameTap._1)
          .get
          // This can throw a InvalidSourceException
          .validateTaps(mode)
      }
  }

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
  override def run(implicit mode : Mode) = {
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
