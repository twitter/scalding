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

import cascading.flow.{ Flow, FlowDef, FlowListener, FlowStep, FlowStepListener, FlowSkipStrategy, FlowStepStrategy }
import cascading.pipe.Pipe
import cascading.property.AppProps
import cascading.stats.CascadingStats

import com.twitter.algebird.Semigroup
import com.twitter.scalding.typed.cascading_backend.CascadingBackend

import org.apache.hadoop.io.serializer.{ Serialization => HSerialization }

import scala.concurrent.{ Future, Promise }
import scala.util.{Try, Success, Failure}

import java.io.{ BufferedWriter, FileOutputStream, OutputStreamWriter }
import java.util.{ List => JList }

import java.util.concurrent.{ Executors, TimeUnit, ThreadFactory, Callable, TimeoutException }
import java.util.concurrent.atomic.AtomicInteger

object Job {
  /**
   * Use reflection to create the job by name.  We use the thread's
   * context classloader so that classes in the submitted jar and any
   * jars included via -libjar can be found.
   */
  def apply(jobName: String, args: Args): Job = {
    Class.forName(jobName, true, Thread.currentThread().getContextClassLoader)
      .getConstructor(classOf[Args])
      .newInstance(args)
      .asInstanceOf[Job]
  }

  /**
   * Make a job reflectively from the given class
   * and the Args contained in the Config.
   */
  def makeJob[J <: Job](cls: Class[J]): Execution[J] =
    Execution.getConfigMode.flatMap { case (conf, mode) =>
      // Now we need to allocate the job
      Execution.from {
        val argsWithMode = Mode.putMode(mode, conf.getArgs)
        cls.getConstructor(classOf[Args])
          .newInstance(argsWithMode)
      }
    }

  /**
   * Create a job reflectively from a class, which handles threading
   * through the Args and Mode correctly in the way Job subclasses expect
   */
  def toExecutionFromClass[J <: Job](cls: Class[J], onEmpty: Execution[Unit]): Execution[Unit] =
    makeJob(cls).flatMap(toExecution(_, onEmpty))

  /**
   * Convert Jobs that only use the TypedPipe API to an Execution
   *
   * This can fail for some exotic jobs, but for standard subclasses
   * of Job (that don't override existing methods in Job except config)
   * it should work
   *
   * onEmpty is the execution to run if you have an empty job. Common
   * values might be Execution.unit or
   * Execution.failed(new Exeception("unexpected empty execution"))
   */
  def toExecution(job: Job, onEmpty: Execution[Unit]): Execution[Unit] =
    job match {
      case (exJob: ExecutionJob[_]) => exJob.execution.unit
      case _ =>
        val ex = CascadingBackend.flowDefToExecution(job.flowDef, None).getOrElse(onEmpty)

        // next may have a side effect so we
        // evaluate this *after* the current Execution
        val nextJobEx: Execution[Unit] =
          Execution.from(job.next).flatMap { // putting inside Execution.from memoizes this call
            case None => Execution.unit
            case Some(nextJob) => toExecution(nextJob, onEmpty)
          }

        for {
          conf <- Execution.fromTry(Config.tryFrom(job.config))
          // since we are doing an Execution[Unit], it is always safe to cleanup temp on finish
          _ <- Execution.withConfig(ex)(_ => conf.setExecutionCleanupOnFinish(true))
          _ <- nextJobEx
        } yield ()
      }
}

/**
 * Job is a convenience class to make using Scalding easier.
 * Subclasses of Job automatically have a number of nice implicits to enable more concise
 * syntax, including:
 *   conversion from Pipe, Source or Iterable to RichPipe
 *   conversion from Source or Iterable to Pipe
 *   conversion to collections or Tuple[1-22] to cascading.tuple.Fields
 *
 * Additionally, the job provides an implicit Mode and FlowDef so that functions that
 * register starts or ends of a flow graph, specifically anything that reads or writes data
 * on Hadoop, has the needed implicits available.
 *
 * If you want to write code outside of a Job, you will want to either:
 *
 * make all methods that may read or write data accept implicit FlowDef and Mode parameters.
 *
 * OR:
 *
 * write code that rather than returning values, it returns a (FlowDef, Mode) => T,
 * these functions can be combined Monadically using algebird.monad.Reader.
 */
class Job(val args: Args) extends FieldConversions with java.io.Serializable {
  Tracing.init()

  // Set specific Mode
  implicit def mode: Mode = Mode.getMode(args).getOrElse(sys.error("No Mode defined"))

  /**
   * Use this if a map or reduce phase takes a while before emitting tuples.
   */
  def keepAlive(): Unit = {
    val flowProcess = RuntimeStats.getFlowProcessForUniqueId(uniqueId)
    flowProcess.keepAlive()
  }

  /**
   * you should never call this directly, it is here to make
   * the DSL work.  Just know, you can treat a Pipe as a RichPipe
   * within a Job
   */
  implicit def pipeToRichPipe(pipe: Pipe): RichPipe = new RichPipe(pipe)
  /**
   * This implicit is to enable RichPipe methods directly on Source
   * objects, such as map/flatMap, etc...
   *
   * Note that Mappable is a subclass of Source, and Mappable already
   * has mapTo and flatMapTo BUT WITHOUT incoming fields used (see
   * the Mappable trait). This creates some confusion when using these methods
   * (this is an unfortunate mistake in our design that was not noticed until later).
   * To remove ambiguity, explicitly call .read on any Source that you begin
   * operating with a mapTo/flatMapTo.
   */
  implicit def sourceToRichPipe(src: Source): RichPipe = new RichPipe(src.read)

  // This converts an Iterable into a Pipe or RichPipe with index (int-based) fields
  implicit def toPipe[T](iter: Iterable[T])(implicit set: TupleSetter[T], conv: TupleConverter[T]): Pipe =
    IterableSource[T](iter)(set, conv).read

  implicit def iterableToRichPipe[T](iter: Iterable[T])(implicit set: TupleSetter[T], conv: TupleConverter[T]): RichPipe =
    RichPipe(toPipe(iter)(set, conv))

  // Provide args as an implicit val for extensions such as the Checkpoint extension.
  implicit protected def _implicitJobArgs: Args = args

  // Override this if you want to change how the mapred.job.name is written in Hadoop
  def name: String = Config.defaultFrom(mode).toMap.getOrElse("mapred.job.name", getClass.getName)

  //This is the FlowDef used by all Sources this job creates
  @transient
  implicit protected val flowDef: FlowDef = {
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  // Do this before the job is submitted, because the flowDef is transient
  private[this] val uniqueId = UniqueID.getIDFor(flowDef)

  /**
   * Copy this job
   * By default, this uses reflection and the single argument Args constructor
   */
  def clone(nextargs: Args): Job =
    this.getClass
      .getConstructor(classOf[Args])
      .newInstance(Mode.putMode(mode, nextargs))
      .asInstanceOf[Job]

  /**
   * Implement this method if you want some other jobs to run after the current
   * job. These will not execute until the current job has run successfully.
   */
  def next: Option[Job] = None

  /**
   * Keep 100k tuples in memory by default before spilling
   * Turn this up as high as you can without getting OOM.
   *
   * This is ignored if there is a value set in the incoming jobConf on Hadoop
   */
  def defaultSpillThreshold: Int = 100 * 1000

  /** Override this to control how dates are parsed */
  implicit def dateParser: DateParser = DateParser.default

  // Generated the MD5 hex of the bytes in the job classfile
  def classIdentifier: String = Config.md5Identifier(getClass)

  /**
   * This is the exact config that is passed to the Cascading FlowConnector.
   * By default:
   *   if there are no spill thresholds in mode.config, we replace with defaultSpillThreshold
   *   we overwrite io.serializations with ioSerializations
   *   we overwrite cascading.tuple.element.comparator.default to defaultComparator
   *   we add some scalding keys for debugging/logging
   *
   * Tip: override this method, call super, and ++ your additional
   * map to add or overwrite more options
   *
   * This returns Map[AnyRef, AnyRef] for compatibility with older code
   */
  def config: Map[AnyRef, AnyRef] = {
    val base = Config.empty
      .setListSpillThreshold(defaultSpillThreshold)
      .setMapSpillThreshold(defaultSpillThreshold)
      .setMapSideAggregationThreshold(defaultSpillThreshold)

    // This is setting a property for cascading/driven
    AppProps.addApplicationFramework(null,
      String.format("scalding:%s", scaldingVersion))

    val modeConf = mode match {
      case h: HadoopMode => Config.fromHadoop(h.jobConf)
      case _: CascadingLocal => Config.unitTestDefault
      case _ => Config.empty
    }

    val init = base ++ modeConf

    defaultComparator.map(init.setDefaultComparator)
      .getOrElse(init)
      .setSerialization(Right(classOf[serialization.KryoHadoop]), ioSerializations)
      .addCascadingClassSerializationTokens(reflectedClasses)
      .setScaldingVersion
      .setCascadingAppName(name)
      .setCascadingAppId(name)
      .setScaldingFlowClass(getClass)
      .setArgs(args)
      .maybeSetSubmittedTimestamp()._2
      .toMap.toMap[AnyRef, AnyRef] // linter:disable:TypeToType // the second one is to lift from String -> AnyRef
  }

  private def reflectedClasses: Set[Class[_]] =
    if (args.optional(Args.jobClassReflection).map(_.toBoolean).getOrElse(true)) {
      ReferencedClassFinder.findReferencedClasses(getClass)
    } else Set.empty


  /**
   * This is here so that Mappable.toIterator can find an implicit config
   */
  implicit protected def scaldingConfig: Config = Config.tryFrom(config).get

  def skipStrategy: Option[FlowSkipStrategy] = None

  /**
   * Specify a callback to run before the start of each flow step.
   *
   * Defaults to what Config.getReducerEstimator specifies.
   * @see ExecutionContext.buildFlow
   */
  def stepStrategy: Option[FlowStepStrategy[_]] = None

  private def executionContext: Try[ExecutionContext] =
    Config.tryFrom(config).map { conf =>
      ExecutionContext.newContext(conf)(flowDef, mode)
    }

  /**
   * combine the config, flowDef and the Mode to produce a flow
   */
  def buildFlow: Flow[_] =
    executionContext
      .flatMap(_.buildFlow)
      .flatMap[Flow[_]] {
        case None =>
          Failure(new IllegalStateException("sink taps are required"))
        case Some(flow) =>
          listeners.foreach { flow.addListener(_) }
          stepListeners.foreach { flow.addStepListener(_) }
          skipStrategy.foreach { flow.setFlowSkipStrategy(_) }
          stepStrategy.foreach { strategy =>
            val existing = flow.getFlowStepStrategy
            val composed =
              if (existing == null)
                strategy
              else
                FlowStepStrategies[Any].plus(
                  existing.asInstanceOf[FlowStepStrategy[Any]],
                  strategy.asInstanceOf[FlowStepStrategy[Any]])
            flow.setFlowStepStrategy(composed)
          }
          Success(flow)
      }
      .get

  // called before run
  // only override if you do not use flowDef
  def validate(): Unit = {
    CascadingBackend.planTypedWrites(flowDef, mode)
    FlowStateMap.validateSources(flowDef, mode)
  }

  // called after successfull run
  // only override if you do not use flowDef
  def clear(): Unit = {
    FlowStateMap.clear(flowDef)
  }

  protected def handleStats(statsData: CascadingStats): Unit = {
    scaldingCascadingStats = Some(statsData)
    // TODO: Why the two ways to do stats? Answer: jank-den.
    if (args.boolean("scalding.flowstats")) {
      val statsFilename = args.getOrElse("scalding.flowstats", name + "._flowstats.json")
      val br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statsFilename), "utf-8"))
      br.write(JobStats(statsData).toJson)
      br.close()
    }
    // Print custom counters unless --scalding.nocounters is used or there are no custom stats
    if (!args.boolean("scalding.nocounters")) {
      val jobStats = Stats.getAllCustomCounters()(statsData)
      if (!jobStats.isEmpty) {
        println("Dumping custom counters:")
        jobStats.foreach {
          case (counter, value) =>
            println("%s\t%s".format(counter, value))
        }
      }
    }
  }

  // TODO design a better way to test stats.
  // This awful name is designed to avoid collision
  // with subclasses
  @transient
  private[scalding] var scaldingCascadingStats: Option[CascadingStats] = None

  /**
   * Save the Flow object after a run to allow clients to inspect the job.
   * @see HadoopPlatformJobTest
   */
  @transient
  private[scalding] var completedFlow: Option[Flow[_]] = None

  //Override this if you need to do some extra processing other than complete the flow
  def run(): Boolean = {
    val flow = buildFlow
    flow.complete()
    val statsData = flow.getFlowStats
    handleStats(statsData)
    completedFlow = Some(flow)
    statsData.isSuccessful
  }

  //override these to add any listeners you need
  def listeners: List[FlowListener] = Nil
  def stepListeners: List[FlowStepListener] = Nil

  /**
   * These are user-defined serializations IN-ADDITION to (but deduped)
   * with the required serializations
   */
  def ioSerializations: List[Class[_ <: HSerialization[_]]] = Nil
  /**
   * Override this if you want to customize comparisons/hashing for your job
   * the config method overwrites using this before sending to cascading
   * The one we use by default is needed used to make Joins in the
   * Fields-API more robust to Long vs Int differences.
   * If you only use the Typed-API, consider changing this to return None
   */
  def defaultComparator: Option[Class[_ <: java.util.Comparator[_]]] =
    Some(classOf[IntegralComparator])

  /**
   * This is implicit so that a Source can be used as the argument
   * to a join or other method that accepts Pipe.
   */
  implicit def read(src: Source): Pipe = src.read
  /**
   * This is only here for Java jobs which cannot automatically
   * access the implicit Pipe => RichPipe which makes: pipe.write( )
   * convenient
   */
  def write(pipe: Pipe, src: Source): Unit = { src.writeFrom(pipe) }

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
  def timeout[T](timeout: AbsoluteDuration)(t: => T): Option[T] = {
    val f = timeoutExecutor.submit(new Callable[Option[T]] {
      def call(): Option[T] = Some(t)
    })
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
  implicit lazy val tz: java.util.TimeZone = args.optional("tz") match {
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
    val DateRange(s, e) = DateRange.parse(args.list("date"))
    (s, e)
  }

  implicit lazy val dateRange: DateRange = DateRange(startDate, if (period > 0) startDate + Days(period) - Millisecs(1) else endDate)

  override def next: Option[Job] =
    if (period > 0) {
      val nextStartDate = startDate + Days(period)
      if (nextStartDate + Days(period - 1) > endDate)
        None // we're done
      else // return a new job with the new startDate
        Some(clone(args + ("date" -> List(nextStartDate.toString("yyyy-MM-dd"), endDate.toString("yyyy-MM-dd")))))
    } else
      None
}

// DefaultDateRangeJob with default time zone as UTC instead of Pacific.
trait UtcDateRangeJob extends DefaultDateRangeJob {
  override def defaultTimeZone = DateOps.UTC
}

/**
 * This is a simple job that allows you to launch Execution[T]
 * instances using scalding.Tool and scald.rb. You cannot print
 * the graph.
 */
abstract class ExecutionJob[+T](args: Args) extends Job(args) {
  import scala.concurrent.{ Await, ExecutionContext => scEC }
  /**
   * To avoid serialization issues, this should not be a val, but a def,
   * and prefer to keep as much as possible inside the method.
   */
  def execution: Execution[T]

  /*
   * Override this to control the execution context used
   * to execute futures
   */
  protected def concurrentExecutionContext: scEC = scEC.global

  @transient private[this] val resultPromise: Promise[T] = Promise[T]()
  def result: Future[T] = resultPromise.future

  override def buildFlow: Flow[_] =
    sys.error("ExecutionJobs do not have a single accessible flow. " +
      "You cannot print the graph as it may be dynamically built or recurrent")

  final override def run = {
    val r = Config.tryFrom(config)
      .map { conf =>
        Await.result(execution.run(conf, mode)(concurrentExecutionContext),
          scala.concurrent.duration.Duration.Inf)
      }
    if (!resultPromise.tryComplete(r)) {
      // The test framework can call this more than once.
      println("Warning: run called more than once, should not happen in production")
    }
    // Force an exception if the run failed
    r.get
    true
  }
}

/*
 * Run a list of shell commands through bash in the given order. Return success
 * when all commands succeed. Excution stops after the first failure. The
 * failing command is printed to stdout.
 */
class ScriptJob(cmds: Iterable[String]) extends Job(Args("")) {
  override def run = {
    try {
      cmds.dropWhile {
        cmd: String =>
          {
            new java.lang.ProcessBuilder("bash", "-c", cmd).start().waitFor() match {
              case x if x != 0 =>
                println(cmd + " failed, exitStatus: " + x)
                false
              case 0 => true
            }
          }
      }.isEmpty
    } catch {
      case e: Exception => {
        e.printStackTrace
        false
      }
    }
  }
}

/**
 * Allows custom counter verification logic when the job completes.
 */
trait CounterVerification extends Job {
  /**
   * Verify counter values. The job will fail if this returns false or throws an exception.
   */
  def verifyCounters(counters: Map[StatKey, Long]): Try[Unit]

  /**
   * Override this to false to skip counter verification in tests.
   */
  def verifyCountersInTest: Boolean = true

  override def listeners: List[FlowListener] = {
    if (this.mode.isInstanceOf[TestMode] && !this.verifyCountersInTest) {
      super.listeners
    } else {
      super.listeners :+ new StatsFlowListener(this.verifyCounters)
    }
  }
}

private[scalding] case class FlowStepStrategies[A]() extends Semigroup[FlowStepStrategy[A]] {
  /**
   * Returns a new FlowStepStrategy that runs both strategies in sequence.
   */
  def plus(l: FlowStepStrategy[A], r: FlowStepStrategy[A]): FlowStepStrategy[A] =
    new FlowStepStrategy[A] {
      override def apply(
        flow: Flow[A],
        predecessorSteps: JList[FlowStep[A]],
        flowStep: FlowStep[A]): Unit = {
        l.apply(flow, predecessorSteps, flowStep)
        r.apply(flow, predecessorSteps, flowStep)
      }
    }
}
