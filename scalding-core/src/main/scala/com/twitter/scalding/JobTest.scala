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
package com.twitter.scalding

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import cascading.stats.CascadingStats
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

object JobTest {

  @deprecated(message = "Use the non-reflection based JobTest apply methods", since = "0.16.1")
  def apply(jobName: String) =
    new JobTest((args: Args) => Job(jobName, args))

  def apply(cons: (Args) => Job) =
    new JobTest(cons)

  def apply[T <: Job: Manifest] = {
    val cons = { (args: Args) =>
      manifest[T].runtimeClass
        .getConstructor(classOf[Args])
        .newInstance(args)
        .asInstanceOf[Job]
    }
    new JobTest(cons)
  }

  // We have to memoize to return the same buffer each time.
  private case class MemoizedSourceFn[T](
      fn: Source => Option[Iterable[T]],
      setter: TupleSetter[T]
  ) extends (Source => Option[mutable.Buffer[Tuple]]) {
    private val memo = mutable.Map[Source, Option[mutable.Buffer[Tuple]]]()
    private val lock = new Object()

    def apply(src: Source): Option[mutable.Buffer[Tuple]] = lock.synchronized {
      memo.getOrElseUpdate(src, fn(src).map(elements => elements.map(t => setter(t)).toBuffer))
    }
  }
}

object CascadeTest {
  def apply(jobName: String) =
    new CascadeTest((args: Args) => Job(jobName, args))
}

/**
 * This class is used to construct unit tests for scalding jobs. You should not use it unless you are writing
 * tests. For examples of how to do that, see the tests included in the main scalding repository:
 * https://github.com/twitter/scalding/tree/master/scalding-core/src/test/scala/com/twitter/scalding
 */
class JobTest(cons: (Args) => Job) {
  private var argsMap = Map[String, List[String]]()
  private val callbacks = mutable.Buffer[() => Unit]()
  private val statsCallbacks = mutable.Buffer[(CascadingStats) => Unit]()
  // TODO: Switch the following maps and sets from Source to String keys
  // to guard for scala equality bugs
  private var sourceMap: (Source) => Option[mutable.Buffer[Tuple]] = { _ => None }
  private var sinkSet = Set[Source]()
  private var fileSet = Set[String]()
  private var validateJob = false

  def arg(inArg: String, value: List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg: String, value: String) = {
    argsMap += inArg -> List(value)
    this
  }

  private def sourceBuffer[T: TupleSetter](s: Source, tups: Iterable[T]): JobTest = {
    source(src => if (src == s) Some(tups) else None)
    this
  }

  /** Add a function to produce a mock when a certain source is requested */
  def source[T](fn: Source => Option[Iterable[T]])(implicit setter: TupleSetter[T]): JobTest = {
    val memoized = JobTest.MemoizedSourceFn(fn, setter)
    val oldSm = sourceMap
    sourceMap = { src: Source =>
      memoized(src).orElse(oldSm(src))
    }
    this
  }

  /**
   * Enables syntax like: .ifSource { case Tsv("in") => List(1, 2, 3) } We need a different function name from
   * source to help the compiler
   */
  def ifSource[T](fn: PartialFunction[Source, Iterable[T]])(implicit setter: TupleSetter[T]): JobTest =
    source(fn.lift)

  def source[T](s: Source, iTuple: Iterable[T])(implicit setter: TupleSetter[T]): JobTest =
    sourceBuffer(s, iTuple)

  // This use of `_.get` is probably safe, but difficult to prove correct
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def sink[A](s: Source)(op: mutable.Buffer[A] => Unit)(implicit conv: TupleConverter[A]) = {
    if (sourceMap(s).isEmpty) {
      // if s is also used as a source, we shouldn't reset its buffer
      source(s, new mutable.ListBuffer[Tuple])
    }
    val buffer = sourceMap(s).get
    /* NOTE: `HadoopTest.finalize` depends on `sinkSet` matching the set of
     * "keys" in the `sourceMap`.  Do not change the following line unless
     * you also modify the `finalize` function accordingly.
     */
    sinkSet += s
    callbacks += (() => op(buffer.map(tup => conv(new TupleEntry(tup)))))
    this
  }

  def typedSink[A](s: Source with TypedSink[A])(op: mutable.Buffer[A] => Unit)(implicit
      conv: TupleConverter[A]
  ) =
    sink[A](s)(op)

  // Used to pass an assertion about a counter defined by the given group and name.
  // If this test is checking for multiple jobs chained by next, this only checks
  // for the counters in the final job's FlowStat.
  def counter(counter: String, group: String = Stats.ScaldingGroup)(op: Long => Unit) = {
    statsCallbacks += ((stats: CascadingStats) => op(Stats.getCounterValue(counter, group)(stats)))
    this
  }

  // Used to check an assertion on all custom counters of a given scalding job.
  def counters(op: Map[String, Long] => Unit) = {
    statsCallbacks += ((stats: CascadingStats) => op(Stats.getAllCustomCounters()(stats)))
    this
  }

  // Simulates the existence of a file so that mode.fileExists returns true.  We
  // do not simulate the file contents; that should be done through mock
  // sources.
  def registerFile(filename: String) = {
    fileSet += filename
    this
  }

  def run = {
    runJob(initJob(false), true)
    this
  }

  def runWithoutNext(useHadoop: Boolean = false) = {
    runJob(initJob(useHadoop), false)
    this
  }

  def runHadoop = {
    runJob(initJob(true), true)
    this
  }

  def runHadoopWithConf(conf: Configuration) = {
    runJob(initJob(true, Some(conf)), true)
    this
  }

  // This is just syntax to end the "builder" pattern to satify some test frameworks
  def finish(): Unit = ()

  def validate(v: Boolean) = {
    validateJob = v
    this
  }

  def getArgs: Args =
    new Args(argsMap)

  /**
   * This method does not mutate the JobTest instance
   */
  def getTestMode(useHadoop: Boolean, optConfig: Option[Configuration] = None): TestMode = {
    // Create a global mode to use for testing.
    val testMode: TestMode =
      if (useHadoop) {
        val conf = optConfig.getOrElse(new JobConf)
        // Set the polling to a lower value to speed up tests:
        conf.set("jobclient.completion.poll.interval", "100")
        conf.set("cascading.flow.job.pollinginterval", "5")
        conf.set("mapreduce.framework.name", "local")
        // Work around for local hadoop race
        conf.set("mapred.local.dir", "/tmp/hadoop/%s/mapred/local".format(java.util.UUID.randomUUID))
        HadoopTest(conf, sourceMap)
      } else {
        Test(sourceMap)
      }
    testMode.registerTestFiles(fileSet)
    testMode
  }

  /**
   * Run the clean ups and checks after a job has executed
   */
  def postRunChecks(mode: Mode): Unit = {
    mode match {
      case hadoopTest @ HadoopTest(_, _) => {
        /* NOTE: `HadoopTest.finalize` depends on `sinkSet` matching the set of
         * "keys" in the `sourceMap`.  Do not change the following line unless
         * you also modify the `finalize` function accordingly.
         */
        // The sinks are written to disk, we need to clean them up:
        sinkSet.foreach(hadoopTest.finalize(_))
      }
      case _ => ()
    }
    // Now it is time to check the test conditions:
    callbacks.foreach(cb => cb())
  }

  // Registers test files, initializes the global mode, and creates a job.
  private[scalding] def initJob(useHadoop: Boolean, job: Option[Configuration] = None): Job = {
    val testMode = getTestMode(useHadoop, job)
    // Construct a job.
    cons(Mode.putMode(testMode, getArgs))
  }

  @tailrec
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private final def runJob(job: Job, runNext: Boolean): Unit = {
    // Disable automatic cascading update
    System.setProperty("cascading.update.skip", "true")

    // create cascading 3.0 planner trace files during tests
    if (System.getenv.asScala.getOrElse("SCALDING_CASCADING3_DEBUG", "0") == "1") {
      System.setProperty("cascading.planner.plan.path", "target/test/cascading/traceplan/" + job.name)
      System.setProperty(
        "cascading.planner.plan.transforms.path",
        "target/test/cascading/traceplan/" + job.name + "/transform"
      )
      System.setProperty(
        "cascading.planner.stats.path",
        "target/test/cascading/traceplan/" + job.name + "/stats"
      )
    }

    if (validateJob) {
      job.validate()
    }
    job.run()
    // Make sure to clean the state:
    job.clear()

    val next: Option[Job] = if (runNext) { job.next }
    else { None }
    next match {
      case Some(nextjob) => runJob(nextjob, runNext)
      case None =>
        postRunChecks(job.mode)
        statsCallbacks.foreach(cb => cb(job.scaldingCascadingStats.get))
    }
  }
}

class CascadeTest(cons: (Args) => Job) extends JobTest(cons) {}
