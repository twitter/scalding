package com.twitter.scalding

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.annotation.tailrec
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender

object JobTest {
  def apply(jobName : String) = {
    new JobTest((args : Args) => Job(jobName,args))
  }
  def apply(cons : (Args) => Job) = {
    new JobTest(cons)
  }
  def apply[T <: Job : Manifest] = {
    val cons = { (args : Args) => manifest[T].erasure
      .getConstructor(classOf[Args])
      .newInstance(args)
      .asInstanceOf[Job] }
    new JobTest(cons)
  }
}

object CascadeTest {
  def apply(jobName : String) = {
    new CascadeTest((args : Args) => Job(jobName,args))
  }
}

/**
 * This class is used to construct unit tests for scalding jobs.
 * You should not use it unless you are writing tests.
 * For examples of how to do that, see the tests included in the
 * main scalding repository:
 * https://github.com/twitter/scalding/tree/master/src/test/scala/com/twitter/scalding
 */
class JobTest(cons : (Args) => Job) {
  //turning off all logging
  Logger.getRootLogger.removeAllAppenders()
  Logger.getRootLogger.addAppender(new NullAppender)

  private var argsMap = Map[String, List[String]]()
  private val callbacks = Buffer[() => Unit]()
  // TODO: Switch the following maps and sets from Source to String keys
  // to guard for scala equality bugs
  private var sourceMap: (Source) => Option[Buffer[Tuple]] = { _ => None }
  private var sinkSet = Set[Source]()
  private var fileSet = Set[String]()

  def arg(inArg : String, value : List[String]) = {
    argsMap += inArg -> value
    this
  }

  def arg(inArg : String, value : String) = {
    argsMap += inArg -> List(value)
    this
  }

  private def sourceBuffer(s: Source, tups: Buffer[Tuple]): JobTest = {
    source {src => if(src == s) Some(tups) else None }
    this
  }

  /** Add a function to produce a mock when a certain source is requested */
  def source(fn: Source => Option[Buffer[Tuple]]): JobTest = {
    val oldSm = sourceMap
    sourceMap = { src: Source => fn(src).orElse(oldSm(src)) }
    this
  }
  def source(fn: PartialFunction[Source, Buffer[Tuple]]): JobTest =
    source(fn.lift)

  def source(s : Source, iTuple : Iterable[Product]): JobTest =
    sourceBuffer(s, iTuple.map{ TupleSetter.ProductSetter(_) }.toBuffer)

  def source[T](s : Source, iTuple : Iterable[T])(implicit setter: TupleSetter[T]): JobTest =
    sourceBuffer(s, iTuple.map{ setter(_) }.toBuffer)

  def sink[A](s : Source)(op : Buffer[A] => Unit )
    (implicit conv : TupleConverter[A]) = {
    if (sourceMap(s).isEmpty) {
      // if s is also used as a source, we shouldn't reset its buffer
      source(s, new ListBuffer[Tuple])
    }
    val buffer = sourceMap(s).get
    sinkSet += s
    callbacks += (() => op(buffer.map { tup => conv(new TupleEntry(tup)) }))
    this
  }

  // Simulates the existance of a file so that mode.fileExists returns true.  We
  // do not simulate the file contents; that should be done through mock
  // sources.
  def registerFile(filename : String) = {
    fileSet += filename
    this
  }


  def run = {
    runJob(initJob(false), true)
    this
  }

  def runWithoutNext(useHadoop : Boolean = false) = {
    runJob(initJob(useHadoop), false)
    this
  }

  def runHadoop = {
    runJob(initJob(true), true)
    this
  }

  def runHadoopWithConf(conf : JobConf) = {
    runJob(initJob(true, Some(conf)), true)
    this
  }

  // This SITS is unfortunately needed to get around Specs
  def finish : Unit = { () }

  // Registers test files, initializes the global mode, and creates a job.
  private def initJob(useHadoop : Boolean, job: Option[JobConf] = None) : Job = {
    // Create a global mode to use for testing.
    val testMode : TestMode =
      if (useHadoop) {
        val conf = job.getOrElse(new JobConf)
        // Set the polling to a lower value to speed up tests:
        conf.set("jobclient.completion.poll.interval", "100")
        conf.set("cascading.flow.job.pollinginterval", "5")
        // Work around for local hadoop race
        conf.set("mapred.local.dir", "/tmp/hadoop/%s/mapred/local".format(java.util.UUID.randomUUID))
        HadoopTest(conf, sourceMap)
      } else {
        Test(sourceMap)
      }
    testMode.registerTestFiles(fileSet)
    val args = new Args(argsMap)

    // Construct a job.
    cons(Mode.putMode(testMode, args))
  }

  @tailrec
  private final def runJob(job : Job, runNext : Boolean) : Unit = {

    this match {
      case x: CascadeTest => job.run
      case x: JobTest => job.buildFlow.complete
    }

    val next : Option[Job] = if (runNext) { job.next } else { None }
    next match {
      case Some(nextjob) => runJob(nextjob, runNext)
      case None => {
        job.mode match {
          case hadoopTest @ HadoopTest(_,_) => {
            // The sinks are written to disk, we need to clean them up:
            sinkSet.foreach{ hadoopTest.finalize(_) }
          }
          case _ => ()
        }
        // Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }
}

class CascadeTest(cons : (Args) => Job) extends JobTest(cons) { }
