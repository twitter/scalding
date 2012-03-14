package com.twitter.scalding

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.annotation.tailrec

import cascading.tuple.Tuple

import org.apache.hadoop.mapred.JobConf

object JobTest {
  def apply(jobName : String) = new JobTest(jobName)
}

/**
 * This class is used to construct unit tests for scalding jobs.
 * You should not use it unless you are writing tests.
 * For examples of how to do that, see the tests included in the
 * main scalding repository:
 * https://github.com/twitter/scalding/tree/master/src/test/scala/com/twitter/scalding
 */
class JobTest(jobName : String) extends TupleConversions {
  private var argsMap = Map[String, List[String]]()
  private val callbacks = Buffer[() => Unit]()
  private var sourceMap = Map[Source, Buffer[Tuple]]()
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

  def source(s : Source, iTuple : Iterable[Product]) = {
    sourceMap += s -> iTuple.toList.map{ productToTuple(_) }.toBuffer
    this
  }

  def sink[A](s : Source)(op : Buffer[A] => Unit )
    (implicit conv : TupleConverter[A]) = {
    val buffer = new ListBuffer[Tuple]
    sourceMap += s -> buffer
    sinkSet += s
    callbacks += (() => op(buffer.map{conv(_)}))
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
    runJob(initJob(Test(sourceMap)), true)
    this
  }

  def runWithoutNext = {
    runJob(initJob(Test(sourceMap)), false)
    this
  }

  def runHadoop = {
    runJob(initJob(HadoopTest(new JobConf(), sourceMap)), true)
    this
  }

  // This SITS is unfortunately needed to get around Specs
  def finish : Unit = { () }

  // Registers test files, initializes the global mode, and creates a job.
  private def initJob(testMode : TestMode) : Job = {
    // First register test files and set the global mode.
    testMode.registerTestFiles(fileSet)
    Mode.mode = testMode
    Job(jobName, new Args(argsMap))
  }

  @tailrec
  private final def runJob(job : Job, runNext : Boolean) : Unit = {
    job.buildFlow.complete
    val next : Option[Job] = if (runNext) { job.next } else { None }
    next match {
      case Some(nextjob) => runJob(nextjob, runNext)
      case None => {
        Mode.mode match {
          case HadoopTest(_,_) => sinkSet.foreach{ _.finalizeHadoopTestOutput(Mode.mode) }
          case _ => ()
        }
        // Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }
}
