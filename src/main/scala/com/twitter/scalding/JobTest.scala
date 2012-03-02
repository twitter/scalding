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

  def run = {
    Mode.mode = Test(sourceMap)
    runAll(Job(jobName, new Args(argsMap)))
    this
  }

  def runHadoop = {
    Mode.mode = HadoopTest(new JobConf(), sourceMap)
    runAll(Job(jobName, new Args(argsMap)), true)
    this
  }

  // This SITS is unfortunately needed to get around Specs
  def finish : Unit = { () }

  @tailrec
  final def runAll(job : Job, useHadoop : Boolean = false) : Unit = {
    job.buildFlow.complete
    job.next match {
      case Some(nextjob) => runAll(nextjob, useHadoop)
      case None => {
        if(useHadoop) {
          sinkSet.foreach{ _.finalizeHadoopTestOutput(Mode.mode) }
        }
        //Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }

  def runWithoutNext = {
    Mode.mode = Test(sourceMap)
    Job(jobName, new Args(argsMap)).buildFlow.complete
    callbacks.foreach { cb => cb() }
    this
  }

}
