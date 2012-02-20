package com.twitter.scalding

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.annotation.tailrec

import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

object JobTest {
  def apply(jobName : String) = new JobTest(jobName)
}

class JobTest(jobName : String) extends TupleConversions {
  private var argsMap = Map[String, List[String]]()
  private val callbacks = Buffer[() => Unit]()
  private var sourceMap = Map[Source, Buffer[Tuple]]()

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
    callbacks += (() => op(buffer.map{conv(_)}))
    this
  }

  def run {
    Mode.mode = Test(sourceMap)
    runAll(Job(jobName, new Args(argsMap)))
  }

  @tailrec
  final def runAll(job : Job) : Unit = {
    job.buildFlow.complete
    job.next match {
      case Some(nextjob) => runAll(nextjob)
      case None => {
        //Now it is time to check the test conditions:
        callbacks.foreach { cb => cb() }
      }
    }
  }

  def runWithoutNext {
    Mode.mode = Test(sourceMap)
    Job(jobName, new Args(argsMap)).buildFlow.complete
    callbacks.foreach { cb => cb() }
  }

}
