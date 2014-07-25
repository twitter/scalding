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

import com.twitter.algebird.monad.Reader
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.twitter.scalding.Dsl.flowDefToRichFlowDef

import scala.concurrent.{ Await, Future, Promise, ExecutionContext => ConcurrentExecutionContext }
import scala.util.{ Failure, Success, Try }
import cascading.flow.{ FlowDef, Flow, FlowListener }

/*
 * This has all the state needed to build a single flow
 * This is used with the implicit-arg-as-dependency-injection
 * style and with the Reader-as-dependency-injection
 */
trait ExecutionContext {
  def config: Config
  def flowDef: FlowDef
  def mode: Mode

  final def buildFlow: Try[Flow[_]] =
    // For some horrible reason, using Try( ) instead of the below gets me stuck:
    // [error]
    // /Users/oscar/workspace/scalding/scalding-core/src/main/scala/com/twitter/scalding/Execution.scala:92:
    // type mismatch;
    // [error]  found   : cascading.flow.Flow[_]
    // [error]  required: cascading.flow.Flow[?0(in method buildFlow)] where type ?0(in method
    //   buildFlow)
    // [error] Note: Any >: ?0, but Java-defined trait Flow is invariant in type Config.
    // [error] You may wish to investigate a wildcard type such as `_ >: ?0`. (SLS 3.2.10)
    // [error]       (resultT, Try(mode.newFlowConnector(finalConf).connect(newFlowDef)))
    try {
      // identify the flowDef
      val withId = config.setUniqueId(UniqueID.getIDFor(flowDef))
      val flow = mode.newFlowConnector(withId).connect(flowDef)
      Success(flow)
    } catch {
      case err: Throwable => Failure(err)
    }

  /**
   * Asynchronously execute the plan currently
   * contained in the FlowDef
   */
  final def run: Future[JobStats] =
    buildFlow match {
      case Success(flow) => Execution.run(flow)
      case Failure(err) => Future.failed(err)
    }

  /**
   * Synchronously execute the plan in the FlowDef
   */
  final def waitFor: Try[JobStats] =
    buildFlow.flatMap(Execution.waitFor(_))
}

/*
 * import ExecutionContext._
 * is generally needed to use the ExecutionContext as the single
 * dependency injected. For instance, TypedPipe needs FlowDef and Mode
 * in many cases, so if you have an implicit ExecutionContext, you need
 * modeFromImplicit, etc... below.
 */
object ExecutionContext {
  /*
   * implicit val ec = ExecutionContext.newContext(config)
   * can be used inside of a Job to get an ExecutionContext if you want
   * to call a function that requires an implicit ExecutionContext
   */
  def newContext(conf: Config)(implicit fd: FlowDef, m: Mode): ExecutionContext =
    new ExecutionContext {
      def config = conf
      def flowDef = fd
      def mode = m
    }

  /*
   * Creates a new ExecutionContext, with an empty FlowDef, given the Config and the Mode
   */
  def newContextEmpty(conf: Config, md: Mode): ExecutionContext = {
    val newFlowDef = new FlowDef
    conf.getCascadingAppName.foreach(newFlowDef.setName)
    newContext(conf)(newFlowDef, md)
  }

  implicit def modeFromContext(implicit ec: ExecutionContext): Mode = ec.mode
  implicit def flowDefFromContext(implicit ec: ExecutionContext): FlowDef = ec.flowDef
}

/**
 * This is a Monad, that represents a computation and a result
 */
sealed trait Execution[+T] {
  import Execution.{ Mapped, FactoryExecution, FlatMapped, Zipped }

  /*
   * First run this Execution, then move to the result
   * of the function
   */
  def flatMap[U](fn: T => Execution[U]): Execution[U] =
    FlatMapped(this, fn)

  def flatten[U](implicit ev: T <:< Execution[U]): Execution[U] =
    flatMap(ev)

  def map[U](fn: T => U): Execution[U] =
    Mapped(this, fn)

  def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext): Future[T]

  /**
   * This is convenience for when we don't care about the result.
   * same a .map(_ => ())
   */
  def unit: Execution[Unit] = map(_ => ())

  // This waits synchronously on run, using the global execution context
  def waitFor(conf: Config, mode: Mode): Try[T] =
    Try(Await.result(run(conf, mode)(ConcurrentExecutionContext.global),
      scala.concurrent.duration.Duration.Inf))

  /*
   * run this and that in parallel, without any dependency
   */
  def zip[U](that: Execution[U]): Execution[(T, U)] = that match {
    // push zips as low as possible
    case fact @ FactoryExecution(_) => fact.zip(this).map(_.swap)
    case _ => Zipped(this, that)
  }
}

object Execution {

  private case class Const[T](get: () => T) extends Execution[T] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      Future(get())

    override def unit = Const(() => ())
  }
  private case class FlatMapped[S, T](prev: Execution[S], fn: S => Execution[T]) extends Execution[T] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = for {
      s <- prev.run(conf, mode)
      next = fn(s)
      t <- next.run(conf, mode)
    } yield t
  }
  private case class Mapped[S, T](prev: Execution[S], fn: S => T) extends Execution[T] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      prev.run(conf, mode).map(fn)

    // Don't bother applying the function if we are mapped
    override def unit = prev.unit
  }
  private case class Zipped[S, T](one: Execution[S], two: Execution[T]) extends Execution[(S, T)] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      one.run(conf, mode).zip(two.run(conf, mode))

    // Make sure we remove any mapping functions on both sides
    override def unit = one.unit.zip(two.unit).map(_ => ())
  }
  /*
   * This is the main class the represents a flow without any combinators
   */
  private case class FlowDefExecution[T](result: (Config, Mode) => (FlowDef, (JobStats => Future[T]))) extends Execution[T] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = {
      for {
        (flowDef, fn) <- Future(result(conf, mode))
        jobStats <- ExecutionContext.newContext(conf)(flowDef, mode).run
        t <- fn(jobStats)
      } yield t
    }

    /*
     * Cascading can run parallel Executions in the same flow if they are both FlowDefExecutions
     */
    override def zip[U](that: Execution[U]): Execution[(T, U)] =
      that match {
        case FlowDefExecution(result2) =>
          FlowDefExecution({ (conf, m) =>
            val (fd1, fn1) = result(conf, m)
            val (fd2, fn2) = result2(conf, m)

            val merged = fd1.copy
            merged.mergeFrom(fd2)
            (merged, { (js: JobStats) => fn1(js).zip(fn2(js)) })
          })
        case _ => super.zip(that)
      }
  }
  private case class FactoryExecution[T](result: (Config, Mode) => Execution[T]) extends Execution[T] {
    def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      unwrap(conf, mode, this).run(conf, mode)

    @annotation.tailrec
    private def unwrap[U](conf: Config, mode: Mode, that: Execution[U]): Execution[U] =
      that match {
        case FactoryExecution(fn) => unwrap(conf, mode, fn(conf, mode))
        case nonFactory => nonFactory
      }
    /*
     * Cascading can run parallel Executions in the same flow if they are both FlowDefExecutions
     */
    override def zip[U](that: Execution[U]): Execution[(T, U)] =
      that match {
        case FactoryExecution(result2) =>
          FactoryExecution({ (conf, m) =>
            val exec1 = unwrap(conf, m, result(conf, m))
            val exec2 = unwrap(conf, m, result2(conf, m))
            exec1.zip(exec2)
          })
        case _ =>
          FactoryExecution({ (conf, m) =>
            val exec1 = unwrap(conf, m, result(conf, m))
            exec1.zip(that)
          })
      }
  }

  /**
   * This makes a constant execution that runs no job.
   */
  def from[T](t: => T): Execution[T] = Const(() => t)

  private[scalding] def factory[T](fn: (Config, Mode) => Execution[T]): Execution[T] =
    FactoryExecution(fn)

  /**
   * This converts a function into an Execution monad. The flowDef returned
   * is never mutated. The returned callback funcion is called after the flow
   * is run and succeeds.
   */
  def fromFn[T](
    fn: (Config, Mode) => ((FlowDef, JobStats => Future[T]))): Execution[T] =
    FlowDefExecution(fn)

  /**
   * This creates a new ExecutionContext, passes to the reader, builds the flow
   * and cleans up the state of the FlowDef
   */
  def buildFlow[T](conf: Config, mode: Mode)(op: Reader[ExecutionContext, T]): (T, Try[Flow[_]]) = {
    val ec = ExecutionContext.newContextEmpty(conf, mode)
    try {
      // This mutates the newFlowDef in ec
      val resultT = op(ec)
      (resultT, ec.buildFlow)
    } finally {
      // Make sure to clean up all state with flowDef
      FlowStateMap.clear(ec.flowDef)
    }
  }

  /**
   * Here is the recommended way to run scalding as a library
   * Put all your logic is calls like this:
   * import ExecutionContext._
   *
   * Reader(implicit ec: ExecutionContext =>
   *   //job here
   * )
   * you can compose these readers in flatMaps:
   * for {
   *   firstPipe <- job1
   *   secondPipe <- job2
   * } yield firstPipe.group.join(secondPipe.join)
   *
   * Note that the only config considered is in conf.
   * The caller is responsible for setting up the Config
   * completely.
   *
   * Here is a minimal example:
   * val future = Execution.run(Local(true), Config.default) { implicit ec: ExecutionContext =>
   *   //do logic here
   * }
   * Or one for Hadoop:
   * val jobConf = new JobConf
   * val future = Execution.run(Hdfs(jobConf, true), Config.hadoopWithDefaults(jobConf)) { implicit ec: ExecutionContext =>
   *   //do logic here
   * }
   * If you want to be synchronous, use waitFor instead of run
   */
  def run[T](conf: Config, mode: Mode)(op: Reader[ExecutionContext, T]): (T, Future[JobStats]) = {
    val (t, tryFlow) = buildFlow(conf, mode)(op)
    tryFlow match {
      case Success(flow) => (t, run(flow))
      case Failure(err) => (t, Future.failed(err))
    }
  }

  /*
   * This runs a Flow using Cascading's built in threads. The resulting JobStats
   * are put into a promise when they are ready
   */
  def run[C](flow: Flow[C]): Future[JobStats] =
    // This is in Java because of the cascading API's raw types on FlowListener
    FlowListenerPromise.start(flow, { f: Flow[C] => JobStats(f.getFlowStats) })

  /*
   * If you want scalding to fail if the sources cannot be validated, then
   * use this.
   * Alteratively, in your Reader, call Source.validateTaps(Mode) to
   * control which sources individually need validation
   * Suggested use:
   * for {
   *   result <- job
   *   mightErr <- validateSources
   * } yield mightErr.map(_ => result)
   */
  def validateSources: Reader[ExecutionContext, Try[Unit]] =
    Reader { ec => Try(FlowStateMap.validateSources(ec.flowDef, ec.mode)) }

  def waitFor[T](conf: Config, mode: Mode)(op: Reader[ExecutionContext, T]): (T, Try[JobStats]) = {
    val (t, tryFlow) = buildFlow(conf, mode)(op)
    (t, tryFlow.flatMap(waitFor(_)))
  }
  /*
   * This blocks the current thread until the job completes with either success or
   * failure.
   */
  def waitFor[C](flow: Flow[C]): Try[JobStats] =
    Try {
      flow.complete;
      JobStats(flow.getStats)
    }

  def zip[A, B](ax: Execution[A], bx: Execution[B]): Execution[(A, B)] =
    ax.zip(bx)

  def zip[A, B, C](ax: Execution[A], bx: Execution[B], cx: Execution[C]): Execution[(A, B, C)] =
    ax.zip(bx).zip(cx).map { case ((a, b), c) => (a, b, c) }

  def zip[A, B, C, D](ax: Execution[A],
    bx: Execution[B],
    cx: Execution[C],
    dx: Execution[D]): Execution[(A, B, C, D)] =
    ax.zip(bx).zip(cx).zip(dx).map { case (((a, b), c), d) => (a, b, c, d) }

  def zip[A, B, C, D, E](ax: Execution[A],
    bx: Execution[B],
    cx: Execution[C],
    dx: Execution[D],
    ex: Execution[E]): Execution[(A, B, C, D, E)] =
    ax.zip(bx).zip(cx).zip(dx).zip(ex).map { case ((((a, b), c), d), e) => (a, b, c, d, e) }

  /*
   * If you have many Executions, it is better to combine them with
   * zip than flatMap (which is sequential)
   */
  def zipAll[T](exs: Seq[Execution[T]]): Execution[Seq[T]] = {
    @annotation.tailrec
    def go(xs: List[Execution[T]], acc: Execution[List[T]]): Execution[List[T]] = xs match {
      case Nil => acc
      case h :: tail => go(tail, h.zip(acc).map { case (y, ys) => y :: ys })
    }
    // This pushes all of them onto a list, and then reverse to keep order
    go(exs.toList, from(Nil)).map(_.reverse)
  }
}
