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
import com.twitter.algebird.Monoid
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.twitter.scalding.Dsl.flowDefToRichFlowDef

import scala.concurrent.{ Await, Future, Promise, ExecutionContext => ConcurrentExecutionContext }
import scala.util.{ Failure, Success, Try }
import cascading.flow.{ FlowDef, Flow }

/**
 * Execution[T] represents and computation that can be run and
 * will produce a value T and keep track of counters incremented
 * inside of TypedPipes using a Stat.
 *
 * Execution[T] is the recommended way to compose multistep computations
 * that involve branching (if/then), intermediate calls to remote
 * services, file operations, or looping (e.g. testing for convergence).
 *
 * Library functions are encouraged to implement functions from
 * TypedPipes or ValuePipes to Execution[R] for some result R.
 * Refrain from calling run in library code. Let the caller
 * of your library call run.
 *
 * Note this is a Monad, meaning flatMap composes in series as you expect.
 * It is also an applicative functor, which means zip (called join
 * in some libraries) composes two Executions is parallel. Prefer
 * zip to flatMap if you want to run two Executions in parallel.
 */
sealed trait Execution[+T] {
  import Execution.{ FactoryExecution, FlatMapped, MapCounters, Mapped, OnComplete, RecoverWith, Zipped }

  /**
   * Scala uses the filter method in for syntax for pattern matches that can fail.
   * If this filter is false, the result of run will be an exception in the future
   */
  def filter(pred: T => Boolean): Execution[T] =
    flatMap {
      case good if pred(good) => Execution.from(good)
      case failed => Execution.from(sys.error("Filter failed on: " + failed.toString))
    }

  /**
   * First run this Execution, then move to the result
   * of the function
   */
  def flatMap[U](fn: T => Execution[U]): Execution[U] =
    FlatMapped(this, fn)

  /**
   * This is the same as flatMap(identity)
   */
  def flatten[U](implicit ev: T <:< Execution[U]): Execution[U] =
    flatMap(ev)

  /**
   * Apply a pure function to the result. This may not
   * be called if subsequently the result is discarded with .unit
   * For side effects see onComplete.
   */
  def map[U](fn: T => U): Execution[U] =
    Mapped(this, fn)

  /**
   * Reads the counters into the value, but does not reset them.
   * You may want .getAndResetCounters.
   */
  def getCounters: Execution[(T, ExecutionCounters)] =
    MapCounters[T, (T, ExecutionCounters)](this, { case tc @ (t, c) => (tc, c) })

  /**
   * Reads the counters and resets them to zero. Probably what
   * you want in a loop that is using counters to check for
   * convergence.
   */
  def getAndResetCounters: Execution[(T, ExecutionCounters)] =
    getCounters.resetCounters

  /**
   * This function is called when the current run is completed. This is
   * a only a side effect (see unit return).
   * Note, this is the only way to force a side effect. Map and FlatMap
   * are not safe for side effects. ALSO You must run the result. If
   * you throw away the result of this call, your fn will never be
   * called.
   */
  def onComplete(fn: Try[T] => Unit): Execution[T] = OnComplete(this, fn)

  /**
   * This allows you to handle a failure by giving a replacement execution
   * in some cases. This execution may be a retry if you know that your
   * execution can have spurious errors, or it could be a constant or an
   * alternate way to compute. Be very careful creating looping retries that
   * could hammer your cluster when the data is missing or when
   * when there is some real problem with your job logic.
   */
  def recoverWith[U >: T](rec: PartialFunction[Throwable, Execution[U]]): Execution[U] =
    RecoverWith(this, rec)

  /**
   * Resets the counters back to zero. This is useful if
   * you want to reset before a zip or a call to flatMap
   */
  def resetCounters: Execution[T] =
    MapCounters[T, T](this, { case (t, c) => (t, ExecutionCounters.empty) })

  /**
   * This causes the Execution to occur. The result is not cached, so each call
   * to run will result in the computation being re-run. Avoid calling this
   * until the last possible moment by using flatMap, zip and recoverWith.
   *
   * Seriously: pro-style is for this to be called only once in a program.
   */
  def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext): Future[T] =
    runStats(conf, mode)(cec).map(_._1)

  protected def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext): Future[(T, ExecutionCounters)]

  /**
   * This is convenience for when we don't care about the result.
   * like .map(_ => ())
   * Note: When called after a map, the map never happens. Use onComplete
   * to attach side effects.
   *
   * .map(fn).unit == .unit
   */
  def unit: Execution[Unit] = map(_ => ())

  /**
   * This waits synchronously on run, using the global execution context
   * Avoid calling this if possible, prefering run or just Execution
   * composition. Every time someone calls this, be very suspect. It is
   * always code smell. Very seldom should you need to wait on a future.
   */
  def waitFor(conf: Config, mode: Mode): Try[T] = {
    Try(Await.result(run(conf, mode)(ConcurrentExecutionContext.global),
      scala.concurrent.duration.Duration.Inf))
  }

  /**
   * This is here to silence warnings in for comprehensions, but is
   * identical to .filter.
   *
   * Users should never directly call this method, call .filter
   */
  def withFilter(p: T => Boolean): Execution[T] = filter(p)
  /*
   * run this and that in parallel, without any dependency. This will
   * be done in a single cascading flow if possible.
   */
  def zip[U](that: Execution[U]): Execution[(T, U)] = that match {
    // push zips as low as possible
    case fact @ FactoryExecution(_) => fact.zip(this).map(_.swap)
    case _ => Zipped(this, that)
  }
}

/**
 * Execution has many methods for creating Execution[T] instances, which
 * are the preferred way to compose computations in scalding libraries.
 */
object Execution {

  private case class FutureConst[T](get: ConcurrentExecutionContext => Future[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      Future(get(cec))
        .flatMap(identity)
        .map((_, ExecutionCounters.empty))
    // Note that unit is not optimized away, since Futures are often used with side-effects, so,
    // we ensure that get is always called in contrast to Mapped, which assumes that fn is pure.
  }
  private case class FlatMapped[S, T](prev: Execution[S], fn: S => Execution[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = for {
      (s, st1) <- prev.runStats(conf, mode)
      next = fn(s)
      (t, st2) <- next.runStats(conf, mode)
    } yield (t, Monoid.plus(st1, st2))
  }
  private case class Mapped[S, T](prev: Execution[S], fn: S => T) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      prev.runStats(conf, mode).map { case (s, stats) => (fn(s), stats) }

    // Don't bother applying the function if we are mapped
    override def unit = prev.unit
  }
  private case class MapCounters[T, U](prev: Execution[T],
    fn: ((T, ExecutionCounters)) => (U, ExecutionCounters)) extends Execution[U] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      prev.runStats(conf, mode).map(fn)
  }
  private case class OnComplete[T](prev: Execution[T], fn: Try[T] => Unit) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = {
      val fut = prev.runStats(conf, mode)
      fut.map(_._1).onComplete(fn)
      fut
    }
  }
  private case class RecoverWith[T](prev: Execution[T], fn: PartialFunction[Throwable, Execution[T]]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      prev.runStats(conf, mode).recoverWith(fn.andThen(_.runStats(conf, mode)))
  }
  private case class Zipped[S, T](one: Execution[S], two: Execution[T]) extends Execution[(S, T)] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      one.runStats(conf, mode).zip(two.runStats(conf, mode))
        .map { case ((s, ss), (t, st)) => ((s, t), Monoid.plus(ss, st)) }

    // Make sure we remove any mapping functions on both sides
    override def unit = one.unit.zip(two.unit).map(_ => ())
  }
  private case class UniqueIdExecution[T](fn: UniqueID => Execution[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = {
      val (uid, nextConf) = conf.ensureUniqueId
      fn(uid).runStats(nextConf, mode)
    }
  }
  /*
   * This is the main class the represents a flow without any combinators
   */
  private case class FlowDefExecution[T](result: (Config, Mode) => (FlowDef, (JobStats => Future[T]))) extends Execution[T] {
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) = {
      for {
        (flowDef, fn) <- toFuture(Try(result(conf, mode)))
        _ = FlowStateMap.validateSources(flowDef, mode)
        jobStats <- ExecutionContext.newContext(conf)(flowDef, mode).run
        _ = FlowStateMap.clear(flowDef)
        t <- fn(jobStats)
      } yield (t, ExecutionCounters.fromJobStats(jobStats))
    }

    /*
     * Cascading can run parallel Executions in the same flow if they are both FlowDefExecutions
     */
    override def zip[U](that: Execution[U]): Execution[(T, U)] =
      that match {
        /*
         * This merging parallelism only works if the names of the
         * sources are distinct. Scalding allocates uuids to each
         * pipe that starts a head, so a collision should be HIGHLY
         * unlikely.
         */
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
    def runStats(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext) =
      unwrap(conf, mode, this).runStats(conf, mode)

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

  private def toFuture[R](t: Try[R]): Future[R] =
    t match {
      case Success(s) => Future.successful(s)
      case Failure(err) => Future.failed(err)
    }
  /**
   * This makes a constant execution that runs no job.
   * Note this is a lazy parameter that is evaluated every
   * time run is called.
   */
  def from[T](t: => T): Execution[T] = fromFuture { _ => toFuture(Try(t)) }

  /**
   * The call to fn will happen when the run method on the result is called.
   * The ConcurrentExecutionContext will be the same one used on run.
   * This is intended for cases where you need to make asynchronous calls
   * in the middle or end of execution. Presumably this is used with flatMap
   * either before or after
   */
  def fromFuture[T](fn: ConcurrentExecutionContext => Future[T]): Execution[T] = FutureConst(fn)

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
   * Use this to read the configuration, which may contain Args or options
   * which describe input on which to run
   */
  def getConfig: Execution[Config] = factory { case (conf, _) => from(conf) }

  /**
   * Use this to use counters/stats with Execution. You do this:
   * Execution.withId { implicit uid =>
   *   val myStat = Stat("myStat") // uid is implicitly pulled in
   *   pipe.map { t =>
   *     if(someCase(t)) myStat.inc
   *     fn(t)
   *   }
   *   .writeExecution(mySink)
   * }
   */
  def withId[T](fn: UniqueID => Execution[T]): Execution[T] = UniqueIdExecution(fn)

  /**
   * This creates a new ExecutionContext, passes to the reader, builds the flow
   * and cleans up the state of the FlowDef
   */
  @deprecated("Use Execution[T]", "2014-07-29")
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

  /*
   * This is a low-level method that should be avoided if you are reading
   * the docs rather than the source, and may be removed.
   * You should be using Execution[T] to compose.
   */
  @deprecated("Use Execution[T].run", "2014-07-29")
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
   * This is a low-level method that should be avoided if you are reading
   * the docs rather than the source, and may be removed.
   * You should be using Execution[T] to compose.
   *
   * If you want scalding to fail if the sources cannot be validated, then
   * use this (highly recommended and the default for Execution[T])
   *
   * Alteratively, in your Reader, call Source.validateTaps(Mode) to
   * control which sources individually need validation
   * Suggested use:
   * for {
   *   result <- job
   *   mightErr <- validateSources
   * } yield mightErr.map(_ => result)
   */
  @deprecated("Use Execution[T].run", "2014-07-29")
  def validateSources: Reader[ExecutionContext, Try[Unit]] =
    Reader { ec => Try(FlowStateMap.validateSources(ec.flowDef, ec.mode)) }

  /*
   * This is a low-level method that should be avoided if you are reading
   * the docs rather than the source, and may be removed.
   * You should be using Execution[T] to compose.
   */
  @deprecated("Use Execution[T].run", "2014-07-29")
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

  /**
   * combine several executions and run them in parallel when .run is called
   */
  def zip[A, B](ax: Execution[A], bx: Execution[B]): Execution[(A, B)] =
    ax.zip(bx)

  /**
   * combine several executions and run them in parallel when .run is called
   */
  def zip[A, B, C](ax: Execution[A], bx: Execution[B], cx: Execution[C]): Execution[(A, B, C)] =
    ax.zip(bx).zip(cx).map { case ((a, b), c) => (a, b, c) }

  /**
   * combine several executions and run them in parallel when .run is called
   */
  def zip[A, B, C, D](ax: Execution[A],
    bx: Execution[B],
    cx: Execution[C],
    dx: Execution[D]): Execution[(A, B, C, D)] =
    ax.zip(bx).zip(cx).zip(dx).map { case (((a, b), c), d) => (a, b, c, d) }

  /**
   * combine several executions and run them in parallel when .run is called
   */
  def zip[A, B, C, D, E](ax: Execution[A],
    bx: Execution[B],
    cx: Execution[C],
    dx: Execution[D],
    ex: Execution[E]): Execution[(A, B, C, D, E)] =
    ax.zip(bx).zip(cx).zip(dx).zip(ex).map { case ((((a, b), c), d), e) => (a, b, c, d, e) }

  /*
   * If you have many Executions, it is better to combine them with
   * zip than flatMap (which is sequential). sequence just calls
   * zip on each item in the input sequence.
   *
   * Note, despite the name, which is taken from the standard scala Future API,
   * these executions are executed in parallel: run is called on all at the
   * same time, not one after the other.
   */
  def sequence[T](exs: Seq[Execution[T]]): Execution[Seq[T]] = {
    @annotation.tailrec
    def go(xs: List[Execution[T]], acc: Execution[List[T]]): Execution[List[T]] = xs match {
      case Nil => acc
      case h :: tail => go(tail, h.zip(acc).map { case (y, ys) => y :: ys })
    }
    // This pushes all of them onto a list, and then reverse to keep order
    go(exs.toList, from(Nil)).map(_.reverse)
  }
}

/**
 * This represents the counters portion of the JobStats that are returned.
 * Counters are just a vector of longs with counter name, group keys.
 */
trait ExecutionCounters {
  /**
   * immutable set of the keys.
   */
  def keys: Set[StatKey]
  /**
   * Same as get(key).getOrElse(0L)
   * Note if a counter is never incremented, get returns None.
   * But you can't tell 0L that comes from None vs. a counter
   * that was incremented then decremented.
   */
  def apply(key: StatKey): Long = get(key).getOrElse(0L)
  /**
   * If the counter is present, return it.
   */
  def get(key: StatKey): Option[Long]
  def toMap: Map[StatKey, Long] = keys.map { k => (k, get(k).getOrElse(0L)) }.toMap
}

/**
 * The companion gives several ways to create ExecutionCounters from
 * other CascadingStats, JobStats, or Maps
 */
object ExecutionCounters {
  /**
   * This is the zero of the ExecutionCounter Monoid
   */
  def empty: ExecutionCounters = new ExecutionCounters {
    def keys = Set.empty
    def get(key: StatKey) = None
    override def toMap = Map.empty
  }

  /**
   * Just gets the counters from the CascadingStats and ignores
   * all the other fields present
   */
  def fromCascading(cs: cascading.stats.CascadingStats): ExecutionCounters = new ExecutionCounters {
    import scala.collection.JavaConverters._

    val keys = (for {
      group <- cs.getCounterGroups.asScala
      counter <- cs.getCountersFor(group).asScala
    } yield StatKey(counter, group)).toSet

    def get(k: StatKey) =
      if (keys(k)) {
        // Yes, cascading is reversed frow what we did in Stats. :/
        Some(cs.getCounterValue(k.group, k.counter))
      } else None
  }

  /**
   * Gets just the counters from the JobStats
   */
  def fromJobStats(js: JobStats): ExecutionCounters = {
    val counters = js.counters
    new ExecutionCounters {
      def keys = for {
        group <- counters.keySet
        counter <- counters(group).keys
      } yield StatKey(counter, group)

      def get(k: StatKey) = counters.get(k.group).flatMap(_.get(k.counter))
    }
  }
  /**
   * A Simple wrapper over a Map[StatKey, Long]
   */
  def fromMap(allValues: Map[StatKey, Long]): ExecutionCounters =
    new ExecutionCounters {
      def keys = allValues.keySet
      def get(k: StatKey) = allValues.get(k)
      override def toMap = allValues
    }

  /**
   * This allows us to merge the results of two computations. It just
   * does pointwise addition.
   */
  implicit def monoid: Monoid[ExecutionCounters] = new Monoid[ExecutionCounters] {
    override def isNonZero(that: ExecutionCounters) = that.keys.nonEmpty
    def zero = ExecutionCounters.empty
    def plus(left: ExecutionCounters, right: ExecutionCounters) = {
      fromMap((left.keys ++ right.keys)
        .map { k => (k, left(k) + right(k)) }
        .toMap)
    }
  }
}
