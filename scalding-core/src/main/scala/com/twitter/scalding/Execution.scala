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
import com.twitter.algebird.{ Monoid, Monad }
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.twitter.scalding.Dsl.flowDefToRichFlowDef
import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue }
import scala.concurrent.{ Await, Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal
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
sealed trait Execution[+T] extends java.io.Serializable {
  import Execution.{ EvalCache, FlatMapped, GetCounters, ResetCounters, Mapped, OnComplete, RecoverWith, Zipped }

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
    GetCounters(this)

  /**
   * Reads the counters and resets them to zero. Probably what
   * you want in a loop that is using counters to check for
   * convergence.
   */
  def getAndResetCounters: Execution[(T, ExecutionCounters)] =
    getCounters.resetCounters

  /**
   * This function is called when the current run is completed. This is
   * only a side effect (see unit return).
   *
   * ALSO You must .run the result. If
   * you throw away the result of this call, your fn will never be
   * called. When you run the result, the Future you get will not
   * be complete unless fn has completed running. If fn throws, it
   * will be handled be the scala.concurrent.ExecutionContext.reportFailure
   * NOT by returning a Failure in the Future.
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
    ResetCounters(this)

  /**
   * This causes the Execution to occur. The result is not cached, so each call
   * to run will result in the computation being re-run. Avoid calling this
   * until the last possible moment by using flatMap, zip and recoverWith.
   *
   * Seriously: pro-style is for this to be called only once in a program.
   */
  final def run(conf: Config, mode: Mode)(implicit cec: ConcurrentExecutionContext): Future[T] = {
    val ec = new EvalCache
    val result = runStats(conf, mode, ec)(cec).map(_._1)
    // When the final future in complete we stop the submit thread
    result.onComplete { _ => ec.finished() }
    // wait till the end to start the thread in case the above throws
    ec.start()
    result
  }

  /**
   * This is the internal method that must be implemented
   * Given a config, mode, and cache of evaluations for this config and mode,
   * return the new cache with as much evaluation as possible before the future
   * completes, and a future of the result, counters and cache after the future
   * is complete
   */
  protected def runStats(conf: Config,
    mode: Mode,
    cache: EvalCache)(implicit cec: ConcurrentExecutionContext): Future[(T, ExecutionCounters)]

  /**
   * This is convenience for when we don't care about the result.
   * like .map(_ => ())
   */
  def unit: Execution[Unit] = map(_ => ())

  /**
   * This waits synchronously on run, using the global execution context
   * Avoid calling this if possible, prefering run or just Execution
   * composition. Every time someone calls this, be very suspect. It is
   * always code smell. Very seldom should you need to wait on a future.
   */
  def waitFor(conf: Config, mode: Mode): Try[T] =
    Try(Await.result(run(conf, mode)(ConcurrentExecutionContext.global),
      scala.concurrent.duration.Duration.Inf))

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
  def zip[U](that: Execution[U]): Execution[(T, U)] =
    Zipped(this, that)
}

/**
 * Execution has many methods for creating Execution[T] instances, which
 * are the preferred way to compose computations in scalding libraries.
 */
object Execution {
  /**
   * This is an instance of Monad for execution so it can be used
   * in functions that apply to all Monads
   */
  implicit object ExecutionMonad extends Monad[Execution] {
    override def apply[T](t: T): Execution[T] = Execution.from(t)
    override def map[T, U](e: Execution[T])(fn: T => U): Execution[U] = e.map(fn)
    override def flatMap[T, U](e: Execution[T])(fn: T => Execution[U]): Execution[U] = e.flatMap(fn)
    override def join[T, U](t: Execution[T], u: Execution[U]): Execution[(T, U)] = t.zip(u)
  }

  /**
   * This is a mutable state that is kept internal to an execution
   * as it is evaluating.
   */
  private[scalding] class EvalCache {
    private[this] val cache =
      new ConcurrentHashMap[Execution[Any], Future[(Any, ExecutionCounters)]]()

    /**
     * We send messages from other threads into the submit thread here
     */
    sealed trait FlowDefAction
    case class RunFlowDef(conf: Config,
      mode: Mode,
      fd: FlowDef,
      result: Promise[JobStats]) extends FlowDefAction
    case object Stop extends FlowDefAction
    private val messageQueue = new LinkedBlockingQueue[FlowDefAction]()
    /**
     * Hadoop and/or cascading has some issues, it seems, with starting jobs
     * from multiple threads. This thread does all the Flow starting.
     */
    private val thread = new Thread(new Runnable {
      def run() {
        @annotation.tailrec
        def go(): Unit = messageQueue.take match {
          case Stop => ()
          case RunFlowDef(conf, mode, fd, promise) =>
            try {
              promise.completeWith(
                ExecutionContext.newContext(conf)(fd, mode).run)
            } catch {
              case t: Throwable =>
                // something bad happened, but this thread is a daemon
                // that should only stop if all others have stopped or
                // we have received the stop message.
                // Stopping this thread prematurely can deadlock
                // futures from the promise we have.
                // In a sense, this thread does not exist logically and
                // must forward all exceptions to threads that requested
                // this work be started.
                promise.tryFailure(t)
            }
            // Loop
            go()
        }

        // Now we actually run the recursive loop
        go()
      }
    })

    def runFlowDef(conf: Config, mode: Mode, fd: FlowDef): Future[JobStats] =
      try {
        val promise = Promise[JobStats]()
        val fut = promise.future
        messageQueue.put(RunFlowDef(conf, mode, fd, promise))
        // Don't do any work after the .put call, we want no chance for exception
        // after the put
        fut
      } catch {
        case NonFatal(e) =>
          Future.failed(e)
      }

    def start(): Unit = {
      // Make sure this thread can't keep us running if all others are gone
      thread.setDaemon(true)
      thread.start()
    }
    /*
     * This is called after we are done submitting all jobs
     */
    def finished(): Unit = messageQueue.put(Stop)

    def getOrElseInsert[T](ex: Execution[T],
      res: => Future[(T, ExecutionCounters)])(implicit ec: ConcurrentExecutionContext): Future[(T, ExecutionCounters)] = {
      /*
       * Since we don't want to evaluate res twice, we make a promise
       * which we will use if it has not already been evaluated
       */
      val promise = Promise[(T, ExecutionCounters)]()
      val fut = promise.future
      cache.putIfAbsent(ex, fut) match {
        case null =>
          // note res is by-name, so we just evaluate it now:
          promise.completeWith(res)
          fut
        case exists => exists.asInstanceOf[Future[(T, ExecutionCounters)]]
      }
    }
  }

  private case class FutureConst[T](get: ConcurrentExecutionContext => Future[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        for {
          futt <- toFuture(Try(get(cec)))
          t <- futt
        } yield (t, ExecutionCounters.empty))

    // Note that unit is not optimized away, since Futures are often used with side-effects, so,
    // we ensure that get is always called in contrast to Mapped, which assumes that fn is pure.
  }
  private case class FlatMapped[S, T](prev: Execution[S], fn: S => Execution[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        for {
          (s, st1) <- prev.runStats(conf, mode, cache)
          next = fn(s)
          fut2 = next.runStats(conf, mode, cache)
          (t, st2) <- fut2
        } yield (t, Monoid.plus(st1, st2)))
  }

  private case class Mapped[S, T](prev: Execution[S], fn: S => T) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        prev.runStats(conf, mode, cache)
          .map { case (s, stats) => (fn(s), stats) })
  }
  private case class GetCounters[T](prev: Execution[T]) extends Execution[(T, ExecutionCounters)] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        prev.runStats(conf, mode, cache).map { case tc @ (t, c) => (tc, c) })
  }
  private case class ResetCounters[T](prev: Execution[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        prev.runStats(conf, mode, cache).map { case (t, _) => (t, ExecutionCounters.empty) })
  }

  private case class OnComplete[T](prev: Execution[T], fn: Try[T] => Unit) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this, {
        val res = prev.runStats(conf, mode, cache)
        /**
         * The result we give is only completed AFTER fn is run
         * so callers can wait on the result of this OnComplete
         */
        val finished = Promise[(T, ExecutionCounters)]()
        res.onComplete { tryT =>
          try {
            fn(tryT.map(_._1))
          } finally {
            // Do our best to signal when we are done
            finished.complete(tryT)
          }
        }
        finished.future
      })
  }
  private case class RecoverWith[T](prev: Execution[T], fn: PartialFunction[Throwable, Execution[T]]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        prev.runStats(conf, mode, cache)
          .recoverWith(fn.andThen(_.runStats(conf, mode, cache))))
  }
  private case class Zipped[S, T](one: Execution[S], two: Execution[T]) extends Execution[(S, T)] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this, {
        val f1 = one.runStats(conf, mode, cache)
        val f2 = two.runStats(conf, mode, cache)
        f1.zip(f2)
          .map { case ((s, ss), (t, st)) => ((s, t), Monoid.plus(ss, st)) }
      })
  }
  private case class UniqueIdExecution[T](fn: UniqueID => Execution[T]) extends Execution[T] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this, {
        val (uid, nextConf) = conf.ensureUniqueId
        fn(uid).runStats(nextConf, mode, cache)
      })
  }
  /*
   * This allows you to run any cascading flowDef as an Execution.
   */
  private case class FlowDefExecution(result: (Config, Mode) => FlowDef) extends Execution[Unit] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        for {
          flowDef <- toFuture(Try(result(conf, mode)))
          _ = FlowStateMap.validateSources(flowDef, mode)
          jobStats <- cache.runFlowDef(conf, mode, flowDef)
          _ = FlowStateMap.clear(flowDef)
        } yield ((), ExecutionCounters.fromJobStats(jobStats)))
  }

  /*
   * This is here so we can call without knowing the type T
   * but with proof that pipe matches sink
   */
  private case class ToWrite[T](pipe: TypedPipe[T], sink: TypedSink[T]) {
    def write(flowDef: FlowDef, mode: Mode): Unit = {
      // This has the side effect of mutating flowDef
      pipe.write(sink)(flowDef, mode)
      ()
    }
  }
  /**
   * This is the fundamental execution that actually happens in TypedPipes, all the rest
   * are based on on this one. By keeping the Pipe and the Sink, can inspect the Execution
   * DAG and optimize it later (a goal, but not done yet).
   */
  private case class WriteExecution(head: ToWrite[_], tail: List[ToWrite[_]]) extends Execution[Unit] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      cache.getOrElseInsert(this,
        for {
          flowDef <- toFuture(Try { val fd = new FlowDef; (head :: tail).foreach(_.write(fd, mode)); fd })
          _ = FlowStateMap.validateSources(flowDef, mode)
          jobStats <- cache.runFlowDef(conf, mode, flowDef)
          _ = FlowStateMap.clear(flowDef)
        } yield ((), ExecutionCounters.fromJobStats(jobStats)))
  }

  /**
   * This is called Reader, because it just returns its input to run as the output
   */
  private case object ReaderExecution extends Execution[(Config, Mode)] {
    def runStats(conf: Config, mode: Mode, cache: EvalCache)(implicit cec: ConcurrentExecutionContext) =
      Future.successful(((conf, mode), ExecutionCounters.empty))
  }

  private def toFuture[R](t: Try[R]): Future[R] =
    t match {
      case Success(s) => Future.successful(s)
      case Failure(err) => Future.failed(err)
    }

  /**
   * This creates a definitely failed Execution.
   */
  def failed(t: Throwable): Execution[Nothing] = fromTry(Failure(t))

  /**
   * This makes a constant execution that runs no job.
   * Note this is a lazy parameter that is evaluated every
   * time run is called.
   */
  def from[T](t: => T): Execution[T] = fromTry(Try(t))
  def fromTry[T](t: => Try[T]): Execution[T] = fromFuture { _ => toFuture(t) }

  /**
   * The call to fn will happen when the run method on the result is called.
   * The ConcurrentExecutionContext will be the same one used on run.
   * This is intended for cases where you need to make asynchronous calls
   * in the middle or end of execution. Presumably this is used with flatMap
   * either before or after
   */
  def fromFuture[T](fn: ConcurrentExecutionContext => Future[T]): Execution[T] = FutureConst(fn)

  /** Returns a constant Execution[Unit] */
  val unit: Execution[Unit] = from(())

  /**
   * This converts a function into an Execution monad. The flowDef returned
   * is never mutated.
   */
  def fromFn(fn: (Config, Mode) => FlowDef): Execution[Unit] =
    FlowDefExecution(fn)

  /**
   * Creates an Execution to do a write
   */
  private[scalding] def write[T](pipe: TypedPipe[T], sink: TypedSink[T]): Execution[Unit] =
    WriteExecution(ToWrite(pipe, sink), Nil)

  /**
   * Convenience method to get the Args
   */
  def getArgs: Execution[Args] = ReaderExecution.map(_._1.getArgs)
  /**
   * Use this to read the configuration, which may contain Args or options
   * which describe input on which to run
   */
  def getConfig: Execution[Config] = ReaderExecution.map(_._1)

  /** Use this to get the mode, which may contain the job conf */
  def getMode: Execution[Mode] = ReaderExecution.map(_._2)

  /** Use this to get the config and mode. */
  def getConfigMode: Execution[(Config, Mode)] = ReaderExecution

  /**
   * This is convenience method only here to make it slightly cleaner
   * to get Args, which are in the Config
   */
  def withArgs[T](fn: Args => Execution[T]): Execution[T] =
    getConfig.flatMap { conf => fn(conf.getArgs) }

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
