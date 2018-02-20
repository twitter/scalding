package com.twitter.scalding.typed.cascading_backend

import cascading.flow.{ FlowDef, Flow }
import com.twitter.scalding.{
  source,
  typed,
  CascadingLocal,
  Config,
  Execution,
  ExecutionContext,
  ExecutionCounters,
  FlowStateMap,
  FutureCache,
  HadoopMode,
  JobStats,
  Mappable,
  Mode,
  TypedPipe
}
import com.twitter.scalding.typed.TypedSink
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import com.stripe.dagon.{ Dag, Rule, HMap }
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.concurrent.{ Await, Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

import Execution.{ Writer, ToWrite }

object AsyncFlowDefRunner {
  /**
   * We send messages from other threads into the submit thread here
   */
  private sealed trait FlowDefAction
  private final case class RunFlowDef(conf: Config,
    mode: Mode,
    fd: FlowDef,
    result: Promise[(Long, JobStats)]) extends FlowDefAction
  private case object Stop extends FlowDefAction

  /**
   * This is a Thread used as a shutdown hook to clean up temporary files created by some Execution
   *
   * If the job is aborted the shutdown hook may not run and the temporary files will not get cleaned up
   */
  final case class TempFileCleanup(filesToCleanup: List[String], mode: Mode) extends Thread {

    val LOG = LoggerFactory.getLogger(this.getClass)

    override def run(): Unit = {
      val fs = mode match {
        case localMode: CascadingLocal => FileSystem.getLocal(new Configuration)
        case hdfsMode: HadoopMode => FileSystem.get(hdfsMode.jobConf)
      }

      filesToCleanup.foreach { file: String =>
        try {
          val path = new Path(file)
          if (fs.exists(path)) {
            // The "true" parameter here indicates that we should recursively delete everything under the given path
            fs.delete(path, true)
          }
        } catch {
          // If we fail in deleting a temp file, log the error but don't fail the run
          case e: Throwable => LOG.warn(s"Unable to delete temp file $file", e)
        }
      }
    }
  }
}

/**
 * This holds an internal thread to run
 * This holds an internal thread to submit run
 * a Config, Mode, FlowDef and return a Future holding the
 * JobStats
 */
class AsyncFlowDefRunner extends Writer { self =>
  import AsyncFlowDefRunner._

  private[this] val mutex = new AnyRef

  type StateKey[T] = (Config, Mode, TypedPipe[T])
  type WorkVal[T] = Future[TypedPipe[T]]

  private case class FilesToCleanUp(onFinish: Set[String], onShutdown: Set[String]) {
    def addFile(conf: Config, m: Mode, s: String): FilesToCleanUp =
      if (conf.getExecutionCleanupOnFinish) copy(onFinish = onFinish + s)
      else copy(onShutdown = onShutdown + s)
  }
  private object FilesToCleanUp {
    def empty: FilesToCleanUp = FilesToCleanUp(Set.empty, Set.empty)
  }
  /**
   * @param filesToCleanup temporary files created by forceToDiskExecution
   * @param initToOpt this is the mapping between user's TypedPipes and their optimized versions
   * which are actually run.
   * @param forcedPipes these are all the side effecting forcing of TypedPipes into simple
   * SourcePipes or IterablePipes. These are for both toIterableExecution and forceToDiskExecution
   */
  private case class State(
    filesToCleanup: Map[Mode, FilesToCleanUp],
    initToOpt: HMap[TypedPipe, TypedPipe],
    forcedPipes: HMap[StateKey, WorkVal]) {

    def addFilesToCleanup(conf: Config, m: Mode, s: Option[String]): State =
      s match {
        case Some(path) =>
          val ftc0 = filesToCleanup.getOrElse(m, FilesToCleanUp.empty)
          val ftc1 = ftc0.addFile(conf, m, path)
          copy(filesToCleanup = filesToCleanup + (m -> ftc1))
        case None => this
      }

    /**
     * Returns true if we actually add this optimized pipe. We do this
     * because we don't want to take the side effect twice.
     */
    def addForce[T](c: Config,
      m: Mode,
      init: TypedPipe[T],
      opt: TypedPipe[T],
      p: Future[TypedPipe[T]]): (State, Boolean) =

      forcedPipes.get((c, m, opt)) match {
        case None =>
          (copy(forcedPipes = forcedPipes + ((c, m, opt) -> p),
            initToOpt = initToOpt + (init -> opt)), true)
        case Some(_) =>
          (copy(initToOpt = initToOpt + (init -> opt)), false)
      }

    def getForce[T](c: Config,
      m: Mode,
      init: TypedPipe[T]): Option[Future[TypedPipe[T]]] =

      initToOpt.get(init).map { opt =>
        forcedPipes.get((c, m, opt)) match {
          case None =>
            sys.error(s"invariant violation: initToOpt mapping exists for $init, but no forcedPipe")
          case Some(p) => p
        }
      }
  }

  private[this] var state: State = State(Map.empty, HMap.empty, HMap.empty)

  private def updateState[S](fn: State => (State, S)): S =
    mutex.synchronized {
      val (st1, s) = fn(state)
      state = st1
      s
    }
  private def getState: State =
    updateState { s => (s, s) }

  private val messageQueue: LinkedBlockingQueue[AsyncFlowDefRunner.FlowDefAction] =
    new LinkedBlockingQueue[AsyncFlowDefRunner.FlowDefAction]()

  /**
   * Hadoop and/or cascading has some issues, it seems, with starting jobs
   * from multiple threads. This thread does all the Flow starting.
   */
  private lazy val thread = new Thread(new Runnable {
    def run(): Unit = {
      @annotation.tailrec
      def go(id: Long): Unit = messageQueue.take match {
        case Stop => ()
        case RunFlowDef(conf, mode, fd, promise) =>
          try {
            if (fd.getSinks.isEmpty) {
              // These is nothing to do:
              promise.success((id, JobStats.empty))
            } else {
              val ctx = ExecutionContext.newContext(conf.setScaldingFlowCounterValue(id))(fd, mode)
              ctx.buildFlow match {
                case Success(flow) =>
                  val future = FlowListenerPromise
                    .start(flow, { f: Flow[_] => (id, JobStats(f.getFlowStats)) })

                  promise.completeWith(future)
                case Failure(err) =>
                  promise.failure(err)
              }
            }
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
          go(id + 1)
      }

      // Now we actually run the recursive loop
      go(0)
    }
  })

  def runFlowDef(conf: Config, mode: Mode, fd: FlowDef): Future[(Long, JobStats)] =
    try {
      val promise = Promise[(Long, JobStats)]()
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
  def finished(): Unit = {
    messageQueue.put(Stop)
    // get an immutable copy
    val cleanUp = getState.filesToCleanup
    cleanUp.foreach {
      case (mode, filesToRm) =>
        if (filesToRm.onShutdown.nonEmpty) {
          Runtime.getRuntime.addShutdownHook(TempFileCleanup(filesToRm.onShutdown.toList, mode))
        }
        if (filesToRm.onFinish.nonEmpty) {
          val cleanUpThread = TempFileCleanup(filesToRm.onFinish.toList, mode)
          // run it that the outer most execution is complete
          cleanUpThread.start()
        }
    }
  }

  /**
   * This evaluates the fn in a Try, validates the sources
   * calls runFlowDef, then clears the FlowStateMap
   */
  def validateAndRun(conf: Config, mode: Mode)(
    fn: (Config, Mode) => FlowDef)(
      implicit cec: ConcurrentExecutionContext): Future[(Long, ExecutionCounters)] =
    for {
      flowDef <- Future(fn(conf, mode))
      _ = FlowStateMap.validateSources(flowDef, mode)
      (id, jobStats) <- runFlowDef(conf, mode, flowDef)
      _ = FlowStateMap.clear(flowDef)
    } yield (id, ExecutionCounters.fromJobStats(jobStats))

  def execute(
    conf: Config,
    mode: Mode,
    writes: List[ToWrite])(implicit cec: ConcurrentExecutionContext): Future[(Long, ExecutionCounters)] = {

    import Execution.ToWrite._

    val done = Promise[Unit]()

    val phases: Seq[Rule[TypedPipe]] =
      CascadingBackend.defaultOptimizationRules(conf)

    val toOptimized = ToWrite.optimizeWriteBatch(writes, phases)

    def prepareFD(c: Config, m: Mode): FlowDef = {
      val fd = new FlowDef

      def write[A](tpipe: TypedPipe[A], dest: TypedSink[A]): Unit = {
        // We have already applied the optimizations to the batch of writes above
        val pipe = CascadingBackend.toPipeUnoptimized(tpipe, dest.sinkFields)(fd, mode, dest.setter)
        dest.writeFrom(pipe)(fd, mode)
      }

      def force[A](init: TypedPipe[A], opt: TypedPipe[A]): Unit = {
        val pipePromise = Promise[TypedPipe[A]]()
        val fut = pipePromise.future
        // This updates mutable state
        val sinkOpt = updateState { s =>
          val (nextState, added) = s.addForce(conf, mode, init, opt, fut)
          if (added) {
            val uuid = UUID.randomUUID
            val (sink, forcedPipe, clean) = forceToDisk(uuid, c, m, opt)
            (nextState.addFilesToCleanup(conf, m, clean), Some((sink, forcedPipe)))
          } else {
            (nextState, None)
          }
        }

        sinkOpt.foreach {
          case (sink, fp) =>
            // We write the optimized pipe
            write(opt, sink)
            val pipeFut = done.future.map(_ => fp())
            pipePromise.completeWith(pipeFut)
        }
      }
      def addIter[A](init: TypedPipe[A], optimized: Either[Iterable[A], Mappable[A]]): Unit = {
        val result = optimized match {
          case Left(iter) if iter.isEmpty => TypedPipe.EmptyTypedPipe
          case Left(iter) => TypedPipe.IterablePipe(iter)
          case Right(mappable) => TypedPipe.SourcePipe(mappable)
        }
        val fut = Future.successful(result)
        updateState(_.addForce(conf, mode, init, result, fut))
      }

      writes.foreach {
        case Force(init) =>
          val opt = toOptimized(init)
          force(init, opt)
        case ToIterable(init) =>
          def step[A](opt: TypedPipe[A]): Unit = {
            opt match {
              case TypedPipe.EmptyTypedPipe => addIter(init, Left(Nil))
              case TypedPipe.IterablePipe(as) => addIter(init, Left(as))
              case TypedPipe.SourcePipe(src: Mappable[A]) => addIter(init, Right(src))
              case other =>
                // we need to write the pipe out first.
                force(init, opt)
              // now, when we go to check for the pipe later, it
              // will be a SourcePipe of a Mappable by construction
            }
          }
          step(toOptimized(init))

        case SimpleWrite(pipe, sink) =>
          write(toOptimized(pipe), sink)
      }

      fd
    }

    val resultFuture = validateAndRun(conf, mode)(prepareFD _)

    // When we are done, the forced pipes are ready:
    done.completeWith(resultFuture.map(_ => ()))
    resultFuture
  }

  def getForced[T](
    conf: Config,
    m: Mode,
    initial: TypedPipe[T])(implicit cec: ConcurrentExecutionContext): Future[TypedPipe[T]] =

    getState.getForce(conf, m, initial) match {
      case Some(fut) => fut
      case None =>
        val msg =
          s"logic error: getForced($conf, $m, $initial) does not have a forced pipe."
        Future.failed(new Exception(msg))
    }

  def getIterable[T](
    conf: Config,
    m: Mode,
    initial: TypedPipe[T])(implicit cec: ConcurrentExecutionContext): Future[Iterable[T]] =

    getForced(conf, m, initial).flatMap {
      case TypedPipe.EmptyTypedPipe => Future.successful(Nil)
      case TypedPipe.IterablePipe(iter) => Future.successful(iter)
      case TypedPipe.SourcePipe(src: Mappable[T]) =>
        Future.successful(
          new Iterable[T] {
            def iterator = src.toIterator(conf, m)
          })
      case other =>
        val msg =
          s"logic error: expected an Iterable pipe. ($conf, $m, $initial) -> $other is not iterable"
        Future.failed(new Exception(msg))
    }

  private def forceToDisk[T]( // linter:disable:UnusedParameter
    uuid: UUID,
    conf: Config,
    mode: Mode,
    pipe: TypedPipe[T] // note, we don't use this, but it fixes the type T
    ): (typed.TypedSink[T], () => TypedPipe[T], Option[String]) =

    mode match {
      case _: CascadingLocal => // Local or Test mode
        val inMemoryDest = new typed.MemorySink[T]
        /**
         * This is a bit tricky. readResults has to be called after the job has
         * run, so we need to do this inside the function which will
         * be called after the job has run
         */
        (inMemoryDest, () => TypedPipe.from(inMemoryDest.readResults), None)
      case _: HadoopMode =>
        val temporaryPath: String = {
          val tmpDir = conf.get("hadoop.tmp.dir")
            .orElse(conf.get("cascading.tmp.dir"))
            .getOrElse("/tmp")

          tmpDir + "/scalding/snapshot-" + uuid + ".seq"
        }
        val cleanup = Some(temporaryPath)
        val srcSink = source.TypedSequenceFile[T](temporaryPath)
        (srcSink, () => TypedPipe.from(srcSink), cleanup)
    }
}
