package com.twitter.scalding.typed.cascading_backend

import cascading.flow.{ FlowDef, Flow }
import com.twitter.scalding.{
  CascadingLocal,
  Config,
  Execution,
  ExecutionContext,
  ExecutionCounters,
  FlowStateMap,
  FutureCache,
  HadoopMode,
  JobStats,
  Mode
}
import com.twitter.scalding.cascading_interop.FlowListenerPromise
import java.util.concurrent.LinkedBlockingQueue
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.concurrent.{ Await, Future, ExecutionContext => ConcurrentExecutionContext, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

object AsyncFlowDefRunner {
  /**
   * We send messages from other threads into the submit thread here
   */
  private sealed trait FlowDefAction
  private case class RunFlowDef(conf: Config,
    mode: Mode,
    fd: FlowDef,
    result: Promise[(Long, JobStats)]) extends FlowDefAction
  private case object Stop extends FlowDefAction

  /**
   * This is a Thread used as a shutdown hook to clean up temporary files created by some Execution
   *
   * If the job is aborted the shutdown hook may not run and the temporary files will not get cleaned up
   */
  case class TempFileCleanup(filesToCleanup: List[String], mode: Mode) extends Thread {

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
 * This holds an internal thread to submit run
 * a Config, Mode, FlowDef and return a Future holding the
 * JobStats
 */
class AsyncFlowDefRunner { self =>
  import AsyncFlowDefRunner._

  private[this] val filesToCleanup = mutable.Set[String]()
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
            val ctx = ExecutionContext.newContext(conf)(fd, mode)
            ctx.buildFlow match {
              case Success(flow) =>
                val future = FlowListenerPromise
                  .start(flow, { f: Flow[_] => (id, JobStats(f.getFlowStats)) })

                promise.completeWith(future)
              case Failure(err) =>
                promise.failure(err)
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
  def finished(mode: Mode): Unit = {
    messageQueue.put(Stop)
    // get an immutable copy
    val cleanUp = filesToCleanup.synchronized { filesToCleanup.toList }
    if (cleanUp.nonEmpty) {
      Runtime.getRuntime.addShutdownHook(TempFileCleanup(cleanUp, mode))
    }
  }

  def addFilesToCleanup(files: TraversableOnce[String]): Unit =
    filesToCleanup.synchronized {
      filesToCleanup ++= files
    }

  private def toFuture[T](t: Try[T]): Future[T] =
    t match {
      case Success(s) => Future.successful(s)
      case Failure(err) => Future.failed(err)
    }

  /**
   * This evaluates the fn in a Try, validates the sources
   * calls runFlowDef, then clears the FlowStateMap
   */
  def validateAndRun(conf: Config, mode: Mode)(
    fn: (Config, Mode) => FlowDef)(
      implicit cec: ConcurrentExecutionContext): Future[Map[Long, ExecutionCounters]] =
    for {
      flowDef <- toFuture(Try(fn(conf, mode)))
      _ = FlowStateMap.validateSources(flowDef, mode)
      (id, jobStats) <- runFlowDef(conf, mode, flowDef)
      _ = FlowStateMap.clear(flowDef)
    } yield Map(id -> ExecutionCounters.fromJobStats(jobStats))
}
