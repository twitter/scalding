package com.twitter.scalding.beam_backend

import cascading.flow.FlowDef
import com.stripe.dagon.Rule
import com.twitter.scalding.Execution.{ToWrite, Writer}
import com.twitter.scalding.typed._
import com.twitter.scalding.{CFuture, CancellationHandler, Config, Execution, ExecutionCounters, Mode}
import java.nio.channels.Channels
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.FileSystems
import scala.annotation.tailrec
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

case class TempSource[A](path: String, coder: Coder[A]) extends TypedSource[A] {
  def error = sys.error("beam sources don't work in cascading")
  def converter[U >: A] = error
  def read(implicit flowDef: FlowDef, mode: Mode) = error
}

class BeamWriter(val beamMode: BeamMode) extends Writer {
  private val state = new AtomicLong()

  private val sourceCounter: AtomicLong = new AtomicLong(0L)

  val tempSources: scala.collection.concurrent.Map[TypedPipe[_], TempSource[_]] =
    new ConcurrentHashMap[TypedPipe[_], TempSource[_]]().asScala

  override def start(): Unit = ()

  override def finished(): Unit = {
    // `FileSystems.delete` fails to delete dir as it contains files, hence we delete the files in the dir
    // There is a temp subdir with name starting with "." which is not matched by `match`.
    // So currently a single empty dir is left behind.
    val resources = tempSources.values
      .map(ts => s"${ts.path}*")
      .flatMap(path => FileSystems.`match`(path).metadata().map(_.resourceId()))
    FileSystems.delete(resources.toList)
  }

  def getForced[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[TypedPipe[T]] =
    tempSources.get(initial) match {
      case Some(source) => Future.successful(TypedPipe.from(source).asInstanceOf[TypedPipe[T]])
      case None => Future.failed(new IllegalStateException(s"TypedPipe = $initial, has not yet been forced"))
    }

  def getIterable[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[Iterable[T]] =
    tempSources.get(initial) match {
      case Some(TempSource(path, coder)) =>
        val c: Coder[T] = coder.asInstanceOf[Coder[T]]

        // Single dir by default just matches the dir, we need to match files inside
        val matchedResources = FileSystems.`match`(s"$path*").metadata().asScala
        val records = matchedResources.iterator.flatMap { resource =>
          val is = Channels.newInputStream(FileSystems.open(resource.resourceId()))
          InputStreamIterator.closingIterator(is, c)
        }.toList

        Future(new Iterable[T] {
          override def iterator: Iterator[T] = records.toIterator
        })
      case _ => Future.failed(new IllegalStateException(s"TypedPipe = $initial has no existing Iterable"))
    }

  override def execute(conf: Config, writes: List[ToWrite[_]])(implicit
      cec: ExecutionContext
  ): CFuture[(Long, ExecutionCounters)] = {
    import Execution.ToWrite._
    val planner = BeamPlanner.plan(conf, beamMode.sources)
    val phases: Seq[Rule[TypedPipe]] = BeamPlanner.defaultOptimizationRules(conf)
    val optimizedWrites = ToWrite.optimizeWriteBatch(writes, phases)
    val pipeline = Pipeline.create(beamMode.pipelineOptions)

    @tailrec
    def rec(optimizedWrites: List[OptimizedWrite[TypedPipe, _]]): Unit =
      optimizedWrites match {
        case Nil => ()
        case x :: xs =>
          x match {
            case OptimizedWrite(pipe, ToWrite.SimpleWrite(opt, sink)) => {
              val pcoll = planner(opt).run(pipeline)
              beamMode.sink(sink) match {
                case Some(ssink) =>
                  ssink.write(pcoll, conf)
                case _ => throw new Exception(s"unknown sink: $sink when writing $pipe")
              }
              rec(xs)
            }
            case OptimizedWrite(pipe, toWrite @ (ToIterable(_) | Force(_))) if !tempSources.contains(pipe) =>
              val opt = toWrite match {
                case ToIterable(o) => o
                case Force(o)      => o
              }
              val pcoll = planner(opt).run(pipeline)
              val tempLocation = pcoll.getPipeline.getOptions.getTempLocation
              require(tempLocation != null, "Temp location cannot be null when using toIterableExecution")

              val outputPath = BeamWriter.addPaths(tempLocation, sourceCounter.getAndIncrement().toString)
              // Here we add a sink transformation on the PCollection.
              // This does not run till the final `pipeline.run` step
              new BeamTempFileSink(outputPath).write(pcoll, conf)
              tempSources += ((pipe, TempSource(outputPath, pcoll.getCoder)))

            // we know that tempSources.contains(pipe) on this branch, which means it was already computed.
            case OptimizedWrite(_, ToIterable(_) | Force(_)) => ()
          }
      }
    rec(optimizedWrites)
    val result = pipeline.run
    val runId = state.getAndIncrement()
    CFuture(
      Future {
        result.waitUntilFinish()
        (runId, ExecutionCounters.empty)
      },
      CancellationHandler.fromFn { ec =>
        Future { result.cancel(); () }(ec)
      }
    )
  }
}

object BeamWriter {
  // This is manually done because java.nio.File.Paths & java.io.File convert "gs://" to "gs:/"
  def addPaths(basePath: String, dir: String): String =
    if (basePath.endsWith("/")) s"$basePath$dir/"
    else s"$basePath/$dir/"
}
