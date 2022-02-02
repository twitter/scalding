package com.twitter.scalding.beam_backend

import com.stripe.dagon.Rule
import com.twitter.scalding.Execution.{ToWrite, Writer}
import com.twitter.scalding.typed._
import com.twitter.scalding.{CFuture, CancellationHandler, Config, Execution, ExecutionCounters}
import java.io.InputStream
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.FileSystems
import scala.annotation.tailrec
import scala.collection.convert.decorateAsScala._
import scala.concurrent.{ExecutionContext, Future}

class BeamWriter(val beamMode: BeamMode) extends Writer {
  private val state = new AtomicLong()

  private val sourceCounter: AtomicLong = new AtomicLong(0L)
  private val iterablePipes: scala.collection.concurrent.Map[TypedPipe[_], (Coder[_], String)] =
    new ConcurrentHashMap[TypedPipe[_], (Coder[_], String)]().asScala

  override def start(): Unit = ()

  override def finished(): Unit = ()

  def getForced[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[TypedPipe[T]] = ???

  def getIterable[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[Iterable[T]] =
    iterablePipes.get(initial) match {
      case Some((coder, path)) =>
        val c: Coder[T] = coder.asInstanceOf[Coder[T]]
        Future(new Iterable[T] {
          // Single dir by default just matches the dir, we need to match files inside
          val matchedResources = FileSystems.`match`(s"$path*").metadata().asScala

          override def iterator: Iterator[T] = {
            // an empty Iterator that closes an InputStream when it is iterated
            def closeIt(is: InputStream): Iterator[T] =
              new Iterator[T] {
                def hasNext: Boolean = {
                  is.close()
                  false
                }
                def next = Iterator.empty.next
              }

            matchedResources.iterator
              .flatMap { resource =>
                val is = Channels.newInputStream(FileSystems.open(resource.resourceId()))
                new InputStreamIterator(is, c) ++ closeIt(is)
              }
          }
        })
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
            case OptimizedWrite(pipe, ToWrite.ToIterable(opt)) =>
              val pcoll = planner(opt).run(pipeline)
              val tempLocation = pcoll.getPipeline.getOptions.getTempLocation
              require(tempLocation != null, "Temp location cannot be null when using toIterableExecution")

              val outputPath = BeamWriter.addPaths(tempLocation, sourceCounter.getAndIncrement().toString)
              // Here we add a sink transformation on the PCollection.
              // This does not run till the final `pipeline.run` step
              new BeamFileIO(outputPath).write(pcoll, conf)
              iterablePipes += ((pipe, (pcoll.getCoder, outputPath)))

            //TODO: handle Force
            case _ => ???
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
