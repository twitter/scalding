package com.twitter.scalding.beam_backend

import com.stripe.dagon.Rule
import com.twitter.scalding.Execution.{ToWrite, Writer}
import com.twitter.scalding.typed._
import com.twitter.scalding.{CFuture, CancellationHandler, Config, Execution, ExecutionCounters}
import java.util.concurrent.atomic.AtomicLong
import org.apache.beam.sdk.Pipeline
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

class BeamWriter(val beamMode: BeamMode) extends Writer {
  private val state = new AtomicLong()

  override def start(): Unit = ()

  override def finished(): Unit = ()

  def getForced[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[TypedPipe[T]] = ???

  def getIterable[T](conf: Config, initial: TypedPipe[T])(implicit
      cec: ExecutionContext
  ): Future[Iterable[T]] = ???

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
                  ssink.write(pipeline, conf, pcoll)
                case _ => throw new Exception(s"unknown sink: $sink when writing $pipe")
              }
              rec(xs)
            }
            //TODO: handle Force and ToIterable
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
