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

import cascading.flow.hadoop.HadoopFlow
import cascading.flow.planner.BaseFlowStep
import cascading.flow.{ Flow, FlowDef, FlowStepStrategy }
import cascading.pipe.Pipe
import com.twitter.scalding.estimation.memory.MemoryEstimatorStepStrategy
import com.twitter.scalding.reducer_estimation.ReducerEstimatorStepStrategy
import com.twitter.scalding.serialization.CascadingBinaryComparator
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import org.apache.hadoop.mapred.JobConf
import org.slf4j.{ Logger, LoggerFactory }
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

/*
 * This has all the state needed to build a single flow
 * This is used with the implicit-arg-as-dependency-injection
 * style and with the Reader-as-dependency-injection
 */
trait ExecutionContext {
  def config: Config
  def flowDef: FlowDef
  def mode: CascadingMode

  private def getIdentifierOpt(descriptions: Seq[String]): Option[String] = {
    if (descriptions.nonEmpty) Some(descriptions.distinct.mkString(", ")) else None
  }

  private def updateStepConfigWithDescriptions(step: BaseFlowStep[JobConf]): Unit = {
    val conf = step.getConfig
    getIdentifierOpt(ExecutionContext.getDesc(step)).foreach(descriptionString => {
      conf.set(Config.StepDescriptions, descriptionString)
    })
  }

  /**
   * @return
   *    Success(Some(flow)) -- when everything is right and we can build a flow from flowDef
   *    Success(None)       -- when flowDef doesn't have sinks, even after we applied pending writes
   *    Failure(exception)  -- when it’s impossible to build a flow
   */
  final def buildFlow: Try[Option[Flow[_]]] =
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
      // Set the name:
      def withCounterSuffix(name: String): String =
        config.getScaldingFlowCounterValue match {
          case None => name
          case Some(counter) =>
            s"$name (execution-step $counter)"
        }

      val name: Option[String] = Option(flowDef.getName)
        .orElse(config.getCascadingAppName)
        .orElse(config.getScaldingExecutionId)
        .map(withCounterSuffix(_))

      name.foreach(flowDef.setName)

      // Do the optimization of the typed pipes, and register them
      CascadingBackend.planTypedWrites(flowDef, mode)

      // We can have empty flowDef even after applying pending writers
      if (flowDef.getSinks.isEmpty) {
        Success(None)
      } else {
        // identify the flowDef
        val configWithId = config.addUniqueId(UniqueID.getIDFor(flowDef))
        val flow = mode.newFlowConnector(configWithId).connect(flowDef)

        config.getRequireOrderedSerializationMode.map { mode =>
          // This will throw, but be caught by the outer try if
          // we have groupby/cogroupby not using OrderedSerializations
          CascadingBinaryComparator.checkForOrderedSerialization(flow, mode).get
        }

        flow match {
          case hadoopFlow: HadoopFlow =>
            val flowSteps = hadoopFlow.getFlowSteps.asScala
            flowSteps.foreach {
              case baseFlowStep: BaseFlowStep[JobConf] =>
                updateStepConfigWithDescriptions(baseFlowStep)
            }
          case _ => // descriptions not yet supported in other modes
        }

        // if any reducer estimators have been set, register the step strategy
        // which instantiates and runs them
        mode match {
          case _: HadoopMode =>
            val reducerEstimatorStrategy: Seq[FlowStepStrategy[JobConf]] = config
              .get(Config.ReducerEstimators).toList.map(_ => ReducerEstimatorStepStrategy)
            val memoryEstimatorStrategy: Seq[FlowStepStrategy[JobConf]] = config
              .get(Config.MemoryEstimators).toList.map(_ => MemoryEstimatorStepStrategy)

            val otherStrategies: Seq[FlowStepStrategy[JobConf]] = config.getFlowStepStrategies.map {
              case Success(fn) => fn(mode, configWithId)
              case Failure(e) => throw new Exception("Failed to decode flow step strategy when submitting job", e)
            }

            val optionalFinalStrategy = FlowStepStrategies()
              .sumOption(reducerEstimatorStrategy ++ memoryEstimatorStrategy ++ otherStrategies)

            optionalFinalStrategy.foreach { strategy =>
              flow.setFlowStepStrategy(strategy)
            }

            config.getFlowListeners.foreach {
              case Success(fn) => flow.addListener(fn(mode, configWithId))
              case Failure(e) => throw new Exception("Failed to decode flow listener", e)
            }

            config.getFlowStepListeners.foreach {
              case Success(fn) => flow.addStepListener(fn(mode, configWithId))
              case Failure(e) => new Exception("Failed to decode flow step listener when submitting job", e)
            }
          case _: CascadingLocal =>
            config.getFlowStepStrategies.foreach {
              case Success(fn) => flow.setFlowStepStrategy(fn(mode, configWithId))
              case Failure(e) => throw new Exception("Failed to decode flow step strategy when submitting job", e)
            }

            config.getFlowListeners.foreach {
              case Success(fn) => flow.addListener(fn(mode, configWithId))
              case Failure(e) => throw new Exception("Failed to decode flow listener", e)
            }

            config.getFlowStepListeners.foreach {
              case Success(fn) => flow.addStepListener(fn(mode, configWithId))
              case Failure(e) => new Exception("Failed to decode flow step listener when submitting job", e)
            }

          case _ => ()
        }
        Success(Option(flow))
      }
    } catch {
      case err: Throwable => Failure(err)
    }

  /**
   * Asynchronously execute the plan currently
   * contained in the FlowDef
   */
  final def run: Future[JobStats] =
    buildFlow match {
      case Success(Some(flow)) => Execution.run(flow)
      case Success(None) => Future.successful(JobStats.empty)
      case Failure(err) => Future.failed(err)
    }

  /**
   * Synchronously execute the plan in the FlowDef
   */
  final def waitFor: Try[JobStats] =
    buildFlow.flatMap {
      case Some(flow) => Execution.waitFor(flow)
      case None => Success(JobStats.empty)
    }
}

/*
 * import ExecutionContext._
 * is generally needed to use the ExecutionContext as the single
 * dependency injected. For instance, TypedPipe needs FlowDef and Mode
 * in many cases, so if you have an implicit ExecutionContext, you need
 * modeFromImplicit, etc... below.
 */
object ExecutionContext {
  private val LOG: Logger = LoggerFactory.getLogger(ExecutionContext.getClass)

  private[scalding] def getDesc[T](baseFlowStep: BaseFlowStep[T]): Seq[String] = {
    baseFlowStep.getGraph.vertexSet.asScala.flatMap {
      case pipe: Pipe => RichPipe.getPipeDescriptions(pipe)
      case _ => List() // no descriptions
    }(collection.breakOut)
  }
  /*
   * implicit val ec = ExecutionContext.newContext(config)
   * can be used inside of a Job to get an ExecutionContext if you want
   * to call a function that requires an implicit ExecutionContext
   */
  def newContext(conf: Config)(implicit fd: FlowDef, m: Mode): ExecutionContext =
    new ExecutionContext {
      def config = conf
      def flowDef = fd
      def mode = CascadingMode.cast(m)
    }

  implicit def modeFromContext(implicit ec: ExecutionContext): Mode = ec.mode
  implicit def flowDefFromContext(implicit ec: ExecutionContext): FlowDef = ec.flowDef
}

