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

import java.util.Properties

import cascading.flow.hadoop.HadoopFlow
import cascading.flow._
import cascading.flow.planner.{ BaseFlowNode, BaseFlowStep }
import cascading.pipe.Pipe
import com.twitter.scalding.reducer_estimation.ReducerEstimatorStepStrategy
import com.twitter.scalding.serialization.CascadingBinaryComparator
import org.apache.hadoop.mapred.JobConf
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import org.slf4j.{ Logger, LoggerFactory }

/*
 * This has all the state needed to build a single flow
 * This is used with the implicit-arg-as-dependency-injection
 * style and with the Reader-as-dependency-injection
 */
trait ExecutionContext {
  def config: Config
  def flowDef: FlowDef
  def mode: Mode

  import ExecutionContext._

  private def getIdentifierOpt(descriptions: Seq[String]): Option[String] = {
    if (descriptions.nonEmpty) Some(descriptions.distinct.mkString(", ")) else None
  }

  private def updateStepConfigWithDescriptions(step: BaseFlowStep[_]): Unit = {
    import ConfigBridge._

    getIdentifierOpt(ExecutionContext.getDesc(step))
      .foreach(descriptionString => step.setConfigValue(Config.StepDescriptions, descriptionString))
  }

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
      // Set the name:
      val name: Option[String] = Option(flowDef.getName)
        .orElse(config.getCascadingAppName)
        .orElse(config.getScaldingExecutionId)

      name.foreach(flowDef.setName)

      // identify the flowDef
      val configWithId = config.addUniqueId(UniqueID.getIDFor(flowDef))
      val flow = mode.newFlowConnector(configWithId).connect(flowDef)
      if (config.getRequireOrderedSerialization) {
        // This will throw, but be caught by the outer try if
        // we have groupby/cogroupby not using OrderedSerializations
        CascadingBinaryComparator.checkForOrderedSerialization(flow).get
      }

      flow match {
        case baseFlow: BaseFlow[_] =>
          val flowSteps = baseFlow.getFlowSteps.asScala
          flowSteps.foreach {
            case baseFlowStep: BaseFlowStep[_] =>
              updateStepConfigWithDescriptions(baseFlowStep)
            case anyOtherBaseFlowStep => throw new NotImplementedError("unknown flowStep type ${anyOtherBaseFlowStep.getClass}")
          }
        case _ => // descriptions not yet supported in other modes
      }

      // if any reducer estimators have been set, register the step strategy
      // which instantiates and runs them
      mode match {
        case _: HadoopMode =>
          val reducerEstimatorStrategy: Seq[FlowStepStrategy[JobConf]] = config.get(Config.ReducerEstimators).toList.map(_ => ReducerEstimatorStepStrategy)

          val otherStrategies: Seq[FlowStepStrategy[JobConf]] = config.getFlowStepStrategies.map {
            case Success(fn) => fn(mode, configWithId)
            case Failure(e) => throw new Exception("Failed to decode flow step strategy when submitting job", e)
          }

          val optionalFinalStrategy = FlowStepStrategies().sumOption(reducerEstimatorStrategy ++ otherStrategies)

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

        case _ => ()
      }

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
  private val LOG: Logger = LoggerFactory.getLogger(ExecutionContext.getClass)

  private[scalding] def getDesc[T](baseFlowStep: BaseFlowStep[T]): Seq[String] = {
    baseFlowStep.getElementGraph.vertexSet.asScala.toSeq.flatMap(_ match {
      case pipe: Pipe => RichPipe.getPipeDescriptions(pipe)
      case _ => List() // no descriptions
    })
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
      def mode = m
    }

  implicit def modeFromContext(implicit ec: ExecutionContext): Mode = ec.mode
  implicit def flowDefFromContext(implicit ec: ExecutionContext): FlowDef = ec.flowDef
}

