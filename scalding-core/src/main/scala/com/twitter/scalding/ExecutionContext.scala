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

import cascading.flow.{ FlowDef, Flow }
import com.twitter.scalding.reducer_estimation.ReducerEstimatorStepStrategy
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
      val withId = config.addUniqueId(UniqueID.getIDFor(flowDef))
      val flow = mode.newFlowConnector(withId).connect(flowDef)

      // if any reducer estimators have been set, register the step strategy
      // which instantiates and runs them
      mode match {
        case _: HadoopMode =>
          config.get(Config.ReducerEstimators)
            .foreach(_ => flow.setFlowStepStrategy(ReducerEstimatorStepStrategy))
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

