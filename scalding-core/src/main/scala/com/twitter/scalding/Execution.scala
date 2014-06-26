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
import com.twitter.scalding.cascading_interop.FlowListenerPromise

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }
import cascading.flow.{ FlowDef, Flow, FlowListener }

import java.util.UUID

/*
 * This has all the state needed to build a single flow
 * This is used with the implicit-arg-as-dependency-injection
 * style and with the Reader-as-dependency-injection
 */
trait ExecutionContext {
  def mode: Mode
  def flowDef: FlowDef
  def uniqueId: UniqueID
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
   * implicit val ec = ExecutionContext.newContext
   * can be used inside of a Job to get an ExecutionContext if you want
   * to call a function that requires an implicit ExecutionContext
   */
  def newContext(implicit fd: FlowDef, m: Mode, u: UniqueID): ExecutionContext =
    new ExecutionContext {
      def mode = m
      def flowDef = fd
      def uniqueId = u
    }
  implicit def modeFromContext(implicit ec: ExecutionContext): Mode = ec.mode
  implicit def flowDefFromContext(implicit ec: ExecutionContext): FlowDef = ec.flowDef
  implicit def uniqueIdFromContext(implicit ec: ExecutionContext): UniqueID = ec.uniqueId
}

object Execution {
  /*
   * Here is the recommended way to run scalding as a library
   * Put all your logic is calls like this:
   * import ExecutionContext._
   *
   * Reader(implicit ec: ExecutionContext =>
   *   //job here
   * )
   * you can compose these readers in flatMaps:
   * for {
   *   firstPipe <- job1
   *   secondPipe <- job2
   * } yield firstPipe.group.join(secondPipe.join)
   *
   * Note that the only config considered is in conf.
   * The caller is responsible for setting up the Config
   * completely
   */
  def buildFlow[T](mode: Mode, conf: Config)(op: Reader[ExecutionContext, T]): (T, Try[Flow[_]]) = {
    val newFlowDef = new FlowDef
    conf.getCascadingAppName.foreach(newFlowDef.setName)
    // Set up the uniqueID, which is used to access to counters
    val uniqueId = UniqueID(UUID.randomUUID.toString)
    val finalConf = conf.setUniqueId(uniqueId)
    val ec = ExecutionContext.newContext(newFlowDef, mode, uniqueId)
    try {
      val resultT = op(ec)

      // The newFlowDef is ready now, and mutates newFlowDef as a side effect. :(
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

      val tryFlow = try {
        val flow = mode.newFlowConnector(finalConf).connect(newFlowDef)
        Success(flow)
      }
      catch {
        case err: Throwable => Failure(err)
      }
      (resultT, tryFlow)
    } finally {
      FlowStateMap.clear(newFlowDef)
    }
  }

  /*
   * If you want scalding to fail if the sources cannot be validated, then
   * use this.
   * Alteratively, in your Reader, call Source.validateTaps(Mode) to
   * control which sources individually need validation
   * Suggested use:
   * for {
   *   result <- job
   *   mightErr <- validate
   * } yield mightErr.map(_ => result)
   */
  def validate: Reader[ExecutionContext, Try[Unit]] =
    Reader { ec => Try(FlowStateMap.validateSources(ec.flowDef, ec.mode)) }

  def run[T](mode: Mode, conf: Config)(op: Reader[ExecutionContext, T]): (T, Future[JobStats]) = {
    val (t, tryFlow) = buildFlow(mode, conf)(op)
    val fut = tryFlow match {
      case Success(flow) => run(flow)
      case Failure(err) => Future.failed(err)
    }
    (t, fut)
  }

  /*
   * This runs a Flow using Cascading's built in threads. The resulting JobStats
   * are put into a promise when they are ready
   */
  def run[C](flow: Flow[C]): Future[JobStats] =
    // This is in Java because of the cascading API's raw types on FlowListener
    FlowListenerPromise.start(flow, { f: Flow[C] => JobStats(f.getFlowStats) })

  def waitFor[T](mode: Mode, conf: Config)(op: Reader[ExecutionContext, T]): (T, Try[JobStats]) = {
    val (t, tryFlow) = buildFlow(mode, conf)(op)
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
}
