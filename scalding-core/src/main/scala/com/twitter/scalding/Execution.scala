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
import scala.util.Try
import cascading.flow.{ FlowDef, Flow, FlowListener }

object Execution {
  /*
   * (FlowDef, Mode) can be thought of as the context needed to
   * do side-effects in scalding (schedule reads or writes)
   * This is here to make it easier to use this context with
   * the Reader monad in algebird
   *
   * import these if you want to use the pair style of implicit
   * needed in the Reader style
   */
  implicit def flowFromPair(implicit fdm: (FlowDef, Mode)): FlowDef = fdm._1
  implicit def modeFromPair(implicit fdm: (FlowDef, Mode)): Mode = fdm._2

  /*
   * Here is the recommended way to run scalding as a library
   * Put all your logic is calls like this:
   * import scalding._ // or flowFromPair and modeFromPair
   *
   * Reader(implicit fdm: (FlowDef, Mode) =>
   *   //job here
   * )
   * you can compose these readers in flatMaps:
   * for {
   *   firstPipe <- job1
   *   secondPipe <- job2
   * } yield firstPipe.group.join(secondPipe.join)
   */
  def buildFlow[T](mode: Mode, conf: Config)(op: Reader[(FlowDef, Mode), T]): (T, Flow[_]) = {
    val newFlowDef = new FlowDef
    // This is ready now, and mutates newFlowDef as a side effect. :(
    try {
      val resultT = op((newFlowDef, mode))
      val flow = mode.newFlowConnector(conf.toMap.toMap).connect(newFlowDef)
      (resultT, flow)
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
  def validate: Reader[(FlowDef, Mode), Try[Unit]] =
    Reader { case (fd, m) => Try(FlowStateMap.validateSources(fd, m)) }

  def run[T](mode: Mode, conf: Config)(op: Reader[(FlowDef, Mode), T]): (T, Future[JobStats]) = {
    val (t, flow) = buildFlow(mode, conf)(op)
    (t, run(flow))
  }

  /*
   * This runs a Flow using Cascading's built in threads. The resulting JobStats
   * are put into a promise when they are ready
   */
  def run[C](flow: Flow[C]): Future[JobStats] = {
    val result = FlowListenerPromise.listen(flow, { f: Flow[C] => JobStats(f.getFlowStats) }).future
    // Now we need to actually start the job flow
    flow.start
    result
  }
}
