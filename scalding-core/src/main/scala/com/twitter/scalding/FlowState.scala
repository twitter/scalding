/*
Copyright 2012 Twitter, Inc.

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

import cascading.flow.FlowDef
import com.twitter.algebird.Monoid
import java.util.WeakHashMap

/**
 * Immutable state that we attach to the Flow using the FlowStateMap
 *
 * There are three kinds of things we want to attach to FlowDefs:
 *
 * 1) which scalding Sources are being read (sourceMap), so we can
 * call validateTaps on each of them before we run (see validateSources)
 *
 * 2) the configuration updates that need to be applied to the Pipe
 * instances in the Typed API (this could be removed by better plumbing in CascadingBackend)
 *
 * 3) The list of TypedPipe writes that have not yet been planned. We want
 * to defer planning as long as possible so the optimizer can see as much
 * as possible of the graph to make the best decisions.
 */
case class FlowState(
  sourceMap: Map[String, Source],
  flowConfigUpdates: Set[(String, String)],
  pendingTypedWrites: List[FlowStateMap.TypedWrite[_]]) {

  def getSourceNamed(name: String): Option[Source] =
    sourceMap.get(name)

  def validateSources(mode: Mode): Unit =
    // This can throw a InvalidSourceException
    sourceMap.values.toSet[Source].foreach(_.validateTaps(mode))

  def merge(that: FlowState): FlowState =
    FlowState(sourceMap = sourceMap ++ that.sourceMap,
      flowConfigUpdates = flowConfigUpdates ++ that.flowConfigUpdates,
      pendingTypedWrites = pendingTypedWrites ::: that.pendingTypedWrites)
}

object FlowState {
  val empty: FlowState = FlowState(Map.empty, Set.empty, Nil)

  def withSource(id: String, s: Source): FlowState =
    FlowState(Map(id -> s), Set.empty, Nil)

  def withConfigSetting(k: String, v: String): FlowState =
    FlowState(Map.empty, Set((k, v)), Nil)

  def withTypedWrite[A](p: TypedPipe[A], s: TypedSink[A], m: Mode): FlowState =
    FlowState(Map.empty, Set.empty, FlowStateMap.TypedWrite(p, s, m) :: Nil)

  implicit val monoid: Monoid[FlowState] =
    Monoid.from(empty)(_.merge(_))
}

/**
 * This is a mutable threadsafe store for attaching scalding
 * information to the mutable flowDef
 *
 * NOTE: there is a subtle bug in scala regarding case classes
 * with multiple sets of arguments, and their equality.
 * For this reason, we use Source.sourceId as the key in this map
 */
private[scalding] object FlowStateMap {
  // Make sure we don't hold FlowState after the FlowDef is gone
  @transient private val flowMap = new WeakHashMap[FlowDef, FlowState]()

  case class TypedWrite[T](pipe: TypedPipe[T], sink: TypedSink[T], mode: Mode)
  /**
   * Function to update a state.
   *
   * note if fn mutates the FlowStateMap, this can easily
   * be incorrect (you can lose a write), any mutation
   * that itself mutates the FlowState is responsible
   * for returning the correct value from fn.
   */
  private def mutate[T](fd: FlowDef)(fn: FlowState => (FlowState, T)): T = {
    flowMap.synchronized {
      val (newState, t) = fn(apply(fd))
      flowMap.put(fd, newState)
      t
    }
  }

  /**
   * Get the FlowState or return FlowState.empty
   */
  def apply(fd: FlowDef): FlowState =
    get(fd).getOrElse(FlowState.empty)

  def get(fd: FlowDef): Option[FlowState] =
    flowMap.synchronized { Option(flowMap.get(fd)) }

  def clear(fd: FlowDef): Unit =
    flowMap.synchronized { flowMap.remove(fd) }

  /**
   * Merge a FlowState into the current one for
   * this FlowDef and return the value before
   * the merge
   */
  def merge(fd: FlowDef, state: FlowState): FlowState =
    mutate(fd) { fs =>
      val newFs = fs.merge(state)
      (newFs, fs)
    }

  /**
   * Remove a set of writes (called by the cascading planner)
   *
   * returns the original
   */
  def removeWrites(fd: FlowDef): FlowState =
    mutate(fd) { fs => (fs.copy(pendingTypedWrites = Nil), fs) }

  def validateSources(flowDef: FlowDef, mode: Mode): Unit =
    /*
     * We don't need to validate if there are no sources, this comes up for
     * cases of no-op jobs
     */
    if (!flowDef.getSources.isEmpty) {
      get(flowDef)
        .getOrElse(sys.error("Could not find a flowState for flowDef: %s".format(flowDef)))
        .validateSources(mode)
    } else ()
}

