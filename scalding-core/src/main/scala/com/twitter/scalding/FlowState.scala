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
import cascading.pipe.Pipe
import cascading.flow.FlowDef
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
/**
 * Immutable state that we attach to the Flow using the FlowStateMap
 */
case class FlowState(flowDef: FlowDef, sourceMap: Map[String, (Source, Pipe)] = Map.empty) {
 /**
  * Cascading can't handle multiple head pipes with the same
  * name.  This handles them by caching the source and only
  * having a single head pipe to represent each head.
  */
  def getReadPipe(s: Source, p: => Pipe) : (FlowState, Pipe) =
    sourceMap.get(s.toString) match {
      case Some((src, pipe)) =>
        if (src.toString == s.toString && (src != s)) {
          // We have seen errors with case class equals, and names so we are paranoid here:
          throw new Exception(
            "Duplicate Source.toString are equal, but values are not.  May result in invalid data: " +
            s.toString)
        }
        (this, pipe)
      case None =>
        val newPipe = p // evaluate the call by name
        (FlowState(flowDef, sourceMap + (s.toString -> (s, newPipe))), newPipe)
    }

  def getSourceNamed(name : String) : Option[Source] =
    sourceMap.get(name).map { _._1 }

  def validateSources(mode: Mode): Unit = {
    flowDef.getSources
      .asInstanceOf[JMap[String,AnyRef]]
      .asScala
      // this is a map of (name, Tap)
      .foreach { nameTap =>
        // Each named source must be present:
        getSourceNamed(nameTap._1)
          .get
          // This can throw a InvalidSourceException
          .validateTaps(mode)
      }
  }
}

/** This is a mutable threadsafe store for attaching scalding
 * information to the mutable flowDef
 *
 * NOTE: there is a subtle bug in scala regarding case classes
 * with multiple sets of arguments, and their equality.
 * For this reason, we use Source.toString as the key in this map
 */
object FlowStateMap {
  import scala.collection.mutable.{Map => MMap}
  protected val flowMap = MMap[FlowDef, FlowState]()

  /** Function to update a state.
   */
  def mutate[T](fd: FlowDef)(fn: FlowState => (FlowState, T)): T = {
    flowMap.synchronized {
      val (newstate, t) = fn(flowMap.getOrElseUpdate(fd, FlowState(fd)))
      flowMap += fd -> newstate
      t
    }
  }
  def get(fd: FlowDef): Option[FlowState] =
    flowMap.synchronized { flowMap.get(fd) }

  def clear(fd: FlowDef): Unit =
    flowMap.synchronized { flowMap -= fd }

  def validateSources(flowDef: FlowDef, mode: Mode): Unit =
    get(flowDef).get.validateSources(mode)
}

