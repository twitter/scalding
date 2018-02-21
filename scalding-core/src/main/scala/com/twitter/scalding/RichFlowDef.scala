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

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.twitter.algebird.Monoid
import java.util.{ Map => JMap, List => JList }

/**
 * This is an enrichment-pattern class for cascading.flow.FlowDef.
 * The rule is to never use this class directly in input or return types, but
 * only to add methods to FlowDef.
 */
class RichFlowDef(val fd: FlowDef) {
  // allow .asScala conversions
  import collection.JavaConverters._

  // RichPipe and RichFlowDef implicits
  import Dsl._

  def copy: FlowDef = {
    val newFd = new FlowDef
    newFd.mergeFrom(fd)
    newFd
  }

  /**
   * Merge state from FlowDef excluding Sources/Sinks/Tails (sometimes we don't want both)
   */
  private[scalding] def mergeMiscFrom(o: FlowDef): Unit = {
    // See the cascading code that this string is a "," separated set.
    StringUtility.fastSplit(o.getTags, ",").foreach(fd.addTag)

    mergeLeft(fd.getTraps, o.getTraps)
    mergeLeft(fd.getCheckpoints, o.getCheckpoints)

    appendLeft(fd.getClassPath, o.getClassPath)

    fd.setAssertionLevel(preferLeft(fd.getAssertionLevel, o.getAssertionLevel))
    fd.setName(preferLeft(fd.getName, o.getName))
  }

  private[this] def preferLeft[T](left: T, right: T): T =
    Option(left).getOrElse(right)

  private[this] def mergeLeft[K, V](left: JMap[K, V], right: JMap[K, V]): Unit = {
    right.asScala.foreach {
      case (k, v) =>
        if (!left.containsKey(k)) left.put(k, v)
    }
  }
  private[this] def appendLeft[T](left: JList[T], right: JList[T]): Unit = {
    val existing = left.asScala.toSet
    right.asScala
      .filterNot(existing)
      .foreach(left.add)
  }

  def isEmpty: Boolean = {
    fd.getTraps.isEmpty &&
      fd.getCheckpoints.isEmpty &&
      fd.getSources.isEmpty &&
      fd.getSinks.isEmpty &&
      fd.getTails.isEmpty
  }
  /**
   * Mutate current flow def to add all sources/sinks/etc from given FlowDef
   */
  def mergeFrom(o: FlowDef): Unit = {
    mergeLeft(fd.getSources, o.getSources)
    mergeLeft(fd.getSinks, o.getSinks)
    appendLeft(fd.getTails, o.getTails)

    fd.mergeMiscFrom(o)
    // Merge the FlowState
    val oFS = FlowStateMap(o)
    FlowStateMap.merge(fd, oFS)
  }

  /**
   * find all heads reachable from the tails (as a set of names)
   */
  def heads: Set[Pipe] = fd.getTails.asScala.flatMap(_.getHeads).toSet

  /**
   * New flow def with only sources upstream from tails.
   */
  def withoutUnusedSources: FlowDef = {

    // add taps associated with heads to localFlow
    val filteredSources = fd.getSources.asScala.filterKeys(heads.map(p => p.getName)).asJava

    val newFd = fd.copy
    newFd.getSources.clear()
    newFd.addSources(filteredSources)

    newFd
  }

  /**
   * FlowDef that only includes things upstream from the given Pipe
   */
  def onlyUpstreamFrom(pipe: Pipe): FlowDef = {
    val newFd = new FlowDef
    // don't copy any sources/sinks
    newFd.mergeMiscFrom(fd)

    val sourceTaps = fd.getSources
    val newSrcs = newFd.getSources

    val upipes = pipe.upstreamPipes
    val headNames: Set[String] = upipes
      .filter(_.getPrevious.length == 0) // implies _ is a head
      .map(_.getName)

    headNames
      .foreach { head =>
        // TODO: make sure we handle checkpoints correctly
        if (!newSrcs.containsKey(head)) {
          newFd.addSource(head, sourceTaps.get(head))
        }
      }

    val sinks = fd.getSinks
    if (sinks.containsKey(pipe.getName)) {
      newFd.addTailSink(pipe, sinks.get(pipe.getName))
    }

    // Update the FlowState:
    FlowStateMap.get(fd)
      .foreach { thisFS =>
        /**
         * these are all the sources that are upstream
         * of the pipe in question
         */
        val subFlowState =
          Monoid.sum(
            thisFS
              .sourceMap
              .collect {
                case (name, source) if headNames(name) =>
                  FlowState.withSource(name, source)
              })
        /*
         * We assume all the old config updates need to be
         * done, but this may an over approximation and not
         * be 100% correct. We have been doing it for a while
         * however
         *
         * Note, this method is only used to convert a pipe
         * to a TypedPipe. So, we assume there should be
         * no pending typed writes upstream of this pipe
         * that are relevant to this pipe when brought
         * into the TypedAPI
         */
        val withConfig = thisFS.copy(sourceMap = Map.empty, pendingTypedWrites = Nil)

        /*
         * Note that newFd was just allocated, so it has no
         * FlowState at all, we verify that here to be defensive
         * since this is not performance critical code
         */
        require(FlowStateMap(newFd) == FlowState.empty, s"FlowState is not empty: ${FlowStateMap(newFd)}")

        FlowStateMap.merge(newFd, Monoid.plus(subFlowState, withConfig))
      }

    newFd
  }
}
