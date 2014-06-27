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

/**
 * This is an enrichment-pattern class for cascading.flow.FlowDef.
 * The rule is to never use this class directly in input or return types, but
 * only to add methods to FlowDef.
 */
class RichFlowDef(val fd: FlowDef) {
  import Dsl.flowDefToRichFlowDef

  def copy = {
    val newFd = new FlowDef
    newFd.merge(fd)
    newFd
  }

  /**
   * Mutate current flow def to add all sources/sinks/etc from given FlowDef
   */
  def merge(o: FlowDef): FlowDef = {
    fd.addSources(o.getSources)
    fd.addSinks(o.getSinks)
    fd.addTails(o.getTails)
    fd.addTags(o.getTags)
    fd.addTraps(o.getTraps)
    fd.addCheckpoints(o.getCheckpoints)
    fd.setAssertionLevel(o.getAssertionLevel)
    fd.setName(o.getName)
    fd
  }

  /**
   * New flow def with only sources upstream from tails.
   */
  def withoutUnusedSources = {
    import collection.JavaConverters._

    // find all heads reachable from the tails (as a set of names)
    val heads = fd.getTails.asScala
      .flatMap(_.getHeads).map(_.getName).toSet
    // add taps associated with heads to localFlow
    val filteredSources = fd.getSources.asScala.filterKeys(src => heads.contains(src)).asJava

    val newFd = fd.copy
    newFd.getSources.clear()
    newFd.addSources(filteredSources)

    newFd
  }

}
