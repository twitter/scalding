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

package com.twitter.scalding.lingual

import cascading.cascade.CascadeConnector
import cascading.flow.{ Flow, FlowDef }
import cascading.lingual.flow.SQLPlanner
import com.twitter.scalding._
import scala.collection.mutable.ListBuffer
import scala.util.Try

trait LingualJob extends Job {
  import FileSourceExtensions._

  /**
   * Check if the user has set two outputs.  Lingual will pick one and not fail but the user
   *  may not expect this behavior
   */
  private var outputSet = false

  /**
   * If the user created a scalding Pipe in the job the flowdef will have sources set.
   * By checking those we will know if we need to run Scalding or not.
   */
  private def hasScaldingFlow = flowDef.getSources.size() > 0

  /**
   * TODO: Add a cascades validation code here.
   * Should validate sources in both lingual and scalding
   */
  override def validate {}

  @transient
  protected val addedTables = new ListBuffer[LingualTable[_]]

  /**
   * Called once the cascade flows are complete
   */
  protected def cascadeComplete(): Unit = {}

  /**
   * Create a flow to contain the Lingual job.
   */
  @transient
  protected val lingualFlow = {
    val name = getClass.getCanonicalName + "Lingual"
    val fd = new FlowDef
    fd.setName(name)
    fd
  }

  protected def query: String

  private def getFlow(flowToConnect: FlowDef): Try[Flow[_]] =
    Config.tryFrom(config).map { conf =>
      mode.newFlowConnector(conf).connect(flowToConnect)
    }

  override def run = {
    lingualFlow.addAssemblyPlanner(new SQLPlanner().setSql(query))

    val flows = if (hasScaldingFlow) {
      println("Scalding/Lingual Job")
      Seq(getFlow(lingualFlow).get, getFlow(flowDef).get)
    } else {
      println("Lingual Job")
      Seq(getFlow(lingualFlow).get)
    }

    val cascade = new CascadeConnector().connect(flows: _*)
    cascade.complete()

    addedTables.foreach(_.cleanup(mode))
    addedTables.clear()

    cascadeComplete()

    val statsData = cascade.getCascadeStats

    handleStats(statsData)
    statsData.isSuccessful
  }

  def table[A, B](table: FlatMappedLingualTable[A, B]) {
    addedTables += table
    table.addToFlowDef(lingualFlow, mode)
  }

  def table(name: String, f: FileSource) {
    lingualFlow.addSource(name, f.sourceTap(mode))
  }

  def output(f: FileSource) {
    if (outputSet) {
      sys.error("Can only have one output sink with a Lingual Job.")
    } else {
      outputSet = true
      lingualFlow.addSink("output", f.sinkTap(mode))
    }
  }
}