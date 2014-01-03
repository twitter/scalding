package com.twitter.scalding

import cascading.stats.CascadeStats
import cascading.flow.FlowProcess
import cascading.stats.FlowStats

/**
 * Wrapper around a FlowProcess useful, for e.g. incrementing counters.
 */
object Stats extends java.io.Serializable {
  private var flowProcess : Option[FlowProcess[_]] = None
  private var flowStats: Option[FlowStats] = None
  private var cascadeStats: Option[CascadeStats] = None

  def setFlowProcess(fp: FlowProcess[_]) = { flowProcess = Some(fp) }

  def setFlowStats(fs: FlowStats) = { flowStats = Some(fs) }

  def setCascadeStats(cs: CascadeStats) = { cascadeStats = Some(cs) }

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(group: String, counter: String): Option[Long] = {
    (cascadeStats, flowStats) match {
      case (Some(cs), _) => Some(cs.getCounterValue(group, counter))
      case (_, Some(fs)) => Some(fs.getCounterValue(group, counter))
      case _ => None
    }
  }

  def incrementCounter(group: String, counter: String, amount: Long): Unit =
    flowProcess.foreach { _.increment(group, counter, amount) }

  // Use this if a map or reduce phase takes a while before emitting tuples.
  def keepAlive(): Unit = flowProcess.foreach{ _.keepAlive() }
}
