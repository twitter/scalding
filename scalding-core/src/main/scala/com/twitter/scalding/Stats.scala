package com.twitter.scalding

import cascading.flow.FlowProcess
import cascading.stats.CascadingStats
import java.util.{Collections, WeakHashMap}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.ref.WeakReference

case class Stat(name: String, group: String = Stats.ScaldingGroup)(@transient implicit val uniqueIdCont: UniqueID) {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val uniqueId = uniqueIdCont.get
  lazy val flowProcess: FlowProcess[_] = RuntimeStats.getFlowProcessForUniqueId(uniqueId)

  def incBy(amount: Long) = flowProcess.increment(group, name, amount)

  def inc = incBy(1L)
}

/**
 * Wrapper around a FlowProcess useful, for e.g. incrementing counters.
 */
object RuntimeStats extends java.io.Serializable {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val flowMappingStore: mutable.Map[String, WeakReference[FlowProcess[_]]] =
    Collections.synchronizedMap(new WeakHashMap[String, WeakReference[FlowProcess[_]]])

  def getFlowProcessForUniqueId(uniqueId: String): FlowProcess[_] = {
    (for {
     weakFlowProcess <- flowMappingStore.get(uniqueId)
     flowProcess <- weakFlowProcess.get
    } yield {
     flowProcess
    }).getOrElse {
     sys.error("Error in job deployment, the FlowProcess for unique id %s isn't available".format(uniqueId))
    }
  }

  def addFlowProcess(fp: FlowProcess[_]) {
    val uniqueJobIdObj = fp.getProperty(Job.UNIQUE_JOB_ID)
    if(uniqueJobIdObj != null) {
      val uniqueId = uniqueJobIdObj.asInstanceOf[String]
      logger.debug("Adding flow process id: " + uniqueId)
      flowMappingStore.put(uniqueId, new WeakReference(fp))
    }
  }
}

object Stats {
  // This is the group that we assign all custom counters to
  val ScaldingGroup = "Scalding Custom"

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(counter: String, group: String = ScaldingGroup)
              (implicit cascadingStats: CascadingStats): Long =
                  cascadingStats.getCounterValue(group, counter)

  // Returns a map of all custom counter names and their counts.
  def getAllCustomCounters()(implicit cascadingStats: CascadingStats): Map[String, Long] = {
    val counts = for {
      counter <- cascadingStats.getCountersFor(ScaldingGroup).asScala
      value = getCounterValue(counter)
    } yield (counter, value)
    counts.toMap
  }
}