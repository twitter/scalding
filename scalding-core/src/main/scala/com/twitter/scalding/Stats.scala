package com.twitter.scalding

import cascading.flow.{ FlowDef, FlowProcess }
import cascading.stats.CascadingStats
import java.util.{ Collections, WeakHashMap }
import org.slf4j.{ Logger, LoggerFactory }
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.ref.WeakReference

/*
 * This can be a bit tricky to use, but it is important that incBy and inc
 * are called INSIDE any map or reduce functions.
 * Like:
 * val stat = Stat("test")
 * .map { x =>
 *    stat.inc
 *    2 * x
 * }
 * NOT: map( { stat.inc; { x => 2*x } } )
 * which increments on the submitter before creating the function. See the difference?
 */
case class Stat(name: String, group: String = Stats.ScaldingGroup)(@transient implicit val flowDef: FlowDef) {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val uniqueId = UniqueID.getIDFor(flowDef).get
  lazy val flowProcess: FlowProcess[_] = RuntimeStats.getFlowProcessForUniqueId(uniqueId)

  def incBy(amount: Long): Unit = flowProcess.increment(group, name, amount)

  def inc: Unit = incBy(1L)
}

/**
 * Used to inject a typed unique identifier to uniquely name each scalding flow.
 * This is here mostly to deal with the case of testing where there are many
 * concurrent threads running Flows. Users should never have to worry about
 * these
 */
case class UniqueID(get: String)

object UniqueID {
  val UNIQUE_JOB_ID = "scalding.job.uniqueId"
  def getIDFor(fd: FlowDef): UniqueID =
    /*
     * In real deploys, this can even be a constant, but for testing
     * we need to allocate unique IDs to prevent different jobs running
     * at the same time from touching each other's counters.
     */
    UniqueID(System.identityHashCode(fd).toString)
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
    val uniqueJobIdObj = fp.getProperty(UniqueID.UNIQUE_JOB_ID)
    if (uniqueJobIdObj != null) {
      val uniqueId = uniqueJobIdObj.asInstanceOf[String]
      logger.debug("Adding flow process id: " + uniqueId)
      flowMappingStore.put(uniqueId, new WeakReference(fp))
    }
  }

  /**
   * For serialization, you may need to do:
   * val keepAlive = RuntimeStats.getKeepAliveFunction
   * outside of a closure passed to map/etc..., and then call:
   * keepAlive()
   * inside of your closure (mapping, reducing function)
   */
  def getKeepAliveFunction(implicit flowDef: FlowDef): () => Unit = {
    // Don't capture the flowDef, just the id
    val id = UniqueID.getIDFor(flowDef).get
    () => {
      val flowProcess = RuntimeStats.getFlowProcessForUniqueId(id)
      flowProcess.keepAlive
    }
  }
}

object Stats {
  // This is the group that we assign all custom counters to
  val ScaldingGroup = "Scalding Custom"

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(counter: String, group: String = ScaldingGroup)(implicit cascadingStats: CascadingStats): Long =
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
