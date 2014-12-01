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
trait Stat extends java.io.Serializable {
  /**
   * increment by the given amount
   */
  def incBy(amount: Long): Unit
  /** increment by 1L */
  def inc: Unit = incBy(1L)
  /** increment by -1L (decrement) */
  def dec: Unit = incBy(-1L)
}

case class StatKey(counter: String, group: String) extends java.io.Serializable

object StatKey {
  // This is implicit to allow Stat("c", "g") to work.
  implicit def fromCounterGroup(counterGroup: (String, String)): StatKey = counterGroup match {
    case (c, g) => StatKey(c, g)
  }
  // Create a Stat in the ScaldingGroup
  implicit def fromCounterDefaultGroup(counter: String): StatKey =
    StatKey(counter, Stats.ScaldingGroup)
}

object Stat {
  def apply(key: StatKey)(implicit uid: UniqueID): Stat = new Stat {
    // This is materialized on the mappers, and will throw an exception if users incBy before then
    private[this] lazy val flowProcess: FlowProcess[_] = RuntimeStats.getFlowProcessForUniqueId(uid)

    def incBy(amount: Long): Unit = flowProcess.increment(key.group, key.counter, amount)
  }
}

object Stats {
  // This is the group that we assign all custom counters to
  val ScaldingGroup = "Scalding Custom"

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(key: StatKey)(implicit cascadingStats: CascadingStats): Long =
    cascadingStats.getCounterValue(key.group, key.counter)

  // Returns a map of all custom counter names and their counts.
  def getAllCustomCounters()(implicit cascadingStats: CascadingStats): Map[String, Long] = {
    val counts = for {
      counter <- cascadingStats.getCountersFor(ScaldingGroup).asScala
      value = getCounterValue(counter)
    } yield (counter, value)
    counts.toMap
  }
}

/**
 * Used to inject a typed unique identifier to uniquely name each scalding flow.
 * This is here mostly to deal with the case of testing where there are many
 * concurrent threads running Flows. Users should never have to worry about
 * these
 */
case class UniqueID(get: String) {
  assert(get.indexOf(',') == -1, "UniqueID cannot contain ,: " + get)
}

object UniqueID {
  val UNIQUE_JOB_ID = "scalding.job.uniqueId"
  private val id = new java.util.concurrent.atomic.AtomicInteger(0)

  def getRandom: UniqueID = {
    // This number is unique as long as we don't create more than 10^6 per milli
    // across separate jobs. which seems very unlikely.
    val unique = (System.currentTimeMillis << 20) ^ (id.getAndIncrement.toLong)
    UniqueID(unique.toString)
  }

  implicit def getIDFor(implicit fd: FlowDef): UniqueID =
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

  def getFlowProcessForUniqueId(uniqueId: UniqueID): FlowProcess[_] = {
    (for {
      weakFlowProcess <- flowMappingStore.get(uniqueId.get)
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
      uniqueJobIdObj.asInstanceOf[String].split(",").foreach { uniqueId =>
        logger.debug("Adding flow process id: " + uniqueId)
        flowMappingStore.put(uniqueId, new WeakReference(fp))
      }
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
    val id = UniqueID.getIDFor(flowDef)
    () => {
      val flowProcess = RuntimeStats.getFlowProcessForUniqueId(id)
      flowProcess.keepAlive
    }
  }
}
