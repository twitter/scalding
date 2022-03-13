package com.twitter.scalding

import cascading.flow.{Flow, FlowDef, FlowListener, FlowProcess}
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.stats.CascadingStats
import java.util.concurrent.ConcurrentHashMap
import org.apache.hadoop.mapreduce.Counter
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.ref.WeakReference
import scala.util.Try

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
  def inc(): Unit = incBy(1L)

  /** increment by -1L (decrement) */
  def dec(): Unit = incBy(-1L)
  def key: StatKey
}


private[scalding] object CounterImpl {
  def apply(fp: FlowProcess[_], statKey: StatKey): CounterImpl =
    fp match {
      case hFP: HadoopFlowProcess => HadoopFlowPCounterImpl(hFP, statKey)
      case _                      => GenericFlowPCounterImpl(fp, statKey)
    }
}

sealed private[scalding] trait CounterImpl {
  def increment(amount: Long): Unit
}

private[scalding] final case class GenericFlowPCounterImpl(fp: FlowProcess[_], statKey: StatKey)
    extends CounterImpl {
  override def increment(amount: Long): Unit = fp.increment(statKey.group, statKey.counter, amount)
}

private[scalding] final case class HadoopFlowPCounterImpl(fp: HadoopFlowProcess, statKey: StatKey)
    extends CounterImpl {
  // we use a nullable type here for efficiency
  private[this] val counter: Counter = (for {
    r <- Option(fp.getReporter)
    c <- Option(r.getCounter(statKey.group, statKey.counter))
  } yield c).orNull

  def skipNull: Boolean =
    fp.getProperty(Config.SkipNullCounters) match {
      case null  => false // by default don't skip
      case isset => isset.toString.toBoolean
    }

  require(
    (counter != null) || skipNull,
    s"counter for $statKey is null and ${Config.SkipNullCounters} is not set to true"
  )

  override def increment(amount: Long): Unit =
    if (counter != null) counter.increment(amount) else ()
}

object Stat {

  def apply(k: StatKey)(implicit uid: UniqueID): Stat = new Stat {
    // This is materialized on the mappers, and will throw an exception if users incBy before then
    private[this] lazy val cntr = CounterImpl(RuntimeStats.getFlowProcessForUniqueId(uid), k)

    def incBy(amount: Long): Unit = cntr.increment(amount)
    def key: StatKey = k
  }

  implicit def toStatKey(stat: Stat): StatKey = stat.key
}

object Stats {
  // This is the group that we assign all custom counters to
  val ScaldingGroup = StatKey.ScaldingGroup

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(key: StatKey)(implicit cascadingStats: CascadingStats): Long =
    cascadingStats.getCounterValue(key.group, key.counter)

  // Returns a map of all custom counter names and their counts.
  def getAllCustomCounters()(implicit cascadingStats: CascadingStats): Map[String, Long] =
    cascadingStats
      .getCountersFor(ScaldingGroup)
      .asScala
      .map { counter =>
        val value = getCounterValue(counter)
        (counter, value)
      }
      .toMap
}

/**
 * Wrapper around a FlowProcess useful, for e.g. incrementing counters.
 */
object RuntimeStats extends java.io.Serializable {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val flowMappingStore: mutable.Map[String, WeakReference[FlowProcess[_]]] = {
    (new ConcurrentHashMap[String, WeakReference[FlowProcess[_]]]).asScala
  }

  def getFlowProcessForUniqueId(uniqueId: UniqueID): FlowProcess[_] =
    (for {
      weakFlowProcess <- flowMappingStore.get(uniqueId.get)
      flowProcess <- weakFlowProcess.get
    } yield {
      flowProcess
    }).getOrElse {
      logger.debug(
        s"The FlowProcess for unique id $uniqueId isn't available. Returning a NullFlowProcess instead."
      )
      FlowProcess.NULL
    }

  private[this] var prevFP: FlowProcess[_] = null
  def addFlowProcess(fp: FlowProcess[_]): Unit =
    if (!(prevFP eq fp)) {
      val uniqueJobIdObj = fp.getProperty(UniqueID.UNIQUE_JOB_ID)
      if (uniqueJobIdObj != null) {
        // for speed concern, use a while loop instead of foreach here
        var splitted = StringUtility.fastSplit(uniqueJobIdObj.asInstanceOf[String], ",")
        while (!splitted.isEmpty) {
          val uniqueId = splitted.head
          splitted = splitted.tail
          logger.debug("Adding flow process id: " + uniqueId)
          flowMappingStore.put(uniqueId, new WeakReference(fp))
        }
      }
      prevFP = fp
    }

  /**
   * For serialization, you may need to do: val keepAlive = RuntimeStats.getKeepAliveFunction outside of a
   * closure passed to map/etc..., and then call: keepAlive() inside of your closure (mapping, reducing
   * function)
   */
  def getKeepAliveFunction(implicit flowDef: FlowDef): () => Unit = {
    // Don't capture the flowDef, just the id
    val id = UniqueID.fromSystemHashCode(flowDef)
    () => {
      val flowProcess = RuntimeStats.getFlowProcessForUniqueId(id)
      flowProcess.keepAlive()
    }
  }
}

/**
 * FlowListener that checks counter values against a function.
 */
class StatsFlowListener(f: Map[StatKey, Long] => Try[Unit]) extends FlowListener {

  private var success = true

  override def onCompleted(flow: Flow[_]): Unit =
    if (success) {
      val stats = flow.getFlowStats
      val keys =
        stats.getCounterGroups.asScala.flatMap(g => stats.getCountersFor(g).asScala.map(c => StatKey(c, g)))
      val values = keys.map(k => (k, stats.getCounterValue(k.group, k.counter))).toMap
      f(values).get
    }

  override def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = {
    success = false
    false
  }

  override def onStarting(flow: Flow[_]): Unit = {}

  override def onStopping(flow: Flow[_]): Unit = {}

}
