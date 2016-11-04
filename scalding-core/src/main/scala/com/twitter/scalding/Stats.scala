package com.twitter.scalding

import java.lang.reflect.Constructor

import cascading.flow.{ Flow, FlowDef, FlowListener, FlowProcess }
import cascading.stats.CascadingStats
import java.util.concurrent.ConcurrentHashMap

import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.ref.WeakReference
import scala.util.Try
import scala.reflect.runtime.universe._

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

case class StatKey(counter: String, group: String) extends java.io.Serializable

object StatKey {
  // This is implicit to allow Stat("c", "g") to work.
  implicit def fromCounterGroup(counterGroup: (String, String)): StatKey = counterGroup match {
    case (c, g) => StatKey(c, g)
  }
  // Create a Stat in the ScaldingGroup
  implicit def fromCounterDefaultGroup(counter: String): StatKey =
    StatKey(counter, Stats.ScaldingGroup)
  implicit def fromStat(stat: Stat): StatKey = stat.key
}

private[scalding] object CounterImpl {
  val CounterImplClass = "scalding.stats.counter.impl.class"

  /*
  Note: knowing which counter implementation class to use is a fabric-dependent question. Prior to Cascading 3, the
  question of "which fabric" was easy: it was either Local or Hadoop, and both types were available wherever Scalding
  is loaded.

  We cannot have access to the fabric's Mode / ExecutionMode implementation, because instantiating counters happens
  at clusterside runtime, not planning time[*]. The FlowProcess[_] detailed type typically also depends on the fabric,
  with exceptions: Legacy Hadoop (1.x) and Hadoop2-MR1 use the same FlowProcess implementation class name,
  HadoopFlowProcess.
  Anyway, we can't use pattern-matching on the specific FlowProcess[_] type, as this would require scalding-core to
  know about specific fabric implementation details, which it can no longer do (post-Cascading 3).

  Solution found for now:
    1. the fabric-specific ExecutionMode is responsible for setting the class name of the appropriate counter
    implementation in a config key (named CounterImplClass above)
    2. clusterside, CounterImpl$ retrieves this class name and uses reflection to instanciate the correct counter
    implementation.


  [*] even if Mode was accessible at runtime, it'd require a pervasive addition of (possibly implicit) parameters
    whenever a join between pipes is possible, which would cause significant changes to the API

   */
  def apply(fp: FlowProcess[_], statKey: StatKey): CounterImpl = {
    /* TODO: comment this stuff. */
    val klassName = Option(fp.getStringProperty(CounterImplClass)).getOrElse(sys.error(CounterImplClass + " property is missing"))

    val memo = scala.collection.mutable.Map[String, Constructor[CounterImpl]]()
    val ctor = memo.synchronized {
      memo.getOrElse(klassName,
        {
          val klass = Class.forName(klassName).asInstanceOf[Class[CounterImpl]]
          val ctor = klass.getConstructor(classOf[FlowProcess[_]], classOf[StatKey])
          memo.put(klassName, ctor)
          ctor
        })
    }
    ctor.newInstance(fp, statKey)
  }

  private[scalding] def upcast[T <: FlowProcess[_]](fp: FlowProcess[_])(implicit ev: TypeTag[T]): T = fp match {
    case hfp: T @unchecked if (ev == typeTag[T]) => hfp // see below
    case _ => throw new IllegalArgumentException(s"Provided flow process instance ${fp} should have been of type ${ev}")

    /* note: we jump through the additioanl hoop of passing implicit ev:TypeTag[T] and verifying the equality as
       due to the JVM's type erasure, we can't tell the difference between a T and a FlowProcess[_] at runtime. */
  }

}

private[scalding] trait CounterImpl {
  def increment(amount: Long): Unit
}

private[scalding] case class GenericFlowPCounterImpl(fp: FlowProcess[_], statKey: StatKey) extends CounterImpl {
  /* Note: most other CounterImpl implementations will need to provide an alternate constructor matching the above
  signature. Suggested definition:

  private[scalding] case class MyFlowPCounterImpl(fp: MyFlowProcess, statKey: StatKey) extends CounterImpl {
    def this(fp: FlowProcess[_], statKey: StatKey) { // this alternate ctor is the one that will actually be used at runtime
      this(CounterImpl.upcast[MyFlowProcess](fp), statKey)
    }
    override def increment(amount: Long): Unit = ???
  }

   */

  override def increment(amount: Long): Unit = fp.increment(statKey.group, statKey.counter, amount)
}

object Stat {

  def apply(k: StatKey)(implicit uid: UniqueID): Stat = new Stat {
    // This is materialized on the mappers, and will throw an exception if users incBy before then
    private[this] lazy val cntr = CounterImpl(RuntimeStats.getFlowProcessForUniqueId(uid), k)

    def incBy(amount: Long): Unit = cntr.increment(amount)
    def key: StatKey = k
  }
}

object Stats {
  // This is the group that we assign all custom counters to
  val ScaldingGroup = "Scalding Custom"

  // When getting a counter value, cascadeStats takes precedence (if set) and
  // flowStats is used after that. Returns None if neither is defined.
  def getCounterValue(key: StatKey)(implicit cascadingStats: CascadingStats[_]): Long =
    cascadingStats.getCounterValue(key.group, key.counter)

  // Returns a map of all custom counter names and their counts.
  def getAllCustomCounters()(implicit cascadingStats: CascadingStats[_]): Map[String, Long] =
    cascadingStats.getCountersFor(ScaldingGroup)
      .asScala
      .map { counter =>
        val value = getCounterValue(counter)
        (counter, value)
      }
      .toMap
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

  private val flowMappingStore: mutable.Map[String, WeakReference[FlowProcess[_]]] = {
    (new ConcurrentHashMap[String, WeakReference[FlowProcess[_]]]).asScala
  }

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

  private[this] var prevFP: FlowProcess[_] = null
  def addFlowProcess(fp: FlowProcess[_]): Unit = {
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
      flowProcess.keepAlive()
    }
  }
}

/**
 * FlowListener that checks counter values against a function.
 */
class StatsFlowListener(f: Map[StatKey, Long] => Try[Unit]) extends FlowListener {

  private var success = true

  override def onCompleted(flow: Flow[_]): Unit = {
    if (success) {
      val stats = flow.getFlowStats
      val keys = stats.getCounterGroups.asScala.flatMap(g => stats.getCountersFor(g).asScala.map(c => StatKey(c, g)))
      val values = keys.map(k => (k, stats.getCounterValue(k.group, k.counter))).toMap
      f(values).get
    }
  }

  override def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = {
    success = false
    false
  }

  override def onStarting(flow: Flow[_]): Unit = {}

  override def onStopping(flow: Flow[_]): Unit = {}

}
