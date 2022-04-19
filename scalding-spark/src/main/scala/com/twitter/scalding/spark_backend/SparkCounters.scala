package com.twitter.scalding.spark_backend

import com.twitter.scalding.{ExecutionCounters, StatKey}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.immutable.{Map => ImmutableMap}

object SparkCounters {
  def withRandomContextPrefix(sparkSession: SparkSession): SparkCounters =
    new SparkCounters(sparkSession, java.util.UUID.randomUUID.toString)
}

/**
 * We use a new SparkCounters instance for each SparkWriter to keep track of scalding counters
 */
class SparkCounters(sparkSession: SparkSession, counterPrefix: String) {

  /**
   * This counter is specific to the spark driver. Executors will serialize SparkCountersInternal but it will
   * be a stateless instance obtained through the copyAndReset method. You can read the internal
   * implementation here:
   * https://github.com/apache/spark/blob/87c744b60507f82e1722f1488f1741cb2bb8e8e5/core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala#L165
   */
  @transient private val accumulator: SparkCountersInternal = new SparkCountersInternal

  /**
   * This should only be called inside of SparkWriter execution.
   */
  def getAccumulator: SparkCountersInternal = accumulator

  /**
   * Register for spark to be aware of accumulator. Note that accumulators are now automatically garbage
   * collected: https://github.com/apache/spark/pull/4021 so we don't need to explicitly release them.
   */
  sparkSession.sparkContext.register(accumulator, counterPrefix)

  /**
   * We return an ExecutionCounters interface and reset the counter state. This should only be called at
   * SparkWriter once execution has completely finished.
   */
  def asExecutionCounters(): ExecutionCounters = {
    val copyState = MutableMap[StatKey, Long]()
    accumulator.synchronized {
      for (kvPair <- accumulator.value) {
        val keyEntry = StatKey(kvPair._1._2, kvPair._1._1)
        copyState(keyEntry) = kvPair._2
      }
      accumulator.reset()
    }
    val immutableCopyState = copyState.toMap
    new ExecutionCounters {
      def keys = immutableCopyState.keySet
      def get(key: StatKey) = immutableCopyState.get(key)
    }
  }

}

/**
 * A note about synchronization of counter state: the counter API in spark was originally designed to not
 * require thread safety by merging changes only linearly on the executor after each RDD action.
 *
 * However... it seems like there was an oversight in the design because there have been reported crashes
 * where thread safety is required. Look at the following for reference:
 * https://issues.apache.org/jira/browse/SPARK-17463, https://github.com/apache/spark/pull/15371, and
 * https://github.com/apache/spark/pull/15063
 *
 * Out of abundance of caution until this patch is no longer required, we are following the example pattern
 * for implementations such as CollectionAccumulator:
 * https://github.com/apache/spark/blob/87c744b60507f82e1722f1488f1741cb2bb8e8e5/core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala#L472
 *
 * In that example, all operations (add, value, copy, and merge) are synchronized across threads
 */
sealed class SparkCountersInternal(
    mapState: MutableMap[(String, String), Long] = MutableMap[(String, String), Long]()
) extends AccumulatorV2[((String, String), Long), ImmutableMap[(String, String), Long]]
    with Serializable {

  /**
   * Should be used inside of spark executor actions for a side-effect free counter implementation.
   */
  def add(in: Iterator[((String, String), Long)]): Unit = mapState.synchronized {
    while (in.hasNext) {
      val count = in.next()
      val prev = mapState.get(count._1) match {
        case Some(v) => v
        case None    => 0L
      }
      mapState(count._1) = prev + count._2
    }
  }

  // internal for spark do not call
  override def add(in: ((String, String), Long)): Unit = mapState.synchronized {
    val prev = mapState.get(in._1) match {
      case Some(v) => v
      case None    => 0L
    }
    mapState(in._1) = prev + in._2
  }

  // internal for spark do not call
  override def value() = mapState.synchronized {
    mapState.toMap
  }

  // internal for spark do not call
  override def copy(): SparkCountersInternal = mapState.synchronized {
    val copyState = MutableMap[(String, String), Long]()
    mapState.synchronized(
      copyState ++= mapState
    )
    new SparkCountersInternal(copyState)
  }

  // internal to spark do not call
  override def copyAndReset(): SparkCountersInternal = new SparkCountersInternal

  // internal for spark do not call
  override def merge(other: AccumulatorV2[((String, String), Long), ImmutableMap[(String, String), Long]]) =
    mapState.synchronized {
      for (kvPair <- other.value) {
        val prev = mapState.get(kvPair._1) match {
          case Some(v) => v
          case None    => 0L
        }
        mapState(kvPair._1) = prev + kvPair._2
      }
    }

  // internal for spark do not call
  override def isZero(): Boolean = mapState.synchronized {
    mapState.isEmpty
  }

  // internal for spark do not call
  override def reset(): Unit = mapState.synchronized {
    mapState.clear()
  }

}
