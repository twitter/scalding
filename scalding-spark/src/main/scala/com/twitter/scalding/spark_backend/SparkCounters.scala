package com.twitter.scalding.spark_backend

import com.twitter.scalding.{ExecutionCounters, StatKey}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MutableMap}

object SparkCounters {
  def withRandomContextPrefix(sparkSession: SparkSession): SparkCounters =
    new SparkCounters(sparkSession, java.util.UUID.randomUUID.toString)
}

/**
 * We use a new SparkCounters instance for each SparkWriter to keep track of scalding counters
 */
class SparkCounters(sparkSession: SparkSession, counterPrefix: String) {

  val accumulator: SparkCountersInternal = new SparkCountersInternal

  /**
   * Register for spark to be aware of accumulator. Note that accumulators are now automatically garbage
   * collected: https://github.com/apache/spark/pull/4021 so we don't need to explicitly release them.
   */
  sparkSession.sparkContext.register(accumulator, counterPrefix)

  /**
   * We return an ExecutionCounters interface. The results will not be stable unless spark execution has
   * finished.
   */
  def asExecutionCounters(): ExecutionCounters = {
    val copyState = MutableMap[StatKey, Long]()
    for (kvPair <- accumulator.value) {
      val keyEntry = StatKey(kvPair._1._2, kvPair._1._1)
      copyState(keyEntry) = kvPair._2
    }
    val immutableCopyState = copyState.toMap
    new ExecutionCounters {
      def keys = immutableCopyState.keySet
      def get(key: StatKey) = immutableCopyState.get(key)
    }
  }

}

sealed class SparkCountersInternal(
    mapState: MutableMap[(String, String), Long] = MutableMap[(String, String), Long]()
) extends AccumulatorV2[((String, String), Long), MutableMap[(String, String), Long]]
    with Serializable {

  /**
   * Should be used inside of spark executor actions for a side-effect free counter implementation.
   */
  def add(in: Iterator[((String, String), Long)]): Unit =
    while (in.hasNext) {
      val count = in.next()
      val prev = mapState.get(count._1) match {
        case Some(v) => v
        case None    => 0L
      }
      mapState(count._1) = prev + count._2
    }

  // internal for spark do not call
  override def add(in: ((String, String), Long)): Unit = {
    val prev = mapState.get(in._1) match {
      case Some(v) => v
      case None    => 0L
    }
    mapState(in._1) = prev + in._2
  }

  // internal for spark do not call
  override def value() = mapState

  // internal for spark do not call
  override def copy(): SparkCountersInternal = {
    val copyState = MutableMap[(String, String), Long]()
    copyState ++= mapState
    new SparkCountersInternal(copyState)
  }

  // internal for spark do not call
  override def merge(other: AccumulatorV2[((String, String), Long), MutableMap[(String, String), Long]]) =
    for (kvPair <- other.value) {
      val prev = mapState.get(kvPair._1) match {
        case Some(v) => v
        case None    => 0L
      }
      mapState(kvPair._1) = prev + kvPair._2
    }

  // internal for spark do not call
  override def isZero(): Boolean = mapState.isEmpty

  // internal for spark do not call
  override def reset(): Unit = mapState.clear()

}
