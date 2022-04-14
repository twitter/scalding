package com.twitter.scalding.spark_backend

import com.twitter.scalding.{ExecutionCounters, StatKey}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MutableMap}

/**
 * Spark Counters is a singleton responsible for managing counters in the spark backend
 *
 * Inspiration has been taken from
 * https://stackoverflow.com/questions/66438570/create-accumulator-on-executor-dynamically
 */
class SparkCounters(sparkSession: SparkSession, counterPrefix: String = java.util.UUID.randomUUID.toString) {

  val accumulator: SparkCountersInternal = new SparkCountersInternal

  /**
   * register for spark to be aware of accumulator. Note that accumulators are now automatically garbage
   * collected: https://github.com/apache/spark/pull/4021 so we don't need to explicitly release them
   */
  {
    sparkSession.sparkContext.register(accumulator, counterPrefix)
  }

  /**
   * we return an ExecutionCounters interface.
   *
   * Values returned from keys/get will not be stable until spark execution is complete
   */
  def asExecutionCounters(): ExecutionCounters = {
    val copyState = MutableMap[StatKey, Long]()
    for (kvPair <- accumulator.value) {
      val keyEntry = StatKey(kvPair._1._2, kvPair._1._1)
      copyState.getOrElseUpdate(keyEntry, 0L)
      copyState(keyEntry) += kvPair._2
    }
    new ExecutionCounters {
      def keys = copyState.keys.toSeq.toSet
      def get(key: StatKey) = copyState.get(key)
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
      val count = in.next
      mapState.getOrElseUpdate(count._1, 0L)
      mapState(count._1) += count._2
    }

  // internal for spark do not call
  override def add(in: ((String, String), Long)): Unit = {
    mapState.getOrElseUpdate(in._1, 0L)
    mapState(in._1) += in._2
  }

  // internal for spark do not call
  override def value() = mapState

  // internal for spark do not call
  override def copy(): SparkCountersInternal = {
    val copyState = MutableMap[(String, String), Long]()
    for (kvPair <- mapState) {
      copyState.getOrElseUpdate(kvPair._1, kvPair._2)
    }
    new SparkCountersInternal(copyState)
  }

  // internal for spark do not call
  override def merge(other: AccumulatorV2[((String, String), Long), MutableMap[(String, String), Long]]) =
    for (kvPair <- other.value) {
      mapState.getOrElseUpdate(kvPair._1, 0L)
      mapState(kvPair._1) += kvPair._2
    }

  // internal for spark do not call
  override def isZero(): Boolean = mapState.isEmpty

  // internal for spark do not call
  override def reset(): Unit = mapState.clear()

}
