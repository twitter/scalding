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
object SparkCounters {

  var accumulator: SparkCountersInternal = null

  /**
   * This should be called once prior to starting execution on the spark backend. The spark context must
   * already be active in the JVM.
   */
  def register(): Unit = {
    val sparkContext = SparkSession.getActiveSession
      .getOrElse(throw new IllegalStateException("SparkSession is not active!"))
      .sparkContext
    val accum = new SparkCountersInternal
    sparkContext.register(accum, "[scalding/spark-backend] accumulator")
    accumulator = accum
  }

  /**
   * Adds a long value to a (counterGroup, counterName). Should be used inside of executors for side-effect
   * free counter implementation
   */
  def addToCounter(counterGroup: String, counterName: String, diffVal: Long): Unit =
    getAccumulator.add(counterGroup, counterName, diffVal)

  /**
   * we return an ExecutionCounters interface.
   *
   * Values returned from keys/get will not be stable until spark execution is complete
   */
  def lazyEvaluateAsExecutionCounters(): ExecutionCounters =
    new ExecutionCounters {
      def keys =
        getAccumulator.value.foldLeft(Set[StatKey]())((ssk, kvPair) =>
          ssk + StatKey(kvPair._1._1, kvPair._1._2)
        )
      def get(key: StatKey) =
        getAccumulator.value.get((key.group, key.counter))
    }

  private def getAccumulator(): SparkCountersInternal = {
    if (accumulator == null) {
      throw new IllegalStateException("SparkCounters.register has not been called!")
    }
    accumulator
  }

}

sealed class SparkCountersInternal(
    val mapState: MutableMap[(String, String), Long] = MutableMap[(String, String), Long]()
) extends AccumulatorV2[(String, String, Long), MutableMap[(String, String), Long]]
    with Serializable {

  // internal for spark do not call
  override def add(in: (String, String, Long)): Unit = {
    val counterGroup = in._1
    val counterName = in._2
    val counterDiff = in._3
    mapState.getOrElseUpdate((counterGroup, counterName), 0)
    mapState((counterGroup, counterName)) += counterDiff
  }

  // internal for spark do not call
  override def value() = mapState

  // internal for spark do not call
  override def copy(): SparkCountersInternal = {
    val copyState = mapState.foldLeft(MutableMap[(String, String), Long]()) {
      (newMap: MutableMap[(String, String), Long], kvPair) =>
        newMap.getOrElseUpdate(kvPair._1, 0)
        newMap(kvPair._1) += kvPair._2
        newMap
    }
    new SparkCountersInternal(copyState)
  }

  // internal for spark do not call
  override def merge(other: AccumulatorV2[(String, String, Long), MutableMap[(String, String), Long]]) =
    for (kvPair <- other.value) {
      mapState.getOrElseUpdate(kvPair._1, 0)
      mapState(kvPair._1) += kvPair._2
    }

  // internal for spark do not call
  override def isZero(): Boolean = mapState.isEmpty

  // internal for spark do not call
  override def reset(): Unit = mapState.clear()

}
