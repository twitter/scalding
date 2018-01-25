package com.twitter.scalding.estimation

import cascading.flow.{ Flow, FlowStep }
import com.twitter.algebird.Monoid
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.util.{ Failure, Success }

case class FlowStrategyInfo(
  flow: Flow[JobConf],
  predecessorSteps: Seq[FlowStep[JobConf]],
  step: FlowStep[JobConf])

/**
 * Trait for estimation some parameters of Job.
 * @tparam T return type of estimation
 */
trait Estimator[T] {
  def estimate(info: FlowStrategyInfo): Option[T]
}

case class FallbackEstimator[T](first: Estimator[T], fallback: Estimator[T]) extends Estimator[T] {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def estimate(info: FlowStrategyInfo): Option[T] = {
    first.estimate(info).orElse {
      LOG.warn(s"$first estimator failed. Falling back to $fallback.")
      fallback.estimate(info)
    }
  }
}

class FallbackEstimatorMonoid[T] extends Monoid[Estimator[T]] {
  override def zero: Estimator[T] = new Estimator[T] {
    override def estimate(info: FlowStrategyInfo): Option[T] = None
  }

  override def plus(l: Estimator[T], r: Estimator[T]): Estimator[T] = FallbackEstimator(l, r)
}

trait HistoryEstimator[T] extends Estimator[T] {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def maxHistoryItems(conf: JobConf): Int

  def historyService: HistoryService

  override def estimate(info: FlowStrategyInfo): Option[T] = {
    val conf = info.step.getConfig

    historyService.fetchHistory(info, maxHistoryItems(conf)) match {
      case Success(history) if history.isEmpty =>
        LOG.warn(s"No matching history found for $info")
        None
      case Success(history) =>
        LOG.info(s"${history.length} history entries found for $info")
        val estimation = estimate(info, conf, history)
        LOG.info(s"$getClass estimate: $estimation")
        estimation
      case Failure(f) =>
        LOG.warn(s"Unable to fetch history in $getClass", f)
        None
    }
  }

  protected def estimate(info: FlowStrategyInfo, conf: JobConf, history: Seq[FlowStepHistory]): Option[T]
}
