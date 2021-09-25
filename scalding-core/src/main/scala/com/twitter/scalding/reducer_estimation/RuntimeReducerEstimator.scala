package com.twitter.scalding.reducer_estimation

import com.twitter.scalding.estimation.{ Common, FlowStepHistory, FlowStrategyInfo }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

/**
 * Estimator that uses the input size and a fixed "bytesPerReducer" target.
 *
 * Bytes per reducer can be configured with configuration parameter, defaults to 1 GB.
 */
trait RuntimeEstimationScheme {

  /**
   *  Given a list of times that each reducer took in a certain FlowStep,
   *  aggregates these times into a single estimate of the time that
   *  a "typical" reducer took.
   *  Suggested implementation: mean or median.
   */
  def estimateTaskTime(times: Seq[Double]): Option[Double]

  /**
   *  Given a list of "typical" times observed in a series of jobs of
   *  the same FlowStep, aggregates these times into a single estimate of
   *  the time that a "typical" reducer took in a "typical" job.
   *  Suggested implementation: mean or median.
   */
  def estimateJobTime(times: Seq[Double]): Option[Double]
}

object MedianEstimationScheme extends RuntimeEstimationScheme {
  def estimateJobTime(times: Seq[Double]) = median(times)
  def estimateTaskTime(times: Seq[Double]) = median(times)
}

object MeanEstimationScheme extends RuntimeEstimationScheme {
  def estimateJobTime(times: Seq[Double]) = mean(times)
  def estimateTaskTime(times: Seq[Double]) = mean(times)
}

object RuntimeReducerEstimator {
  import ReducerHistoryEstimator.ReducerRichTask

  val RuntimePerReducer = "scalding.reducer.estimator.runtime.per.reducer"
  val EstimationScheme = "scalding.reducer.estimator.runtime.estimation.scheme"
  val IgnoreInputSize = "scalding.reducer.estimator.runtime.ignore.input.size"

  /** Get the target bytes/reducer from the JobConf */
  def getRuntimePerReducer(conf: JobConf): Long = {
    val default = 10 * 60 * 1000 // 10 mins
    conf.getLong(RuntimePerReducer, default)
  }

  /**
   * Whether to use the median or the mean in the runtime estimation process.
   * Default is median.
   */
  def getRuntimeEstimationScheme(conf: JobConf): RuntimeEstimationScheme = {
    val default = "median"
    conf.get(EstimationScheme, default) match {
      case "mean" => MeanEstimationScheme
      case "median" => MedianEstimationScheme
      case _ =>
        throw new Exception(s"""Value of $EstimationScheme must be "mean", "median", or not specified.""")
    }
  }

  /**
   * Whether to ignore the input size of the data.
   * If true, RuntimeReducerEstimator uses a non-input scaled estimator.
   * If false, RuntimeReducerEstimator uses an input-scaled estimator
   * first, and uses a non-input-scaled estimator as a fallback.
   * Default is false.
   */
  def getRuntimeIgnoreInputSize(conf: JobConf): Boolean = {
    val default = false
    conf.getBoolean(IgnoreInputSize, default)
  }

  def getReduceTimes(history: Seq[FlowStepHistory]): Seq[Seq[Double]] =
    history.map { h =>
      h.tasks
        .filter { t => t.taskType.contains("REDUCE") && t.status.contains("SUCCEEDED") }
        .flatMap { t =>
          t.finishTime.zip(t.startTime)
            .filter {
              case (finishedTime, startTime) =>
                finishedTime > startTime
            }
            .map {
              case (finishedTime, startTime) =>
                (finishedTime - startTime).toDouble
            }
        }
    }
}

trait BasicRuntimeReducerEstimator extends ReducerHistoryEstimator {
  import RuntimeReducerEstimator._

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def runtimeEstimationScheme: RuntimeEstimationScheme

  override protected def estimate(
    info: FlowStrategyInfo,
    conf: JobConf,
    history: Seq[FlowStepHistory]): Option[Int] = {
    val reduceTimes: Seq[Seq[Double]] = getReduceTimes(history)

    LOG.info(
      s"""|
          |History items have the following numbers of tasks:
          | ${history.map(_.tasks.length)},
          |and the following numbers of tasks have valid task histories:
          | ${reduceTimes.map(_.length)}""".stripMargin)

    // total time taken in the step = time per reducer * number of reducers
    val jobTimes: Seq[Option[Double]] = reduceTimes
      .map { xs => runtimeEstimationScheme.estimateTaskTime(xs).map(_ * xs.length) }

    // time per step, averaged over all the steps
    val typicalJobTime: Option[Double] = runtimeEstimationScheme.estimateJobTime(jobTimes.flatten)

    val desiredRuntime: Long = getRuntimePerReducer(info.step.getConfig)

    val estimate = typicalJobTime.map { t: Double => (t / desiredRuntime).ceil.toInt }

    LOG.info(
      s"""
         | - Typical job time: $typicalJobTime
         | - Desired runtime: $desiredRuntime
         | - Estimate: $estimate
      """.stripMargin)

    estimate
  }
}

trait InputScaledRuntimeReducerEstimator extends ReducerHistoryEstimator {
  import RuntimeReducerEstimator._

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def runtimeEstimationScheme: RuntimeEstimationScheme

  override protected def estimate(
    info: FlowStrategyInfo,
    conf: JobConf,
    history: Seq[FlowStepHistory]): Option[Int] = {
    val reduceTimes: Seq[Seq[Double]] = getReduceTimes(history)

    LOG.info(
      s"""|
          |History items have the following numbers of tasks:
          | ${history.map(_.tasks.length)},
          |and the following numbers of tasks have valid task histories:
          | ${reduceTimes.map(_.length)}""".stripMargin)

    // total time taken in the step = time per reducer * number of reducers
    val jobTimes: Seq[Option[Double]] = reduceTimes
      .map { xs => runtimeEstimationScheme.estimateTaskTime(xs).map(_ * xs.length) }

    // time-to-byte ratio for a step = time per reducer * number of reducers / number of bytes
    val timeToByteRatios: Seq[Double] = jobTimes
      .zip {
        history.map(_.hdfsBytesRead)
      }
      .collect { case (Some(time), bytes) => time / bytes }

    // time-to-byte ratio, averaged over all the steps
    val typicalTimeToByteRatio: Option[Double] = runtimeEstimationScheme
      .estimateJobTime(timeToByteRatios)

    val desiredRuntime = getRuntimePerReducer(info.step.getConfig)
    val inputBytes = Common.totalInputSize(info.step)

    if (inputBytes == 0) {
      LOG.warn("Input bytes is zero in current step.")
      None
    } else {
      // numReducers = time-per-byte * numBytes / desiredRuntime
      val estimate = typicalTimeToByteRatio.map { t: Double =>
        (t * inputBytes / desiredRuntime).ceil.toInt
      }

      LOG.info(
        s"""
           | - HDFS bytes read: ${history.map(_.hdfsBytesRead)}
           | - Time-to-byte-ratios: $timeToByteRatios
           | - Typical type-to-byte-ratio: $typicalTimeToByteRatio
           | - Desired runtime: $desiredRuntime
           | - Input bytes: $inputBytes
           | - Estimate: $estimate
        """.stripMargin)
      estimate
    }
  }
}

trait RuntimeReducerEstimator extends ReducerHistoryEstimator {
  override def estimate(info: FlowStrategyInfo): Option[Int] = {
    val estimationScheme = RuntimeReducerEstimator.getRuntimeEstimationScheme(info.step.getConfig)

    val runtimeHistoryService = historyService

    val basicEstimator = new BasicRuntimeReducerEstimator {
      def runtimeEstimationScheme = estimationScheme

      def historyService = runtimeHistoryService
    }

    val combinedEstimator = if (RuntimeReducerEstimator.getRuntimeIgnoreInputSize(info.step.getConfig)) {
      basicEstimator
    } else {
      val inputScaledEstimator = new InputScaledRuntimeReducerEstimator {
        def runtimeEstimationScheme = estimationScheme

        def historyService = runtimeHistoryService
      }
      ReducerEstimatorStepStrategy.estimatorMonoid.plus(inputScaledEstimator, basicEstimator)
    }

    combinedEstimator.estimate(info)
  }

  override protected def estimate(info: FlowStrategyInfo, conf: JobConf, history: Seq[FlowStepHistory]): Option[Int] =
    estimate(info)
}
