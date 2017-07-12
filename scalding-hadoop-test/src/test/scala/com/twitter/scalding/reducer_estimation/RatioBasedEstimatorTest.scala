package com.twitter.scalding.reducer_estimation

import com.twitter.scalding._
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopSharedPlatformTest }
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

class SimpleJobWithNoSetReducers(args: Args, customConfig: Config) extends Job(args) {
  import HipJob._

  override def config = super.config ++ customConfig.toMap.toMap

  TypedPipe.from(inSrc)
    .flatMap(_.split("[^\\w]+"))
    .map(_.toLowerCase -> 1)
    .group
    .sum
    .write(counts)
}

object EmptyHistoryService extends HistoryService {
  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] = Success(Nil)
}

object ErrorHistoryService extends HistoryService {
  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    Failure(new RuntimeException("Failed to fetch job history"))
}

object HistoryServiceWithData {

  // we only care about these two input size fields for RatioBasedEstimator
  def makeHistory(inputHdfsBytesRead: Long, mapOutputBytes: Long): FlowStepHistory =
    makeHistory(inputHdfsBytesRead, mapOutputBytes, Seq())

  def makeHistory(inputHdfsBytesRead: Long, mapOutputBytes: Long, taskRuntimes: Seq[Long]): FlowStepHistory = {
    val random = new scala.util.Random(123)
    val tasks = taskRuntimes.map { time =>
      val startTime = random.nextLong
      Task(
        taskType = "REDUCE",
        status = "SUCCEEDED",
        startTime = startTime,
        finishTime = startTime + time)
    }

    FlowStepHistory(
      keys = null,
      submitTime = 0,
      launchTime = 0L,
      finishTime = 0L,
      totalMaps = 0L,
      totalReduces = 0L,
      finishedMaps = 0L,
      finishedReduces = 0L,
      failedMaps = 0L,
      failedReduces = 0L,
      mapFileBytesRead = 0L,
      mapFileBytesWritten = 0L,
      mapOutputBytes = mapOutputBytes,
      reduceFileBytesRead = 0l,
      hdfsBytesRead = inputHdfsBytesRead,
      hdfsBytesWritten = 0L,
      mapperTimeMillis = 0L,
      reducerTimeMillis = 0L,
      reduceShuffleBytes = 0L,
      cost = 1.1,
      tasks = tasks)
  }

  def inputSize = HipJob.InSrcFileSize
}

abstract class HistoryServiceWithData extends HistoryService

object ValidHistoryService extends HistoryServiceWithData {
  import HistoryServiceWithData._

  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    // past reducer ratio 0.5
    Success(
      Seq(
        makeHistory(10, 1), // below threshold, ignored
        makeHistory(inputSize, inputSize / 2),
        makeHistory(inputSize, inputSize / 2),
        makeHistory(inputSize, inputSize / 2)))
}

object SmallDataExplosionHistoryService extends HistoryServiceWithData {
  import HistoryServiceWithData._

  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] = {
    // huge ratio, but data is still small overall

    val outSize = inputSize * 1000

    Success(
      Seq(
        makeHistory(inputSize, outSize),
        makeHistory(inputSize, outSize),
        makeHistory(inputSize, outSize)))
  }
}

object InvalidHistoryService extends HistoryServiceWithData {
  import HistoryServiceWithData._

  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    // all entries below the 10% threshold for past input size
    Success(
      Seq(
        makeHistory(10, 1),
        makeHistory(10, 1),
        makeHistory(10, 1)))
}

class EmptyHistoryBasedEstimator extends RatioBasedEstimator {
  override val historyService = EmptyHistoryService
}

class ErrorHistoryBasedEstimator extends RatioBasedEstimator {
  override val historyService = ErrorHistoryService
}

class ValidHistoryBasedEstimator extends RatioBasedEstimator {
  override val historyService = ValidHistoryService
}

class SmallDataExplosionHistoryBasedEstimator extends RatioBasedEstimator {
  override val historyService = SmallDataExplosionHistoryService
}

class InvalidHistoryBasedEstimator extends RatioBasedEstimator {
  override val historyService = InvalidHistoryService
}

class RatioBasedReducerEstimatorTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  import HipJob._

  "Single-step job with ratio-based reducer estimator" should {
    "not set reducers when no history is found" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[EmptyHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = steps.head.getConfig
          conf.getNumReduceTasks should equal (1) // default
        }
        .run()
    }

    "not set reducers when error fetching history" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[ErrorHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = steps.head.getConfig
          conf.getNumReduceTasks should equal (1) // default
        }
        .run()
    }

    "set reducers correctly when there is valid history" in {
      val customConfig = Config.empty
        .addReducerEstimator(classOf[ValidHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          // base estimate from input size reducer = 3
          // reducer ratio from history = 0.5
          // final estimate = ceil(3 * 0.5) = 2
          val conf = steps.head.getConfig
          conf.getNumReduceTasks should equal (2)
        }
        .run()
    }

    /*
     * If the InputSizeReducerEstimator decides that less than 1 reducer is necessary, it
     * rounds up to 1. If the RatioBasedEstimator relies on this, it will use the rounded-up
     * value to calculate the number of reducers. In the case of data explosion on a small dataset,
     * you end up with a very large number of reducers because this rounding error is multiplied.
     * This regression test ensures that this is no longer the case.
     *
     * see https://github.com/twitter/scalding/issues/1541 for more details.
     */
    "handle mapper output explosion over small data correctly" in {
      val customConfig = Config.empty
        .addReducerEstimator(classOf[SmallDataExplosionHistoryBasedEstimator]) +
        // set the bytes per reducer to to 500x input size, so that we estimate needing 2 reducers,
        // even though there's a very large explosion in input data size, the data is still pretty small
        (InputSizeReducerEstimator.BytesPerReducer -> (HistoryServiceWithData.inputSize * 500).toString) +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = steps.head.getConfig
          conf.getNumReduceTasks should equal (2) // used to pick 1000 with the rounding error
        }.run()
    }

    "not set reducers when there is no valid history" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InvalidHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = steps.head.getConfig
          conf.getNumReduceTasks should equal (1) // default
        }
        .run()
    }
  }
}
