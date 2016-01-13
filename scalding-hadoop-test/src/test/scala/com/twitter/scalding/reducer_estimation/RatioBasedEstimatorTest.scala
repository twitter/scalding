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
  def makeHistory(inputHdfsBytesRead: Long, inputHdfsReduceFileBytesRead: Long): FlowStepHistory =
    makeHistory(inputHdfsBytesRead, inputHdfsReduceFileBytesRead, Seq())

  def makeHistory(inputHdfsBytesRead: Long, inputHdfsReduceFileBytesRead: Long, taskRuntimes: Seq[Long]): FlowStepHistory = {
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
      reduceFileBytesRead = inputHdfsReduceFileBytesRead,
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

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should equal (1) // default
        }
        .run
    }

    "not set reducers when error fetching history" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[ErrorHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should equal (1) // default
        }
        .run
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
          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should equal (2)
        }
        .run
    }

    "not set reducers when there is no valid history" in {
      val customConfig = Config.empty.addReducerEstimator(classOf[InvalidHistoryBasedEstimator]) +
        (InputSizeReducerEstimator.BytesPerReducer -> "1k") +
        (RatioBasedEstimator.inputRatioThresholdKey -> 0.10f.toString)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)
          conf.getNumReducers should equal (1) // default
        }
        .run
    }
  }
}
