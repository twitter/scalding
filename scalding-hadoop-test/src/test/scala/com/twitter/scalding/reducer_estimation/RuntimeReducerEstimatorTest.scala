package com.twitter.scalding.reducer_estimation

import com.twitter.scalding._
import com.twitter.scalding.reducer_estimation.RuntimeReducerEstimator.{ RuntimePerReducer, EstimationScheme, IgnoreInputSize }
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopSharedPlatformTest }
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._
import scala.util.{ Success, Try }

object HistoryService1 extends HistoryServiceWithData {
  import HistoryServiceWithData._

  def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    Success(
      Seq(
        makeHistory(inputSize * 2, 0, List(10, 1000, 3000)),
        makeHistory(inputSize / 2, 0, List(10, 200, 400)),
        makeHistory(inputSize * 4, 0, List(10, 2400, 3000))))
}

class Estimator1 extends RuntimeReducerEstimator {
  override val historyService = HistoryService1
}

class EmptyRuntimeEstimator extends RatioBasedEstimator {
  override val historyService = EmptyHistoryService
}

class ErrorRuntimeEstimator extends RatioBasedEstimator {
  override val historyService = ErrorHistoryService
}

class DummyEstimator extends ReducerEstimator {
  def estimateReducers(info: FlowStrategyInfo) = Some(42)
}

class RuntimeReducerEstimatorTest extends WordSpec with Matchers with HadoopSharedPlatformTest {

  "Single-step job with runtime-based reducer estimator" should {
    "set reducers correctly with median estimation scheme" in {
      val config = Config.empty
        .addReducerEstimator(classOf[Estimator1])
        .+(RuntimePerReducer -> "25")
      // + (EstimationScheme -> "median")
      // + (IgnoreInputSize -> false)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig
          // So our histories are (taking median runtimes):
          //
          // 2 * inputSize bytes, 3 reducers * 1000 ms for each reducer
          // inputSize / 2 bytes, 3 reducers * 200 ms for each reducer
          // inputSize * 4 bytes, 3 reducers * 2400 ms for each reducer
          //
          // If we scale by input size, we get:
          //
          // (1500 / inputSize) ms per byte
          // (1200 / inputSize) ms per byte
          // (1800 / inputSize) ms per byte
          //
          // The median of these is (1500 / inputSize) ms per byte,
          // so we anticipate that processing (inputSize bytes)
          // will take 1500 ms total.
          // To do this in 25 ms, we need 60 reducers.
          assert(conf.getNumReduceTasks == 60)
        }
        .run()
    }

    "set reducers correctly with mean estimation scheme" in {
      val config = Config.empty
        .addReducerEstimator(classOf[Estimator1])
        .+(RuntimePerReducer -> "25")
        .+(EstimationScheme -> "mean")
      // + (IgnoreInputSize -> false)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig
          // So our histories are (taking mean runtimes):
          //
          // 2 * inputSize bytes,  3 reducers * 1336.67 ms for each reducer
          // inputSize / 2 bytes,  3 reducer  *  203.33 ms for each reducer
          // inputSize * 4 bytes,  3 reducer  * 1803.33 ms for each reducer
          //
          // If we scale by input size, we get:
          //
          // (2005   / inputSize) ms per byte
          // (1220   / inputSize) ms per byte
          // (1352.5 / inputSize) ms per byte
          //
          // The mean of these is (1525.8 / inputSize) ms per byte,
          // so we anticipate that processing (inputSize bytes)
          // will take 1525.8 ms total.
          //
          // To do this in 25 ms, we need 61.03 reducers, which rounds up to 62.
          assert(conf.getNumReduceTasks == 62)
        }
        .run()
    }

    "set reducers correctly with mean estimation scheme ignoring input size" in {
      val config = Config.empty
        .addReducerEstimator(classOf[Estimator1])
        .+(RuntimePerReducer -> "25")
        .+(EstimationScheme -> "mean")
        .+(IgnoreInputSize -> "true")

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig
          // So our histories are (taking mean runtimes):
          //
          // 2 * inputSize bytes, 3 reducers * 1337 ms for each reducer
          // inputSize / 2 bytes, 3 reducers *  203 ms for each reducer
          // inputSize * 4 bytes, 3 reducers * 1803 ms for each reducer
          //
          // We don't scale by input size.
          //
          // The mean of these is 3342 ms, so we anticipate
          // that the job will take 3342 ms total.
          //
          // To do this in 25 ms, we need 134 reducers.
          assert(conf.getNumReduceTasks == 134)
        }
        .run()
    }

    "set reducers correctly with median estimation scheme ignoring input size" in {
      val config = Config.empty
        .addReducerEstimator(classOf[Estimator1])
        .+(RuntimePerReducer -> "25")
        .+(IgnoreInputSize -> "true")
      // + (EstimationScheme -> "median")

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig
          // So our histories are (taking median runtimes):
          //
          // 2 * inputSize bytes, 3 reducers * 1000 ms for each reducer
          // inputSize / 2 bytes, 3 reducers *  200 ms for each reducer
          // inputSize * 4 bytes, 3 reducers * 2400 ms for each reducer
          //
          // We don't scale by input size.
          //
          // The median of these is 3000 ms, so we anticipate
          // that the job will take 3000 ms total.
          //
          // To do this in 25 ms, we need 120 reducers.
          assert(conf.getNumReduceTasks == 120)
        }
        .run()
    }

    "not set reducers when history service is empty" in {
      val config = Config.empty
        .addReducerEstimator(classOf[EmptyRuntimeEstimator])
        .addReducerEstimator(classOf[DummyEstimator])

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig

          // EmptyRuntimeEstimator should have returned None,
          // so it should have fallen back to DummyEstimator,
          // which returns 42.
          assert(conf.getNumReduceTasks == 42)
        }
    }

    "not set reducers when history service fails" in {
      val config = Config.empty
        .addReducerEstimator(classOf[ErrorRuntimeEstimator])
        .addReducerEstimator(classOf[DummyEstimator])

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, config), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          assert(steps.length == 1)

          val conf = steps.head.getConfig

          // ErrorRuntimeEstimator should have returned None,
          // so it should have fallen back to DummyEstimator,
          // which returns 42.
          assert(conf.getNumReduceTasks == 42)
        }
    }
  }
}
