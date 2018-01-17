package com.twitter.scalding.estimation.memory

import com.twitter.scalding.Config
import com.twitter.scalding.estimation.{ FlowStepHistory, FlowStrategyInfo, HistoryService, Task }
import com.twitter.scalding.platform.{ HadoopPlatformJobTest, HadoopSharedPlatformTest }
import com.twitter.scalding.reducer_estimation._
import org.apache.hadoop.mapred.JobConf
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._
import scala.util.{ Success, Try }

class MemoryEstimatorTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  "Single-step job with memory estimator" should {
    "without history don't override memory settings" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[EmptySmoothedMemoryEstimator].getName)

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)

          conf.get(Config.MapMemory) shouldBe None
          conf.get(Config.MapJavaOpts) shouldBe None
          conf.get(Config.ReduceMemory) shouldBe None
          conf.get(Config.ReduceJavaOpts) shouldBe None
        }
        .run()
    }

    "run with correct number of memory" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[SmoothedMemoryEstimatorWithData].getName)

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)

          conf.get(Config.MapMemory) shouldBe Some("1536")
          conf.get(Config.MapJavaOpts) shouldBe Some(" -Xmx1228m")
          conf.get(Config.ReduceMemory) shouldBe Some("1536")
          conf.get(Config.ReduceJavaOpts) shouldBe Some(" -Xmx1228m")
        }
        .run()
    }

    "respect cap when estimated memory is above the configured max" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[SmoothedMemoryEstimatorWithMoreThanMaxCap].getName)

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)

          conf.get(Config.MapMemory) shouldBe Some("8192")
          conf.get(Config.MapJavaOpts) shouldBe Some(" -Xmx6553m")
          conf.get(Config.ReduceMemory) shouldBe Some("8192")
          conf.get(Config.ReduceJavaOpts) shouldBe Some(" -Xmx6553m")
        }
        .run()
    }

    "respect cap when estimated memory is below the configured min" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[SmoothedMemoryEstimatorWithLessThanMinCap].getName)

      HadoopPlatformJobTest(new SimpleJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)

          conf.get(Config.MapMemory) shouldBe Some("1024")
          conf.get(Config.MapJavaOpts) shouldBe Some(" -Xmx819m")
          conf.get(Config.ReduceMemory) shouldBe Some("1024")
          conf.get(Config.ReduceJavaOpts) shouldBe Some(" -Xmx819m")
        }
        .run()
    }

    "not set memory when error fetching history" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[ErrorHistoryBasedMemoryEstimator].getName)

      HadoopPlatformJobTest(new SimpleJobWithNoSetReducers(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala
          steps should have size 1

          val conf = Config.fromHadoop(steps.head.getConfig)

          conf.get(Config.MapMemory) shouldBe None
          conf.get(Config.MapJavaOpts) shouldBe None
          conf.get(Config.ReduceMemory) shouldBe None
          conf.get(Config.ReduceJavaOpts) shouldBe None

        }
        .run()
    }
  }

  "Multi-step job with memory estimator" should {
    "run with correct number of memory in each step" in {
      val customConfig = Config.empty +
        (Config.MemoryEstimators -> classOf[SmoothedMemoryEstimatorWithData].getName)

      HadoopPlatformJobTest(new HipJob(_, customConfig), cluster)
        .inspectCompletedFlow { flow =>
          val steps = flow.getFlowSteps.asScala

          val mapsMemory = steps.map(_.getConfig.getInt(Config.MapMemory, 0)).toList
          val mapsJavaOpts = steps.map(_.getConfig.get(Config.MapJavaOpts, "")).toList

          mapsMemory shouldBe List(1536, 0, 1024)
          mapsJavaOpts shouldBe List(" -Xmx1228m", "", " -Xmx819m")

          val reducersMemory = steps.map(_.getConfig.getInt(Config.ReduceMemory, 0)).toList
          val reducersJavaOpts = steps.map(_.getConfig.get(Config.ReduceJavaOpts, "")).toList

          reducersMemory shouldBe List(1536, 0, 1024)
          reducersJavaOpts shouldBe List(" -Xmx1228m", "", " -Xmx819m")
        }
        .run()
    }
  }
}

object EmptyHistoryService extends HistoryService {
  override def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    Success(Seq.empty)
}

class CustomHistoryService(val history: JobConf => Seq[(String, Long)]) extends HistoryService {
  import Utils._

  override def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] = {
    if (info.step.getOrdinal == 1) {
      makeHistory(info.step.getConfig, history)
    } else if (info.step.getOrdinal == 2) {
      Success(Nil)
    } else {
      makeHistory(info.step.getConfig, _ => Seq(
        "MAP" -> 512.megabyte,
        "REDUCE" -> 512.megabyte))
    }
  }

  def makeHistory(conf: JobConf, history: JobConf => Seq[(String, Long)]): Success[Seq[FlowStepHistory]] =
    Success(history(conf).map {
      case (taskType, memory) =>
        val task = Task(
          details = Map(
            Task.TaskType -> taskType),
          counters = Map(
            SmoothedHistoryMemoryEstimator.CommittedHeapBytes -> memory))
        val tasks = Seq(task)
        FlowStepHistory(
          keys = null,
          submitTimeMillis = 0,
          launchTimeMillis = 0L,
          finishTimeMillis = 0L,
          totalMaps = 0L,
          totalReduces = 0L,
          finishedMaps = 0L,
          finishedReduces = 0L,
          failedMaps = 0L,
          failedReduces = 0L,
          mapFileBytesRead = 0L,
          mapFileBytesWritten = 0L,
          mapOutputBytes = 0l,
          reduceFileBytesRead = 0l,
          hdfsBytesRead = 0l,
          hdfsBytesWritten = 0L,
          mapperTimeMillis = 0L,
          reducerTimeMillis = 0L,
          reduceShuffleBytes = 0L,
          cost = 1.1,
          tasks = tasks)
    })
}

class EmptySmoothedMemoryEstimator extends SmoothedHistoryMemoryEstimator {
  override def historyService: HistoryService = EmptyHistoryService
}

class SmoothedMemoryEstimatorWithData extends SmoothedHistoryMemoryEstimator {
  import Utils._

  override def historyService: HistoryService = new CustomHistoryService(_ => Seq(
    "MAP" -> 800.megabytes,
    "REDUCE" -> 800.megabytes,
    "MAP" -> 1024.megabytes,
    "REDUCE" -> 1024.megabytes,
    "MAP" -> 1300.megabytes,
    "REDUCE" -> 1300.megabytes,
    "MAP" -> 723.megabytes,
    "REDUCE" -> 723.megabytes))
}

class SmoothedMemoryEstimatorWithMoreThanMaxCap extends SmoothedHistoryMemoryEstimator {
  import Utils._

  override def historyService: HistoryService = new CustomHistoryService(conf => Seq(
    "MAP" -> (MemoryEstimatorConfig.getMaxContainerMemory(conf).megabyte + 1.gigabyte),
    "REDUCE" -> (MemoryEstimatorConfig.getMaxContainerMemory(conf).megabyte + 1.gigabyte)))
}

class SmoothedMemoryEstimatorWithLessThanMinCap extends SmoothedHistoryMemoryEstimator {
  import Utils._

  override def historyService: HistoryService = new CustomHistoryService(conf => Seq(
    "MAP" -> (MemoryEstimatorConfig.getMinContainerMemory(conf).megabyte - 500.megabyte),
    "REDUCE" -> (MemoryEstimatorConfig.getMinContainerMemory(conf).megabyte - 500.megabyte)))
}

class ErrorHistoryBasedMemoryEstimator extends SmoothedHistoryMemoryEstimator {
  override val historyService = ErrorHistoryService
}

object Utils {
  implicit class StorageUnit(val wrapped: Long) extends AnyVal {
    def fromMegabytes(megabytes: Long): Long = megabytes * 1024 * 1024
    def fromGigabytes(gigabytes: Long): Long = gigabytes * 1024 * 1024 * 1024

    def megabyte: Long = megabytes
    def megabytes: Long = fromMegabytes(wrapped)
    def gigabyte: Long = gigabytes
    def gigabytes: Long = fromGigabytes(wrapped)

    def inMegabytes: Long = wrapped / (1024L * 1024)
  }

  implicit def doubleToLong(value: Double): StorageUnit = new StorageUnit(value.toLong)
}