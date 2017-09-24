package com.twitter.scalding.estimation.memory

import cascading.flow.FlowStep
import com.twitter.scalding.estimation.{ FlowStepHistory, FlowStrategyInfo, HistoryService, Task }
import org.apache.hadoop.mapred.JobConf
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{ Matchers, WordSpec }
import scala.util.{ Success, Try }

class SmoothedHistoryMemoryEstimatorTest extends WordSpec with Matchers {
  import Utils._

  "A memory history estimator" should {
    "return None without history" in {
      SmoothedMemoryEstimator.empty.estimate(TestFlowStrategyInfo.dummy) shouldBe None
    }

    "estimate correct numbers for only reducers" in {
      val estimation = SmoothedMemoryEstimator
        .makeHistory(Seq(
          "REDUCE" -> 1024.megabytes))
        .estimate(TestFlowStrategyInfo.dummy)

      estimation shouldBe reduceEstimate((1228, 1536))
    }

    "estimate correct numbers for only mappers" in {
      val estimation = SmoothedMemoryEstimator
        .makeHistory(Seq(
          "MAP" -> 1024.megabytes))
        .estimate(TestFlowStrategyInfo.dummy)

      estimation shouldBe mapEstimate((1228, 1536))
    }

    "estimate correct numbers" in {
      val estimation = SmoothedMemoryEstimator
        .makeHistory(Seq(
          "MAP" -> 800.megabytes,
          "REDUCE" -> 800.megabytes,
          "MAP" -> 1024.megabytes,
          "REDUCE" -> 1024.megabytes,
          "MAP" -> 1300.megabytes,
          "REDUCE" -> 1300.megabytes,
          "MAP" -> 723.megabytes,
          "REDUCE" -> 723.megabytes))
        .estimate(TestFlowStrategyInfo.dummy)

      estimation shouldBe Some(MemoryEstimate(Some((1228, 1536)), Some((1228, 1536))))
    }

    "estimate less than max cap" in {
      val conf = TestFlowStrategyInfo.dummy.step.getConfig
      val estimation = SmoothedMemoryEstimator
        .makeHistory(Seq(
          "MAP" -> (MemoryEstimatorConfig.getMaxContainerMemory(conf).megabyte + 1.gigabyte)))
        .estimate(TestFlowStrategyInfo.dummy)

      val expectedEstimation = (
        (MemoryEstimatorConfig.getMaxContainerMemory(conf) / MemoryEstimatorConfig.getXmxScaleFactor(conf)).toLong,
        MemoryEstimatorConfig.getMaxContainerMemory(conf))

      estimation shouldBe mapEstimate(expectedEstimation)
    }

    "estimate not less than min cap" in {
      val conf = TestFlowStrategyInfo.dummy.step.getConfig
      val estimation = SmoothedMemoryEstimator
        .makeHistory(Seq(
          "MAP" -> (MemoryEstimatorConfig.getMinContainerMemory(conf).megabyte - 500.megabyte)))
        .estimate(TestFlowStrategyInfo.dummy)

      val expectedEstimation = (
        (MemoryEstimatorConfig.getMinContainerMemory(conf) / MemoryEstimatorConfig.getXmxScaleFactor(conf)).toLong,
        MemoryEstimatorConfig.getMinContainerMemory(conf))

      estimation shouldBe mapEstimate(expectedEstimation)
    }
  }
}

object EmptyHistoryService extends HistoryService {
  override def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    Success(Seq.empty)
}

class DummyHistoryService(val history: Seq[(String, Long)]) extends HistoryService {
  override def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] = {
    Success(history.map {
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
}

class SmoothedMemoryEstimator(override val historyService: HistoryService) extends SmoothedHistoryMemoryEstimator

object SmoothedMemoryEstimator {
  def empty: SmoothedMemoryEstimator = new SmoothedMemoryEstimator(EmptyHistoryService)

  def makeHistory(history: Seq[(String, Long)]): SmoothedMemoryEstimator =
    new SmoothedMemoryEstimator(new DummyHistoryService(history))
}

object TestFlowStrategyInfo {
  def dummy: FlowStrategyInfo = {
    val mockedConf = spy(new JobConf())
    val mockedStep = mock(classOf[FlowStep[JobConf]])
    val mockedInfo = mock(classOf[FlowStrategyInfo])

    when(mockedConf.get(anyString())).thenReturn(null)
    when(mockedStep.getConfig).thenReturn(mockedConf)
    when(mockedInfo.step).thenReturn(mockedStep)

    mockedInfo
  }
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

  def mapEstimate(value: (Long, Long)): Some[MemoryEstimate] =
    Some(MemoryEstimate(mapMemoryInMB = Some(value), None))

  def reduceEstimate(value: (Long, Long)): Some[MemoryEstimate] =
    Some(MemoryEstimate(None, reduceMemoryInMB = Some(value)))
}