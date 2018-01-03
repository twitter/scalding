package com.twitter.scalding.hraven.estimation

import cascading.flow.FlowStep
import com.twitter.hraven.JobDescFactory.RESOURCE_MANAGER_KEY
import com.twitter.hraven.rest.client.HRavenRestClient
import com.twitter.hraven.util.JSONUtil
import com.twitter.hraven.{ Flow, TaskDetails }
import com.twitter.scalding.estimation.FlowStrategyInfo
import com.twitter.scalding.hraven.estimation.memory.HRavenMemoryHistoryService
import com.twitter.scalding.hraven.reducer_estimation.HRavenReducerHistoryService
import java.util
import org.apache.hadoop.mapred.JobConf
import org.codehaus.jackson.`type`.TypeReference
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.JavaConverters._
import scala.util.Try

class HRavenHistoryServiceTest extends WordSpec with Matchers {
  "A HRaven history service" should {
    "work as HRaven memory history service" in {
      val tasks = List(7, 6, 6)

      val historyService = new HRavenMemoryHistoryService {
        override def hRavenClient(conf: JobConf): Try[HRavenRestClient] =
          HRavenMockedClient(super.hRavenClient(conf), detailFields, counterFields)
      }

      val history = historyService.fetchHistory(
        TestFlowStrategyInfo.dummy(),
        HRavenMockedClient.nFetch)

      if (history.isFailure) {
        history.get
      } else {
        history.foreach(_.foreach { step =>
          tasks should contain(step.tasks.size)

          step.tasks.foreach { task =>
            assert(task.details.nonEmpty)
            assert(task.counters.nonEmpty)
          }
        })
      }
    }

    "work as HRaven reducer history service" in {
      val tasks = List(7, 6, 6)

      val historyService = new HRavenReducerHistoryService {
        override def hRavenClient(conf: JobConf): Try[HRavenRestClient] =
          HRavenMockedClient(super.hRavenClient(conf), detailFields, counterFields)
      }

      val history = historyService.fetchHistory(
        TestFlowStrategyInfo.dummy(),
        HRavenMockedClient.nFetch)

      if (history.isFailure) {
        history.get
      } else {
        history.foreach(_.foreach { step =>
          tasks should contain(step.tasks.size)

          step.tasks.foreach { task =>
            assert(task.details.nonEmpty)
            assert(task.counters.isEmpty)
          }
        })
      }
    }
  }
}

object TestFlowStrategyInfo {
  def dummy(stepNum: Int = 1): FlowStrategyInfo = {
    val mockedConf = spy(new JobConf())

    HRavenMockedClient.configure(mockedConf)

    val mockedStep = mock(classOf[FlowStep[JobConf]])
    val mockedInfo = mock(classOf[FlowStrategyInfo])

    when(mockedStep.getConfig).thenReturn(mockedConf)
    when(mockedStep.getOrdinal).thenReturn(stepNum)
    when(mockedInfo.step).thenReturn(mockedStep)

    mockedInfo
  }
}

object HRavenMockedClient {
  val cluster = "test@cluster"
  val user = "testuser"
  val batch = "somegoodjob"
  val signature = "02CFBD0A94AD5E297C2E4D6665B3B6F0"
  val nFetch = 3

  val jobs = List("job_1470171371859_6609558", "job_1470171371859_6608570", "job_1470171371859_6607542")

  val RequiredJobConfigs = Seq("cascading.flow.step.num")

  def apply(
    hRaven: Try[HRavenRestClient],
    detailFields: List[String],
    counterFields: List[String]): Try[HRavenRestClient] = {
    hRaven.map { hRaven =>
      val client = spy(hRaven)

      doReturn(HRavenMockedClient.cluster)
        .when(client)
        .getCluster(anyString())

      doReturn(flowsResponse)
        .when(client)
        .fetchFlowsWithConfig(anyString(), anyString(), anyString(), anyString(), anyInt(), anyVararg())

      for (jobId <- jobs) {
        val response = jobResponse(jobId)

        doReturn(response)
          .when(client)
          .fetchTaskDetails(cluster, jobId, detailFields.asJava, counterFields.asJava)

        doReturn(response)
          .when(client)
          .fetchTaskDetails(cluster, jobId, detailFields.asJava)
      }

      client
    }
  }

  def configure(conf: JobConf): Unit = {
    conf.set(HRavenClient.apiHostnameKey, "test")
    conf.set(RESOURCE_MANAGER_KEY, "test.com:5053")
    conf.set("hraven.history.user.name", HRavenMockedClient.user)
    conf.set("batch.desc", HRavenMockedClient.batch)
    conf.set("scalding.flow.class.signature", HRavenMockedClient.signature)
    conf.set("hraven.estimator.max.flow.histor", HRavenMockedClient.nFetch.toString)
  }

  def flowsResponse: util.List[Flow] =
    JSONUtil.readJson(
      getClass.getResourceAsStream("../../../../../flowResponse.json"),
      new TypeReference[util.List[Flow]] {}).asInstanceOf[util.List[Flow]]

  def jobResponse(jobId: String): util.List[TaskDetails] =
    JSONUtil.readJson(
      getClass.getResourceAsStream(s"../../../../../jobResponse_$jobId.json"),
      new TypeReference[util.List[TaskDetails]] {}).asInstanceOf[util.List[TaskDetails]]
}
