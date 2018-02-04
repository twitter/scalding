package com.twitter.scalding.hraven.estimation

import cascading.flow.FlowStep
import com.twitter.hraven.JobDescFactory.{ JOBTRACKER_KEY, RESOURCE_MANAGER_KEY }
import com.twitter.hraven.rest.client.HRavenRestClient
import com.twitter.hraven.{ Constants, CounterMap, Flow, HadoopVersion, JobDetails, TaskDetails }
import com.twitter.scalding.estimation.{ FlowStepHistory, FlowStepKeys, FlowStrategyInfo, HistoryService, Task }
import java.io.IOException
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

object HRavenClient {
  import HRavenHistoryService.jobConfToRichConfig

  val apiHostnameKey = "hraven.api.hostname"
  val clientConnectTimeoutKey = "hraven.client.connect.timeout"
  val clientReadTimeoutKey = "hraven.client.read.timeout"

  private final val clientConnectTimeoutDefault = 30000
  private final val clientReadTimeoutDefault = 30000

  def apply(conf: JobConf): Try[HRavenRestClient] =
    conf.getFirstKey(apiHostnameKey)
      .map(new HRavenRestClient(_,
        conf.getInt(clientConnectTimeoutKey, clientConnectTimeoutDefault),
        conf.getInt(clientReadTimeoutKey, clientReadTimeoutDefault)))
}

object HRavenHistoryService {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  case class MissingFieldsException(fields: Seq[String]) extends Exception

  /**
   * Add some helper methods to JobConf
   */
  case class RichConfig(conf: JobConf) {

    val OldMaxFetch = "hraven.reducer.estimator.max.flow.history"
    val MaxFetch = "hraven.estimator.max.flow.history"
    val MaxFetchDefault = 8

    def maxFetch: Int = {
      val max = conf.getInt(MaxFetch, -1)
      if (max == -1) {
        conf.getInt(OldMaxFetch, MaxFetchDefault)
      } else {
        max
      }
    }

    /**
     * Try fields in order until one returns a value.
     * Logs a warning if nothing was found.
     */
    def getFirstKey(fields: String*): Try[String] =
      fields.collectFirst {
        case f if conf.get(f) != null => Success(conf.get(f))
      }.getOrElse {
        LOG.warn("Missing required config param: " + fields.mkString(" or "))
        Failure(MissingFieldsException(fields))
      }
  }

  implicit def jobConfToRichConfig(conf: JobConf): RichConfig = RichConfig(conf)
}

/**
 * History Service that gives ability to query hRaven for info about past runs.
 */
trait HRavenHistoryService extends HistoryService {
  import HRavenHistoryService.jobConfToRichConfig

  private val LOG = LoggerFactory.getLogger(this.getClass)
  private val RequiredJobConfigs = Seq("cascading.flow.step.num")
  private val MapOutputBytesKey = "MAP_OUTPUT_BYTES"

  protected val detailFields: List[String]
  protected val counterFields: List[String]

  protected def details(taskDetails: TaskDetails): Option[Map[String, Any]]
  protected def counters(taskCounters: CounterMap): Option[Map[String, Long]]

  def hRavenClient(conf: JobConf): Try[HRavenRestClient] = HRavenClient(conf)

  /**
   * Fetch flows until it finds one that was successful
   * (using "HdfsBytesRead > 0" as a marker for successful jobs since it seems
   *  that this is only set on completion of jobs)
   *
   * TODO: query hRaven for successful jobs (first need to add ability to filter
   *       results in hRaven REST API)
   */
  private def fetchSuccessfulFlows(
    client: HRavenRestClient,
    cluster: String,
    user: String,
    batch: String,
    signature: String,
    stepNum: Int,
    max: Int,
    nFetch: Int): Try[Seq[Flow]] =
    Try(client
      .fetchFlowsWithConfig(cluster, user, batch, signature, nFetch, RequiredJobConfigs: _*))
      .flatMap { flows =>
        Try {
          // Ugly mutable code to add task info to flows
          flows.asScala.foreach { flow =>
            flow.getJobs.asScala
              .filter { step =>
                step.getConfiguration.get("cascading.flow.step.num").toInt == stepNum
              }
              .foreach { job =>
                // client.fetchTaskDetails might throw IOException
                val tasks = if (counterFields.isEmpty) {
                  client.fetchTaskDetails(flow.getCluster, job.getJobId, detailFields.asJava)
                } else {
                  client.fetchTaskDetails(flow.getCluster, job.getJobId, detailFields.asJava, counterFields.asJava)
                }
                job.addTasks(tasks)
              }
          }

          val successfulFlows = flows.asScala.filter(_.getHdfsBytesRead > 0).take(max)
          if (successfulFlows.isEmpty) {
            LOG.warn("Unable to find any successful flows in the last " + nFetch + " jobs.")
          }
          successfulFlows
        }
      }.recoverWith {
        case e: IOException =>
          LOG.error("Error making API request to hRaven. HRavenHistoryService will be disabled.")
          Failure(e)
      }

  /**
   * Fetch info from hRaven for the last time the given JobStep ran.
   * Finds the last successful complete flow and selects the corresponding
   * step from it.
   *
   * @param step  FlowStep to get info for
   * @return      Details about the previous successful run.
   */
  def fetchPastJobDetails(step: FlowStep[JobConf], max: Int): Try[Seq[JobDetails]] = {
    val conf = step.getConfig
    val stepNum = step.getOrdinal

    def findMatchingJobStep(pastFlow: Flow) =
      pastFlow.getJobs.asScala.find { step =>
        try {
          step.getConfiguration.get("cascading.flow.step.num").toInt == stepNum
        } catch {
          case _: NumberFormatException => false
        }
      } orElse {
        LOG.warn("No matching job step in the retrieved hRaven flow.")
        None
      }

    def lookupClusterName(client: HRavenRestClient): Try[String] = {
      // regex for case matching URL to get hostname out
      val hostRegex = """(.*):\d+""".r

      // first try resource manager (for Hadoop v2), then fallback to job tracker
      conf.getFirstKey(RESOURCE_MANAGER_KEY, JOBTRACKER_KEY).flatMap {
        // extract hostname from hostname:port
        case hostRegex(host) =>
          // convert hostname -> cluster name (e.g. dw2@smf1)
          Try(client.getCluster(host))
      }
    }

    val flowsTry = for {
      // connect to hRaven REST API
      client <- hRavenClient(conf)

      // lookup cluster name used by hRaven
      cluster <- lookupClusterName(client)

      // get identifying info for this job
      user <- conf.getFirstKey("hraven.history.user.name", "user.name")
      batch <- conf.getFirstKey("batch.desc")
      signature <- conf.getFirstKey("scalding.flow.class.signature")

      // query hRaven for matching flows
      flows <- fetchSuccessfulFlows(client, cluster, user, batch, signature, stepNum, max, conf.maxFetch)

    } yield flows

    // Find the FlowStep in the hRaven flow that corresponds to the current step
    // *Note*: when hRaven says "Job" it means "FlowStep"
    flowsTry.map(flows => flows.flatMap(findMatchingJobStep))
  }

  override def fetchHistory(info: FlowStrategyInfo, maxHistory: Int): Try[Seq[FlowStepHistory]] =
    fetchPastJobDetails(info.step, maxHistory).map { history =>
      for {
        step <- history // linter:disable:MergeMaps
        keys = FlowStepKeys(step.getJobName, step.getUser, step.getPriority, step.getStatus, step.getVersion, "")
        // update HRavenHistoryService.TaskDetailFields when consuming additional task fields from hraven below
        tasks = step.getTasks.asScala.flatMap { taskDetails =>
          details(taskDetails).zip(counters(taskDetails.getCounters)).map {
            case (details, counters) =>
              Task(details, counters)
          }
        }
      } yield toFlowStepHistory(keys, step, tasks)
    }

  private def toFlowStepHistory(keys: FlowStepKeys, step: JobDetails, tasks: Seq[Task]) = {
    FlowStepHistory(
      keys = keys,
      submitTimeMillis = step.getSubmitTime,
      launchTimeMillis = step.getLaunchTime,
      finishTimeMillis = step.getFinishTime,
      totalMaps = step.getTotalMaps,
      totalReduces = step.getTotalReduces,
      finishedMaps = step.getFinishedMaps,
      finishedReduces = step.getFinishedReduces,
      failedMaps = step.getFailedMaps,
      failedReduces = step.getFailedReduces,
      mapFileBytesRead = step.getMapFileBytesRead,
      mapFileBytesWritten = step.getMapFileBytesWritten,
      mapOutputBytes = mapOutputBytes(step),
      reduceFileBytesRead = step.getReduceFileBytesRead,
      hdfsBytesRead = step.getHdfsBytesRead,
      hdfsBytesWritten = step.getHdfsBytesWritten,
      mapperTimeMillis = step.getMapSlotMillis,
      reducerTimeMillis = step.getReduceSlotMillis,
      reduceShuffleBytes = step.getReduceShuffleBytes,
      cost = 0,
      tasks = tasks)
  }

  private def mapOutputBytes(step: JobDetails): Long = {
    if (step.getHadoopVersion == HadoopVersion.TWO) {
      getCounterValueAsLong(step.getMapCounters, Constants.TASK_COUNTER_HADOOP2, MapOutputBytesKey)
    } else {
      getCounterValueAsLong(step.getMapCounters, Constants.TASK_COUNTER, MapOutputBytesKey)
    }
  }

  private def getCounterValueAsLong(counters: CounterMap, counterGroupName: String, counterName: String): Long = {
    val counter = counters.getCounter(counterGroupName, counterName)
    if (counter != null) counter.getValue else 0L
  }
}
