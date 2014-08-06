package com.twitter.scalding.hraven.reducer_estimation

import cascading.flow.FlowStep
import com.twitter.hraven.{ Flow => HRavenFlow, JobDetails }
import com.twitter.hraven.rest.client.HRavenRestClient
import com.twitter.scalding.reducer_estimation.{RatioBasedEstimator, FlowStepHistory, HistoryService}
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import com.twitter.hraven.JobDescFactory.{ JOBTRACKER_KEY, RESOURCE_MANAGER_KEY }

object HRavenHistoryService {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * Add some helper methods to JobConf
   */
  case class RichConfig(conf: JobConf) {

    val MaxFetch = "hraven.reducer.estimator.max.flow.history"
    val MaxFetchDefault = 8

    def maxFetch: Int = conf.getInt(MaxFetch, MaxFetchDefault)

    /**
     * Try fields in order until one returns a value.
     * Logs a warning if nothing was found.
     */
    def getFirstKey(fields: String*): Option[String] =
      fields.collectFirst {
        case f if conf.get(f) != null => conf.get(f)
      } orElse {
        LOG.warn("Missing required config param: " + fields.mkString(" or "))
        None
      }

  }
  implicit def jobConfToRichConfig(conf: JobConf) = RichConfig(conf)
}

object HRavenClient {
  import HRavenHistoryService.jobConfToRichConfig

  private final val apiHostnameKey = "hraven.api.hostname"
  private final val hRavenClientConnectTimeout = 30000
  private final val hRavenReadTimeout = 30000

  def apply(conf: JobConf): Option[HRavenRestClient] =
    conf.getFirstKey(apiHostnameKey)
      .map(new HRavenRestClient(_, hRavenClientConnectTimeout, hRavenReadTimeout))
}

/**
 * Mixin for ReducerEstimators to give them the ability to query hRaven for
 * info about past runs.
 */
trait HRavenHistoryService extends HistoryService {

  // enrichments on JobConf, LOG
  import HRavenHistoryService._

  /**
   * Fetch flows until it finds one that was successful
   * (using "HdfsBytesRead > 0" as a marker for successful jobs since it seems
   *  that this is only set on completion of jobs)
   *
   * TODO: query hRaven for successful jobs (first need to add ability to filter
   *       results in hRaven REST API)
   */
  @tailrec
  private def fetchSuccessfulFlow(client: HRavenRestClient, cluster: String, user: String, batch: String, signature: String, maxFetch: Int, limit: Int = 1): Option[HRavenFlow] =
    client.fetchFlows(cluster, user, batch, signature, limit).asScala.headOption match {
      case Some(flow) if flow.getHdfsBytesRead > 0 =>
        Some(flow)
      case None if limit < maxFetch =>
        fetchSuccessfulFlow(client, cluster, user, batch, signature, maxFetch, limit * 2)
      case None =>
        LOG.warn("Unable to find a successful flow in the last " + maxFetch + " jobs.")
        None
    }

  /**
   * Fetch info from hRaven for the last time the given JobStep ran.
   * Finds the last successful complete flow and selects the corresponding
   * step from it.
   *
   * @param step  FlowStep to get info for
   * @return      Details about the previous successful run.
   */
  def fetchPastJobDetails(step: FlowStep[JobConf]): Option[JobDetails] = {
    val conf = step.getConfig
    val stepNum = step.getStepNum

    def findMatchingJobStep(pastFlow: HRavenFlow) =
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

    def lookupClusterName(client: HRavenRestClient) = {
      // regex for case matching URL to get hostname out
      val hostRegex = """(.*):\d+""".r

      // first try resource manager (for Hadoop v2), then fallback to job tracker
      conf.getFirstKey(RESOURCE_MANAGER_KEY, JOBTRACKER_KEY)
        // extract hostname from hostname:port
        .collect { case hostRegex(host) => host }
        // convert hostname -> cluster name (e.g. dw2@smf1)
        .map(client.getCluster)
    }

    for {
      // connect to hRaven REST API
      client <- HRavenClient(conf)

      // lookup cluster name used by hRaven
      cluster <- lookupClusterName(client)

      // get identifying info for this job
      user <- conf.getFirstKey("hraven.history.user.name", "user.name")
      batch <- conf.getFirstKey("batch.desc")
      signature <- conf.getFirstKey("scalding.flow.class.signature")

      // query hRaven for matching flows
      hFlow <- fetchSuccessfulFlow(client, cluster, user, batch, signature, conf.maxFetch)

      // Find the FlowStep in the hRaven flow that corresponds to the current step
      // *Note*: when hRaven says "Job" it means "FlowStep"
      job <- findMatchingJobStep(hFlow)

    } yield job
  }

  protected def mapperBytes(pastStep: JobDetails): Option[Long] = {
    val pastInputBytes = pastStep.getHdfsBytesRead
    if (pastInputBytes <= 0) {
      LOG.warn("Invalid value in JobDetails: HdfsBytesRead = " + pastInputBytes)
      None
    } else {
      Some(pastInputBytes)
    }
  }

  protected def reducerBytes(pastStep: JobDetails): Option[Long] = {
    val reducerBytes = pastStep.getReduceFileBytesRead
    if (reducerBytes <= 0) {
      LOG.warn("Invalid value in JobDetails: ReduceBytesRead = " + reducerBytes)
      None
    } else {
      Some(reducerBytes)
    }
  }

  override def fetchHistory(f: FlowStep[JobConf], max: Int): Seq[FlowStepHistory] =
    for {
      j <- fetchPastJobDetails(f).toSeq
      mapperBytes <- mapperBytes(j)
      reducerBytes <- reducerBytes(j)
    } yield FlowStepHistory(mapperBytes, reducerBytes)

}

case class HRavenRatioBasedEstimator() extends RatioBasedEstimator with HRavenHistoryService
