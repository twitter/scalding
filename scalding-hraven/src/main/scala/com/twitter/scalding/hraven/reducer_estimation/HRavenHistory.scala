package com.twitter.scalding.hraven.reducer_estimation

import cascading.flow.FlowStep
import com.twitter.hraven.{ Flow => HRavenFlow, JobDetails }
import com.twitter.hraven.rest.client.HRavenRestClient
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import com.twitter.hraven.JobDescFactory.{ JOBTRACKER_KEY, RESOURCE_MANAGER_KEY }

object HRavenHistory {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   * Add some helper methods to JobConf
   */
  case class RichConfig(conf: JobConf) {
    val MaxFetch = "hraven.reducer.estimator.max.flow.history"
    val MaxFetchDefault = 8

    def maxFetch: Int = conf.getInt(MaxFetch, MaxFetchDefault)

    def getRequired(fields: String*): Option[String] =
      fields.collectFirst {
        case f if conf.get(f) != null => conf.get(f)
      } orElse {
        LOG.warn("Missing required config param: " + fields.mkString(" or "))
        None
      }

  }
  implicit def jobConfToRichConfig(conf: JobConf) = RichConfig(conf)
}

/**
 * Mixin for ReducerEstimators to give them the ability to query hRaven for
 * info about past runs.
 */
trait HRavenHistory {

  // enrichments on JobConf, LOG
  import HRavenHistory._

  private final val API_HOSTNAME = "hraven.prod.hadoop.service.local.twitter.com"
  private final val CONNECT_TIMEOUT = 30000
  private final val READ_TIMEOUT = 30000
  private val hRavenClient = new HRavenRestClient(API_HOSTNAME, CONNECT_TIMEOUT, READ_TIMEOUT)

  /**
   * Fetch flows until it finds one that was successful
   * (using "HdfsBytesRead > 0" as a marker for successful jobs since it seems
   *  that this is only set on completion of jobs)
   *
   * TODO: query hRaven for successful jobs (go/jira/CSTOOLS-520)
   */
  @tailrec
  private def fetchSuccessfulFlow(cluster: String, user: String, batch: String, signature: String, maxFetch: Int, limit: Int = 1): Option[HRavenFlow] =
    hRavenClient.fetchFlows(cluster, user, batch, signature, limit).asScala.headOption match {
      case Some(flow) if flow.getHdfsBytesRead > 0 =>
        Some(flow)
      case None if limit < maxFetch =>
        fetchSuccessfulFlow(cluster, user, batch, signature, limit * 2)
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

    // find cluster name
    val hostRegex = """(.*):\d+""".r
    // first try resource manager (for Hadoop v2), then fallback to job tracker
    val clusterOption = conf.getRequired(RESOURCE_MANAGER_KEY, JOBTRACKER_KEY)
      // extract hostname from hostname:port
      .collect { case hostRegex(host) => host }
      // convert hostname -> cluster name (e.g. dw2@smf1)
      .map(hRavenClient.getCluster)

    for {
      // get identifying info for this job
      user <- conf.getRequired("hraven.history.user.name", "user.name")
      batch <- conf.getRequired("batch.desc")
      signature <- conf.getRequired("scalding.flow.class.signature")

      // query hRaven for matching flows
      cluster <- clusterOption
      hFlow <- fetchSuccessfulFlow(cluster, user, batch, signature, conf.maxFetch)

      // Find the FlowStep in the hRaven flow that corresponds to the current step
      // *Note*: when hRaven says "Job" it means "FlowStep"
      job <- findMatchingJobStep(hFlow)
    } yield job
  }

}
