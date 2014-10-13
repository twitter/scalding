package com.twitter.scalding.hraven.reducer_estimation

import java.io.IOException

import cascading.flow.FlowStep
import com.twitter.hraven.{ Flow => HRavenFlow, JobDetails }
import com.twitter.hraven.rest.client.HRavenRestClient
import com.twitter.scalding.reducer_estimation.{ RatioBasedEstimator, FlowStepHistory, HistoryService }
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import com.twitter.hraven.JobDescFactory.{ JOBTRACKER_KEY, RESOURCE_MANAGER_KEY }

import scala.util.{ Failure, Success, Try }

object HRavenHistoryService {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  val RequiredJobConfigs = Seq("cascading.flow.step.num")

  case class MissingFieldsException(fields: Seq[String]) extends Exception

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
    def getFirstKey(fields: String*): Try[String] =
      fields.collectFirst {
        case f if conf.get(f) != null => Success(conf.get(f))
      }.getOrElse {
        LOG.warn("Missing required config param: " + fields.mkString(" or "))
        Failure(MissingFieldsException(fields))
      }

  }
  implicit def jobConfToRichConfig(conf: JobConf) = RichConfig(conf)
}

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
  private def fetchSuccessfulFlows(client: HRavenRestClient, cluster: String, user: String, batch: String, signature: String, max: Int, nFetch: Int): Try[Seq[HRavenFlow]] = {
    Try(client.fetchFlowsWithConfig(cluster, user, batch, signature, nFetch, RequiredJobConfigs: _*)).map { flows =>
      val successfulFlows = flows.asScala.filter(_.getHdfsBytesRead > 0).take(max)
      if (successfulFlows.isEmpty) {
        LOG.warn("Unable to find any successful flows in the last " + nFetch + " jobs.")
      }
      successfulFlows
    } recoverWith {
      case e: IOException =>
        LOG.error("Error making API request to hRaven. HRavenHistoryService will be disabled.")
        Failure(e)
    }
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
      client <- HRavenClient(conf)

      // lookup cluster name used by hRaven
      cluster <- lookupClusterName(client)

      // get identifying info for this job
      user <- conf.getFirstKey("hraven.history.user.name", "user.name")
      batch <- conf.getFirstKey("batch.desc")
      signature <- conf.getFirstKey("scalding.flow.class.signature")

      // query hRaven for matching flows
      flows <- fetchSuccessfulFlows(client, cluster, user, batch, signature, max, conf.maxFetch)

    } yield flows

    // Find the FlowStep in the hRaven flow that corresponds to the current step
    // *Note*: when hRaven says "Job" it means "FlowStep"
    flowsTry.map(flows => flows.flatMap(findMatchingJobStep))
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

  override def fetchHistory(f: FlowStep[JobConf], max: Int): Try[Seq[FlowStepHistory]] =
    fetchPastJobDetails(f, max).map { history =>
      for {
        step <- history
        mapperBytes <- mapperBytes(step)
        reducerBytes <- reducerBytes(step)
      } yield FlowStepHistory(mapperBytes, reducerBytes)
    }

}

class HRavenRatioBasedEstimator extends RatioBasedEstimator with HRavenHistoryService
