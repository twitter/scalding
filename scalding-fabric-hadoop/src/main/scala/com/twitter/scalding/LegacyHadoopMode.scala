package com.twitter.scalding

import java.io.File
import java.util
import java.util.UUID

import cascading.flow._
import cascading.flow.hadoop.{ HadoopFlowConnector, HadoopFlowProcess }
import cascading.flow.planner.BaseFlowStep
import cascading.tuple.Tuple
import com.twitter.scalding.reducer_estimation.ReducerEstimatorStepStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ Set => MSet }

import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import scala.util.{ Failure, Success }

/* TODO: move me into scalding-fabric-hadoop! */
class LegacyHadoopExecutionMode(override val mode: Mode, @transient override val jobConf: Configuration) extends HadoopExecutionModeBase[JobConf] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = new HadoopFlowConnector(rawConf)
  protected def defaultConfiguration: JobConf = new JobConf(true) // initialize the default config

  override protected def newFlowProcess(conf: JobConf): FlowProcess[JobConf] = new HadoopFlowProcess(conf)
}

class LegacyHadoopModeCommon(override val strictSources: Boolean, @transient override val jobConf: Configuration) extends HadoopFamilyMode {
  val name = "hadoop"

  override val storageMode: StorageMode = new HdfsStorageMode(strictSources, jobConf)
  override val executionMode: ExecutionMode = new LegacyHadoopExecutionMode(this, jobConf)
}

case class LegacyHadoopMode(override val strictSources: Boolean, @transient override val jobConf: Configuration)
  extends LegacyHadoopModeCommon(strictSources, jobConf) {}

@deprecated("please use LegacyHadoopMode instead, or let Mode.apply decide", "0.17.0")
case class Hdfs(override val strictSources: Boolean, @transient override val jobConf: Configuration)
  extends LegacyHadoopModeCommon(strictSources, jobConf) {}

case class HadoopTest(@transient jobConf: Configuration,
  @transient override val buffers: Source => Option[Buffer[Tuple]])
  extends HadoopFamilyTestMode {

  val strictSources = false
  val name = "hadoop-test"

  override val storageMode: TestStorageMode = new HdfsTestStorageMode(false, jobConf, this.getWritePathFor)
  override val executionMode: ExecutionMode = new LegacyHadoopExecutionMode(this, jobConf)
}
