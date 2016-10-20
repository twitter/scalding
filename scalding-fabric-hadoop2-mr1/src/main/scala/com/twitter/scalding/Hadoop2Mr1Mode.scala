package com.twitter.scalding

import java.util

import cascading.flow.{ FlowConnector, FlowProcess }
import cascading.flow.hadoop.HadoopFlowProcess
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector
import cascading.tuple.Tuple
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import scala.collection.mutable.Buffer

class Hadoop2Mr1ExecutionMode(override val mode: Mode, @transient override val jobConf: Configuration) extends HadoopExecutionModeBase[JobConf] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = new Hadoop2MR1FlowConnector(rawConf)

  protected def defaultConfiguration: JobConf = new JobConf(true) // initialize the default config

  override protected def newFlowProcess(conf: JobConf): FlowProcess[JobConf] = new HadoopFlowProcess(conf)
}

case class Hadoop2Mr1Mode(strictSources: Boolean, @transient jobConf: Configuration) extends HadoopFamilyMode {
  val name = "hadoop2-mr1"

  override val storageMode: StorageMode = new HdfsStorageMode(strictSources, jobConf)
  override val executionMode: ExecutionMode = new Hadoop2Mr1ExecutionMode(this, jobConf)
}

case class Hadoop2Mr1TestMode(@transient jobConf: Configuration,
  @transient override val buffers: Source => Option[Buffer[Tuple]])
  extends HadoopFamilyTestMode {

  val strictSources = false
  val name = "hadoop2-mr1-test"

  override val storageMode: TestStorageMode = new HdfsTestStorageMode(false, jobConf, this.getWritePathFor)
  override val executionMode: ExecutionMode = new Hadoop2Mr1ExecutionMode(this, jobConf)
}
