package com.twitter.scalding

import java.util

import cascading.flow.{ FlowConnector, FlowProcess }
import cascading.flow.tez.{ Hadoop2TezFlowConnector, Hadoop2TezFlowProcess }
import cascading.tuple.Tuple
import org.apache.hadoop.conf.Configuration
import org.apache.tez.dag.api.TezConfiguration

import scala.collection.mutable.Buffer

class TezExecutionMode(override val mode: Mode, @transient override val jobConf: Configuration) extends HadoopExecutionModeBase[TezConfiguration] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = new Hadoop2TezFlowConnector(rawConf)

  protected def defaultConfiguration: TezConfiguration = new TezConfiguration(true) // initialize the default config

  protected def newFlowProcess(conf: TezConfiguration): FlowProcess[TezConfiguration] = new Hadoop2TezFlowProcess(conf)
}

case class TezMode(strictSources: Boolean, @transient jobConf: Configuration) extends HadoopFamilyMode {
  val name = "hadoop2-tez"

  override val storageMode: StorageMode = new HdfsStorageMode(strictSources, jobConf)
  override val executionMode: ExecutionMode = new TezExecutionMode(this, jobConf)
}

/* TODO: TezTestMode based on HadoopTest */
case class TezTestMode(@transient jobConf: Configuration,
  @transient override val buffers: Source => Option[Buffer[Tuple]])
  extends HadoopFamilyTestMode {

  val strictSources = false
  val name = "tez-test"

  override val storageMode: TestStorageMode = new HdfsTestStorageMode(false, jobConf, this.getWritePathFor)
  override val executionMode: ExecutionMode = new TezExecutionMode(this, jobConf)
}
