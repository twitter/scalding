package com.twitter.scalding

import java.util

import cascading.flow.{ FlowConnector, FlowProcess }
import cascading.tuple.Tuple
import com.dataartisans.flink.cascading.FlinkConnector
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.Buffer

class FlinkExecutionMode(override val mode: Mode, @transient override val jobConf: Configuration) extends HadoopExecutionModeBase[Configuration] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = new FlinkConnector(rawConf)

  protected def defaultConfiguration: Configuration = new Configuration(true) // initialize the default config

  protected def newFlowProcess(conf: Configuration): FlowProcess[Configuration] = new FlinkFlowProcess(conf)

  /* FIXME: how are stats counters to be ported to Flink? Is there any need to do something different? Using the generic approach for now */

}

case class FlinkMode(strictSources: Boolean, @transient jobConf: Configuration) extends HadoopFamilyMode {
  val name = "flink"

  override val storageMode: StorageMode = new HdfsStorageMode(strictSources, jobConf)
  override val executionMode: ExecutionMode = new FlinkExecutionMode(this, jobConf)
}

case class FlinkTestMode(@transient jobConf: Configuration,
  @transient override val buffers: Source => Option[Buffer[Tuple]])
  extends HadoopFamilyTestMode {

  val strictSources = false
  val name = "flink-test"

  override val storageMode: TestStorageMode = new HdfsTestStorageMode(false, jobConf, this.getWritePathFor)
  override val executionMode: ExecutionMode = new FlinkExecutionMode(this, jobConf)
}
