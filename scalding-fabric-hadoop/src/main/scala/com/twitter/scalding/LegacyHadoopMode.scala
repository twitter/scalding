package com.twitter.scalding

import java.util

import cascading.flow._
import cascading.flow.hadoop.{ HadoopFlowConnector, HadoopFlowProcess }
import cascading.tuple.Tuple
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf

import scala.collection.mutable.Buffer

class LegacyHadoopExecutionMode(override val mode: Mode,
  @transient override val jobConf: Configuration)
  extends HadoopExecutionModeBase[JobConf] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = new HadoopFlowConnector(rawConf)
  protected def defaultConfiguration: JobConf = new JobConf(true) // initialize the default config

  override protected def newFlowProcess(conf: JobConf): HadoopFlowProcess = new HadoopFlowProcess(conf)

  override private[scalding] def setupCounterCreation(conf: Config): Config =
    conf + (CounterImpl.CounterImplClass -> classOf[HadoopFlowPCounterImpl].getCanonicalName)
}
private[scalding] case class HadoopFlowPCounterImpl(fp: HadoopFlowProcess, statKey: StatKey) extends CounterImpl {
  def this(fp: FlowProcess[_], statKey: StatKey) { // this alternate ctor is the one that will actually be used at runtime
    this(CounterImpl.upcast[HadoopFlowProcess](fp), statKey)
  }

  private[this] val cntr = fp.getReporter().getCounter(statKey.group, statKey.counter)
  override def increment(amount: Long): Unit = cntr.increment(amount)
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
