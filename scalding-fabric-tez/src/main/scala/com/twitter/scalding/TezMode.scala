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

  val TezGatherPartitionNum = "cascading.flow.runtime.gather.partitions.num"

  protected def newFlowProcess(conf: TezConfiguration): FlowProcess[TezConfiguration] = {
    val ownrednum = Option(conf.get(TezGatherPartitionNum))

    val confToUse = ownrednum match {
      case Some(value) => conf // User already specified the Gather Partitions parameters in Tez terms; no override needed.
      case None => {
        val newConf = new TezConfiguration(conf)
        newConf.set(TezGatherPartitionNum, Option(conf.get(Config.HadoopNumReducers)).getOrElse("2"))
        newConf
      }
    }

    new Hadoop2TezFlowProcess(confToUse)
  }

  override private[scalding] def setupCounterCreation(conf: Config): Config =
    conf + (CounterImpl.CounterImplClass -> classOf[TezFlowPCounterImpl].getCanonicalName)
}

private[scalding] case class TezFlowPCounterImpl(fp: Hadoop2TezFlowProcess, statKey: StatKey) extends CounterImpl {
  def this(fp: FlowProcess[_], statKey: StatKey) { // this alternate ctor is the one that will actually be used at runtime
    this(CounterImpl.upcast[Hadoop2TezFlowProcess](fp), statKey)
  }

  private[this] val cntr = fp.getReporter.getCounter(statKey.group, statKey.counter)
  override def increment(amount: Long): Unit = cntr.increment(amount)
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
