package com.twitter.scalding

import java.util

import cascading.flow.{ FlowConnector, FlowProcess, FlowRuntimeProps }
import cascading.tuple.Tuple
import com.dataartisans.flink.cascading.FlinkConnector
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess
import org.apache.flink.api.common.{ ExecutionMode => FlinkApiExecutionMode }
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.ConfigConstants
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.Buffer

class FlinkExecutionMode(override val mode: Mode, @transient override val jobConf: Configuration) extends HadoopExecutionModeBase[Configuration] {

  override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = {
    // val env = ExecutionEnvironment.createRemoteEnvironment(host, port, jarfile,   ...)
    val env = ExecutionEnvironment.createLocalEnvironment(4)
    new FlinkConnector(env, rawConf)
  }

  protected def defaultConfiguration: Configuration = new Configuration(true) // initialize the default config

  protected def newFlowProcess(conf: Configuration): FlowProcess[Configuration] = {
    val jobManHost = Option(conf.get(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY))
    val jobManPort = Option(conf.get(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY)).map(_.toInt)

    val resourceManPort = Option(conf.get(ConfigConstants.RESOURCE_MANAGER_IPC_PORT_KEY)).map(_.toInt)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setExecutionMode(FlinkApiExecutionMode.BATCH)

    new FlinkFlowProcess(conf)
  }

  override private[scalding] def setupCounterCreation(conf: Config): Config = {
    val nconf = conf - CounterImpl.CounterImplClass +
      (CounterImpl.CounterImplClass -> classOf[FlinkFlowPCounterImpl].getCanonicalName)

    nconf
  }
}

private[scalding] case class FlinkFlowPCounterImpl(fp: FlinkFlowProcess, statKey: StatKey) extends CounterImpl {
  def this(fp: FlowProcess[_], statKey: StatKey) { // this alternate ctor is the one that will actually be used at runtime
    this(CounterImpl.upcast[FlinkFlowProcess](fp), statKey)
  }

  override def increment(amount: Long): Unit = fp.increment(statKey.group, statKey.counter, amount)
  // to access the value: fp.getCounterValue(statKey.group, statKey.counter)
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
  override val executionMode: ExecutionMode = new FlinkExecutionMode(this, jobConf) {

    override protected def newFlowConnector(rawConf: util.Map[AnyRef, AnyRef]): FlowConnector = {
      // val env = ExecutionEnvironment.createRemoteEnvironment(host, port, jarfile,   ...)
      val env = ExecutionEnvironment.createLocalEnvironment(4)
      new FlinkConnector(env, rawConf)
    }

    override private[scalding] def setupCounterCreation(conf: Config): Config =
      super.setupCounterCreation(conf)
  }
}
